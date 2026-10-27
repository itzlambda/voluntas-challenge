//! The subscription manager (SPEC section 8): places markets on connections,
//! sends `AddMarket` before subscribing and `RemoveMarket` before
//! unsubscribing.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Context;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::core::book_connection::ConnectionCommand;
use crate::core::capture::Capture;
use crate::core::fixture::FixtureEvent;
use crate::core::routing::Workers;
use crate::model::{
    ConnectionId, MarketBooks, MarketId, MarketSources, OrderBookId, WorkerMessage,
};

/// Input from discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagerInput {
    /// Markets to subscribe; one batch (for example a completed scan) gives one
    /// subscribe message per connection.
    Add(Vec<MarketBooks>),
    Remove(Vec<MarketId>),
}

/// Starts a connection task and returns its command sender.
pub type SpawnConnection =
    Box<dyn FnMut(ConnectionId) -> mpsc::UnboundedSender<ConnectionCommand> + Send + Sync>;

struct Connection {
    commands: mpsc::UnboundedSender<ConnectionCommand>,
    markets: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Placement {
    generation: u64,
    sources: MarketSources,
    order_books: [OrderBookId; 2],
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagerTotals {
    pub markets: usize,
    pub connections: usize,
    pub added: u64,
    pub removed: u64,
}

pub struct SubscriptionManager {
    workers: Workers,
    spawn: SpawnConnection,
    capacity: usize,
    connections: Vec<Connection>,
    /// Even IDs: fill the lowest primary connection with room.
    primary_room: BTreeSet<ConnectionId>,
    /// Odd IDs: fill the least-loaded replica, breaking ties by ID.
    replica_room: BTreeSet<(usize, ConnectionId)>,
    placement: HashMap<MarketId, Placement>,
    capture: Option<Capture>,
    totals: ManagerTotals,
}

impl SubscriptionManager {
    pub fn new(
        workers: Workers,
        markets_per_connection: usize,
        spawn: SpawnConnection,
        capture: Option<Capture>,
    ) -> Self {
        Self {
            workers,
            spawn,
            capacity: markets_per_connection,
            connections: Vec::new(),
            primary_room: BTreeSet::new(),
            replica_room: BTreeSet::new(),
            placement: HashMap::new(),
            capture,
            totals: ManagerTotals::default(),
        }
    }

    pub fn markets(&self) -> usize {
        self.placement.len()
    }

    pub fn connections(&self) -> usize {
        self.connections.len()
    }

    /// Runs until cancelled or until discovery closes its sender. Dropping
    /// the manager drops its worker senders.
    pub async fn run(
        mut self,
        mut inputs: mpsc::Receiver<ManagerInput>,
        cancel: CancellationToken,
    ) -> anyhow::Result<ManagerTotals> {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                input = inputs.recv() => match input {
                    Some(ManagerInput::Add(markets)) => self.add(markets).await?,
                    Some(ManagerInput::Remove(markets)) => self.remove(markets).await?,
                    None => break,
                },
            }
        }
        self.totals.markets = self.placement.len();
        self.totals.connections = self.connections.len();
        Ok(self.totals)
    }

    pub async fn add(&mut self, mut markets: Vec<MarketBooks>) -> anyhow::Result<()> {
        anyhow::ensure!(self.capacity > 0, "markets per connection must be positive");
        let mut seen = HashSet::new();
        markets.retain(|books| {
            !self.placement.contains_key(&books.market) && seen.insert(books.market.clone())
        });
        // Plan both pools before assigning the batch. Empty replica sockets
        // then fill round-robin instead of reproducing sequential groups.
        let needed = (self.placement.len() + markets.len()).div_ceil(self.capacity);
        self.open_to(needed)?;
        let mut per_connection: BTreeMap<u16, Vec<MarketBooks>> = BTreeMap::new();
        for books in markets {
            let sources = self.place()?;
            self.attach(books, sources, &mut per_connection).await?;
        }
        self.send_all(per_connection, ConnectionCommand::Subscribe);
        Ok(())
    }

    pub async fn remove(&mut self, markets: Vec<MarketId>) -> anyhow::Result<()> {
        let mut per_connection: BTreeMap<u16, Vec<MarketBooks>> = BTreeMap::new();
        for market in markets {
            self.detach(&market, &mut per_connection).await?;
        }
        self.send_all(per_connection, ConnectionCommand::Unsubscribe);
        Ok(())
    }

    /// `AddMarket` to the market's worker, then queue the subscription.
    async fn attach(
        &mut self,
        books: MarketBooks,
        sources: MarketSources,
        per_connection: &mut BTreeMap<u16, Vec<MarketBooks>>,
    ) -> anyhow::Result<()> {
        let message = WorkerMessage::AddMarket {
            generation: books.generation,
            market: books.market.clone(),
            order_books: books.order_books.to_vec(),
            sources,
        };
        self.workers.send(message).await?;
        if let Some(capture) = &self.capture {
            capture.record(&FixtureEvent::AddMarket {
                generation: books.generation,
                market: books.market.to_string(),
                order_books: books.order_books.iter().map(ToString::to_string).collect(),
                connection: sources.connections()[0].0,
                standby: Some(sources.connections()[1].0),
            });
        }
        self.placement.insert(
            books.market.clone(),
            Placement {
                generation: books.generation,
                sources,
                order_books: books.order_books.clone(),
            },
        );
        for connection in sources.connections() {
            per_connection
                .entry(connection.0)
                .or_default()
                .push(books.clone());
        }
        self.totals.added += 1;
        Ok(())
    }

    /// `RemoveMarket` to the market's worker, then queue the unsubscribe.
    async fn detach(
        &mut self,
        market: &MarketId,
        per_connection: &mut BTreeMap<u16, Vec<MarketBooks>>,
    ) -> anyhow::Result<()> {
        let Some(placement) = self.placement.remove(market) else {
            return Ok(());
        };
        self.workers
            .send(WorkerMessage::RemoveMarket(market.clone()))
            .await?;
        if let Some(capture) = &self.capture {
            capture.record(&FixtureEvent::RemoveMarket {
                market: market.to_string(),
            });
        }
        for &connection in placement.sources.connections() {
            let slot = &mut self.connections[usize::from(connection.0)];
            if connection.0 % 2 == 1 {
                self.replica_room.remove(&(slot.markets, connection));
            }
            slot.markets -= 1;
            if connection.0 % 2 == 0 {
                self.primary_room.insert(connection);
            } else {
                self.replica_room.insert((slot.markets, connection));
            }
            per_connection
                .entry(connection.0)
                .or_default()
                .push(MarketBooks {
                    generation: placement.generation,
                    market: market.clone(),
                    order_books: placement.order_books.clone(),
                });
        }
        self.totals.removed += 1;
        Ok(())
    }

    fn send_all(
        &self,
        per_connection: BTreeMap<u16, Vec<MarketBooks>>,
        command: fn(Vec<MarketBooks>) -> ConnectionCommand,
    ) {
        for (connection, markets) in per_connection {
            // A connection task only stops at shutdown.
            let _ = self.connections[usize::from(connection)]
                .commands
                .send(command(markets));
        }
    }

    fn place(&mut self) -> anyhow::Result<MarketSources> {
        let primary = *self.primary_room.first().context("primary pool is full")?;
        let (_, replica) = self
            .replica_room
            .pop_first()
            .context("replica pool is full")?;
        let slot = &mut self.connections[usize::from(primary.0)];
        slot.markets += 1;
        if slot.markets == self.capacity {
            self.primary_room.remove(&primary);
        }
        let slot = &mut self.connections[usize::from(replica.0)];
        slot.markets += 1;
        if slot.markets < self.capacity {
            self.replica_room.insert((slot.markets, replica));
        }
        MarketSources::mirrored(primary, replica)
    }

    /// Grow both pools to the required batch capacity. Check the complete
    /// allocation before starting tasks or registering any market.
    fn open_to(&mut self, per_pool: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            per_pool <= usize::from(u16::MAX).div_ceil(2),
            "connection ID capacity exceeded"
        );
        while self.connections.len() / 2 < per_pool {
            let primary = ConnectionId(u16::try_from(self.connections.len())?);
            let replica = ConnectionId(u16::try_from(self.connections.len() + 1)?);
            for id in [primary, replica] {
                self.connections.push(Connection {
                    commands: (self.spawn)(id),
                    markets: 0,
                });
            }
            self.primary_room.insert(primary);
            self.replica_room.insert((0, replica));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::core::subscriptions::*;

    type Log = Arc<Mutex<Vec<(u16, ConnectionCommand)>>>;

    /// A manager whose connections are fake: commands are recorded.
    fn manager(capacity: usize) -> (SubscriptionManager, Vec<mpsc::Receiver<WorkerMessage>>, Log) {
        let (workers, inputs): (Vec<_>, Vec<_>) = (0..2).map(|_| mpsc::channel(1024)).unzip();
        let log: Log = Arc::default();
        let spawn_log = log.clone();
        let spawn: SpawnConnection = Box::new(move |id| {
            let (tx, mut rx) = mpsc::unbounded_channel();
            let log = spawn_log.clone();
            tokio::spawn(async move {
                while let Some(command) = rx.recv().await {
                    log.lock().unwrap().push((id.0, command));
                }
            });
            tx
        });
        (
            SubscriptionManager::new(Workers::from(workers), capacity, spawn, None),
            inputs,
            log,
        )
    }

    fn market(n: usize) -> MarketBooks {
        MarketBooks {
            generation: 0,
            market: MarketId::from(format!("0x{n:x}").as_str()),
            order_books: [
                OrderBookId::from(format!("{n}1").as_str()),
                OrderBookId::from(format!("{n}2").as_str()),
            ],
        }
    }

    fn drain(inputs: &mut [mpsc::Receiver<WorkerMessage>]) -> Vec<WorkerMessage> {
        let mut out = Vec::new();
        for input in inputs {
            while let Ok(message) = input.try_recv() {
                out.push(message);
            }
        }
        out
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn fills_connections_and_batches_subscriptions() {
        let (mut manager, mut inputs, log) = manager(2);
        manager.add((1..=5).map(market).collect()).await.unwrap();
        settle().await;
        assert_eq!(manager.connections(), 6);
        // AddMarket reached the workers before any subscribe was sent.
        let adds = drain(&mut inputs);
        assert_eq!(adds.len(), 5);
        assert!(
            adds.iter()
                .all(|m| matches!(m, WorkerMessage::AddMarket { .. }))
        );
        let log = log.lock().unwrap().clone();
        assert_eq!(log.len(), 6, "one subscribe per connection for the batch");
        let by_connection: BTreeMap<_, _> = log.into_iter().collect();
        assert_eq!(
            by_connection,
            BTreeMap::from([
                (0, ConnectionCommand::Subscribe(vec![market(1), market(2)])),
                (1, ConnectionCommand::Subscribe(vec![market(1), market(4)])),
                (2, ConnectionCommand::Subscribe(vec![market(3), market(4)])),
                (3, ConnectionCommand::Subscribe(vec![market(2), market(5)])),
                (4, ConnectionCommand::Subscribe(vec![market(5)])),
                (5, ConnectionCommand::Subscribe(vec![market(3)])),
            ])
        );
    }

    #[tokio::test]
    async fn removal_frees_a_slot_for_reuse() {
        let (mut manager, mut inputs, log) = manager(2);
        manager.add((1..=4).map(market).collect()).await.unwrap();
        let original = manager.placement.clone();
        manager
            .remove(vec![market(1).market, market(1).market])
            .await
            .unwrap();
        manager.add(vec![market(9)]).await.unwrap();
        settle().await;
        assert_eq!(manager.connections(), 4, "freed slots reused");
        let messages = drain(&mut inputs);
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, WorkerMessage::RemoveMarket(_)))
                .count(),
            1
        );
        for n in 2..=4 {
            assert_eq!(
                manager.placement[&market(n).market],
                original[&market(n).market]
            );
        }
        assert_eq!(
            manager.placement[&market(9).market].sources.connections(),
            [ConnectionId(0), ConnectionId(1)]
        );
        assert_eq!(manager.totals.removed, 1);
        let log = log.lock().unwrap();
        for id in [0, 1] {
            assert!(log.iter().any(|(connection, c)| *connection == id
                && matches!(c, ConnectionCommand::Unsubscribe(m) if m == &vec![market(1)])));
            assert!(log.iter().any(|(connection, c)| *connection == id
                && matches!(c, ConnectionCommand::Subscribe(m) if m == &vec![market(9)])));
        }
    }

    #[tokio::test]
    async fn removes_and_reuses_the_actual_cross_pool_sources() {
        let (mut manager, mut inputs, log) = manager(2);
        manager.add((1..=4).map(market).collect()).await.unwrap();
        let original = manager.placement.clone();
        assert_eq!(
            original[&market(2).market].sources.connections(),
            [ConnectionId(0), ConnectionId(3)]
        );
        settle().await;
        log.lock().unwrap().clear();
        drain(&mut inputs);
        manager
            .remove(vec![market(2).market, market(2).market])
            .await
            .unwrap();
        manager.add(vec![market(9)]).await.unwrap();
        settle().await;
        assert_eq!(manager.connections(), 4);
        assert_eq!(
            manager.placement[&market(9).market].sources,
            original[&market(2).market].sources
        );
        for n in [1, 3, 4] {
            assert_eq!(
                manager.placement[&market(n).market],
                original[&market(n).market]
            );
        }
        let commands = log.lock().unwrap().clone();
        assert_eq!(commands.len(), 4);
        for id in [0, 3] {
            assert!(commands.contains(&(id, ConnectionCommand::Unsubscribe(vec![market(2)]))));
            assert!(commands.contains(&(id, ConnectionCommand::Subscribe(vec![market(9)]))));
        }
        let messages = drain(&mut inputs);
        assert_eq!(
            messages
                .iter()
                .filter(|m| matches!(m, WorkerMessage::RemoveMarket(_)))
                .count(),
            1
        );
        assert!(
            messages
                .iter()
                .any(|m| matches!(m, WorkerMessage::AddMarket { sources, .. }
            if sources.connections() == [ConnectionId(0), ConnectionId(3)]))
        );
    }

    #[tokio::test]
    async fn later_additions_use_least_loaded_replica_without_moving_markets() {
        let (mut manager, mut inputs, _) = manager(3);
        manager.add((1..=7).map(market).collect()).await.unwrap();
        let original = manager.placement.clone();
        drain(&mut inputs);
        manager.add(vec![market(8)]).await.unwrap();
        manager.add(vec![market(9)]).await.unwrap();
        assert_eq!(manager.connections(), 6);
        assert_eq!(
            manager.placement[&market(8).market].sources.connections(),
            [ConnectionId(4), ConnectionId(3)]
        );
        assert_eq!(
            manager.placement[&market(9).market].sources.connections(),
            [ConnectionId(4), ConnectionId(5)]
        );
        for (market, placement) in original {
            assert_eq!(manager.placement[&market], placement);
        }
        assert!(manager.connections.iter().all(|c| c.markets == 3));
    }

    #[tokio::test]
    async fn bulk_placement_spreads_primary_neighbours_and_balances_capacity() {
        let (mut manager, mut inputs, _) = manager(3);
        manager
            .add((1..=10).flat_map(|n| [market(n), market(n)]).collect())
            .await
            .unwrap();
        assert_eq!(manager.markets(), 10);
        assert_eq!(
            manager.connections(),
            8,
            "duplicates do not grow either pool"
        );
        assert_eq!(drain(&mut inputs).len(), 10);
        for start in [1, 4, 7] {
            let replicas: BTreeSet<_> = (start..start + 3)
                .map(|n| {
                    let sources = manager.placement[&market(n).market].sources.connections();
                    assert_eq!(sources[0].0 % 2, 0);
                    assert_eq!(sources[1].0 % 2, 1);
                    assert_eq!(sources[0], ConnectionId(((start - 1) / 3 * 2) as u16));
                    sources[1]
                })
                .collect();
            assert_eq!(replicas.len(), 3, "neighbours use distinct replica sockets");
        }
        let loads: Vec<_> = manager
            .connections
            .iter()
            .skip(1)
            .step_by(2)
            .map(|c| c.markets)
            .collect();
        assert_eq!(loads, [3, 3, 2, 2]);
        assert!(manager.connections.iter().all(|c| c.markets <= 3));
        manager.add((1..=10).map(market).collect()).await.unwrap();
        assert_eq!(manager.connections(), 8);
        assert!(drain(&mut inputs).is_empty());
    }

    #[tokio::test]
    async fn capture_records_actual_sources_for_replay() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("capture.jsonl");
        let (capture, file) = Capture::create(&path).unwrap();
        let (mut manager, mut inputs, _) = manager(2);
        manager.capture = Some(capture);
        manager.add((1..=4).map(market).collect()).await.unwrap();
        drain(&mut inputs);
        drop(manager);
        file.finish().unwrap();
        let events =
            crate::core::fixture::parse_jsonl(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, FixtureEvent::AddMarket {
                generation: 0,
            market: id, connection: 0, standby: Some(3), ..
        } if id == &market(2).market.to_string()))
        );
    }

    #[tokio::test]
    async fn market_257_grows_both_pools() {
        let (mut manager, mut inputs, log) = manager(256);
        manager.add((1..=256).map(market).collect()).await.unwrap();
        assert_eq!(manager.connections(), 2);
        let messages = drain(&mut inputs);
        assert_eq!(messages.len(), 256);
        assert!(messages.iter().all(|message| matches!(message,
            WorkerMessage::AddMarket { sources, .. } if sources.connections() == [ConnectionId(0), ConnectionId(1)])));
        manager.add(vec![market(257), market(257)]).await.unwrap();
        assert_eq!(manager.connections(), 4);
        assert_eq!(manager.markets(), 257);
        assert_eq!(manager.totals.added, 257);
        assert!(matches!(drain(&mut inputs).as_slice(),
            [WorkerMessage::AddMarket { sources, .. }] if sources.connections() == [ConnectionId(2), ConnectionId(3)]));
        settle().await;
        let log = log.lock().unwrap();
        assert_eq!(log.len(), 4);
        for id in [0, 1] {
            assert!(log.iter().any(|(connection, command)| *connection == id
                && *command == ConnectionCommand::Subscribe((1..=256).map(market).collect())));
        }
        for id in [2, 3] {
            assert!(log.iter().any(|(connection, command)| *connection == id
                && *command == ConnectionCommand::Subscribe(vec![market(257)])));
        }
    }

    #[tokio::test]
    async fn controls_are_enqueued_before_either_subscription_command() {
        let (worker, mut input) = mpsc::channel(1);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let receivers = commands.clone();
        let spawn: SpawnConnection = Box::new(move |_| {
            let (tx, rx) = mpsc::unbounded_channel();
            receivers.lock().unwrap().push(rx);
            tx
        });
        let mut manager = SubscriptionManager::new(Workers::from(vec![worker]), 2, spawn, None);
        let add = tokio::spawn(async move {
            manager.add(vec![market(1), market(2)]).await.unwrap();
            manager
        });
        settle().await;
        assert_eq!(
            input.len(),
            1,
            "second registration waits for worker capacity"
        );
        assert_eq!(commands.lock().unwrap().len(), 2);
        assert!(
            commands
                .lock()
                .unwrap()
                .iter_mut()
                .all(|rx| rx.try_recv().is_err())
        );
        assert!(matches!(
            input.recv().await,
            Some(WorkerMessage::AddMarket { .. })
        ));
        let mut manager = add.await.unwrap();
        assert!(matches!(
            input.recv().await,
            Some(WorkerMessage::AddMarket { .. })
        ));
        for rx in commands.lock().unwrap().iter_mut() {
            assert_eq!(
                rx.try_recv().unwrap(),
                ConnectionCommand::Subscribe(vec![market(1), market(2)])
            );
        }
        let remove = tokio::spawn(async move {
            manager
                .remove(vec![market(1).market, market(2).market])
                .await
                .unwrap();
            manager
        });
        settle().await;
        assert_eq!(input.len(), 1, "second removal waits for worker capacity");
        assert!(
            commands
                .lock()
                .unwrap()
                .iter_mut()
                .all(|rx| rx.try_recv().is_err())
        );
        assert!(matches!(
            input.recv().await,
            Some(WorkerMessage::RemoveMarket(_))
        ));
        let manager = remove.await.unwrap();
        assert!(matches!(
            input.recv().await,
            Some(WorkerMessage::RemoveMarket(_))
        ));
        assert_eq!(manager.markets(), 0);
        for rx in commands.lock().unwrap().iter_mut() {
            assert_eq!(
                rx.try_recv().unwrap(),
                ConnectionCommand::Unsubscribe(vec![market(1), market(2)])
            );
        }
    }

    #[test]
    fn physical_id_exhaustion_does_not_partially_grow_pools() {
        let spawned = Arc::new(Mutex::new(Vec::new()));
        let spawn_ids = spawned.clone();
        let (tx, _rx) = mpsc::unbounded_channel();
        let spawn_tx = tx.clone();
        let spawn: SpawnConnection = Box::new(move |id| {
            spawn_ids.lock().unwrap().push(id);
            spawn_tx.clone()
        });
        let mut manager = SubscriptionManager::new(Workers::from(Vec::new()), 1, spawn, None);
        manager.connections.resize_with(65_534, || Connection {
            commands: tx.clone(),
            markets: 1,
        });
        manager.open_to(32_768).unwrap();
        assert_eq!(manager.connections(), 65_536);
        assert!(manager.open_to(32_769).is_err());
        assert_eq!(manager.connections(), 65_536);
        assert_eq!(
            *spawned.lock().unwrap(),
            vec![ConnectionId(65_534), ConnectionId(65_535)]
        );
    }

    #[tokio::test]
    async fn totals_count_physical_connections() {
        let (manager, _inputs, _log) = manager(2);
        let (tx, rx) = mpsc::channel(1);
        tx.send(ManagerInput::Add(vec![market(1)])).await.unwrap();
        drop(tx);
        let totals = manager.run(rx, CancellationToken::new()).await.unwrap();
        assert_eq!(totals.connections, 2);
        assert_eq!(totals.markets, 1);
        assert_eq!(totals.added, 1);
    }
}
