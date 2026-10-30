//! The subscription manager (SPEC section 8): places markets on connections,
//! sends `AddMarket` before subscribing and `RemoveMarket` before
//! unsubscribing.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Context;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::core::book_connection::ConnectionCommand;
use crate::core::capture::Capture;
use crate::core::fixture::FixtureEvent;
use crate::core::routing::Workers;
use crate::core::telemetry;
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

/// Primary sockets have even IDs, replicas odd.
const PRIMARY: usize = 0;
const REPLICA: usize = 1;
/// Mixed into each pool's placement key so the two pools see unrelated
/// market orders.
const POOL_SALT: [u64; 2] = [0x9e37_79b9_7f4a_7c15, 0xc2b2_ae3d_27d4_eb4f];
/// A batch larger than the pools (the first scan) sizes them with 20% spare
/// sockets, so later markets spread across the fleet.
const SPARE_NUMERATOR: usize = 6;
const SPARE_DENOMINATOR: usize = 5;
/// When a later batch does not fit, each pool grows by this fraction of its
/// sockets, at least one.
const GROWTH_DIVISOR: usize = 10;
const MAX_PER_POOL: usize = (u16::MAX as usize).div_ceil(2);

/// Placement order of a market within one pool: a fixed hash of the
/// condition ID (FNV-1a, then the splitmix64 finalizer). The standard
/// library hasher is not used because its output may change between Rust
/// releases, and the same market set must give the same layout.
fn placement_key(market: &MarketId, pool: usize) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in market.0.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= POOL_SALT[pool];
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

pub struct SubscriptionManager {
    workers: Workers,
    spawn: SpawnConnection,
    capacity: usize,
    connections: Vec<Connection>,
    /// Per pool: sockets with room, least-loaded first, ties by lowest ID.
    room: [BTreeSet<(usize, ConnectionId)>; 2],
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
            room: [BTreeSet::new(), BTreeSet::new()],
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
        if markets.is_empty() {
            return Ok(());
        }
        // Size both pools for the whole batch before registering any market.
        self.grow_for(markets.len())?;
        // Each pool deals the batch in its own hash order to its least-loaded
        // sockets, so listing-order neighbours land apart and a primary's
        // markets map to many replicas.
        let replicas = self.assign(&markets, REPLICA)?;
        let primaries = self.assign(&markets, PRIMARY)?;
        let mut per_connection: BTreeMap<u16, Vec<MarketBooks>> = BTreeMap::new();
        for (index, books) in markets.into_iter().enumerate() {
            let sources = MarketSources::mirrored(primaries[index], replicas[index])?;
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
            let room = &mut self.room[usize::from(connection.0 % 2)];
            let slot = &mut self.connections[usize::from(connection.0)];
            room.remove(&(slot.markets, connection));
            slot.markets -= 1;
            room.insert((slot.markets, connection));
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

    /// One socket per market in `pool`, returned in batch order. Markets are
    /// dealt in the pool's hash order, each to the least-loaded socket with
    /// room, ties broken by lowest ID.
    fn assign(
        &mut self,
        markets: &[MarketBooks],
        pool: usize,
    ) -> anyhow::Result<Vec<ConnectionId>> {
        let mut order: Vec<(u64, &MarketId, usize)> = markets
            .iter()
            .enumerate()
            .map(|(index, books)| (placement_key(&books.market, pool), &books.market, index))
            .collect();
        order.sort_unstable();
        let mut sockets = vec![ConnectionId(0); markets.len()];
        for (_, _, index) in order {
            let (load, id) = self.room[pool]
                .pop_first()
                .context("connection pool is full")?;
            let slot = &mut self.connections[usize::from(id.0)];
            debug_assert_eq!(slot.markets, load);
            slot.markets += 1;
            if slot.markets < self.capacity {
                self.room[pool].insert((slot.markets, id));
            }
            sockets[index] = id;
        }
        Ok(sockets)
    }

    /// Opens sockets only when `batch` markets do not fit in the free slots.
    /// Both pools always hold the same number of markets and sockets.
    fn grow_for(&mut self, batch: usize) -> anyhow::Result<()> {
        let per_pool = self.connections.len() / 2;
        let total = self.placement.len() + batch;
        let fits = |sockets: usize| sockets * self.capacity >= total;
        if fits(per_pool) {
            return Ok(());
        }
        let target = if batch > per_pool * self.capacity {
            (total * SPARE_NUMERATOR).div_ceil(self.capacity * SPARE_DENOMINATOR)
        } else {
            let mut sockets = per_pool;
            while !fits(sockets) {
                sockets += (sockets / GROWTH_DIVISOR).max(1);
            }
            sockets
        };
        // Spare capacity is a target; only the batch itself must fit.
        let target = if target > MAX_PER_POOL && fits(MAX_PER_POOL) {
            MAX_PER_POOL
        } else {
            target
        };
        self.open_to(target)?;
        if per_pool > 0 {
            telemetry::emit(
                "connection_pools_grown",
                json!({ "from_per_pool": per_pool, "to_per_pool": target, "markets": total }),
            );
        }
        Ok(())
    }

    /// Grow both pools to `per_pool` sockets each. Check the complete
    /// allocation before starting tasks or registering any market.
    fn open_to(&mut self, per_pool: usize) -> anyhow::Result<()> {
        anyhow::ensure!(per_pool <= MAX_PER_POOL, "connection ID capacity exceeded");
        while self.connections.len() / 2 < per_pool {
            let primary = ConnectionId(u16::try_from(self.connections.len())?);
            let replica = ConnectionId(u16::try_from(self.connections.len() + 1)?);
            for id in [primary, replica] {
                self.connections.push(Connection {
                    commands: (self.spawn)(id),
                    markets: 0,
                });
            }
            self.room[PRIMARY].insert((0, primary));
            self.room[REPLICA].insert((0, replica));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use crate::core::subscriptions::*;

    type Log = Arc<Mutex<Vec<(u16, ConnectionCommand)>>>;

    /// A manager whose connections are fake: commands are recorded.
    fn manager(capacity: usize) -> (SubscriptionManager, Vec<mpsc::Receiver<WorkerMessage>>, Log) {
        let (workers, inputs): (Vec<_>, Vec<_>) = (0..2).map(|_| mpsc::channel(32_768)).unzip();
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

    const PINNED_PRIMARY: u64 = 2_018_097_179_610_769_946;
    const PINNED_REPLICA: u64 = 17_275_474_291_222_178_132;

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

    fn sources(manager: &SubscriptionManager, n: usize) -> [ConnectionId; 2] {
        let connections = manager.placement[&market(n).market].sources.connections();
        [connections[0], connections[1]]
    }

    /// Market counts per socket in one pool, by socket ID.
    fn loads(manager: &SubscriptionManager, pool: usize) -> Vec<usize> {
        manager
            .connections
            .iter()
            .skip(pool)
            .step_by(2)
            .map(|c| c.markets)
            .collect()
    }

    fn spread(loads: &[usize]) -> usize {
        loads.iter().max().unwrap() - loads.iter().min().unwrap()
    }

    #[test]
    fn placement_keys_are_fixed_and_differ_by_pool() {
        let id = MarketId::from("0xabc");
        // Pinned so a hasher change cannot silently reshuffle layouts.
        assert_eq!(placement_key(&id, PRIMARY), placement_key(&id, PRIMARY));
        assert_ne!(placement_key(&id, PRIMARY), placement_key(&id, REPLICA));
        assert_eq!(
            [placement_key(&id, PRIMARY), placement_key(&id, REPLICA)],
            [PINNED_PRIMARY, PINNED_REPLICA]
        );
    }

    #[tokio::test]
    async fn startup_batch_sizes_pools_with_spare_and_batches_subscriptions() {
        let (mut manager, mut inputs, log) = manager(2);
        manager.add((1..=5).map(market).collect()).await.unwrap();
        settle().await;
        // ceil(5 × 1.2 / 2) = 3 sockets per pool.
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
        for (id, command) in log {
            let ConnectionCommand::Subscribe(markets) = command else {
                panic!("unexpected command");
            };
            for books in markets {
                assert!(
                    manager.placement[&books.market]
                        .sources
                        .connections()
                        .contains(&ConnectionId(id))
                );
            }
        }
        for pool in [PRIMARY, REPLICA] {
            assert_eq!(loads(&manager, pool).iter().sum::<usize>(), 5);
            assert!(spread(&loads(&manager, pool)) <= 1);
        }
    }

    #[tokio::test]
    async fn removal_frees_slots_that_the_next_addition_reuses() {
        let (mut manager, mut inputs, log) = manager(2);
        manager.add((1..=5).map(market).collect()).await.unwrap();
        manager.add(vec![market(6)]).await.unwrap();
        assert_eq!(manager.connections(), 6);
        assert!(manager.connections.iter().all(|c| c.markets == 2));
        let original = manager.placement.clone();
        let freed = sources(&manager, 2);
        settle().await;
        log.lock().unwrap().clear();
        drain(&mut inputs);
        manager
            .remove(vec![market(2).market, market(2).market])
            .await
            .unwrap();
        manager.add(vec![market(9)]).await.unwrap();
        settle().await;
        assert_eq!(manager.connections(), 6, "freed slots reused");
        assert_eq!(sources(&manager, 9), freed);
        for n in [1, 3, 4, 5, 6] {
            assert_eq!(
                manager.placement[&market(n).market],
                original[&market(n).market]
            );
        }
        assert_eq!(manager.totals.removed, 1);
        let commands = log.lock().unwrap().clone();
        assert_eq!(commands.len(), 4);
        for id in freed {
            assert!(commands.contains(&(id.0, ConnectionCommand::Unsubscribe(vec![market(2)]))));
            assert!(commands.contains(&(id.0, ConnectionCommand::Subscribe(vec![market(9)]))));
        }
        let messages = drain(&mut inputs);
        assert_eq!(
            messages
                .iter()
                .filter(|m| matches!(m, WorkerMessage::RemoveMarket(_)))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn later_additions_go_to_the_least_loaded_socket_without_moving_markets() {
        let (mut manager, mut inputs, _) = manager(4);
        manager.add((1..=10).map(market).collect()).await.unwrap();
        // ceil(10 × 1.2 / 4) = 3 sockets per pool, holding 4, 3 and 3.
        assert_eq!(manager.connections(), 6);
        let original = manager.placement.clone();
        drain(&mut inputs);
        manager.add(vec![market(11)]).await.unwrap();
        manager.add(vec![market(12)]).await.unwrap();
        assert_eq!(manager.connections(), 6);
        for pool in [PRIMARY, REPLICA] {
            assert_eq!(loads(&manager, pool), [4, 4, 4]);
        }
        assert_ne!(sources(&manager, 11)[0], sources(&manager, 12)[0]);
        assert_ne!(sources(&manager, 11)[1], sources(&manager, 12)[1]);
        for (market, placement) in original {
            assert_eq!(manager.placement[&market], placement);
        }
    }

    #[tokio::test]
    async fn bulk_placement_spreads_markets_within_and_across_pools() {
        let count = 20_000;
        let (mut manager, mut inputs, _) = manager(256);
        manager
            .add((1..=count).flat_map(|n| [market(n), market(n)]).collect())
            .await
            .unwrap();
        assert_eq!(manager.markets(), count);
        // ceil(20,000 × 1.2 / 256) = 94 sockets per pool.
        assert_eq!(manager.connections(), 188, "duplicates do not grow pools");
        assert_eq!(drain(&mut inputs).len(), count);
        for pool in [PRIMARY, REPLICA] {
            assert!(spread(&loads(&manager, pool)) <= 1);
            assert!(loads(&manager, pool).iter().all(|&load| load <= 256));
        }
        // Listing-order neighbours land on different primaries.
        let together = (1..count)
            .filter(|&n| sources(&manager, n)[0] == sources(&manager, n + 1)[0])
            .count();
        assert!(
            together < count / 50,
            "{together} neighbour pairs share a primary"
        );
        // A primary's markets map to many different replicas.
        let mut replicas: HashMap<ConnectionId, (usize, HashSet<ConnectionId>)> = HashMap::new();
        for n in 1..=count {
            let [primary, replica] = sources(&manager, n);
            assert_eq!(primary.0 % 2, 0);
            assert_eq!(replica.0 % 2, 1);
            let entry = replicas.entry(primary).or_default();
            entry.0 += 1;
            entry.1.insert(replica);
        }
        let pool = manager.connections() / 2;
        for (markets, distinct) in replicas.values() {
            assert!(distinct.len() * 4 >= (*markets).min(pool) * 3);
        }
        manager
            .add((1..=count).map(market).collect())
            .await
            .unwrap();
        assert_eq!(manager.connections(), 188);
        assert!(drain(&mut inputs).is_empty());
    }

    #[tokio::test]
    async fn the_same_market_set_gives_the_same_layout() {
        let (mut forward, _forward_inputs, _) = manager(8);
        let (mut reversed, _reversed_inputs, _) = manager(8);
        forward.add((1..=100).map(market).collect()).await.unwrap();
        reversed
            .add((1..=100).rev().map(market).collect())
            .await
            .unwrap();
        assert_eq!(forward.placement, reversed.placement);
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
        let [primary, replica] = sources(&manager, 2);
        drop(manager);
        file.finish().unwrap();
        let events =
            crate::core::fixture::parse_jsonl(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, FixtureEvent::AddMarket {
                generation: 0, market: id, connection, standby: Some(standby), ..
            } if id == &market(2).market.to_string()
                && *connection == primary.0 && *standby == replica.0))
        );
    }

    #[tokio::test]
    async fn a_batch_that_does_not_fit_grows_each_pool_by_a_tenth() {
        let (mut manager, mut inputs, log) = manager(1);
        manager.add((1..=17).map(market).collect()).await.unwrap();
        // ceil(17 × 1.2) = 21 sockets per pool.
        assert_eq!(manager.connections(), 42);
        manager.add((18..=21).map(market).collect()).await.unwrap();
        assert_eq!(manager.connections(), 42, "free slots used before growing");
        let original = manager.placement.clone();
        drain(&mut inputs);
        settle().await;
        log.lock().unwrap().clear();
        manager.add(vec![market(22), market(22)]).await.unwrap();
        assert_eq!(manager.connections(), 46, "21 + 2 sockets per pool");
        assert_eq!(manager.markets(), 22);
        assert_eq!(manager.totals.added, 22);
        let [primary, replica] = sources(&manager, 22);
        assert!(primary.0 >= 42 && replica.0 >= 42);
        for (market, placement) in original {
            assert_eq!(manager.placement[&market], placement);
        }
        settle().await;
        let log = log.lock().unwrap();
        assert_eq!(
            *log,
            [
                (primary.0, ConnectionCommand::Subscribe(vec![market(22)])),
                (replica.0, ConnectionCommand::Subscribe(vec![market(22)])),
            ]
        );
    }

    #[tokio::test]
    async fn a_batch_larger_than_the_pools_resizes_them_with_spare() {
        let (mut manager, _inputs, _) = manager(2);
        manager.add((1..=5).map(market).collect()).await.unwrap();
        assert_eq!(manager.connections(), 6);
        manager.add((6..=15).map(market).collect()).await.unwrap();
        // ceil(15 × 1.2 / 2) = 9 sockets per pool.
        assert_eq!(manager.connections(), 18);
        assert!(manager.connections.iter().all(|c| c.markets <= 2));
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
        let mut manager = SubscriptionManager::new(Workers::from(vec![worker]), 4, spawn, None);
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
