//! The discovery task (SPEC section 7): owns the registry and feeds it
//! lifecycle events and complete reconciliation scans. Its actions go to
//! the subscription manager.

use std::collections::HashSet;
use std::future::{Future, pending};
use std::time::Duration;

use futures_util::future::BoxFuture;

use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use crate::core::metrics::{PUBLICATION_INTERVAL, Publisher, timestamp};
use crate::core::registry::{Action, AddedBy, PendingBatch, Registry};
use crate::core::subscriptions::ManagerInput;
use crate::core::telemetry;
use crate::model::{Listing, ListingId, MarketId, NewMarket};

const LOOKUP_BATCH: usize = 100;
const PENDING_POLL_INTERVAL: Duration = Duration::from_secs(1);
type PendingLookup = BoxFuture<'static, (PendingBatch, anyhow::Result<Vec<Listing>>)>;

/// One page of a reconciliation scan.
#[derive(Debug, Default)]
pub struct ListingPage {
    pub rows: Vec<Listing>,
    pub next_cursor: Option<String>,
}

/// An exchange's market listing: the scan and the ID lookup.
pub trait Listings: Clone + Send + Sync + 'static {
    /// One page of open markets. `nonce` is one cache-busting value per pass.
    fn scan_page(
        &self,
        cursor: Option<String>,
        nonce: String,
    ) -> impl Future<Output = anyhow::Result<ListingPage>> + Send;

    /// Up to 100 markets by listing ID; unknown IDs are absent.
    fn lookup(
        &self,
        ids: Vec<ListingId>,
    ) -> impl Future<Output = anyhow::Result<Vec<Listing>>> + Send;
}

/// From lifecycle connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    Connected(usize),
    Disconnected(usize),
    NewMarket(NewMarket),
    Resolved(MarketId),
}

struct ScanResult {
    /// Failed scans carry no candidate set.
    rows: Option<Vec<Listing>>,
    pages: u64,
}

struct RunningScan {
    started: Instant,
    queued: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiscoveryTotals {
    pub registered: usize,
    pub hints: u64,
    pub scans_completed: u64,
    pub scans_failed: u64,
    pub resolutions: u64,
}

pub struct Discovery<L: Listings> {
    listings: L,
    recovery: mpsc::Receiver<crate::core::recovery::RecoveryState>,
    manager: mpsc::Sender<ManagerInput>,
    registry: Registry,
    scan_interval: Duration,
    totals: DiscoveryTotals,
    metrics: Publisher,
}

impl<L: Listings> Discovery<L> {
    pub fn new(
        listings: L,
        manager: mpsc::Sender<ManagerInput>,
        scan_interval: Duration,
        mut metrics: Publisher,
    ) -> Self {
        metrics.state.initialized = false;
        metrics.publish();
        Self {
            listings,
            recovery: mpsc::channel(1).1,
            manager,
            registry: Registry::default(),
            scan_interval,
            totals: DiscoveryTotals::default(),
            metrics,
        }
    }

    pub fn with_recovery(
        mut self,
        recovery: mpsc::Receiver<crate::core::recovery::RecoveryState>,
    ) -> Self {
        self.recovery = recovery;
        self
    }

    /// Collects configured markets and confirms unavailable or removed IDs periodically.
    pub async fn run_fixed(
        mut self,
        ids: Vec<ListingId>,
        cancel: CancellationToken,
    ) -> anyhow::Result<DiscoveryTotals> {
        for batch in ids.chunks(LOOKUP_BATCH) {
            let rows = tokio::select! {
                _ = cancel.cancelled() => return Ok(self.totals()),
                rows = self.listings.lookup(batch.to_vec()) => rows?,
            };
            for id in batch {
                match rows.iter().find(|row| &row.listing_id == id) {
                    Some(row) => match self.registry.add_configured(row) {
                        Some(action) => self.apply(vec![action], true, &cancel).await?,
                        None => telemetry::emit(
                            "configured_market_skipped",
                            json!({"listing_id": id, "status": row.status}),
                        ),
                    },
                    None => {
                        telemetry::emit("configured_market_not_found", json!({"listing_id": id}))
                    }
                }
            }
        }
        self.metrics.state.initialized = true;
        let mut publication = interval(PUBLICATION_INTERVAL);
        let mut pending_timer = interval(PENDING_POLL_INTERVAL);
        pending_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut lookup: Option<PendingLookup> = None;
        let mut fixed_due = Instant::now() + Duration::from_secs(60);
        let mut fixed_offset = 0;
        let mut prefer_fixed = false;
        loop {
            self.publish_metrics();
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = publication.tick() => {},
                Some(state) = self.recovery.recv() => self.registry.recovery_state(state, Instant::now()),
                _ = pending_timer.tick(), if lookup.is_none() => {
                    let now = Instant::now();
                    prefer_fixed = !prefer_fixed;
                    let batch = if now >= fixed_due && prefer_fixed {
                        let candidates: Vec<_> = ids.iter().skip(fixed_offset).take(LOOKUP_BATCH).filter(|id| !self.registry.contains_listing(id)).cloned().collect();
                        fixed_offset += LOOKUP_BATCH;
                        if fixed_offset >= ids.len() { fixed_offset = 0; fixed_due = now + Duration::from_secs(60); }
                        if candidates.is_empty() { self.registry.poll_due(now, LOOKUP_BATCH) } else { Some(PendingBatch::configured(candidates)) }
                    } else { self.registry.poll_due(now, LOOKUP_BATCH) };
                    if let Some(batch) = batch {
                        let listings = self.listings.clone();
                        lookup = Some(Box::pin(async move { let rows = listings.lookup(batch.ids()).await; (batch, rows) }));
                    }
                }
                (batch, result) = async { match lookup.as_mut() { Some(request) => request.await, None => pending().await } } => {
                    lookup = None;
                    let actions = self.registry.on_lookup(batch, result.ok(), Instant::now());
                    self.apply(actions, true, &cancel).await?;
                }
            }
        }
        self.metrics.publish();
        Ok(self.totals())
    }

    /// Runs lifecycle events and full-scan reconciliation until cancelled.
    pub async fn run(
        mut self,
        mut lifecycle: mpsc::UnboundedReceiver<LifecycleEvent>,
        cancel: CancellationToken,
    ) -> anyhow::Result<DiscoveryTotals> {
        let (scan_tx, mut scan_rx) = mpsc::channel::<ScanResult>(1);
        let mut scan_timer = interval(self.scan_interval);
        scan_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut publication = interval(PUBLICATION_INTERVAL);
        publication.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut pending_timer = interval(PENDING_POLL_INTERVAL);
        pending_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut lookup: Option<PendingLookup> = None;
        let mut scan: Option<RunningScan> = None;
        let mut connected: HashSet<usize> = HashSet::new();
        let mut outage_since: Option<Instant> = None;

        loop {
            if cancel.is_cancelled() {
                break;
            }
            self.publish_metrics();
            let mut trigger = None;
            tokio::select! {
                _ = publication.tick() => {},
                _ = cancel.cancelled() => break,
                Some(state) = self.recovery.recv() => self.registry.recovery_state(state, Instant::now()),
                Some(event) = lifecycle.recv() => match event {
                    LifecycleEvent::NewMarket(hint) => {
                        self.totals.hints += 1;
                        self.registry.queue_hint(&hint, Instant::now());
                    }
                    LifecycleEvent::Resolved(market) => {
                        self.totals.resolutions += 1;
                        if let Some(action) = self.registry.on_resolved(&market) {
                            self.apply(vec![action], true, &cancel).await?;
                        }
                    }
                    LifecycleEvent::Connected(index) => {
                        connected.insert(index);
                        if let Some(since) = outage_since.take() {
                            telemetry::emit("lifecycle_outage", json!({"lasted_s": since.elapsed().as_secs_f64()}));
                            trigger = Some("lifecycle_outage");
                        }
                    }
                    LifecycleEvent::Disconnected(index) => {
                        connected.remove(&index);
                        if connected.is_empty() && outage_since.is_none() {
                            outage_since = Some(Instant::now());
                        }
                    }
                },
                Some(result) = scan_rx.recv() => {
                    let completed = result.rows.is_some();
                    if let Some(rows) = result.rows {
                        let log_adds = self.totals.scans_completed > 0;
                        let actions = self.registry.reconcile(rows);
                        self.apply(actions, log_adds, &cancel).await?;
                        self.totals.scans_completed += 1;
                        self.metrics.state.last_scan = Some(timestamp());
                        self.metrics.state.initialized = true;
                    } else {
                        self.totals.scans_failed += 1;
                    }
                    if let Some(running) = scan.take() {
                        telemetry::emit("scan_completed", json!({
                            "completed": completed,
                            "pages": result.pages,
                            "lasted_s": running.started.elapsed().as_secs_f64(),
                            "registered": self.registry.len(),
                        }));
                        if running.queued {
                            trigger = Some("queued");
                        }
                    }
                },
                _ = scan_timer.tick() => trigger = Some("timer"),
                _ = pending_timer.tick(), if lookup.is_none() => {
                    if let Some(batch) = self.registry.poll_due(Instant::now(), LOOKUP_BATCH) {
                        let listings = self.listings.clone();
                        lookup = Some(Box::pin(async move {
                            let rows = listings.lookup(batch.ids()).await;
                            (batch, rows)
                        }));
                    }
                },
                (batch, result) = async {
                    match lookup.as_mut() {
                        Some(request) => request.await,
                        None => pending().await,
                    }
                } => {
                    lookup = None;
                    let rows = match result {
                        Ok(rows) => Some(rows),
                        Err(error) => {
                            telemetry::emit("pending_lookup_failed", json!({"error": format!("{error:#}")}));
                            None
                        }
                    };
                    let actions = self.registry.on_lookup(batch, rows, Instant::now());
                    self.apply(actions, true, &cancel).await?;
                },
            }

            // A scan is never restarted; a trigger during one queues one more.
            if let Some(reason) = trigger {
                if let Some(running) = &mut scan {
                    running.queued = true;
                } else {
                    telemetry::emit(
                        "scan_started",
                        json!({"trigger": reason, "registered": self.registry.len()}),
                    );
                    scan = Some(RunningScan {
                        started: Instant::now(),
                        queued: false,
                    });
                    self.spawn_scan(scan_tx.clone(), cancel.clone());
                }
            }
        }
        Ok(self.totals())
    }

    fn publish_metrics(&mut self) {
        self.metrics.state.desired_markets = self.registry.len() as u64;
        self.metrics.state.desired_books = self.registry.len() as u64 * 2;
        self.metrics.publish_if_due();
    }

    fn totals(&self) -> DiscoveryTotals {
        DiscoveryTotals {
            registered: self.registry.len(),
            ..self.totals.clone()
        }
    }

    fn spawn_scan(&self, results: mpsc::Sender<ScanResult>, cancel: CancellationToken) {
        let listings = self.listings.clone();
        tokio::spawn(async move {
            let nonce = format!("{:016x}", fastrand::u64(..));
            let mut cursor = None;
            let mut count = 0u64;
            let mut rows = Vec::new();
            let completed = loop {
                let page = tokio::select! {
                    _ = cancel.cancelled() => return,
                    page = listings.scan_page(cursor.take(), nonce.clone()) => page,
                };
                match page {
                    Ok(page) => {
                        count += 1;
                        rows.extend(page.rows);
                        match page.next_cursor {
                            Some(next) => cursor = Some(next),
                            None => break true,
                        }
                    }
                    Err(error) => {
                        telemetry::emit(
                            "scan_page_failed",
                            json!({"page": count, "error": format!("{error:#}")}),
                        );
                        break false;
                    }
                }
            };
            let result = ScanResult {
                rows: completed.then_some(rows),
                pages: count,
            };
            tokio::select! {
                _ = cancel.cancelled() => {},
                _ = results.send(result) => {},
            }
        });
    }

    /// Sends registry actions to the manager: removes first, then additions in one batch.
    /// `log_adds` emits one event per added market (off during the startup
    /// scan, which adds the whole catalog).
    async fn apply(
        &mut self,
        actions: Vec<Action>,
        log_adds: bool,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for action in actions {
            match action {
                Action::Add {
                    listing_id,
                    books,
                    added_by,
                    created_at,
                } => {
                    if log_adds || added_by == AddedBy::Hint {
                        telemetry::emit(
                            "market_added",
                            json!({
                                "listing_id": listing_id,
                                "market": &books.market,
                                "added_by": added_by,
                                "created_at": created_at,
                            }),
                        );
                    }
                    adds.push(books);
                }
                Action::Remove {
                    listing_id,
                    market,
                    reason,
                } => {
                    telemetry::emit(
                        "market_removed",
                        json!({"listing_id": listing_id, "market": &market, "reason": reason}),
                    );
                    removes.push(market);
                }
            }
        }
        if !removes.is_empty() {
            self.send(ManagerInput::Remove(removes), cancel).await?;
        }
        if !adds.is_empty() {
            self.send(ManagerInput::Add(adds), cancel).await?;
        }
        Ok(())
    }

    async fn send(&self, input: ManagerInput, cancel: &CancellationToken) -> anyhow::Result<()> {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Ok(()),
            result = self.manager.send(input) => result.map_err(|_| anyhow::anyhow!("subscription manager stopped")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;

    use crate::core::discovery::{Discovery, LifecycleEvent, ListingPage, Listings};
    use crate::core::subscriptions::ManagerInput;
    use crate::model::{Listing, ListingId, ListingStatus, MarketId, NewMarket, OrderBookId};

    struct Request {
        cursor: Option<String>,
        nonce: String,
        reply: oneshot::Sender<anyhow::Result<ListingPage>>,
    }

    struct LookupRequest {
        ids: Vec<ListingId>,
        reply: oneshot::Sender<anyhow::Result<Vec<Listing>>>,
    }

    #[derive(Clone)]
    struct FakeListings {
        scans: mpsc::UnboundedSender<Request>,
        lookups: mpsc::UnboundedSender<LookupRequest>,
    }

    impl Listings for FakeListings {
        async fn scan_page(
            &self,
            cursor: Option<String>,
            nonce: String,
        ) -> anyhow::Result<ListingPage> {
            let (reply, response) = oneshot::channel();
            self.scans
                .send(Request {
                    cursor,
                    nonce,
                    reply,
                })
                .unwrap();
            response.await.unwrap()
        }

        async fn lookup(&self, ids: Vec<ListingId>) -> anyhow::Result<Vec<Listing>> {
            let (reply, response) = oneshot::channel();
            self.lookups.send(LookupRequest { ids, reply }).unwrap();
            response.await.unwrap()
        }
    }

    fn row(id: u64) -> Listing {
        Listing {
            recovery: crate::model::RecoveryEligibility::Eligible,
            listing_id: ListingId(id.to_string()),
            market: Some(MarketId::from(format!("0x{id}").as_str())),
            order_books: vec![
                OrderBookId::from(format!("{id}1").as_str()),
                OrderBookId::from(format!("{id}2").as_str()),
            ],
            status: ListingStatus::Desired,
            created_at: None,
        }
    }

    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_hint_waits_for_eligible_metadata_before_subscribing() {
        let (requests, mut scans) = mpsc::unbounded_channel();
        let (lookup_tx, mut lookups) = mpsc::unbounded_channel();
        let (manager, mut inputs) = mpsc::channel(16);
        let (events, lifecycle) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let discovery = Discovery::new(
            FakeListings {
                scans: requests,
                lookups: lookup_tx,
            },
            manager,
            Duration::from_secs(3600),
            crate::core::metrics::Metrics::default()
                .register(crate::core::metrics::Component::Discovery),
        );
        let task = tokio::spawn(discovery.run(lifecycle, cancel.clone()));
        scans
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(ListingPage::default()))
            .unwrap();
        let market = row(2);
        let hint = NewMarket {
            listing_id: market.listing_id.clone(),
            market: market.market.clone().unwrap(),
            order_books: market.order_books.clone(),
        };
        for _ in 0..3 {
            events
                .send(LifecycleEvent::NewMarket(hint.clone()))
                .unwrap();
        }
        settle().await;
        assert!(
            inputs.try_recv().is_err(),
            "a hint alone must not subscribe"
        );
        let first = lookups.recv().await.unwrap();
        assert_eq!(first.ids, [ListingId::from("2")]);
        let mut waiting = market.clone();
        waiting.status = ListingStatus::NotDesired;
        first.reply.send(Ok(vec![waiting])).unwrap();
        settle().await;
        assert!(
            inputs.try_recv().is_err(),
            "not accepting orders must remain pending"
        );
        let second = lookups.recv().await.unwrap();
        second.reply.send(Ok(vec![market])).unwrap();
        assert!(
            matches!(inputs.recv().await, Some(ManagerInput::Add(books)) if books.len() == 1 && books[0].market == hint.market)
        );
        cancel.cancel();
        assert_eq!(task.await.unwrap().unwrap().registered, 1);
    }

    #[tokio::test]
    async fn blocked_lookup_keeps_publication_and_resolution_responsive_and_cancels() {
        let (requests, mut scans) = mpsc::unbounded_channel();
        let (lookup_tx, mut lookups) = mpsc::unbounded_channel();
        let (manager, mut inputs) = mpsc::channel(16);
        let (events, lifecycle) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let metrics = crate::core::metrics::Metrics::default();
        let discovery = Discovery::new(
            FakeListings {
                scans: requests,
                lookups: lookup_tx,
            },
            manager,
            Duration::from_secs(3600),
            metrics.register(crate::core::metrics::Component::Discovery),
        );
        let task = tokio::spawn(discovery.run(lifecycle, cancel.clone()));
        scans
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(ListingPage {
                rows: vec![row(1)],
                next_cursor: None,
            }))
            .unwrap();
        assert!(matches!(inputs.recv().await, Some(ManagerInput::Add(_))));
        let market = row(2);
        events
            .send(LifecycleEvent::NewMarket(NewMarket {
                listing_id: market.listing_id,
                market: market.market.unwrap(),
                order_books: market.order_books,
            }))
            .unwrap();
        let blocked = tokio::time::timeout(Duration::from_secs(3), lookups.recv())
            .await
            .unwrap()
            .unwrap();
        let publication = |text: &str| {
            text.lines()
                .find(|line| {
                    line.starts_with(
                        "collector_oldest_required_component_publication_timestamp_seconds ",
                    )
                })
                .unwrap()
                .to_owned()
        };
        let before = publication(&metrics.render().unwrap().text);
        events
            .send(LifecycleEvent::Resolved(MarketId::from("0x1")))
            .unwrap();
        assert!(
            matches!(tokio::time::timeout(Duration::from_secs(1), inputs.recv()).await.unwrap(), Some(ManagerInput::Remove(markets)) if markets == [MarketId::from("0x1")])
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let view = metrics.render().unwrap();
                if view.initialized && publication(&view.text) != before {
                    assert!(view.fresh);
                    assert!(view.text.contains("collector_markets{state=\"desired\"} 0"));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            blocked.reply.is_closed(),
            "cancellation drops the in-flight HTTP future"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_unblocks_admission_waiting_on_the_subscription_manager() {
        let (requests, mut scans) = mpsc::unbounded_channel();
        let (lookup_tx, mut lookups) = mpsc::unbounded_channel();
        let (manager, mut inputs) = mpsc::channel(1);
        let (events, lifecycle) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let discovery = Discovery::new(
            FakeListings {
                scans: requests,
                lookups: lookup_tx,
            },
            manager,
            Duration::from_secs(3600),
            crate::core::metrics::Metrics::default()
                .register(crate::core::metrics::Component::Discovery),
        );
        let task = tokio::spawn(discovery.run(lifecycle, cancel.clone()));
        scans
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(ListingPage {
                rows: vec![row(1)],
                next_cursor: None,
            }))
            .unwrap();
        settle().await;
        let market = row(2);
        events
            .send(LifecycleEvent::NewMarket(NewMarket {
                listing_id: market.listing_id,
                market: market.market.unwrap(),
                order_books: market.order_books,
            }))
            .unwrap();
        lookups
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(vec![row(2)]))
            .unwrap();
        settle().await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(inputs.try_recv(), Ok(ManagerInput::Add(books)) if books[0].market == MarketId::from("0x1"))
        );
        assert!(inputs.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn scans_without_lifecycle_connection_and_preserves_recent_events() {
        let (requests, mut scans) = mpsc::unbounded_channel();
        let (lookup_tx, mut lookups) = mpsc::unbounded_channel();
        let (manager, mut inputs) = mpsc::channel(16);
        let (events, lifecycle) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let discovery = Discovery::new(
            FakeListings {
                scans: requests,
                lookups: lookup_tx,
            },
            manager,
            Duration::from_secs(60),
            crate::core::metrics::Metrics::default()
                .register(crate::core::metrics::Component::Discovery),
        );
        let task = tokio::spawn(discovery.run(lifecycle, cancel.clone()));
        // HTTP discovery starts and repeats even when no lifecycle socket connects.
        let first = scans.recv().await.unwrap();
        assert_eq!(first.cursor, None);
        first
            .reply
            .send(Ok(ListingPage {
                rows: vec![row(1)],
                next_cursor: Some("next".into()),
            }))
            .unwrap();
        let second = scans.recv().await.unwrap();
        assert_eq!(second.cursor.as_deref(), Some("next"));
        assert_eq!(first.nonce, second.nonce);
        assert!(
            inputs.try_recv().is_err(),
            "partial pages must not change subscriptions"
        );

        let hint = row(2);
        events
            .send(LifecycleEvent::NewMarket(NewMarket {
                listing_id: ListingId::from("2"),
                market: hint.market.unwrap(),
                order_books: hint.order_books,
            }))
            .unwrap();
        let confirmation = lookups.recv().await.unwrap();
        assert_eq!(confirmation.ids, [ListingId::from("2")]);
        confirmation.reply.send(Ok(vec![row(2)])).unwrap();
        assert!(
            matches!(inputs.recv().await, Some(ManagerInput::Add(books)) if books[0].market == MarketId::from("0x2"))
        );
        // Resolving a market not yet admitted must suppress its buffered scan row.
        events
            .send(LifecycleEvent::Resolved(MarketId::from("0x1")))
            .unwrap();
        second.reply.send(Ok(ListingPage::default())).unwrap();
        settle().await;
        assert!(
            inputs.try_recv().is_err(),
            "scan cannot undo newer lifecycle events"
        );

        tokio::time::advance(Duration::from_secs(60)).await;
        let next = scans.recv().await.unwrap();
        next.reply
            .send(Ok(ListingPage {
                rows: vec![row(3)],
                next_cursor: None,
            }))
            .unwrap();
        assert!(
            matches!(inputs.recv().await, Some(ManagerInput::Remove(markets)) if markets == vec![MarketId::from("0x2")])
        );
        assert!(
            matches!(inputs.recv().await, Some(ManagerInput::Add(books)) if books[0].market == MarketId::from("0x3"))
        );
        cancel.cancel();
        let totals = task.await.unwrap().unwrap();
        assert_eq!(totals.scans_completed, 2);
        assert_eq!(totals.registered, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_scan_discards_pages_retains_events_and_runs_queued_scan() {
        let (requests, mut scans) = mpsc::unbounded_channel();
        let (lookup_tx, mut lookups) = mpsc::unbounded_channel();
        let (manager, mut inputs) = mpsc::channel(16);
        let (events, lifecycle) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let discovery = Discovery::new(
            FakeListings {
                scans: requests,
                lookups: lookup_tx,
            },
            manager,
            Duration::from_secs(3600),
            crate::core::metrics::Metrics::default()
                .register(crate::core::metrics::Component::Discovery),
        );
        let task = tokio::spawn(discovery.run(lifecycle, cancel.clone()));
        events.send(LifecycleEvent::Connected(0)).unwrap();
        let first = scans.recv().await.unwrap();
        first
            .reply
            .send(Ok(ListingPage {
                rows: vec![row(1)],
                next_cursor: Some("next".into()),
            }))
            .unwrap();
        let second = scans.recv().await.unwrap();
        let hint = row(2);
        events
            .send(LifecycleEvent::NewMarket(NewMarket {
                listing_id: ListingId::from("2"),
                market: hint.market.unwrap(),
                order_books: hint.order_books,
            }))
            .unwrap();
        let confirmation = lookups.recv().await.unwrap();
        confirmation.reply.send(Ok(vec![row(2)])).unwrap();
        assert!(matches!(inputs.recv().await, Some(ManagerInput::Add(_))));
        events.send(LifecycleEvent::Disconnected(0)).unwrap();
        events.send(LifecycleEvent::Connected(0)).unwrap();
        second
            .reply
            .send(Err(anyhow::anyhow!("simulated page failure")))
            .unwrap();
        let queued = scans.recv().await.unwrap();
        assert_eq!(queued.cursor, None);
        assert!(
            inputs.try_recv().is_err(),
            "failed scan neither adds its rows nor removes live markets"
        );
        queued.reply.send(Ok(ListingPage::default())).unwrap();
        settle().await;
        assert!(
            inputs.try_recv().is_err(),
            "failed scan must not clear recent additions"
        );
        cancel.cancel();
        let totals = task.await.unwrap().unwrap();
        assert_eq!(totals.scans_failed, 1);
        assert_eq!(totals.scans_completed, 1);
        assert_eq!(totals.registered, 1);
    }
}
