//! The worker (SPEC section 9): owns a fixed set of markets and their order
//! books, turns `WorkerMessage`s into `OutputRecord`s, and writes periodic
//! snapshots from its own `DelayQueue`.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use serde_json::json;
use tokio::sync::mpsc::{self, error::SendTimeoutError, error::TrySendError};
use tokio::time::Instant;
use tokio_util::time::DelayQueue;
use tokio_util::time::delay_queue::{Expired, Key};

use crate::Side;
use crate::core::config::SNAPSHOT_INTERVAL;
use crate::core::metrics::{
    Component, Distribution, Histogram, Metrics, PUBLICATION_INTERVAL, Publisher,
};
use crate::core::order_book::{OrderBook, OrderBookSide};
use crate::core::telemetry;
use crate::model::{
    ConnectionId, LevelUpdate, MarketData, MarketId, MarketSources, OrderBookEvent, OrderBookId,
    OutputRecord, Price, Qty, UnsyncReason, WorkerMessage,
};

type Slot = usize;

/// Where a tracked order book lives: its market's slot and its position in
/// that market's `order_books`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BookPos {
    slot: Slot,
    index: usize,
}

/// Outcome of validating one level update before any of the event is applied.
enum LevelStep {
    Apply { index: usize, qty: Qty },
    UnknownBook,
    Unsynced,
}

#[derive(Default)]
struct SourceBook {
    order_book: OrderBook,
    synced: bool,
    max_applied_exchange_ts: DateTime<Utc>,
    last_exchange_ts: DateTime<Utc>,
    last_received_at: DateTime<Utc>,
}

struct TrackedOrderBook {
    id: OrderBookId,
    // Exactly two independent source images; publication keeps metadata only.
    sources: [SourceBook; 2],
    last_exchange_ts: DateTime<Utc>,
    last_received_at: DateTime<Utc>,
    snapshot_timer: Option<Key>,
}

struct Market {
    generation: u64,
    attached: [bool; 2],
    id: MarketId,
    order_books: Vec<TrackedOrderBook>,
    sources: MarketSources,
    publisher: Option<usize>,
    previous_publisher: Option<usize>,
    failure_reason: Option<UnsyncReason>,
}

impl Market {
    fn book_index(&self, order_book: &OrderBookId) -> Option<usize> {
        self.order_books
            .iter()
            .position(|book| book.id == *order_book)
    }

    fn ready(&self, source: usize) -> bool {
        self.order_books.iter().all(|b| b.sources[source].synced)
    }

    fn coverage(&self) -> (u64, u64, u64) {
        let initialized = |source: usize| {
            self.order_books
                .iter()
                .filter(|b| b.sources[source].synced)
                .count() as u64
        };
        let books = self.publisher.map(initialized).unwrap_or_else(|| {
            self.sources
                .connections()
                .iter()
                .enumerate()
                .map(|(i, _)| initialized(i))
                .max()
                .unwrap_or(0)
        });
        let synced = if self.sources.is_mirrored() {
            self.publisher.is_some()
        } else {
            self.ready(0)
        };
        let standby = self.sources.is_mirrored()
            && self.publisher.is_some_and(|source| self.ready(1 - source));
        (u64::from(synced), books, u64::from(standby))
    }
}

/// Counters a worker returns on shutdown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerTotals {
    pub messages: u64,
    pub standby_ready_markets: u64,
    pub publisher_switches: [u64; 5],
    pub first_snapshots: u64,
    pub periodic_snapshots: u64,
    pub removal_snapshots: u64,
    /// Periodic snapshots skipped because the book was not synced.
    pub periodic_skipped: u64,
    pub level_updates: u64,
    /// Levels written to catch up a book that was not synced (expected).
    pub catch_up_levels: u64,
    /// Repeat books that differed from a synced book. This counts observed
    /// divergence, without attributing it to missed data or a particular cause.
    pub mismatched_books: u64,
    pub mismatched_levels: u64,
    pub older_full_books: u64,
    /// Events for markets or order books this worker doesn't hold.
    pub dropped_unknown: u64,
    /// Events from a connection other than the market's own.
    pub dropped_other_connection: u64,
    /// Level updates for books that were not synced.
    pub dropped_unsynced: u64,
    pub unsync_marks: u64,
    /// Synced books made unsynced by `MarkUnsynced`.
    pub books_unsynced: u64,
    pub writer_blocked: Duration,
    /// Synchronous book processing, excluding waits for input and output.
    pub apply_time: Duration,
    pub input_queue_peak: usize,
    /// Order books held, and of those synced, when the worker stopped.
    pub books_held: u64,
    pub books_synced: u64,
}

pub struct Worker {
    index: usize,
    markets: Vec<Option<Market>>,
    free_slots: Vec<Slot>,
    by_market: HashMap<MarketId, Slot>,
    by_connection: HashMap<ConnectionId, Vec<Slot>>,
    snapshot_timers: DelayQueue<BookPos>,
    /// Records produced by the current input, sent before the next one.
    pending: Vec<OutputRecord>,
    totals: WorkerTotals,
    synced_markets: u64,
    older_full_book_lag: Histogram,
}

impl Worker {
    pub fn new(index: usize) -> Self {
        Self {
            index,
            markets: Vec::new(),
            free_slots: Vec::new(),
            by_market: HashMap::new(),
            by_connection: HashMap::new(),
            snapshot_timers: DelayQueue::new(),
            pending: Vec::new(),
            totals: WorkerTotals::default(),
            synced_markets: 0,
            older_full_book_lag: Histogram::new(Distribution::Exchange),
        }
    }

    /// Runs until `input` closes and is drained. Waits on the input channel
    /// and the snapshot timers together. An error is fatal: the writer
    /// channel stayed full for `writer_stall_timeout`, or the writer is gone.
    pub async fn run(
        mut self,
        mut input: mpsc::Receiver<WorkerMessage>,
        writer: mpsc::Sender<Vec<OutputRecord>>,
        writer_stall_timeout: Duration,
        mut metrics: Publisher,
    ) -> anyhow::Result<WorkerTotals> {
        let mut publication = tokio::time::interval(PUBLICATION_INTERVAL);
        publication.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            self.publish_metrics(&mut metrics);
            tokio::select! {
                // Timers first, so a busy input can't starve snapshots.
                biased;
                _ = publication.tick() => {
                    continue;
                }
                Some(expired) = std::future::poll_fn(|cx| self.snapshot_timers.poll_expired(cx)) => {
                    self.on_snapshot_timer(expired);
                }
                message = input.recv() => match message {
                    Some(message) => {
                        self.totals.input_queue_peak = self.totals.input_queue_peak.max(input.len() + 1);
                        let started = std::time::Instant::now();
                        self.handle(message)?;
                        self.totals.apply_time += started.elapsed();
                    },
                    None => break,
                },
            }
            self.send_pending(&writer, writer_stall_timeout).await?;
        }
        self.publish_metrics(&mut metrics);
        metrics.publish();
        Ok(self.totals)
    }

    fn publish_metrics(&self, metrics: &mut Publisher) {
        metrics.state.synced_markets = self.synced_markets;
        metrics.state.standby_ready_markets = self.totals.standby_ready_markets;
        metrics.state.publisher_switches = self.totals.publisher_switches;
        metrics.state.synced_books = self.totals.books_synced;
        metrics.state.mismatches = self.totals.mismatched_books;
        metrics.state.older_full_books = self.totals.older_full_books;
        metrics
            .state
            .older_full_book_lag
            .clone_from(&self.older_full_book_lag);
        metrics.state.busy_seconds = self.totals.apply_time.as_secs_f64();
        metrics.state.blocked_seconds = self.totals.writer_blocked.as_secs_f64();
        metrics.publish_if_due();
    }

    async fn send_pending(
        &mut self,
        writer: &mpsc::Sender<Vec<OutputRecord>>,
        stall_timeout: Duration,
    ) -> anyhow::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.pending);
        let batch = match writer.try_send(batch) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Closed(_)) => return Err(anyhow!("writer channel closed")),
            Err(TrySendError::Full(batch)) => batch,
        };
        let blocked_since = Instant::now();
        let result = writer.send_timeout(batch, stall_timeout).await;
        self.totals.writer_blocked += blocked_since.elapsed();
        match result {
            Ok(()) => {}
            Err(SendTimeoutError::Timeout(_)) => {
                return Err(anyhow!(
                    "worker {}: writer channel full for more than {stall_timeout:?}",
                    self.index
                ));
            }
            Err(SendTimeoutError::Closed(_)) => return Err(anyhow!("writer channel closed")),
        }
        Ok(())
    }

    /// Applies one input; output goes to `pending`.
    fn handle(&mut self, message: WorkerMessage) -> anyhow::Result<()> {
        self.totals.messages += 1;
        match message {
            WorkerMessage::SourceAttached {
                market,
                generation,
                connection,
            } => {
                if let Some(slot) = self.by_market.get(&market).copied()
                    && let Some(market) = self.markets[slot].as_mut()
                    && market.generation == generation
                    && let Some(source) = market
                        .sources
                        .connections()
                        .iter()
                        .position(|c| *c == connection)
                {
                    market.attached[source] = true;
                }
            }
            WorkerMessage::MarketData(data) => self.on_market_data(data)?,
            WorkerMessage::AddMarket {
                generation,
                market,
                order_books,
                sources,
            } => self.add_market(market, order_books, sources, generation),
            WorkerMessage::RemoveMarket(market) => self.remove_market(&market),
            WorkerMessage::MarkUnsynced { connection, reason } => {
                self.mark_unsynced(connection, reason)
            }
        }
        Ok(())
    }

    fn adjust_coverage(&mut self, before: (u64, u64, u64), after: (u64, u64, u64)) {
        self.synced_markets = self.synced_markets - before.0 + after.0;
        self.totals.books_synced = self.totals.books_synced - before.1 + after.1;
        self.totals.standby_ready_markets = self.totals.standby_ready_markets - before.2 + after.2;
    }

    fn add_market(
        &mut self,
        id: MarketId,
        order_books: Vec<OrderBookId>,
        sources: MarketSources,
        generation: u64,
    ) {
        if self.by_market.contains_key(&id) {
            telemetry::emit(
                "market_readded_without_remove",
                json!({"worker": self.index, "market": &id}),
            );
            self.remove_market(&id);
        }
        let slot = self.free_slots.pop().unwrap_or_else(|| {
            self.markets.push(None);
            self.markets.len() - 1
        });
        self.totals.books_held += order_books.len() as u64;
        let order_books = order_books
            .into_iter()
            .map(|id| TrackedOrderBook {
                id,
                sources: std::array::from_fn(|_| SourceBook::default()),
                last_exchange_ts: DateTime::UNIX_EPOCH,
                last_received_at: DateTime::UNIX_EPOCH,
                snapshot_timer: None,
            })
            .collect();
        self.by_market.insert(id.clone(), slot);
        for &connection in sources.connections() {
            self.by_connection.entry(connection).or_default().push(slot);
        }
        self.markets[slot] = Some(Market {
            generation,
            attached: [generation == 0; 2],
            id,
            order_books,
            sources,
            publisher: (!sources.is_mirrored()).then_some(0),
            previous_publisher: None,
            failure_reason: None,
        });
    }

    fn remove_market(&mut self, id: &MarketId) {
        let Some(slot) = self.by_market.remove(id) else {
            return;
        };
        let Some(market) = self.markets[slot].take() else {
            return;
        };
        self.adjust_coverage(market.coverage(), (0, 0, 0));
        self.totals.books_held -= market.order_books.len() as u64;
        for book in &market.order_books {
            if let Some(key) = &book.snapshot_timer {
                self.snapshot_timers.remove(key);
                self.pending
                    .push(snapshot_record(&OrderBook::default(), book));
                self.totals.removal_snapshots += 1;
            }
        }
        for connection in market.sources.connections() {
            if let Some(slots) = self.by_connection.get_mut(connection) {
                slots.retain(|&other| other != slot);
                if slots.is_empty() {
                    self.by_connection.remove(connection);
                }
            }
        }
        self.free_slots.push(slot);
    }

    fn mark_unsynced(&mut self, connection: ConnectionId, reason: UnsyncReason) {
        self.totals.unsync_marks += 1;
        let mut books = 0;
        // Each source membership is indexed, including standby connections.
        let slots = self
            .by_connection
            .get(&connection)
            .cloned()
            .unwrap_or_default();
        for slot in slots {
            let Some(market) = self.markets[slot].as_mut() else {
                continue;
            };
            let Some(source) = market
                .sources
                .connections()
                .iter()
                .position(|&c| c == connection)
            else {
                continue;
            };
            let before = market.coverage();
            for book in &mut market.order_books {
                books += u64::from(book.sources[source].synced);
                book.sources[source].synced = false;
                book.sources[source].max_applied_exchange_ts = DateTime::UNIX_EPOCH;
            }
            if market.sources.is_mirrored() && market.publisher == Some(source) {
                market.previous_publisher = Some(source);
                market.failure_reason = Some(reason);
                market.publisher = None;
            }
            select_publisher(
                market,
                slot,
                &mut self.snapshot_timers,
                &mut self.pending,
                &mut self.totals,
            );
            let after = market.coverage();
            self.adjust_coverage(before, after);
        }
        self.totals.books_unsynced += books;
        telemetry::emit(
            "unsynced",
            json!({"worker": self.index, "connection": connection.0, "reason": reason, "books": books}),
        );
    }

    fn on_market_data(&mut self, data: MarketData) -> anyhow::Result<()> {
        let Some(&slot) = self.by_market.get(&data.market) else {
            self.totals.dropped_unknown += 1;
            return Ok(());
        };
        let Some(market) = self.markets[slot].as_mut() else {
            return Ok(());
        };
        let Some(source) = market
            .sources
            .connections()
            .iter()
            .position(|&c| c == data.connection)
        else {
            self.totals.dropped_other_connection += 1;
            return Ok(());
        };
        if !market.attached[source] {
            return Ok(());
        }
        let before = market.coverage();
        let publishing = market.publisher == Some(source);
        let mirrored = market.sources.is_mirrored();
        match data.event {
            OrderBookEvent::Snapshot {
                order_book,
                bids,
                asks,
                timestamp_comparable,
            } => {
                let Some(index) = market.book_index(&order_book) else {
                    self.totals.dropped_unknown += 1;
                    return Ok(());
                };
                let book = &mut market.order_books[index];
                let held = &book.sources[source];
                if timestamp_comparable
                    && held.synced
                    && data.exchange_ts < held.max_applied_exchange_ts
                {
                    let lag = held.max_applied_exchange_ts - data.exchange_ts;
                    self.totals.older_full_books += 1;
                    self.older_full_book_lag
                        .record(lag.num_milliseconds() as f64 / 1_000.);
                    telemetry::emit(
                        "older_full_book_skipped",
                        json!({"worker": self.index, "lag_ms": lag.num_milliseconds()}),
                    );
                    return Ok(());
                }
                anyhow::ensure!(
                    bids.iter().chain(&asks).all(|(_, qty)| qty.0 >= 0),
                    "negative snapshot quantity"
                );
                let received = OrderBook {
                    bids: OrderBookSide::from_levels(&bids),
                    asks: OrderBookSide::from_levels(&asks),
                };
                if publishing {
                    book.last_exchange_ts = data.exchange_ts;
                    book.last_received_at = data.received_at;
                }
                let held = &book.sources[source];
                // Compare only repeat baselines from this source. Recovery in
                // mirrored mode is a replacement, never an invented catch-up.
                let compare = held.synced || (!mirrored && book.snapshot_timer.is_some());
                let mut differences = 0;
                if compare {
                    for (side, old, target) in [
                        (Side::Buy, &held.order_book.bids, &received.bids),
                        (Side::Sell, &held.order_book.asks, &received.asks),
                    ] {
                        for (price, qty) in old.differences(target) {
                            differences += 1;
                            if publishing {
                                self.pending
                                    .push(level_record(book, side, price, qty, None));
                            }
                        }
                    }
                }
                if held.synced && differences > 0 {
                    self.totals.mismatched_books += 1;
                    self.totals.mismatched_levels += differences;
                    telemetry::emit(
                        "mismatch",
                        json!({"worker": self.index, "market": &market.id, "order_book": &book.id, "connection": data.connection.0, "levels": differences}),
                    );
                } else if !mirrored {
                    self.totals.catch_up_levels += differences;
                }
                book.sources[source] = SourceBook {
                    order_book: received,
                    synced: true,
                    max_applied_exchange_ts: data.exchange_ts,
                    last_exchange_ts: data.exchange_ts,
                    last_received_at: data.received_at,
                };
                if publishing && book.snapshot_timer.is_none() {
                    self.pending
                        .push(snapshot_record(&book.sources[source].order_book, book));
                    book.snapshot_timer = Some(
                        self.snapshot_timers
                            .insert(BookPos { slot, index }, SNAPSHOT_INTERVAL),
                    );
                    self.totals.first_snapshots += 1;
                }
            }
            OrderBookEvent::LevelUpdates(updates) => {
                let mut staged = HashMap::new();
                let mut steps = Vec::with_capacity(updates.len());
                for update in &updates {
                    let Some(index) = market.book_index(&update.order_book) else {
                        steps.push(LevelStep::UnknownBook);
                        continue;
                    };
                    let book = &market.order_books[index].sources[source];
                    if !book.synced {
                        steps.push(LevelStep::Unsynced);
                        continue;
                    }
                    let key = (index, update.side, update.price);
                    let held = match update.side {
                        Side::Buy => &book.order_book.bids,
                        Side::Sell => &book.order_book.asks,
                    };
                    let current = staged
                        .get(&key)
                        .copied()
                        .unwrap_or_else(|| held.quantity(update.price));
                    let qty = update.change.resolve(current).with_context(|| {
                        format!("book {} price {}", update.order_book, update.price.0)
                    })?;
                    staged.insert(key, qty);
                    steps.push(LevelStep::Apply { index, qty });
                }
                for (
                    LevelUpdate {
                        order_book: _,
                        side,
                        price,
                        change: _,
                    },
                    step,
                ) in updates.into_iter().zip(steps)
                {
                    let (index, qty) = match step {
                        LevelStep::Apply { index, qty } => (index, qty),
                        LevelStep::UnknownBook => {
                            self.totals.dropped_unknown += 1;
                            continue;
                        }
                        LevelStep::Unsynced => {
                            self.totals.dropped_unsynced += 1;
                            continue;
                        }
                    };
                    let book = &mut market.order_books[index];
                    let held = &mut book.sources[source];
                    match side {
                        Side::Buy => held.order_book.bids.set(price, qty),
                        Side::Sell => held.order_book.asks.set(price, qty),
                    }
                    held.max_applied_exchange_ts =
                        held.max_applied_exchange_ts.max(data.exchange_ts);
                    held.last_exchange_ts = data.exchange_ts;
                    held.last_received_at = data.received_at;
                    if publishing {
                        book.last_exchange_ts = data.exchange_ts;
                        book.last_received_at = data.received_at;
                        self.pending.push(level_record(
                            book,
                            side,
                            price,
                            qty,
                            data.received_instant,
                        ));
                        self.totals.level_updates += 1;
                    }
                }
            }
        }
        select_publisher(
            market,
            slot,
            &mut self.snapshot_timers,
            &mut self.pending,
            &mut self.totals,
        );
        let after = market.coverage();
        self.adjust_coverage(before, after);
        Ok(())
    }

    fn on_snapshot_timer(&mut self, expired: Expired<BookPos>) {
        let deadline = expired.deadline();
        let pos = expired.into_inner();
        let Some(market) = self.markets[pos.slot].as_mut() else {
            return;
        };
        let book = &mut market.order_books[pos.index];
        if let Some(source) = market
            .publisher
            .filter(|&source| book.sources[source].synced)
        {
            self.pending
                .push(snapshot_record(&book.sources[source].order_book, book));
            self.totals.periodic_snapshots += 1;
        } else {
            self.totals.periodic_skipped += 1;
        }
        book.snapshot_timer = Some(
            self.snapshot_timers
                .insert_at(pos, deadline + SNAPSHOT_INTERVAL),
        );
    }
}

/// Select only a complete source, and publish both outcomes in this turn.
fn select_publisher(
    market: &mut Market,
    slot: Slot,
    timers: &mut DelayQueue<BookPos>,
    pending: &mut Vec<OutputRecord>,
    totals: &mut WorkerTotals,
) {
    if !market.sources.is_mirrored() || market.publisher.is_some() {
        return;
    }
    let Some(target) = (0..2).find(|&source| market.ready(source)) else {
        return;
    };
    if let Some(previous) = market.previous_publisher
        && previous != target
    {
        if let Some(index) = market.failure_reason.and_then(UnsyncReason::switch_index) {
            totals.publisher_switches[index] += 1;
        }
        telemetry::emit(
            "publisher_switch",
            json!({
                "market": &market.id,
                "old_connection": market.sources.connections()[previous].0,
                "new_connection": market.sources.connections()[target].0,
                "reason": market.failure_reason,
                "decision_time": Utc::now(),
            }),
        );
    }
    for (index, book) in market.order_books.iter_mut().enumerate() {
        let source = &book.sources[target];
        book.last_exchange_ts = source.last_exchange_ts;
        book.last_received_at = source.last_received_at;
        pending.push(snapshot_record(&source.order_book, book));
        if book.snapshot_timer.is_none() {
            book.snapshot_timer = Some(timers.insert(BookPos { slot, index }, SNAPSHOT_INTERVAL));
            totals.first_snapshots += 1;
        }
    }
    market.publisher = Some(target);
    market.previous_publisher = Some(target);
    market.failure_reason = None;
}

/// A snapshot of `levels` carrying the timestamps of the latest message
/// `book` applied. Levels are best first: bids descending, asks ascending.
fn snapshot_record(levels: &OrderBook, book: &TrackedOrderBook) -> OutputRecord {
    OutputRecord::Snapshot {
        order_book: book.id.clone(),
        bids: levels.bids_best_first(),
        asks: levels.asks_best_first(),
        received_at: book.last_received_at,
        exchange_ts: book.last_exchange_ts,
    }
}

fn level_record(
    book: &TrackedOrderBook,
    side: Side,
    price: Price,
    qty: Qty,
    received_instant: Option<std::time::Instant>,
) -> OutputRecord {
    OutputRecord::LevelUpdate {
        received_instant,
        order_book: book.id.clone(),
        side,
        price,
        qty,
        received_at: book.last_received_at,
        exchange_ts: book.last_exchange_ts,
    }
}

/// Spawns `workers` worker tasks on the current runtime. Returns their input
/// senders and join handles.
pub fn spawn_workers(
    workers: usize,
    queue: usize,
    writer: &mpsc::Sender<Vec<OutputRecord>>,
    writer_stall_timeout: Duration,
    metrics: &Metrics,
) -> (
    Vec<mpsc::Sender<WorkerMessage>>,
    Vec<tokio::task::JoinHandle<anyhow::Result<WorkerTotals>>>,
) {
    (0..workers)
        .map(|index| {
            let (tx, rx) = mpsc::channel(queue);
            let failure = metrics.failure_token();
            let task = Worker::new(index).run(
                rx,
                writer.clone(),
                writer_stall_timeout,
                metrics.register(Component::Worker(index)),
            );
            (
                tx,
                tokio::spawn(async move {
                    let guard = failure.drop_guard();
                    let result = task.await.with_context(|| format!("worker {index}"));
                    if result.is_ok() {
                        guard.disarm();
                    }
                    result
                }),
            )
        })
        .unzip()
}
