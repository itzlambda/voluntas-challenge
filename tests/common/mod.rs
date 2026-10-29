use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::core::config::Config;
use engineering_challenge::core::connection::FrameTotals;
use engineering_challenge::core::fixture::{FixtureEvent, ProtocolCommand, parse_jsonl};
use engineering_challenge::core::pipeline::{ReplayTotals, replay};
use engineering_challenge::core::worker::WorkerTotals;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use serde_json::Value;

const PERIOD_MS: u64 = 60_000;
type Levels = BTreeMap<i64, i64>;
pub type Records = BTreeMap<String, Vec<Record>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Snapshot {
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        received_us: i64,
        exchange_us: i64,
    },
    Update {
        side: Side,
        price: i64,
        qty: i64,
        received_us: i64,
        exchange_us: i64,
    },
}

pub struct Run {
    pub workers: usize,
    pub totals: ReplayTotals,
    pub records: Records,
    pub decode_log: Vec<Value>,
}

pub fn fixture(text: &str) -> Vec<FixtureEvent> {
    parse_jsonl(text).expect("valid fixture JSONL")
}

pub fn sum(totals: &ReplayTotals, counter: impl Fn(&WorkerTotals) -> u64) -> u64 {
    totals.pipeline.workers.iter().map(counter).sum()
}

/// Replay every fixture through all requested worker counts, then inspect the
/// actual SBE file. Only per-symbol order is promised by the output contract.
pub async fn check(events: Vec<FixtureEvent>) -> Vec<Run> {
    check_with_workers(events, &[1, 2, 4]).await
}

/// Check another worker layout against the same independent reference model.
pub async fn check_with_workers(events: Vec<FixtureEvent>, worker_counts: &[usize]) -> Vec<Run> {
    let expected = Reference::rebuild(&events);
    let mut runs: Vec<Run> = Vec::new();
    for &workers in worker_counts {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            workers,
            output: temp.path().join("books.sbe"),
            decode_error_log: temp.path().join("decode-errors.jsonl"),
            writer_queue: 1 << 20,
            ..Config::default()
        };
        let totals = replay(
            &config,
            events.clone(),
            engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::default,
        )
        .await
        .unwrap();
        let (records, bytes) = read_records(&config.output);
        assert_eq!(
            records, expected.records,
            "reference mismatch with N={workers}"
        );
        if let Some(first) = runs.first() {
            assert_eq!(
                records, first.records,
                "worker-count dependent output N={workers}"
            );
        }
        for (symbol, sequence) in &records {
            let mut previous_received = None;
            for record in sequence {
                if let Record::Snapshot { received_us, .. } = record {
                    if let Some(received) = previous_received {
                        assert!(
                            *received_us >= received,
                            "{symbol}: snapshot received time regressed"
                        );
                    }
                    // Exchange timestamps may regress on applied deltas or reconnect.
                    previous_received = Some(*received_us);
                }
            }
        }
        expected.check_totals(&totals, workers);
        assert_eq!(totals.pipeline.writer.bytes, bytes);
        assert_eq!(
            std::fs::metadata(&config.output).unwrap().len() % (64 << 20),
            0
        );
        let decode_log: Vec<Value> = std::fs::read_to_string(&config.decode_error_log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(decode_log.len() as u64, totals.frames.decode_errors);
        runs.push(Run {
            workers,
            totals,
            records,
            decode_log,
        });
    }
    runs
}

pub(crate) fn read_records(path: &Path) -> (Records, u64) {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut records = Records::new();
    let mut bytes = 0;
    while let Some(message) = reader.read_msg().unwrap() {
        let (symbol, record) = match message {
            AnyMessage::Snapshot(decoded) => {
                let message = SnapshotDomain::try_from_decoder(decoded).unwrap();
                bytes += message.encoded_length_with_header().unwrap() as u64;
                (
                    message.symbol,
                    Record::Snapshot {
                        bids: message
                            .bids
                            .into_iter()
                            .map(|level| (level.price_level, level.quantity))
                            .collect(),
                        asks: message
                            .asks
                            .into_iter()
                            .map(|level| (level.price_level, level.quantity))
                            .collect(),
                        received_us: message.received_at.timestamp_micros(),
                        exchange_us: message.exchange_timestamp.timestamp_micros(),
                    },
                )
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let message = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
                bytes += message.encoded_length_with_header().unwrap() as u64;
                (
                    message.symbol,
                    Record::Update {
                        side: message.side,
                        price: message.price_level,
                        qty: message.quantity,
                        received_us: message.backtest_timestamp.timestamp_micros(),
                        exchange_us: message.exchange_timestamp.timestamp_micros(),
                    },
                )
            }
            _ => panic!("unexpected SBE message type"),
        };
        records.entry(symbol).or_default().push(record);
    }
    (records, bytes)
}

#[derive(Default)]
struct Book {
    bids: Levels,
    asks: Levels,
    synced: bool,
    next_snapshot: Option<u64>,
    received_us: i64,
    exchange_us: i64,
    max_exchange_us: i64,
}

impl Book {
    fn snapshot(&self) -> Record {
        Record::Snapshot {
            bids: self.bids.iter().rev().map(|(&p, &q)| (p, q)).collect(),
            asks: self.asks.iter().map(|(&p, &q)| (p, q)).collect(),
            received_us: self.received_us,
            exchange_us: self.exchange_us,
        }
    }
}

struct Market {
    connection: u16,
    books: BTreeMap<String, Book>,
}

#[derive(Default)]
struct Counts {
    first: u64,
    periodic: u64,
    removed: u64,
    skipped: u64,
    updates: u64,
    catch_up: u64,
    older_full_books: u64,
    mismatched_books: u64,
    mismatched_levels: u64,
    unsynced_books: u64,
    dropped_unsynced: u64,
    dropped_unknown: u64,
    controls: u64,
    marks: u64,
}

/// This reducer uses only fixture controls and serde_json::Value. It shares no
/// parsing, routing, book mutation or diff implementation with the collector.
#[derive(Default)]
struct Reference {
    now_ms: u64,
    markets: BTreeMap<String, Market>,
    records: Records,
    counts: Counts,
    frames: FrameTotals,
    checkpoint_keys: BTreeSet<(String, i64, i64)>,
    paused: BTreeSet<(u16, String)>,
    checkpoints: BTreeMap<(String, i64, i64), Record>,
}

impl Reference {
    fn rebuild(events: &[FixtureEvent]) -> Self {
        Self::with_checkpoints(events, BTreeSet::new())
    }

    fn with_checkpoints(
        events: &[FixtureEvent],
        checkpoint_keys: BTreeSet<(String, i64, i64)>,
    ) -> Self {
        assert!(
            matches!(events.last(), Some(FixtureEvent::Advance { millis }) if *millis > 0),
            "end fixtures with an advance so no input lands at shutdown"
        );
        let mut model = Self {
            checkpoint_keys,
            ..Self::default()
        };
        let mut explicit_sessions = BTreeSet::new();
        let mut pending_resets = BTreeSet::new();
        let mut faulted = BTreeSet::new();
        for event in events {
            model.assert_no_deadline_tie(model.now_ms);
            match event {
                FixtureEvent::SourceAttached { .. } => {}
                FixtureEvent::SessionStart { connection } => {
                    explicit_sessions.insert(*connection);
                    pending_resets.remove(connection);
                    faulted.remove(connection);
                }
                FixtureEvent::ProtocolCommand {
                    connection,
                    command,
                } => match command {
                    ProtocolCommand::PrepareBooks { books } => {
                        model
                            .paused
                            .extend(books.iter().map(|book| (*connection, book.to_string())));
                    }
                    ProtocolCommand::RequestBooks { books } => {
                        for book in books {
                            model.paused.remove(&(*connection, book.to_string()));
                        }
                    }
                    ProtocolCommand::Subscribe { books, .. } => {
                        for market in books {
                            for book in &market.order_books {
                                model.paused.remove(&(*connection, book.to_string()));
                            }
                        }
                    }
                    _ => {}
                },
                FixtureEvent::BinaryFrame { .. }
                | FixtureEvent::PingFrame { .. }
                | FixtureEvent::PongFrame { .. } => {
                    model.frames.frames += 1;
                }
                FixtureEvent::Advance { millis } => model.advance(*millis),
                FixtureEvent::AddMarket {
                    generation: _,
                    market,
                    order_books,
                    connection,
                    standby,
                } => {
                    assert!(standby.is_none(), "reference reducer is single-source only");
                    model.counts.controls += 1;
                    model.markets.insert(
                        market.clone(),
                        Market {
                            connection: *connection,
                            books: order_books
                                .iter()
                                .map(|id| (id.clone(), Book::default()))
                                .collect(),
                        },
                    );
                }
                FixtureEvent::RemoveMarket { market } => {
                    model.counts.controls += 1;
                    if let Some(market) = model.markets.remove(market) {
                        for (id, book) in market.books {
                            if book.next_snapshot.is_some() {
                                model.records.entry(id).or_default().push(Record::Snapshot {
                                    bids: Vec::new(),
                                    asks: Vec::new(),
                                    received_us: book.received_us,
                                    exchange_us: book.exchange_us,
                                });
                                model.counts.removed += 1;
                            }
                        }
                    }
                }
                FixtureEvent::MarkUnsynced { connection, reason } => {
                    if *reason == engineering_challenge::model::UnsyncReason::DecodeError
                        && pending_resets.remove(connection)
                    {
                        continue;
                    }
                    model.mark_unsynced(*connection);
                }
                FixtureEvent::Frame {
                    connection,
                    received_at,
                    text,
                } => {
                    if faulted.contains(connection) {
                        continue;
                    }
                    let errors = model.frames.decode_errors;
                    model.frame(*connection, received_at.timestamp_micros(), text);
                    if explicit_sessions.contains(connection) && model.frames.decode_errors > errors
                    {
                        pending_resets.insert(*connection);
                        faulted.insert(*connection);
                    }
                }
            }
        }
        model.assert_no_deadline_tie(model.now_ms);
        model
    }

    fn assert_no_deadline_tie(&self, time: u64) {
        for market in self.markets.values() {
            for book in market.books.values() {
                assert_ne!(
                    book.next_snapshot,
                    Some(time),
                    "fixture input/shutdown on snapshot deadline {time}ms"
                );
            }
        }
    }

    fn advance(&mut self, millis: u64) {
        let end = self.now_ms.checked_add(millis).unwrap();
        // Check every cadence crossed, including skipped deadlines, before any
        // event at the endpoint. Ending exactly at a deadline is disallowed.
        for market in self.markets.values_mut() {
            for (id, book) in &mut market.books {
                while let Some(deadline) = book.next_snapshot {
                    assert_ne!(
                        deadline, end,
                        "fixture advance ends on snapshot deadline {end}ms"
                    );
                    if deadline > end {
                        break;
                    }
                    if book.synced {
                        self.records
                            .entry(id.clone())
                            .or_default()
                            .push(book.snapshot());
                        self.counts.periodic += 1;
                    } else {
                        self.counts.skipped += 1;
                    }
                    book.next_snapshot = Some(deadline + PERIOD_MS);
                }
            }
        }
        self.now_ms = end;
    }

    fn mark_unsynced(&mut self, connection: u16) {
        self.counts.marks += 1;
        for market in self
            .markets
            .values_mut()
            .filter(|m| m.connection == connection)
        {
            for book in market.books.values_mut() {
                self.counts.unsynced_books += u64::from(book.synced);
                book.synced = false;
            }
        }
    }

    fn frame(&mut self, connection: u16, received_us: i64, text: &str) {
        self.frames.frames += 1;
        if text == "PONG" {
            self.frames.pongs += 1;
            return;
        }
        let value: Value = serde_json::from_str(text).expect("fixtures contain valid JSON or PONG");
        let objects = match &value {
            Value::Array(array) => array.as_slice(),
            _ => std::slice::from_ref(&value),
        };
        for object in objects {
            match object["event_type"].as_str() {
                Some("book" | "price_change") => match parse_event(object) {
                    Ok(event) => self.apply(connection, received_us, event),
                    Err(()) => {
                        self.frames.decode_errors += 1;
                        self.mark_unsynced(connection);
                        break;
                    }
                },
                Some("tick_size_change") => {}
                _ => self.frames.ignored += 1,
            }
        }
    }

    fn apply(&mut self, connection: u16, received_us: i64, event: ParsedEvent) {
        let ParsedEvent {
            market,
            exchange_us,
            body,
        } = event;
        // Connection membership and market ownership are separate checks. A
        // symbol placed on this socket under another market passes the former.
        let assets: BTreeSet<String> = self
            .markets
            .values()
            .filter(|m| m.connection == connection)
            .flat_map(|m| m.books.keys().cloned())
            .filter(|id| !self.paused.contains(&(connection, id.clone())))
            .collect();
        let Some(held) = self
            .markets
            .get_mut(&market)
            .filter(|m| m.connection == connection)
        else {
            self.frames.unsubscribed += 1;
            return;
        };
        match body {
            Body::Book { asset, bids, asks } => {
                if !assets.contains(&asset) {
                    self.frames.unsubscribed += 1;
                    return;
                }
                self.frames.events += 1;
                let Some(book) = held.books.get_mut(&asset) else {
                    self.counts.dropped_unknown += 1;
                    return;
                };
                if book.synced && exchange_us < book.max_exchange_us {
                    self.counts.older_full_books += 1;
                    return;
                }
                book.max_exchange_us = if book.synced {
                    book.max_exchange_us.max(exchange_us)
                } else {
                    exchange_us
                };
                book.received_us = received_us;
                book.exchange_us = exchange_us;
                let output = self.records.entry(asset.clone()).or_default();
                if book.next_snapshot.is_none() {
                    book.bids = bids;
                    book.asks = asks;
                    output.push(book.snapshot());
                    book.next_snapshot = Some(self.now_ms + PERIOD_MS);
                    self.counts.first += 1;
                } else {
                    let before = output.len();
                    for (side, old, new) in [
                        (Side::Buy, &book.bids, &bids),
                        (Side::Sell, &book.asks, &asks),
                    ] {
                        let prices: BTreeSet<_> = old.keys().chain(new.keys()).copied().collect();
                        for price in prices {
                            let qty = *new.get(&price).unwrap_or(&0);
                            if old.get(&price).copied().unwrap_or(0) != qty {
                                output.push(Record::Update {
                                    side,
                                    price,
                                    qty,
                                    received_us,
                                    exchange_us,
                                });
                            }
                        }
                    }
                    let differences = (output.len() - before) as u64;
                    if book.synced && differences > 0 {
                        self.counts.mismatched_books += 1;
                        self.counts.mismatched_levels += differences;
                    } else {
                        self.counts.catch_up += differences;
                    }
                    book.bids = bids;
                    book.asks = asks;
                }
                book.synced = true;
                let key = (asset, received_us, exchange_us);
                if self.checkpoint_keys.contains(&key) {
                    self.checkpoints.insert(key, book.snapshot());
                }
            }
            Body::Changes(changes) => {
                let mut routed = false;
                for Change {
                    asset,
                    side,
                    price,
                    qty,
                } in changes
                {
                    if !assets.contains(&asset) {
                        self.frames.unsubscribed += 1;
                        continue;
                    }
                    routed = true;
                    let Some(book) = held.books.get_mut(&asset) else {
                        self.counts.dropped_unknown += 1;
                        continue;
                    };
                    if !book.synced {
                        self.counts.dropped_unsynced += 1;
                        continue;
                    }
                    let levels = match side {
                        Side::Buy => &mut book.bids,
                        Side::Sell => &mut book.asks,
                    };
                    if qty == 0 {
                        levels.remove(&price);
                    } else {
                        levels.insert(price, qty);
                    }
                    book.max_exchange_us = book.max_exchange_us.max(exchange_us);
                    book.received_us = received_us;
                    book.exchange_us = exchange_us;
                    self.records
                        .entry(asset.clone())
                        .or_default()
                        .push(Record::Update {
                            side,
                            price,
                            qty,
                            received_us,
                            exchange_us,
                        });
                    self.counts.updates += 1;
                    let key = (asset, received_us, exchange_us);
                    if self.checkpoint_keys.contains(&key) {
                        self.checkpoints.insert(key, book.snapshot());
                    }
                }
                self.frames.events += u64::from(routed);
            }
        }
    }

    fn check_totals(&self, totals: &ReplayTotals, workers: usize) {
        assert_eq!(totals.pipeline.workers.len(), workers);
        // Delivery latency is telemetry; the reference doesn't model it.
        let expected_frames = FrameTotals {
            exchange_to_receive: totals.frames.exchange_to_receive.clone(),
            negative_exchange_ages: totals.frames.negative_exchange_ages,
            ..self.frames.clone()
        };
        assert_eq!(totals.frames, expected_frames, "frame totals, N={workers}");
        let c = &self.counts;
        for (name, actual, expected) in [
            (
                "messages",
                sum(totals, |w| w.messages),
                c.controls + self.frames.events + c.marks * workers as u64,
            ),
            (
                "first snapshots",
                sum(totals, |w| w.first_snapshots),
                c.first,
            ),
            (
                "periodic snapshots",
                sum(totals, |w| w.periodic_snapshots),
                c.periodic,
            ),
            (
                "removal snapshots",
                sum(totals, |w| w.removal_snapshots),
                c.removed,
            ),
            (
                "skipped snapshots",
                sum(totals, |w| w.periodic_skipped),
                c.skipped,
            ),
            ("updates", sum(totals, |w| w.level_updates), c.updates),
            (
                "older full books",
                sum(totals, |w| w.older_full_books),
                c.older_full_books,
            ),
            (
                "catch-up levels",
                sum(totals, |w| w.catch_up_levels),
                c.catch_up,
            ),
            (
                "mismatched books",
                sum(totals, |w| w.mismatched_books),
                c.mismatched_books,
            ),
            (
                "mismatched levels",
                sum(totals, |w| w.mismatched_levels),
                c.mismatched_levels,
            ),
            (
                "unsynced books",
                sum(totals, |w| w.books_unsynced),
                c.unsynced_books,
            ),
            (
                "unsync marks",
                sum(totals, |w| w.unsync_marks),
                c.marks * workers as u64,
            ),
            (
                "dropped unsynced",
                sum(totals, |w| w.dropped_unsynced),
                c.dropped_unsynced,
            ),
            (
                "dropped unknown",
                sum(totals, |w| w.dropped_unknown),
                c.dropped_unknown,
            ),
            (
                "dropped other connection",
                sum(totals, |w| w.dropped_other_connection),
                0,
            ),
        ] {
            assert_eq!(actual, expected, "{name}, N={workers}");
        }
        assert_eq!(
            totals.pipeline.writer.snapshots,
            c.first + c.periodic + c.removed
        );
        assert_eq!(
            totals.pipeline.writer.level_updates,
            c.updates + c.catch_up + c.mismatched_levels
        );
    }
}

struct ParsedEvent {
    market: String,
    exchange_us: i64,
    body: Body,
}

enum Body {
    Book {
        asset: String,
        bids: Levels,
        asks: Levels,
    },
    Changes(Vec<Change>),
}

struct Change {
    asset: String,
    side: Side,
    price: i64,
    qty: i64,
}

fn string(value: &Value) -> Result<&str, ()> {
    value.as_str().ok_or(())
}

fn parse_event(value: &Value) -> Result<ParsedEvent, ()> {
    let market = string(&value["market"])?.to_owned();
    let millis: i64 = string(&value["timestamp"])?.parse().map_err(|_| ())?;
    if millis < 0 || DateTime::<Utc>::from_timestamp_millis(millis).is_none() {
        return Err(());
    }
    let exchange_us = millis.checked_mul(1000).ok_or(())?;
    let body = match string(&value["event_type"])? {
        "book" => Body::Book {
            asset: string(&value["asset_id"])?.to_owned(),
            bids: parse_levels(&value["bids"])?,
            asks: parse_levels(&value["asks"])?,
        },
        "price_change" => {
            let changes = value["price_changes"]
                .as_array()
                .ok_or(())?
                .iter()
                .map(|v| {
                    Ok(Change {
                        asset: string(&v["asset_id"])?.to_owned(),
                        side: match string(&v["side"])? {
                            "BUY" => Side::Buy,
                            "SELL" => Side::Sell,
                            _ => return Err(()),
                        },
                        price: fixed6(string(&v["price"])?)?,
                        qty: fixed6(string(&v["size"])?)?,
                    })
                })
                .collect::<Result<Vec<_>, ()>>()?;
            Body::Changes(changes)
        }
        _ => return Err(()),
    };
    Ok(ParsedEvent {
        market,
        exchange_us,
        body,
    })
}

fn parse_levels(value: &Value) -> Result<Levels, ()> {
    let mut levels = Levels::new();
    for level in value.as_array().ok_or(())? {
        let price = fixed6(string(&level["price"])?)?;
        let qty = fixed6(string(&level["size"])?)?;
        if qty == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
    }
    Ok(levels)
}

/// Independent integer conversion: concatenate significant decimal digits and
/// multiply once to reach six places; use i128 to detect i64 overflow.
pub fn fixed6(text: &str) -> Result<i64, ()> {
    let parts: Vec<_> = text.split('.').collect();
    if parts.is_empty()
        || parts.len() > 2
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(());
    }
    let fraction = parts.get(1).copied().unwrap_or("").trim_end_matches('0');
    if fraction.len() > 6 {
        return Err(());
    }
    let digits = format!("{}{fraction}", parts[0]);
    let scaled = digits
        .parse::<i128>()
        .map_err(|_| ())?
        .checked_mul(10_i128.pow((6 - fraction.len()) as u32))
        .ok_or(())?;
    i64::try_from(scaled).map_err(|_| ())
}

/// Compare live output against an independent raw-frame reducer, including
/// periodic snapshots whose exact source metadata is absent from output updates.
#[allow(dead_code)] // Used by the opt-in captured-feed integration test.
pub fn audit_live_sbe(events: &[FixtureEvent], path: &Path) {
    let (actual, _) = read_records(path);
    let keys = actual
        .iter()
        .flat_map(|(asset, records)| {
            records.iter().filter_map(|record| {
                if let Record::Snapshot {
                    received_us,
                    exchange_us,
                    ..
                } = record
                {
                    Some((asset.clone(), *received_us, *exchange_us))
                } else {
                    None
                }
            })
        })
        .collect();
    let reference = Reference::with_checkpoints(events, keys);
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        reference.records.keys().collect::<Vec<_>>()
    );
    let mut periodic = 0;
    for (asset, records) in &actual {
        let expected = &reference.records[asset];
        let mut next = 0;
        let mut held = Book::default();
        for record in records {
            if expected.get(next) == Some(record) {
                match record {
                    Record::Snapshot { bids, asks, .. } => {
                        held.bids = bids.iter().copied().collect();
                        held.asks = asks.iter().copied().collect();
                    }
                    Record::Update {
                        side, price, qty, ..
                    } => {
                        let levels = if *side == Side::Buy {
                            &mut held.bids
                        } else {
                            &mut held.asks
                        };
                        if *qty == 0 {
                            levels.remove(price);
                        } else {
                            levels.insert(*price, *qty);
                        }
                    }
                }
                next += 1;
            } else if let Record::Snapshot {
                bids,
                asks,
                received_us,
                exchange_us,
            } = record
            {
                assert_eq!(bids.iter().copied().collect::<Levels>(), held.bids);
                assert_eq!(asks.iter().copied().collect::<Levels>(), held.asks);
                assert_eq!(
                    reference
                        .checkpoints
                        .get(&(asset.clone(), *received_us, *exchange_us)),
                    Some(record)
                );
                periodic += 1;
            } else {
                panic!("live output differs from independent raw reference for {asset} at {next}");
            }
        }
        assert_eq!(
            next,
            expected.len(),
            "live output must contain every expected record"
        );
    }
    println!(
        "live_sbe_books={} periodic_snapshots={} independently_verified=true",
        actual.len(),
        periodic
    );
}
