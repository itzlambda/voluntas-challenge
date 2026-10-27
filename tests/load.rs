//! M1 capacity checks through the full replay and the pre-parsed worker/writer path.
//!
//! Run `LOAD_WORKERS=4 cargo test --release --test load -- --ignored --nocapture`.
//! LOAD_MARKETS=50000 and LOAD_EVENTS=2000000 are defaults; stream messages are
//! additional to the initial books. Frames are generated before timing. Replay
//! includes setup, draining and padded file flush, but not fsync or verification.
//! Replay parses connections serially: this is offline capacity, not concurrent
//! live-connection throughput. `synthetic_load_preparsed_full` decodes before timing,
//! restores frame order and feeds worker buckets concurrently. Its timer starts
//! before the first send and ends after drain and final flush (excluding fsync).
//! LOAD_WRITER_QUEUE overrides the production queue bound in either variant.
//! LOAD_DECODE_THREADS defaults to the available core count; per-thread decode
//! rates include decoding/allocation and frame destruction, but exclude generation,
//! partitioning, merging and worker bucketing. MB means decimal megabytes.
//! Advance events really sleep. The default 10 ms keeps smoke tests short;
//! LOAD_ADVANCE_MS=61000 also checks periodic snapshots
//! without changing the fixed 60 s source interval.

use engineering_challenge::core::metrics::{Component, Metrics};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::core::config::{Config, OUTPUT_CHUNK_BYTES};
use engineering_challenge::core::connection::{ConnectionIds, FrameTotals};
use engineering_challenge::core::fixture::FixtureEvent;
use engineering_challenge::core::pipeline::{Pipeline, PipelineTotals, ReplayTotals, replay};
use engineering_challenge::core::routing::worker_index;
use engineering_challenge::core::worker::Worker;
use engineering_challenge::core::writer::{WriterTotals, spawn_writer};
use engineering_challenge::exchange::polymarket::decode::decode_frame;
use engineering_challenge::model::{
    ConnectionId, MarketData, MarketId, MarketSources, OrderBookId, OutputRecord, WorkerMessage,
};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use tokio::sync::{mpsc, oneshot};

const SEED: u64 = 0xdec0_de01_5eed_1234;
const EPOCH_MS: i64 = 1_792_777_248_021;
const MARKETS_PER_CONNECTION: usize = 100;
const CENT_TO_FIXED: i64 = 10_000;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // SplitMix64 avoids correlated low bits in routing and hot-market draws.
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

// Independent integer-cent model, without production book/parser/diff helpers.
#[derive(Debug, Default, PartialEq, Eq)]
struct Book {
    sides: [BTreeMap<i64, i64>; 2],
}

impl Book {
    fn populated(rng: &mut Rng) -> Self {
        let mut book = Self::default();
        for side in 0..2 {
            for level in 0..10 + rng.below(31) {
                let tick = if side == 0 { 49 - level } else { 51 + level };
                book.sides[side].insert(tick as i64, 1 + rng.below(20_000_000) as i64);
            }
        }
        book
    }

    fn set(&mut self, side: usize, tick: i64, cents: i64) {
        if cents == 0 {
            self.sides[side].remove(&tick);
        } else {
            self.sides[side].insert(tick, cents);
        }
    }

    fn mutate(&mut self, rng: &mut Rng) -> (usize, i64, i64) {
        let side = rng.below(2);
        let level = rng.below(40) as i64;
        let tick = if side == 0 { 49 - level } else { 51 + level };
        let old = self.sides[side].get(&tick).copied().unwrap_or(0);
        let cents = if old > 0 && rng.below(5) == 0 {
            0
        } else {
            let candidate = 1 + rng.below(20_000_000) as i64;
            candidate + i64::from(candidate == old)
        };
        self.set(side, tick, cents);
        (side, tick, cents)
    }
}

struct Market {
    condition: String,
    symbols: [String; 2],
    books: [Book; 2],
}

#[derive(Default)]
struct Counts {
    frames: u64,
    events: u64,
    input_bytes: u64,
    price_entries: u64,
    identical_books: u64,
    changed_books: u64,
}

struct Scenario {
    fixtures: Vec<FixtureEvent>,
    markets: Vec<Market>,
    counts: Counts,
}

fn decimal(cents: i64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

fn book_json(market: &Market, outcome: usize, timestamp: i64) -> String {
    let mut text = format!(
        r#"{{"event_type":"book","market":"{}","asset_id":"{}","timestamp":"{timestamp}","hash":"{timestamp:040x}","bids":["#,
        market.condition, market.symbols[outcome]
    );
    for (side, name) in [(0, "bids"), (1, "asks")] {
        if side == 1 {
            write!(text, r#"],"{name}":["#).unwrap();
        }
        for (index, (&tick, &cents)) in market.books[outcome].sides[side].iter().enumerate() {
            if index > 0 {
                text.push(',');
            }
            write!(
                text,
                r#"{{"price":"{}","size":"{}"}}"#,
                decimal(tick),
                decimal(cents)
            )
            .unwrap();
        }
    }
    text.push_str("]}");
    text
}

impl Scenario {
    fn generate(market_count: usize, stream_events: usize, advance_ms: u64) -> Self {
        assert!(market_count > 0);
        assert!(market_count.div_ceil(MARKETS_PER_CONNECTION) <= usize::from(u16::MAX) + 1);
        let mut rng = Rng(SEED);
        let mut scenario = Self {
            fixtures: Vec::with_capacity(market_count + stream_events + 1_000),
            markets: (0..market_count)
                .map(|index| Market {
                    // Index ensures uniqueness; low hex digit is uniformly random.
                    condition: format!("0x{:047x}{index:016x}{:x}", rng.next(), rng.below(16)),
                    symbols: std::array::from_fn(|outcome| {
                        format!("7{:076}", 10_u128.pow(38) + (2 * index + outcome) as u128)
                    }),
                    books: std::array::from_fn(|_| Book::populated(&mut rng)),
                })
                .collect(),
            counts: Counts::default(),
        };
        for (index, market) in scenario.markets.iter().enumerate() {
            assert_eq!(market.condition.len(), 66);
            assert!(market.symbols.iter().all(|s| s.len() == 77));
            scenario.fixtures.push(FixtureEvent::AddMarket {
                generation: 0,
                market: market.condition.clone(),
                order_books: market.symbols.to_vec(),
                connection: (index / MARKETS_PER_CONNECTION) as u16,
                standby: None,
            });
        }
        // One array per connection: both outcomes, no pacing, before the stream.
        for start in (0..market_count).step_by(MARKETS_PER_CONNECTION) {
            let end = (start + MARKETS_PER_CONNECTION).min(market_count);
            let mut text = String::from("[");
            for index in start..end {
                for outcome in 0..2 {
                    if text.len() > 1 {
                        text.push(',');
                    }
                    text.push_str(&book_json(&scenario.markets[index], outcome, EPOCH_MS));
                }
            }
            text.push(']');
            scenario.push_frame(start, EPOCH_MS, text, 2 * (end - start));
        }
        let hot_markets = market_count.div_ceil(20);
        for sequence in 0..stream_events {
            // 80% of traffic to 5% of markets, spread across connections.
            let index = if rng.below(5) != 0 {
                (rng.below(hot_markets) * 20).min(market_count - 1)
            } else {
                rng.below(market_count)
            };
            // Nominal 50k messages/s timestamps; offered load is unpaced.
            let timestamp = EPOCH_MS + 1 + (sequence / 50) as i64;
            let market = &mut scenario.markets[index];
            let text = if rng.below(5) == 0 {
                let outcome = rng.below(2);
                if rng.below(4) == 0 {
                    market.books[outcome].mutate(&mut rng);
                    scenario.counts.changed_books += 1;
                } else {
                    scenario.counts.identical_books += 1;
                }
                book_json(market, outcome, timestamp)
            } else {
                let entries = if rng.below(5) == 0 { 1 } else { 2 };
                let mut text = format!(
                    r#"{{"event_type":"price_change","market":"{}","timestamp":"{timestamp}","price_changes":["#,
                    market.condition
                );
                let first = rng.below(2);
                for entry in 0..entries {
                    let outcome = (first + entry) % 2;
                    let book = &mut market.books[outcome];
                    let (side, tick, cents) = book.mutate(&mut rng);
                    if entry > 0 {
                        text.push(',');
                    }
                    let bid = book.sides[0].last_key_value().map_or(0, |(&p, _)| p);
                    let ask = book.sides[1].first_key_value().map_or(100, |(&p, _)| p);
                    write!(text,
                        r#"{{"asset_id":"{}","price":"{}","size":"{}","side":"{}","hash":"{timestamp:040x}","best_bid":"{}","best_ask":"{}"}}"#,
                        market.symbols[outcome], decimal(tick), decimal(cents),
                        if side == 0 { "BUY" } else { "SELL" }, decimal(bid), decimal(ask)
                    ).unwrap();
                }
                text.push_str("]}");
                scenario.counts.price_entries += entries as u64;
                text
            };
            scenario.push_frame(index, timestamp, text, 1);
        }
        scenario
            .fixtures
            .push(FixtureEvent::Advance { millis: advance_ms });
        scenario
    }

    fn push_frame(&mut self, market: usize, timestamp: i64, text: String, events: usize) {
        self.counts.frames += 1;
        self.counts.events += events as u64;
        self.counts.input_bytes += text.len() as u64;
        self.fixtures.push(FixtureEvent::Frame {
            connection: (market / MARKETS_PER_CONNECTION) as u16,
            received_at: DateTime::<Utc>::from_timestamp_millis(timestamp).unwrap()
                + chrono::Duration::milliseconds(2),
            text,
        });
    }
}

fn insert_fixed(book: &mut Book, side: usize, price: i64, qty: i64) {
    assert_eq!(price % CENT_TO_FIXED, 0);
    assert_eq!(qty % CENT_TO_FIXED, 0);
    assert!((0..=1_000_000).contains(&price));
    assert!(qty >= 0);
    book.set(side, price / CENT_TO_FIXED, qty / CENT_TO_FIXED);
}

fn verify_file(path: &Path, expected: &[Market], totals: &PipelineTotals) -> anyhow::Result<()> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut rebuilt: HashMap<String, Book> = HashMap::with_capacity(2 * expected.len());
    let (mut snapshots, mut updates, mut bytes) = (0_u64, 0_u64, 0_u64);
    while let Some(message) = reader.read_msg()? {
        bytes += message.encoded_length_with_header()? as u64;
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoder)?;
                let mut book = Book::default();
                for level in snapshot.bids {
                    insert_fixed(&mut book, 0, level.price_level, level.quantity);
                }
                for level in snapshot.asks {
                    insert_fixed(&mut book, 1, level.price_level, level.quantity);
                }
                rebuilt.insert(snapshot.symbol, book);
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                let book = rebuilt
                    .get_mut(&update.symbol)
                    .expect("update before first snapshot");
                let side = match update.side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                };
                insert_fixed(book, side, update.price_level, update.quantity);
                updates += 1;
            }
            _ => panic!("unexpected SBE message"),
        }
    }
    assert_eq!(snapshots, totals.writer.snapshots);
    assert_eq!(updates, totals.writer.level_updates);
    assert_eq!(bytes, totals.writer.bytes);
    assert_eq!(rebuilt.len(), 2 * expected.len());
    for market in expected {
        for outcome in 0..2 {
            assert_eq!(
                rebuilt.get(&market.symbols[outcome]),
                Some(&market.books[outcome]),
                "final state for {}",
                market.symbols[outcome]
            );
        }
    }
    Ok(())
}

fn env_number(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| {
        value.parse().expect("invalid load-test number")
    })
}

#[derive(Clone, Copy)]
enum LoadPath {
    Replay,
    Preparsed,
}

struct InputFrame {
    sequence: usize,
    received_at: DateTime<Utc>,
    text: String,
}

#[derive(Default)]
struct ConnectionFrames {
    ids: ConnectionIds,
    frames: Vec<InputFrame>,
}

struct Prepared {
    buckets: Vec<Vec<WorkerMessage>>,
    frames: FrameTotals,
}

fn predecode(fixtures: Vec<FixtureEvent>, config: &Config, connections: usize) -> Prepared {
    let mut inputs: Vec<ConnectionFrames> = (0..connections).map(|_| Default::default()).collect();
    let mut buckets: Vec<Vec<WorkerMessage>> = (0..config.workers).map(|_| Vec::new()).collect();
    let mut saw_frame = false;
    for (sequence, fixture) in fixtures.into_iter().enumerate() {
        match fixture {
            FixtureEvent::AddMarket {
                generation: 0,
                market,
                order_books,
                connection,
                standby,
            } => {
                assert!(standby.is_none(), "isolated load replay is single-source");
                assert!(!saw_frame, "generator controls must precede data");
                let market = MarketId::from(market.as_str());
                let order_books: Vec<_> = order_books
                    .iter()
                    .map(|id| OrderBookId::from(id.as_str()))
                    .collect();
                inputs[usize::from(connection)]
                    .ids
                    .add(&market, &order_books);
                buckets[worker_index(&market, config.workers)].push(WorkerMessage::AddMarket {
                    generation: 0,
                    market,
                    order_books,
                    sources: MarketSources::single(ConnectionId(connection)),
                });
            }
            FixtureEvent::Frame {
                connection,
                received_at,
                text,
            } => {
                saw_frame = true;
                inputs[usize::from(connection)].frames.push(InputFrame {
                    sequence,
                    received_at,
                    text,
                });
            }
            FixtureEvent::Advance { .. } => {}
            _ => panic!("unexpected control in synthetic workload"),
        }
    }
    let threads = env_number(
        "LOAD_DECODE_THREADS",
        thread::available_parallelism().unwrap().get(),
    )
    .clamp(1, connections);
    let chunk_size = connections.div_ceil(threads);
    let start = Instant::now();
    let decoded = thread::scope(|scope| {
        let tasks: Vec<_> = inputs
            .chunks_mut(chunk_size)
            .enumerate()
            .map(|(index, chunk)| {
                scope.spawn(move || {
                    let start = Instant::now();
                    let (mut bytes, mut events) = (0_u64, 0_u64);
                    let mut decoded: Vec<(usize, Vec<MarketData>)> = Vec::new();
                    for (offset, input) in chunk.iter_mut().enumerate() {
                        let connection = ConnectionId((index * chunk_size + offset) as u16);
                        // Each connection is decoded sequentially by exactly one thread.
                        for frame in std::mem::take(&mut input.frames) {
                            let result = decode_frame(
                                &frame.text,
                                connection,
                                frame.received_at,
                                &input.ids,
                                config.max_frame_bytes,
                            );
                            assert!(result.error.is_none(), "decode error: {:?}", result.error);
                            assert_eq!(
                                result.unsubscribed + result.ignored + result.tick_size_changes,
                                0
                            );
                            assert!(!result.events.is_empty());
                            bytes += frame.text.len() as u64;
                            events += result.events.len() as u64;
                            decoded.push((frame.sequence, result.events));
                        }
                    }
                    (decoded, bytes, events, start.elapsed())
                })
            })
            .collect();
        tasks
            .into_iter()
            .map(|task| task.join().expect("decode thread panicked"))
            .collect::<Vec<_>>()
    });
    let wall = start.elapsed();
    let mut frames = FrameTotals::default();
    let mut bytes = 0_u64;
    for (index, (decoded, input_bytes, events, elapsed)) in decoded.iter().enumerate() {
        frames.frames += decoded.len() as u64;
        frames.events += events;
        bytes += input_bytes;
        println!(
            "DECODE thread={index} frames={} events={events} input_bytes={input_bytes} wall_s={:.3} MB_per_s={:.2} events_per_s={:.0}",
            decoded.len(),
            elapsed.as_secs_f64(),
            *input_bytes as f64 / elapsed.as_secs_f64() / 1_000_000.0,
            *events as f64 / elapsed.as_secs_f64()
        );
    }
    println!(
        "DECODE threads={} frames={} events={} input_bytes={bytes} wall_s={:.3} MB_per_s={:.2} events_per_s={:.0}",
        decoded.len(),
        frames.frames,
        frames.events,
        wall.as_secs_f64(),
        bytes as f64 / wall.as_secs_f64() / 1_000_000.0,
        frames.events as f64 / wall.as_secs_f64()
    );
    let bucket_start = Instant::now();
    let mut ordered: Vec<_> = decoded
        .into_iter()
        .flat_map(|(frames, _, _, _)| frames)
        .collect();
    ordered.sort_unstable_by_key(|(sequence, _)| *sequence);
    for (_, events) in ordered {
        for event in events {
            buckets[worker_index(&event.market, config.workers)]
                .push(WorkerMessage::MarketData(event));
        }
    }
    println!(
        "PREPARE bucket_s={:.3} worker_messages={:?}",
        bucket_start.elapsed().as_secs_f64(),
        buckets.iter().map(Vec::len).collect::<Vec<_>>()
    );
    Prepared { buckets, frames }
}

async fn feed_preparsed(
    config: &Config,
    prepared: Prepared,
    advance_ms: u64,
) -> anyhow::Result<(ReplayTotals, Duration)> {
    let pipeline = Pipeline::start(config)?;
    let start = Instant::now();
    let feeders: Vec<_> = prepared
        .buckets
        .into_iter()
        .map(|bucket| {
            let workers = pipeline.workers().clone();
            tokio::spawn(async move {
                for message in bucket {
                    workers.send(message).await?;
                }
                Ok::<_, anyhow::Error>(())
            })
        })
        .collect();
    let mut failure = None;
    for feeder in feeders {
        match feeder.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failure = Some(error),
            Err(error) => failure = Some(error.into()),
        }
    }
    tokio::time::sleep(Duration::from_millis(advance_ms)).await;
    let finished = pipeline.finish().await;
    if let Some(error) = failure {
        return Err(finished.err().unwrap_or(error));
    }
    let totals = ReplayTotals {
        pipeline: finished?,
        frames: prepared.frames,
    };
    Ok((totals, start.elapsed()))
}

fn run_load(
    markets: usize,
    stream_events: usize,
    workers: usize,
    advance_ms: u64,
    path: LoadPath,
) -> anyhow::Result<()> {
    let total_start = Instant::now();
    let scenario = Scenario::generate(markets, stream_events, advance_ms);
    let generation = total_start.elapsed();
    let temp = tempfile::tempdir()?;
    let config = Config {
        workers,
        writer_queue: env_number("LOAD_WRITER_QUEUE", Config::default().writer_queue),
        output: temp.path().join("books.sbe"),
        decode_error_log: temp.path().join("decode-errors.jsonl"),
        ..Config::default()
    };
    config.validate()?;
    let mut fixtures = Some(scenario.fixtures);
    let (variant, runtime_threads, prepared) = match path {
        LoadPath::Replay => ("replay", 4, None),
        LoadPath::Preparsed => (
            "preparsed",
            thread::available_parallelism()?.get(),
            Some(predecode(
                fixtures.take().unwrap(),
                &config,
                markets.div_ceil(MARKETS_PER_CONNECTION),
            )),
        ),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_threads)
        .enable_time()
        .build()?;
    let (totals, wall) = if let Some(prepared) = prepared {
        runtime.block_on(feed_preparsed(&config, prepared, advance_ms))?
    } else {
        let start = Instant::now();
        let totals = runtime.block_on(replay(
            &config,
            fixtures.take().unwrap(),
            engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::default,
        ))?;
        (totals, start.elapsed())
    };
    assert_eq!(totals.frames.frames, scenario.counts.frames);
    assert_eq!(totals.frames.events, scenario.counts.events);
    assert_eq!(totals.frames.decode_errors, 0);
    assert_eq!(totals.frames.unsubscribed + totals.frames.ignored, 0);
    if matches!(path, LoadPath::Replay) {
        assert_eq!(std::fs::metadata(&config.decode_error_log)?.len(), 0);
    }
    let mut worker_snapshots = 0;
    let mut worker_updates = 0;
    let mut periodic = 0;
    let mut blocked = Duration::ZERO;
    for worker in &totals.pipeline.workers {
        worker_snapshots +=
            worker.first_snapshots + worker.periodic_snapshots + worker.removal_snapshots;
        periodic += worker.periodic_snapshots;
        worker_updates += worker.level_updates + worker.mismatched_levels + worker.catch_up_levels;
        blocked += worker.writer_blocked;
        assert_eq!(
            worker.dropped_unknown + worker.dropped_other_connection + worker.dropped_unsynced,
            0
        );
        assert_eq!(worker.periodic_skipped + worker.catch_up_levels, 0);
    }
    assert_eq!(worker_snapshots - periodic, 2 * markets as u64);
    assert_eq!(totals.pipeline.writer.snapshots, worker_snapshots);
    assert_eq!(totals.pipeline.writer.level_updates, worker_updates);
    assert_eq!(
        worker_updates,
        scenario.counts.price_entries + scenario.counts.changed_books
    );
    assert_eq!(
        totals
            .pipeline
            .workers
            .iter()
            .map(|w| w.messages)
            .sum::<u64>(),
        markets as u64 + scenario.counts.events
    );
    assert_eq!(
        totals
            .pipeline
            .workers
            .iter()
            .map(|w| w.mismatched_books)
            .sum::<u64>(),
        scenario.counts.changed_books
    );
    if advance_ms >= 61_000 {
        assert!(
            periodic >= 2 * markets as u64,
            "periodic timers did not fire"
        );
    }
    let file_bytes = std::fs::metadata(&config.output)?.len();
    assert_eq!(file_bytes % OUTPUT_CHUNK_BYTES as u64, 0);
    assert!(file_bytes >= totals.pipeline.writer.bytes);
    let validation_start = Instant::now();
    verify_file(&config.output, &scenario.markets, &totals.pipeline)?;
    let validation = validation_start.elapsed();
    let writer = &totals.pipeline.writer;
    println!(
        "LOAD variant={variant} markets={markets} books={} connections={} stream_events={stream_events} workers={workers} runtime_threads={runtime_threads} writer_queue={} seed={SEED:x} \
         frames={} events={} input_bytes={} identical_books={} changed_books={} snapshots={} periodic={} level_updates={} \
         replay_s={:.3} events_per_s={:.0} records_per_s={:.0} output_MB_per_s={:.2} writer_bytes={} file_bytes={file_bytes} \
         writer_blocked_s={:.6} writer_blocked_per_worker_s={:?} worker_messages={:?} \
         writer_channel_full={} generation_s={:.3} validation_s={:.3} total_wall_s={:.3}",
        2 * markets,
        markets.div_ceil(MARKETS_PER_CONNECTION),
        config.writer_queue,
        scenario.counts.frames,
        scenario.counts.events,
        scenario.counts.input_bytes,
        scenario.counts.identical_books,
        scenario.counts.changed_books,
        writer.snapshots,
        periodic,
        writer.level_updates,
        wall.as_secs_f64(),
        scenario.counts.events as f64 / wall.as_secs_f64(),
        (writer.snapshots + writer.level_updates) as f64 / wall.as_secs_f64(),
        writer.bytes as f64 / wall.as_secs_f64() / 1_000_000.0,
        writer.bytes,
        blocked.as_secs_f64(),
        totals
            .pipeline
            .workers
            .iter()
            .map(|w| w.writer_blocked.as_secs_f64())
            .collect::<Vec<_>>(),
        totals
            .pipeline
            .workers
            .iter()
            .map(|w| w.messages)
            .collect::<Vec<_>>(),
        blocked > Duration::ZERO,
        generation.as_secs_f64(),
        validation.as_secs_f64(),
        total_start.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Where worker output goes in the scaling test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sink {
    /// Records are received and dropped: measures workers alone.
    Discard,
    /// The real writer thread and SBE file.
    Writer,
}

/// Per-worker message lists, routed for `workers`, from messages in the
/// order a single worker would see them.
fn bucket(messages: &[WorkerMessage], workers: usize) -> Vec<Vec<WorkerMessage>> {
    let mut buckets: Vec<Vec<WorkerMessage>> = (0..workers).map(|_| Vec::new()).collect();
    for message in messages {
        let market = match message {
            WorkerMessage::AddMarket { market, .. } => market,
            WorkerMessage::MarketData(data) => &data.market,
            _ => panic!("unexpected control in synthetic workload"),
        };
        buckets[worker_index(market, workers)].push(message.clone());
    }
    buckets
}

/// Starts the sink thread and returns its totals when the channel closes.
fn spawn_sink(
    sink: Sink,
    config: &Config,
    records: mpsc::Receiver<Vec<OutputRecord>>,
) -> anyhow::Result<oneshot::Receiver<anyhow::Result<WriterTotals>>> {
    match sink {
        Sink::Writer => spawn_writer(
            config.output.clone(),
            records,
            Metrics::default().register(Component::Writer),
        ),
        Sink::Discard => {
            let (done_tx, done_rx) = oneshot::channel();
            let mut records = records;
            thread::spawn(move || {
                let mut totals = WriterTotals::default();
                while let Some(batch) = records.blocking_recv() {
                    for record in batch {
                        match record {
                            OutputRecord::Snapshot { .. } => totals.snapshots += 1,
                            OutputRecord::LevelUpdate { .. } => totals.level_updates += 1,
                        }
                    }
                }
                let _ = done_tx.send(Ok(totals));
            });
            Ok(done_rx)
        }
    }
}

struct ScalingResult {
    wall: Duration,
    workers_wall: Duration,
    per_worker: Vec<(u64, Duration)>,
    totals: PipelineTotals,
}

/// Runs `workers` workers over input queues that are completely filled
/// before the workers start, so nothing else competes while they run and
/// each worker processes as fast as it can until its queue is empty.
fn run_scaling_once(
    messages: &[WorkerMessage],
    config: &Config,
    sink: Sink,
) -> anyhow::Result<ScalingResult> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(thread::available_parallelism()?.get())
        .enable_time()
        .build()?;
    let mut inputs = Vec::new();
    for messages in bucket(messages, config.workers) {
        let (tx, rx) = mpsc::channel(messages.len().max(1));
        for message in messages {
            tx.try_send(message).expect("queue sized to fit the bucket");
        }
        inputs.push(rx);
    }
    let (records_tx, records_rx) = mpsc::channel(config.writer_queue);
    let sink_done = spawn_sink(sink, config, records_rx)?;
    runtime.block_on(async move {
        let start = Instant::now();
        let tasks: Vec<_> = inputs
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                let worker = Worker::new(index).run(
                    input,
                    records_tx.clone(),
                    config.writer_stall_timeout,
                    Metrics::default().register(Component::Worker(index)),
                );
                tokio::spawn(async move {
                    let started = Instant::now();
                    let totals = worker.await;
                    (totals, started.elapsed())
                })
            })
            .collect();
        drop(records_tx);
        let mut totals = PipelineTotals::default();
        let mut per_worker = Vec::new();
        for task in tasks {
            let (worker, elapsed) = task.await?;
            let worker = worker?;
            per_worker.push((worker.messages, elapsed));
            totals.workers.push(worker);
        }
        let workers_wall = start.elapsed();
        totals.writer = sink_done.await??;
        Ok(ScalingResult {
            wall: start.elapsed(),
            workers_wall,
            per_worker,
            totals,
        })
    })
}

/// Generates and decodes the workload once, then measures 1, 2 and 4
/// workers with both sinks. Total input is the same for every run.
fn run_scaling(
    markets: usize,
    stream_events: usize,
    worker_counts: &[usize],
) -> anyhow::Result<()> {
    let scenario = Scenario::generate(markets, stream_events, 10);
    let decode_config = Config {
        workers: 1,
        ..Config::default()
    };
    let mut prepared = predecode(
        scenario.fixtures,
        &decode_config,
        markets.div_ceil(MARKETS_PER_CONNECTION),
    );
    let messages = prepared.buckets.pop().expect("one bucket");
    let events = scenario.counts.events;
    let mut baseline: HashMap<&str, f64> = HashMap::new();
    // LOAD_SCALING_SINK=discard|writer and LOAD_SCALING_WORKERS=N run one case.
    let only_sink = std::env::var("LOAD_SCALING_SINK").ok();
    let only_workers = std::env::var("LOAD_SCALING_WORKERS").ok();
    for sink in [Sink::Discard, Sink::Writer] {
        let sink_name = match sink {
            Sink::Discard => "discard",
            Sink::Writer => "writer",
        };
        if only_sink.as_deref().is_some_and(|only| only != sink_name) {
            continue;
        }
        for &workers in worker_counts {
            if only_workers
                .as_deref()
                .is_some_and(|only| only != workers.to_string())
            {
                continue;
            }
            let temp = tempfile::tempdir()?;
            let config = Config {
                workers,
                writer_queue: env_number("LOAD_WRITER_QUEUE", Config::default().writer_queue),
                output: temp.path().join("books.sbe"),
                decode_error_log: temp.path().join("decode-errors.jsonl"),
                ..Config::default()
            };
            config.validate()?;
            let result = run_scaling_once(&messages, &config, sink)?;

            let totals = &result.totals;
            let worker_records: u64 = totals
                .workers
                .iter()
                .map(|w| {
                    w.first_snapshots
                        + w.periodic_snapshots
                        + w.removal_snapshots
                        + w.level_updates
                        + w.mismatched_levels
                })
                .sum();
            let sink_records = totals.writer.snapshots + totals.writer.level_updates;
            assert_eq!(worker_records, sink_records);
            assert_eq!(
                totals.workers.iter().map(|w| w.messages).sum::<u64>(),
                markets as u64 + events
            );
            for worker in &totals.workers {
                assert_eq!(
                    worker.dropped_unknown
                        + worker.dropped_other_connection
                        + worker.dropped_unsynced,
                    0
                );
            }
            if sink == Sink::Writer {
                verify_file(&config.output, &scenario.markets, totals)?;
            }

            // Until the sink has received (and, for the writer, written) everything.
            let events_per_s = events as f64 / result.wall.as_secs_f64();
            let one = *baseline
                .entry(sink_name)
                .or_insert(events_per_s / workers as f64);
            let blocked: Vec<f64> = totals
                .workers
                .iter()
                .map(|w| w.writer_blocked.as_secs_f64())
                .collect();
            let per_worker_rate: Vec<String> = result
                .per_worker
                .iter()
                .map(|(messages, elapsed)| {
                    format!("{:.0}", *messages as f64 / elapsed.as_secs_f64())
                })
                .collect();
            println!(
                "SCALING sink={sink_name} workers={workers} events={events} records={sink_records} \
                 workers_wall_s={:.3} total_wall_s={:.3} events_per_s={events_per_s:.0} \
                 records_per_s={:.0} speedup_vs_1={:.2} efficiency={:.0}% \
                 per_worker_events_per_s=[{}] per_worker_messages={:?} writer_blocked_s={blocked:.3?} writer_bytes={}",
                result.workers_wall.as_secs_f64(),
                result.wall.as_secs_f64(),
                sink_records as f64 / result.wall.as_secs_f64(),
                events_per_s / one,
                100.0 * events_per_s / (one * workers as f64),
                per_worker_rate.join(", "),
                result
                    .per_worker
                    .iter()
                    .map(|(m, _)| *m)
                    .collect::<Vec<_>>(),
                totals.writer.bytes,
            );
        }
    }
    Ok(())
}

/// Keeps every load path working at a small size with more than one worker.
/// Output correctness across worker counts is covered by `tests/pipeline.rs`;
/// the full-size variants below are opt-in.
#[test]
fn load_paths_smoke() -> anyhow::Result<()> {
    run_load(400, 1_000, 2, 10, LoadPath::Replay)?;
    run_load(400, 1_000, 2, 10, LoadPath::Preparsed)?;
    run_scaling(400, 2_000, &[2])
}

#[test]
#[ignore = "full synthetic load; run in release with LOAD_WORKERS=1, 2, 4"]
fn synthetic_load_full() -> anyhow::Result<()> {
    run_load(
        env_number("LOAD_MARKETS", 50_000),
        env_number("LOAD_EVENTS", 2_000_000),
        env_number("LOAD_WORKERS", 4),
        env_number("LOAD_ADVANCE_MS", 10) as u64,
        LoadPath::Replay,
    )
}

#[test]
#[ignore = "pre-parsed worker/writer capacity; run in release with LOAD_WORKERS=1, 2, 4, 8"]
fn synthetic_load_preparsed_full() -> anyhow::Result<()> {
    run_load(
        env_number("LOAD_MARKETS", 50_000),
        env_number("LOAD_EVENTS", 2_000_000),
        env_number("LOAD_WORKERS", 4),
        env_number("LOAD_ADVANCE_MS", 10) as u64,
        LoadPath::Preparsed,
    )
}

#[test]
#[ignore = "worker scaling at full scale; run in release with --nocapture"]
fn worker_scaling_full() -> anyhow::Result<()> {
    run_scaling(
        env_number("LOAD_MARKETS", 50_000),
        env_number("LOAD_EVENTS", 2_000_000),
        &[1, 2, 4],
    )
}
