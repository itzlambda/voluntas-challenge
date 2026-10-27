//! Base collector against local Gamma and Polymarket protocol servers.
//! The binary test owns SIGINT; paused tests drive the same production tasks.

use engineering_challenge::core::metrics::{Component, Metrics};
#[path = "support/mock_exchange.rs"]
mod mock_exchange;

use std::fs::File;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use engineering_challenge::Side;
use engineering_challenge::core::book_connection::{
    BookConnectionContext, SILENCE_LIMIT, run_book_connection,
};
use engineering_challenge::core::config::Config;
use engineering_challenge::core::decode_log::DecodeErrorLog;
use engineering_challenge::core::discovery::{Discovery, DiscoveryTotals};
use engineering_challenge::core::fixture::{FixtureEvent, parse_jsonl};
use engineering_challenge::core::pipeline::{Pipeline, PipelineTotals};
use engineering_challenge::core::rate_limit::RateLimiter;
use engineering_challenge::core::subscriptions::{
    ManagerTotals, SpawnConnection, SubscriptionManager,
};
use engineering_challenge::exchange::polymarket::gamma::GammaClient;
use engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol;
use engineering_challenge::exchange::protocol::{
    BookProtocol, BookRequestPreparation, DecodeContext, DecodedFrame,
};
use engineering_challenge::model::{MarketBooks, OrderBookId};
use mock_exchange::{
    BASE_MS, Book, Event, MARKET_A, MARKET_B, MARKET_C, MockExchange, Oracle, STEP, change_frame,
    hint, listing, pump, read_sbe, scratch, until, within,
};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{Message, handshake::client::Request};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

fn initial_book() -> Book {
    Book::new(
        &[(220_000, 10_000_001), (400_000, 4_000_000)],
        &[(600_000, 5_000_000), (800_000, 7_000_000)],
    )
}

// A source selection replaces the reader's baseline without a catch-up diff.
fn replacement_full(
    oracle: &mut Oracle,
    market: &str,
    asset: &str,
    book: Book,
    millis: i64,
) -> Value {
    oracle.books.remove(asset);
    oracle.full(market, asset, book, millis)
}

async fn assert_pair_subscription(
    exchange: &mut MockExchange,
    operation: &str,
    assets: &[&str],
    expected: [usize; 2],
) {
    let mut actual = [
        exchange.subscription(operation, assets).await,
        exchange.subscription(operation, assets).await,
    ];
    let mut expected = expected;
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected, "both fixed peers receive {operation}");
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn telemetry(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

async fn settle() {
    for _ in 0..40 {
        pump().await;
    }
}

#[tokio::test]
async fn binary_discovers_reconnects_reconciles_and_drains_on_sigint() {
    binary_lifecycle("-INT").await;
}

#[tokio::test]
async fn binary_discovers_reconnects_reconciles_and_drains_on_sigterm() {
    binary_lifecycle("-TERM").await;
}

async fn binary_lifecycle(shutdown_signal: &str) {
    let mut exchange = MockExchange::start(vec![listing(1, MARKET_A, ["101", "102"])]).await;
    let temp = scratch();
    let output = temp.path().join("books.sbe");
    let stderr = temp.path().join("stderr.jsonl");
    let decode_log = temp.path().join("decode.jsonl");
    let capture = temp.path().join("capture.jsonl");
    let started = chrono::Utc::now().timestamp_micros();
    // stdout is the actual mandatory output path; no fixture replay or fork.
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
            .args([
                "--ws-url",
                &exchange.ws_url,
                "--gamma-url",
                &exchange.gamma_url,
                "--metrics-bind",
                "127.0.0.1:0",
                "--workers",
                "2",
                "--markets-per-connection",
                "2",
                "--scan-interval-secs",
                "1",
                "--lifecycle-connections",
                "1",
                "--gamma-requests-per-second",
                "1000",
                "--connection-opens-per-second",
                "1000",
                "--decode-error-log",
            ])
            .arg(&decode_log)
            .arg("--capture")
            .arg(&capture)
            .stdout(Stdio::from(File::create(&output).unwrap()))
            .stderr(Stdio::from(File::create(&stderr).unwrap()))
            .spawn()
            .unwrap(),
    );

    let lifecycle = exchange
        .wait(|event| {
            matches!(event, Event::Client { value, .. }
        if value["custom_feature_enabled"] == true && value["assets_ids"] == json!([]))
        })
        .await;
    let Event::Client {
        socket: lifecycle, ..
    } = lifecycle
    else {
        unreachable!()
    };
    let first = exchange.subscription("initial", &["101", "102"]).await;
    let peer = exchange.subscription("initial", &["101", "102"]).await;
    assert_ne!(first, peer);
    let mut oracle = Oracle::default();
    let frames = [
        oracle.full(MARKET_A, "101", initial_book(), BASE_MS),
        oracle.full(MARKET_A, "102", Book::default(), BASE_MS),
    ];
    exchange.send(first, json!(frames)).await;
    exchange
        .send(
            first,
            oracle.changes(
                MARKET_A,
                &[
                    ("101", Side::Buy, 220_000, 12_000_001),
                    ("102", Side::Sell, 700_000, 3_000_000),
                    ("101", Side::Buy, 220_000, 2_000_000),
                    ("101", Side::Sell, 800_000, 0),
                ],
                BASE_MS + 1,
            ),
        )
        .await;
    let refreshed = Book::new(
        &[(300_000, 9_000_000), (400_000, 6_000_000)],
        &[(600_000, 5_000_000)],
    );
    exchange
        .send(
            first,
            oracle.full(MARKET_A, "101", refreshed.clone(), BASE_MS + 2),
        )
        .await;
    exchange
        .send(first, oracle.full(MARKET_A, "101", refreshed, BASE_MS + 3))
        .await;
    settle().await;

    exchange.drop_socket(first);
    let second = exchange.subscription("initial", &["101", "102"]).await;
    assert_ne!(first, second);
    // These arrive before either fresh book and must not reach the output.
    exchange
        .send(
            second,
            change_frame(
                MARKET_A,
                &[
                    ("101", Side::Buy, 990_000, 99_000_000),
                    ("102", Side::Sell, 990_000, 99_000_000),
                ],
                BASE_MS + 4,
            ),
        )
        .await;
    exchange
        .send(
            second,
            replacement_full(&mut oracle, MARKET_A, "101", initial_book(), BASE_MS + 5),
        )
        .await;
    exchange
        .send(
            second,
            replacement_full(&mut oracle, MARKET_A, "102", Book::default(), BASE_MS + 6),
        )
        .await;
    exchange
        .send(
            second,
            oracle.changes(
                MARKET_A,
                &[("102", Side::Buy, 200_000, 4_000_000)],
                BASE_MS + 7,
            ),
        )
        .await;
    // Recognized malformed messages reject the whole changes array, including
    // its valid first entry, then reconnect and require fresh books again.
    exchange
        .send(
            second,
            json!({"event_type":"price_change", "market":MARKET_A,
            "timestamp":(BASE_MS + 8).to_string(), "price_changes":[
                {"asset_id":"101","side":"BUY","price":"0.4","size":"999"},
                {"asset_id":"102","side":"BUY","price":"0.1234567","size":"1"}
            ]}),
        )
        .await;
    let third = exchange.subscription("initial", &["101", "102"]).await;
    exchange
        .send(
            third,
            replacement_full(&mut oracle, MARKET_A, "101", initial_book(), BASE_MS + 9),
        )
        .await;
    exchange
        .send(
            third,
            replacement_full(&mut oracle, MARKET_A, "102", Book::default(), BASE_MS + 10),
        )
        .await;

    // The earlier failures deliberately had no ready peer. Initialize it now
    // so the remaining lifecycle checks do not also trigger missing-book retries.
    settle().await;
    let mut replica = Oracle::default();
    exchange
        .send(
            peer,
            json!([
                replica.full(MARKET_A, "101", initial_book(), BASE_MS + 9),
                replica.full(MARKET_A, "102", Book::default(), BASE_MS + 10),
            ]),
        )
        .await;

    // Lifecycle hints wait until Gamma confirms that the market accepts orders.
    let mut pending = listing(2, MARKET_B, ["201", "202"]);
    pending["acceptingOrders"] = false.into();
    exchange.catalog.lock().unwrap().push(pending);
    for _ in 0..3 {
        exchange
            .send(lifecycle, hint(2, MARKET_B, ["201", "202"]))
            .await;
    }
    until(|| {
        exchange
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|q| q.contains("id=2"))
    })
    .await;
    assert!(exchange.drain().iter().all(|event| {
        !matches!(event, Event::Client { value, .. } if value["assets_ids"].as_array().is_some_and(|assets| assets.iter().any(|asset| asset == "201" || asset == "202")))
    }), "a market not accepting orders must not be subscribed");
    exchange.catalog.lock().unwrap()[1]["acceptingOrders"] = true.into();
    assert_pair_subscription(&mut exchange, "subscribe", &["201", "202"], [third, peer]).await;
    for asset in ["201", "202"] {
        let frame = oracle.full(MARKET_B, asset, initial_book(), BASE_MS + 11);
        exchange.send(third, frame.clone()).await;
        settle().await;
        exchange.send(peer, frame).await;
    }
    // No hint for C: periodic Gamma reconciliation must discover it and open
    // another socket because both market slots on the first are occupied.
    exchange
        .catalog
        .lock()
        .unwrap()
        .push(listing(3, MARKET_C, ["301", "302"]));
    let other = exchange.subscription("initial", &["301", "302"]).await;
    let other_peer = exchange.subscription("initial", &["301", "302"]).await;
    assert_ne!(other, other_peer);
    assert!(
        [other, other_peer]
            .iter()
            .all(|id| ![third, peer].contains(id))
    );
    for asset in ["301", "302"] {
        let frame = oracle.full(MARKET_C, asset, Book::default(), BASE_MS + 12);
        exchange.send(other, frame.clone()).await;
        settle().await;
        exchange.send(other_peer, frame).await;
    }

    // NotDesired is reversible: removal then re-add must produce new first
    // snapshots, replacing stale reader state. Late removed-asset data is ignored.
    exchange.catalog.lock().unwrap()[1]["acceptingOrders"] = false.into();
    assert_pair_subscription(&mut exchange, "unsubscribe", &["201", "202"], [third, peer]).await;
    exchange
        .send(
            third,
            change_frame(
                MARKET_B,
                &[("201", Side::Buy, 990_000, 99_000_000)],
                BASE_MS + 13,
            ),
        )
        .await;
    oracle.removed(&["201", "202"]);
    exchange.catalog.lock().unwrap()[1]["acceptingOrders"] = true.into();
    assert_pair_subscription(&mut exchange, "subscribe", &["201", "202"], [third, peer]).await;
    for asset in ["201", "202"] {
        let frame = oracle.full(
            MARKET_B,
            asset,
            Book::new(&[(100_000, 1_000_000)], &[]),
            BASE_MS + 14,
        );
        exchange.send(third, frame.clone()).await;
        settle().await;
        exchange.send(peer, frame).await;
    }

    // Resolution removes B immediately. A subsequent full scan can discover it
    // again after the recent lifecycle buffer has been reconciled and cleared.
    exchange.catalog.lock().unwrap()[1]["acceptingOrders"] = false.into();
    exchange
        .send(
            lifecycle,
            json!({"event_type":"market_resolved", "market":MARKET_B}),
        )
        .await;
    assert_pair_subscription(&mut exchange, "unsubscribe", &["201", "202"], [third, peer]).await;
    oracle.removed(&["201", "202"]);
    exchange.catalog.lock().unwrap()[1]["acceptingOrders"] = true.into();
    assert_pair_subscription(&mut exchange, "subscribe", &["201", "202"], [third, peer]).await;
    for asset in ["201", "202"] {
        let frame = oracle.full(
            MARKET_B,
            asset,
            Book::new(&[(100_000, 1_000_000)], &[]),
            BASE_MS + 14,
        );
        exchange.send(third, frame.clone()).await;
        settle().await;
        exchange.send(peer, frame).await;
    }

    // A disappears from the open scan. Full-scan reconciliation must remove it.
    exchange.catalog.lock().unwrap()[0]["closed"] = true.into();
    assert_pair_subscription(&mut exchange, "unsubscribe", &["101", "102"], [third, peer]).await;
    exchange
        .send(
            third,
            change_frame(
                MARKET_A,
                &[("101", Side::Buy, 990_000, 99_000_000)],
                BASE_MS + 15,
            ),
        )
        .await;
    oracle.removed(&["101", "102"]);
    // The next expected record is a positive fence on the same socket:
    // shutdown cannot make the preceding ignored-frame check pass unread.
    exchange
        .send(
            third,
            oracle.changes(
                MARKET_B,
                &[("201", Side::Buy, 100_000, 2_000_000)],
                BASE_MS + 16,
            ),
        )
        .await;
    settle().await;

    let endpoint = telemetry(&stderr)
        .into_iter()
        .find(|v| v["event"] == "metrics_available")
        .unwrap()["endpoint"]
        .as_str()
        .unwrap()
        .to_owned();
    let deadline = Instant::now() + STEP;
    loop {
        let response = reqwest::get(&endpoint).await.unwrap();
        assert!(response.status().is_success());
        let text = response.text().await.unwrap();
        if text.contains("collector_books{state=\"synced\"} 4")
            && text.contains("collector_decode_errors_total 1")
            && text.contains("collector_markets{state=\"desired\"} 2")
            && text.contains("collector_markets{state=\"synced\"} 2")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "live metrics never caught up: {text}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(
        Command::new("kill")
            .args([shutdown_signal, &process.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + STEP;
    let status = loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "signal shutdown must finish within eight seconds"
        );
        pump().await;
    };
    assert!(
        status.success(),
        "collector failed: {:?}",
        telemetry(&stderr)
    );
    let records = read_sbe(&output, (started, chrono::Utc::now().timestamp_micros()));
    oracle.check(&records);
    let logs = telemetry(&stderr);
    assert!(
        !logs
            .iter()
            .any(|v| v["event"] == "fatal" || v["event"] == "forced_exit")
    );
    assert!(!logs.iter().any(|v| matches!(
        v["event"].as_str(),
        Some("run_summary" | "worker_summary" | "writer_summary" | "health")
    )));
    let snapshots = records
        .values()
        .flatten()
        .filter(|r| matches!(r, mock_exchange::Record::Snapshot(..)))
        .count();
    let updates = records.values().map(Vec::len).sum::<usize>() - snapshots;
    assert_eq!(snapshots, 20);
    assert_eq!(updates, 9);
    println!(
        "binary oracle: {snapshots} snapshots, {updates} updates, 6 asset histories; {shutdown_signal} exit 0"
    );
    assert_eq!(
        std::fs::read_to_string(decode_log).unwrap().lines().count(),
        1
    );
    let requests = exchange.requests.lock().unwrap().clone();
    assert!(
        requests
            .iter()
            .any(|q| q.contains("closed=false") && q.contains("_nonce="))
    );
    assert!(
        requests
            .iter()
            .any(|q| q.contains("closed=any") && q.contains("id=2") && q.contains("_nonce=")),
        "lifecycle admission must confirm current Gamma eligibility"
    );
    assert!(
        requests
            .iter()
            .filter(|q| q.contains("closed=false"))
            .all(|q| q.contains("ascending=true"))
    );
    let captured = parse_jsonl(&std::fs::read_to_string(capture).unwrap()).unwrap();
    let registrations: Vec<_> = captured
        .iter()
        .filter_map(|event| match event {
            FixtureEvent::AddMarket {
                generation: _,
                market,
                connection,
                standby,
                ..
            } => {
                assert!(standby.is_some_and(|peer| peer != *connection));
                Some(market.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        registrations
            .iter()
            .filter(|&&market| market == MARKET_A)
            .count(),
        1,
        "socket recovery must retain the market registration"
    );
    assert_eq!(
        registrations
            .iter()
            .filter(|&&market| market == MARKET_B)
            .count(),
        3
    );
    assert_eq!(
        registrations
            .iter()
            .filter(|&&market| market == MARKET_C)
            .count(),
        1
    );
    assert_eq!(
        captured
            .iter()
            .filter(|event| matches!(event, FixtureEvent::RemoveMarket { .. }))
            .count(),
        3
    );
    assert!(captured.iter().any(|event| matches!(
        event,
        FixtureEvent::MarkUnsynced {
            reason: engineering_challenge::model::UnsyncReason::DecodeError,
            ..
        }
    )));
    assert!(!captured.iter().any(|event| matches!(
        event,
        FixtureEvent::MarkUnsynced {
            reason: engineering_challenge::model::UnsyncReason::Disconnected,
            ..
        }
    )));
    for expected in [lifecycle, third, peer, other, other_peer] {
        exchange
            .wait(|e| matches!(e, Event::Closed(id) if *id == expected))
            .await;
    }
    exchange.finish().await;
}

/// Production Polymarket protocol that counts every frame it decodes, so paused
/// tests can wait until the collector has read a frame before advancing time.
#[derive(Clone)]
struct CountedFrames {
    inner: PolymarketProtocol,
    frames: Arc<AtomicU64>,
}

impl BookProtocol for CountedFrames {
    fn connect_request(&self) -> anyhow::Result<Request> {
        self.inner.connect_request()
    }
    fn start_session(&mut self) {
        self.inner.start_session();
    }
    fn subscribe(&mut self, books: &[MarketBooks], initial: bool) -> Vec<Message> {
        self.inner.subscribe(books, initial)
    }
    fn unsubscribe(&mut self, books: &[MarketBooks]) -> Vec<Message> {
        self.inner.unsubscribe(books)
    }
    fn request_books(&mut self, books: &[OrderBookId]) -> Vec<Message> {
        self.inner.request_books(books)
    }
    fn prepare_book_request(&mut self, books: &[OrderBookId]) -> Option<BookRequestPreparation> {
        self.inner.prepare_book_request(books)
    }
    fn heartbeat_interval(&self) -> Option<Duration> {
        self.inner.heartbeat_interval()
    }
    fn heartbeat(&mut self) -> Option<Message> {
        self.inner.heartbeat()
    }
    fn decode(&mut self, frame: &Message, context: DecodeContext<'_>) -> DecodedFrame {
        self.frames.fetch_add(1, Ordering::Relaxed);
        self.inner.decode(frame, context)
    }
}

/// Uses production task types and endpoints, with only app-owned signal and
/// telemetry supervision omitted. The binary test above exercises that layer.
struct Collector {
    pipeline: Pipeline,
    connections: TaskTracker,
    manager: JoinHandle<anyhow::Result<ManagerTotals>>,
    discovery: JoinHandle<anyhow::Result<DiscoveryTotals>>,
    cancel: CancellationToken,
    metrics: Metrics,
    frames: Arc<AtomicU64>,
    _lifecycle: mpsc::UnboundedSender<engineering_challenge::core::discovery::LifecycleEvent>,
}

impl Collector {
    fn start(exchange: &MockExchange, config: &Config) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let metrics = Metrics::default();
        let pipeline = Pipeline::start_with_metrics(config, &metrics).unwrap();
        let cancel = CancellationToken::new();
        let connections = TaskTracker::new();
        let frames = Arc::new(AtomicU64::new(0));
        let ctx = Arc::new(BookConnectionContext {
            recovery: None,
            workers: pipeline.workers().clone(),
            decode_log: Arc::new(DecodeErrorLog::open(&config.decode_error_log).unwrap()),
            open_limiter: Arc::new(RateLimiter::per_second(1000.0)),
            capture: None,
            cancel: cancel.clone(),
            max_frame_bytes: config.max_frame_bytes,
            silence_limit: SILENCE_LIMIT,
            metrics: metrics.clone(),
        });
        let protocol = CountedFrames {
            inner: PolymarketProtocol::new(exchange.ws_url.clone()),
            frames: frames.clone(),
        };
        let spawn: SpawnConnection = {
            let tracker = connections.clone();
            Box::new(move |id| {
                let (tx, rx) = mpsc::unbounded_channel();
                let ctx = ctx.clone();
                let protocol = protocol.clone();
                tracker.spawn(async move {
                    run_book_connection(id, rx, ctx, protocol).await.unwrap();
                });
                tx
            })
        };
        let manager = SubscriptionManager::new(pipeline.workers().clone(), 2, spawn, None);
        let (manager_tx, manager_rx) = mpsc::channel(64);
        let manager = tokio::spawn(manager.run(manager_rx, cancel.clone()));
        let gamma = GammaClient::new(&exchange.gamma_url, 1000.0).unwrap();
        let discovery = Discovery::new(
            gamma,
            manager_tx,
            Duration::from_secs(3600),
            metrics.register(Component::Discovery),
        );
        let (lifecycle, lifecycle_rx) = mpsc::unbounded_channel();
        // This harness omits lifecycle sockets; model an established subscription.
        lifecycle
            .send(engineering_challenge::core::discovery::LifecycleEvent::Connected(0))
            .unwrap();
        let discovery = tokio::spawn(discovery.run(lifecycle_rx, cancel.clone()));
        Self {
            pipeline,
            connections,
            manager,
            discovery,
            cancel,
            metrics,
            frames,
            _lifecycle: lifecycle,
        }
    }

    async fn send(&self, exchange: &MockExchange, socket: usize, value: Value) {
        let before = self.frames.load(Ordering::Relaxed);
        exchange.send(socket, value).await;
        self.received(before + 1).await;
    }

    async fn received(&self, count: u64) {
        until(|| self.frames.load(Ordering::Relaxed) >= count).await;
        settle().await;
    }

    async fn finish(self) -> PipelineTotals {
        self.cancel.cancel();
        within(async {
            assert_eq!(self.discovery.await.unwrap().unwrap().registered, 1);
            let manager = self.manager.await.unwrap().unwrap();
            assert_eq!(manager.markets, 1);
            assert_eq!(manager.added, 1, "failures never re-register the market");
            assert_eq!(manager.removed, 0, "failures never remove the market");
            assert_eq!(manager.connections, 2, "recovery reuses the fixed pair");
            self.connections.close();
            self.connections.wait().await;
            self.pipeline.finish().await.unwrap()
        })
        .await
    }
}

fn paused_config(temp: &tempfile::TempDir) -> Config {
    Config {
        workers: 2,
        output: temp.path().join("books.sbe"),
        decode_error_log: temp.path().join("decode.jsonl"),
        ..Config::default()
    }
}

async fn advance(seconds: u64) {
    // Small steps let the real interval tasks observe every cadence, without
    // relying on Tokio's automatic time jumps during network I/O.
    for _ in 0..seconds {
        tokio::time::advance(Duration::from_secs(1)).await;
        pump().await;
    }
    settle().await;
}

#[tokio::test(start_paused = true)]
async fn muted_missing_first_book_is_retried_while_other_asset_keeps_receiving() {
    let mut exchange = MockExchange::start(vec![listing(1, MARKET_A, ["101", "102"])]).await;
    let temp = scratch();
    let config = paused_config(&temp);
    let started = chrono::Utc::now().timestamp_micros();
    let collector = Collector::start(&exchange, &config);
    let socket = exchange.subscription("initial", &["101", "102"]).await;
    // The production connection-open limiter spaces the pair by one millisecond.
    tokio::time::advance(Duration::from_millis(2)).await;
    settle().await;
    let peer = exchange.subscription("initial", &["101", "102"]).await;
    assert_ne!(socket, peer);
    let mut pending = Oracle::default();
    let first = pending.full(MARKET_A, "101", initial_book(), BASE_MS);
    for source in [socket, peer] {
        collector.send(&exchange, source, first.clone()).await;
    }
    exchange.drain();
    // Neither source has outcome 102. A full book is required even if deltas
    // and PONGs arrive; only the missing outcome may be resubscribed.
    advance(4).await;
    assert!(
        !exchange
            .drain()
            .iter()
            .any(|event| matches!(event, Event::Client { .. } | Event::Closed(_))),
        "no retry or reconnect before the five-second first-book deadline"
    );
    collector
        .send(
            &exchange,
            socket,
            pending.changes(
                MARKET_A,
                &[("101", Side::Buy, 400_000, 22_000_000)],
                BASE_MS + 12,
            ),
        )
        .await;
    collector.send(&exchange, peer, json!("PONG")).await;
    advance(2).await;
    for attempt in 1..=10 {
        assert_pair_subscription(&mut exchange, "unsubscribe", &["102"], [socket, peer]).await;
        // The healthy outcome remains readable during the unsubscribe gap.
        for source in [socket, peer] {
            collector.send(&exchange, source, json!("PONG")).await;
        }
        advance(6).await;
        assert_pair_subscription(&mut exchange, "subscribe", &["102"], [socket, peer]).await;
        if attempt < 10 {
            for source in [socket, peer] {
                collector.send(&exchange, source, json!("PONG")).await;
            }
            advance(5).await;
        }
    }
    // Metrics publication uses a monotonic wall clock independently of the
    // paused protocol timers. Wait for a published coverage sample.
    let deadline = Instant::now() + STEP;
    let partial = loop {
        let text = collector.metrics.render().unwrap().text;
        if text.contains("collector_books{state=\"synced\"} 1") {
            break text;
        }
        assert!(
            Instant::now() < deadline,
            "coverage was not published: {text}"
        );
        for source in [socket, peer] {
            collector.send(&exchange, source, json!("PONG")).await;
        }
        advance(1).await;
    };
    assert!(
        partial.contains("collector_markets{state=\"synced\"} 0"),
        "a partially initialized market cannot publish: {partial}"
    );
    advance(5).await;
    assert!(
        !exchange
            .drain()
            .iter()
            .any(|event| matches!(event, Event::Client { .. } | Event::Closed(_))),
        "the ten-retry budget is exhausted without reconnecting"
    );
    // A late full book after exhaustion still starts publication, using the
    // most recently applied 101 book.
    let mut oracle = Oracle::default();
    oracle.full(MARKET_A, "101", pending.books["101"].clone(), BASE_MS + 12);
    collector
        .send(
            &exchange,
            socket,
            oracle.full(MARKET_A, "102", Book::default(), BASE_MS + 13),
        )
        .await;
    collector
        .send(
            &exchange,
            socket,
            oracle.changes(
                MARKET_A,
                &[
                    ("101", Side::Buy, 400_000, 8_000_000),
                    ("102", Side::Sell, 600_000, 2_000_000),
                ],
                BASE_MS + 14,
            ),
        )
        .await;
    for _ in 0..6 {
        for source in [socket, peer] {
            collector.send(&exchange, source, json!("PONG")).await;
        }
        advance(10).await;
    }
    advance(1).await;
    oracle.periodic("101");
    oracle.periodic("102");
    let events = exchange.drain();
    assert!(!events.iter().any(|event| matches!(event, Event::Closed(_))));
    for event in &events {
        if let Event::Client {
            socket: source,
            value: message,
        } = event
        {
            assert_eq!(*source, peer, "initialized publisher must not retry");
            assert_eq!(message["assets_ids"], json!(["102"]));
        }
    }
    let totals = collector.finish().await;
    assert_eq!(totals.writer.snapshots, 4);
    assert_eq!(
        totals.writer.level_updates, 2,
        "partial-source updates stay private"
    );
    oracle.check(&read_sbe(
        &config.output,
        (started, chrono::Utc::now().timestamp_micros()),
    ));
    exchange.finish().await;
}

#[tokio::test(start_paused = true)]
async fn silence_promotes_ready_peer_with_snapshots_and_preserves_registration() {
    let mut exchange = MockExchange::start(vec![listing(1, MARKET_A, ["101", "102"])]).await;
    let temp = scratch();
    let config = paused_config(&temp);
    let started = chrono::Utc::now().timestamp_micros();
    let collector = Collector::start(&exchange, &config);
    let socket = exchange.subscription("initial", &["101", "102"]).await;
    // The production connection-open limiter spaces the pair by one millisecond.
    tokio::time::advance(Duration::from_millis(2)).await;
    settle().await;
    let peer = exchange.subscription("initial", &["101", "102"]).await;
    assert_ne!(socket, peer);
    let mut oracle = Oracle::default();
    collector
        .send(
            &exchange,
            socket,
            json!([
                oracle.full(MARKET_A, "101", initial_book(), BASE_MS),
                oracle.full(MARKET_A, "102", Book::default(), BASE_MS),
            ]),
        )
        .await;
    let mut standby = Oracle::default();
    collector
        .send(
            &exchange,
            peer,
            json!([
                standby.full(
                    MARKET_A,
                    "101",
                    Book::new(&[(300_000, 3_000_000)], &[(500_000, 1_000_000)]),
                    BASE_MS
                ),
                standby.full(MARKET_A, "102", initial_book(), BASE_MS),
            ]),
        )
        .await;
    collector
        .send(
            &exchange,
            peer,
            standby.changes(
                MARKET_A,
                &[("101", Side::Buy, 300_000, 4_000_000)],
                BASE_MS + 1,
            ),
        )
        .await;
    exchange.drain();
    // All frames, including PONGs, are withheld from the publisher. The ready
    // standby keeps receiving PONGs, so its books remain eligible for takeover.
    for _ in 0..2 {
        advance(10).await;
        collector.send(&exchange, peer, json!("PONG")).await;
    }
    advance(9).await;
    let events = exchange.drain();
    for source in [socket, peer] {
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::Ping(id) if *id == source))
                .count(),
            2,
            "PING runs every 10 seconds, including on the active source"
        );
    }
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::Closed(_) | Event::Client { .. })),
        "29 seconds without frames must not reconnect"
    );
    advance(5).await;
    let recovered = exchange.subscription("initial", &["101", "102"]).await;
    assert_ne!(recovered, socket);
    assert_ne!(recovered, peer);
    exchange
        .wait(|event| matches!(event, Event::Closed(id) if *id == socket))
        .await;
    // The peer's independently maintained books replace both old baselines.
    replacement_full(
        &mut oracle,
        MARKET_A,
        "101",
        standby.books["101"].clone(),
        BASE_MS + 1,
    );
    replacement_full(
        &mut oracle,
        MARKET_A,
        "102",
        standby.books["102"].clone(),
        BASE_MS,
    );
    collector
        .send(
            &exchange,
            recovered,
            change_frame(
                MARKET_A,
                &[
                    ("101", Side::Buy, 990_000, 99_000_000),
                    ("102", Side::Buy, 990_000, 99_000_000),
                ],
                BASE_MS + 2,
            ),
        )
        .await;
    // Fresh old-source baselines restore standby state without failback.
    let mut returning = Oracle::default();
    collector
        .send(
            &exchange,
            recovered,
            json!([
                returning.full(MARKET_A, "101", initial_book(), BASE_MS + 3),
                returning.full(MARKET_A, "102", Book::default(), BASE_MS + 3),
            ]),
        )
        .await;
    collector
        .send(
            &exchange,
            recovered,
            returning.changes(
                MARKET_A,
                &[("101", Side::Buy, 990_000, 99_000_000)],
                BASE_MS + 4,
            ),
        )
        .await;
    collector
        .send(
            &exchange,
            peer,
            oracle.changes(
                MARKET_A,
                &[("101", Side::Buy, 300_000, 5_000_000)],
                BASE_MS + 5,
            ),
        )
        .await;
    // The original 60-second snapshot cadence survives the switch at 30 s.
    for _ in 0..2 {
        for source in [peer, recovered] {
            collector.send(&exchange, source, json!("PONG")).await;
        }
        advance(10).await;
    }
    advance(7).await;
    oracle.periodic("101");
    oracle.periodic("102");
    let events = exchange.drain();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::Closed(_) | Event::Client { .. })),
        "takeover and recovery keep the same pair membership"
    );
    let metrics = collector.metrics.clone();
    let totals = collector.finish().await;
    assert!(
        metrics
            .render()
            .unwrap()
            .text
            .contains("collector_decode_errors_total 0")
    );
    assert_eq!(
        totals.writer.snapshots, 6,
        "initial, replacement, and original-cadence periodic pairs"
    );
    assert_eq!(
        totals.writer.level_updates, 1,
        "no handoff diff or standby output"
    );
    assert_eq!(
        totals
            .workers
            .iter()
            .map(|worker| worker.dropped_unsynced)
            .sum::<u64>(),
        2
    );
    assert_eq!(
        totals
            .workers
            .iter()
            .map(|worker| worker.books_synced)
            .sum::<u64>(),
        2
    );
    let switches: [u64; 4] = std::array::from_fn(|index| {
        totals
            .workers
            .iter()
            .map(|worker| worker.publisher_switches[index])
            .sum()
    });
    assert_eq!(switches, [0, 0, 1, 0], "one silence switch, no failback");
    oracle.check(&read_sbe(
        &config.output,
        (started, chrono::Utc::now().timestamp_micros()),
    ));
    exchange.finish().await;
}

#[tokio::test]
async fn cancelled_http_connection_does_not_stop_mock_discovery() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let exchange = MockExchange::start(vec![]).await;
    let url = reqwest::Url::parse(&exchange.gamma_url).unwrap();
    let tcp = tokio::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap()))
        .await
        .unwrap();
    drop(tcp);
    let response = reqwest::get(format!("{}/markets/keyset", exchange.gamma_url))
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert_eq!(exchange.requests.lock().unwrap().len(), 1);
    exchange.finish().await;
}

#[tokio::test]
async fn gamma_lookups_refresh_accepting_orders_without_reusing_cache_keys() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut market = listing(1, MARKET_A, ["101", "102"]);
    market["acceptingOrders"] = false.into();
    let exchange = MockExchange::start(vec![market]).await;
    let gamma = GammaClient::new(&exchange.gamma_url, 1000.0).unwrap();
    assert!(!gamma.lookup(&[1]).await.unwrap()[0].desired());
    exchange.catalog.lock().unwrap()[0]["acceptingOrders"] = true.into();
    assert!(gamma.lookup(&[1]).await.unwrap()[0].desired());
    let requests = exchange.requests.lock().unwrap().clone();
    let nonces: Vec<String> = requests
        .iter()
        .map(|target| {
            reqwest::Url::parse(&format!("http://localhost{target}"))
                .unwrap()
                .query_pairs()
                .find(|(key, _)| key == "_nonce")
                .unwrap()
                .1
                .into_owned()
        })
        .collect();
    assert_eq!(nonces.len(), 2);
    assert_ne!(nonces[0], nonces[1]);
    exchange.finish().await;
}
