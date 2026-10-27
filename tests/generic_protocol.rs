//! A second wire protocol exercises shared supervision without a production adapter.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, handshake::client::Request,
};
use tokio_util::sync::CancellationToken;

use engineering_challenge::Side;
use engineering_challenge::core::book_connection::{
    BookConnectionContext, ConnectionCommand, run_book_connection,
};
use engineering_challenge::core::capture::Capture;
use engineering_challenge::core::config::Config;
use engineering_challenge::core::decode_log::DecodeErrorLog;
use engineering_challenge::core::fixture::{FixtureEvent, parse_jsonl};
use engineering_challenge::core::metrics::{Component, Metrics};
use engineering_challenge::core::pipeline::{Pipeline, replay};
use engineering_challenge::core::rate_limit::RateLimiter;
use engineering_challenge::core::worker::Worker;
use engineering_challenge::exchange::protocol::{
    BookProtocol, ContinuityFault, DecodeContext, DecodedFrame,
};
use engineering_challenge::model::{
    ConnectionId, LevelUpdate, Listing, ListingId, ListingStatus, MarketBooks, MarketData,
    MarketId, OrderBookEvent, OrderBookId, OutputRecord, Price, Qty, QuantityChange, UnsyncReason,
    WorkerMessage,
};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[derive(Clone, Default)]
struct TestProtocol {
    url: String,
    attempts: Arc<AtomicUsize>,
    pending: bool,
    sid: Option<u64>,
    seq: u64,
    market: Option<MarketId>,
}

impl BookProtocol for TestProtocol {
    fn connect_request(&self) -> anyhow::Result<Request> {
        let mut request = self.url.as_str().into_client_request()?;
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        request
            .headers_mut()
            .insert("x-test-attempt", attempt.to_string().parse()?);
        Ok(request)
    }
    fn start_session(&mut self) {
        self.pending = false;
        self.sid = None;
        self.seq = 0;
        self.market = None;
    }
    fn subscribe(&mut self, books: &[MarketBooks], initial: bool) -> Vec<Message> {
        self.pending = true;
        self.market = books.first().map(|books| books.market.clone());
        vec![Message::Text(
            json!({"op":"join","request":1,"initial":initial,"books":books})
                .to_string()
                .into(),
        )]
    }
    fn unsubscribe(&mut self, _: &[MarketBooks]) -> Vec<Message> {
        self.pending = false;
        vec![Message::Text(
            json!({"op":"leave","sid":self.sid.take()})
                .to_string()
                .into(),
        )]
    }
    fn request_books(&mut self, books: &[OrderBookId]) -> Vec<Message> {
        vec![Message::Text(
            json!({"op":"refresh","books":books}).to_string().into(),
        )]
    }
    fn heartbeat_interval(&self) -> Option<Duration> {
        None
    }
    fn heartbeat(&mut self) -> Option<Message> {
        panic!("this protocol has no client heartbeat")
    }
    fn decode(&mut self, frame: &Message, context: DecodeContext<'_>) -> DecodedFrame {
        let Message::Binary(bytes) = frame else {
            return DecodedFrame::default();
        };
        let wire: Value = serde_json::from_slice(bytes).unwrap();
        if wire["kind"] == "ack" {
            if self.pending {
                self.sid = wire["sid"].as_u64();
                self.pending = false;
                return DecodedFrame::default();
            }
            return DecodedFrame {
                outgoing: vec![Message::Text("cancel-late-ack".into())],
                ..DecodedFrame::default()
            };
        }
        let Some(sid) = self.sid else {
            return DecodedFrame::default();
        };
        let seq = wire["seq"].as_u64().unwrap();
        if wire["sid"] != sid || seq != self.seq + 1 {
            return DecodedFrame {
                fault: Some(ContinuityFault {
                    reason: UnsyncReason::SequenceGap,
                    detail: "test sequence gap".into(),
                }),
                ..DecodedFrame::default()
            };
        }
        self.seq = seq;
        let book = context
            .ids
            .order_book(wire["book"].as_str().unwrap())
            .unwrap();
        let event = if wire["kind"] == "snapshot" {
            OrderBookEvent::Snapshot {
                order_book: book,
                timestamp_comparable: false,
                bids: vec![(Price(500_000), Qty(wire["qty"].as_i64().unwrap()))],
                asks: vec![],
            }
        } else {
            OrderBookEvent::LevelUpdates(vec![LevelUpdate {
                order_book: book,
                side: Side::Buy,
                price: Price(500_000),
                change: QuantityChange::Add(wire["qty"].as_i64().unwrap()),
            }])
        };
        DecodedFrame {
            events: vec![MarketData {
                received_instant: None,
                market: self.market.clone().unwrap(),
                connection: context.connection,
                received_at: context.received_at,
                exchange_ts: DateTime::from_timestamp_millis(wire["at"].as_i64().unwrap()).unwrap(),
                event,
            }],
            outgoing: vec![Message::Text(format!("processed-{seq}").into())],
            fault: if wire["kind"] == "delta_and_gap" {
                Some(ContinuityFault {
                    reason: UnsyncReason::SequenceGap,
                    detail: "gap after valid event".into(),
                })
            } else {
                None
            },
            ..DecodedFrame::default()
        }
    }
}

fn books() -> MarketBooks {
    MarketBooks {
        generation: 0,
        market: MarketId::from("m"),
        order_books: [OrderBookId::from("a"), OrderBookId::from("b")],
    }
}
fn add_market() -> WorkerMessage {
    let books = books();
    WorkerMessage::AddMarket {
        generation: 0,
        market: books.market,
        order_books: books.order_books.to_vec(),
        sources: engineering_challenge::model::MarketSources::single(ConnectionId(0)),
    }
}
fn wire(kind: &str, sid: u64, seq: u64, book: &str, qty: i64, at: i64) -> Message {
    Message::Binary(
        serde_json::to_vec(&json!({"kind":kind,"sid":sid,"seq":seq,"book":book,"qty":qty,"at":at}))
            .unwrap()
            .into(),
    )
}
fn config(temp: &tempfile::TempDir, name: &str) -> Config {
    Config {
        workers: 1,
        output: temp.path().join(format!("{name}.sbe")),
        decode_error_log: temp.path().join(format!("{name}.jsonl")),
        ..Config::default()
    }
}

async fn next_text<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Message::Text(text) => text.to_string(),
        other => panic!("unexpected client message: {other:?}"),
    }
}

// Tungstenite fixes this callback's error type to its full HTTP response.
#[allow(clippy::result_large_err)]
fn handshake_check(
    attempt: usize,
) -> impl tokio_tungstenite::tungstenite::handshake::server::Callback {
    move |request: &Request, response| {
        assert_eq!(request.headers()["x-test-attempt"], attempt.to_string());
        Ok(response)
    }
}

#[tokio::test]
async fn binary_sequence_protocol_reconnects_and_capture_replays_identical_sbe() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp, "live");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let protocol = TestProtocol {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        attempts: attempts.clone(),
        ..TestProtocol::default()
    };
    let pipeline = Pipeline::start(&config).unwrap();
    pipeline.workers().send(add_market()).await.unwrap();
    let (capture, file) = Capture::create(&temp.path().join("capture.jsonl")).unwrap();
    capture.record(&FixtureEvent::AddMarket {
        generation: 0,
        market: "m".into(),
        order_books: vec!["a".into(), "b".into()],
        connection: 0,
        standby: None,
    });
    let cancel = CancellationToken::new();
    let ctx = Arc::new(BookConnectionContext {
        recovery: None,
        workers: pipeline.workers().clone(),
        decode_log: Arc::new(DecodeErrorLog::open(&config.decode_error_log).unwrap()),
        open_limiter: Arc::new(RateLimiter::per_second(1000.)),
        capture: Some(capture.clone()),
        cancel: cancel.clone(),
        max_frame_bytes: 1 << 20,
        silence_limit: Duration::from_secs(180),
        metrics: Metrics::default(),
    });
    let (commands, rx) = mpsc::unbounded_channel();
    commands
        .send(ConnectionCommand::Subscribe(vec![books()]))
        .unwrap();
    let connection = tokio::spawn(run_book_connection(ConnectionId(0), rx, ctx, protocol));
    let (done, finished) = oneshot::channel();
    let server = tokio::spawn(async move {
        for (attempt, sid) in [(1, 11), (2, 22)] {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_hdr_async(stream, handshake_check(attempt))
                .await
                .unwrap();
            let subscription: Value = serde_json::from_str(&next_text(&mut socket).await).unwrap();
            assert_eq!(subscription["op"], "join");
            assert_eq!(subscription["initial"], true);
            assert_eq!(subscription["books"][0]["market"], "m");
            socket
                .send(Message::Ping(vec![1, 2, 3].into()))
                .await
                .unwrap();
            assert_eq!(
                socket.next().await.unwrap().unwrap(),
                Message::Pong(vec![1, 2, 3].into())
            );
            socket
                .send(Message::Binary(
                    serde_json::to_vec(&json!({"kind":"ack","sid":sid}))
                        .unwrap()
                        .into(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Binary(
                    serde_json::to_vec(&json!({"kind":"ack","sid":999}))
                        .unwrap()
                        .into(),
                ))
                .await
                .unwrap();
            assert_eq!(next_text(&mut socket).await, "cancel-late-ack");
            let frames = if attempt == 1 {
                vec![
                    wire("snapshot", sid, 1, "a", 10, 200),
                    wire("delta", sid, 2, "a", 2, 200),
                ]
            } else {
                // The first relative update must be dropped until the new snapshot.
                vec![
                    wire("delta", sid, 1, "a", 100, 100),
                    wire("snapshot", sid, 2, "a", 20, 100),
                    wire("delta", sid, 3, "a", 3, 100),
                    wire("snapshot", sid, 4, "b", 5, 100),
                ]
            };
            for (index, frame) in frames.into_iter().enumerate() {
                socket.send(frame).await.unwrap();
                assert_eq!(
                    next_text(&mut socket).await,
                    format!("processed-{}", index + 1)
                );
            }
            if attempt == 1 {
                socket
                    .send(wire("delta_and_gap", sid, 3, "a", 1, 200))
                    .await
                    .unwrap();
                // A protocol fault must close the old session before the replacement.
                assert!(matches!(
                    socket.next().await.unwrap().unwrap(),
                    Message::Close(_)
                ));
                let _ = socket.flush().await;
            } else {
                done.send(()).unwrap();
                while let Some(Ok(message)) = socket.next().await {
                    if matches!(message, Message::Close(_)) {
                        let _ = socket.flush().await;
                        break;
                    }
                }
                break;
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(10), finished)
        .await
        .unwrap()
        .unwrap();
    cancel.cancel();
    connection.await.unwrap().unwrap();
    drop(commands);
    drop(capture);
    file.finish().unwrap();
    let totals = pipeline.finish().await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(totals.workers[0].unsync_marks, 1);
    assert_eq!(totals.workers[0].dropped_unsynced, 1);
    server.await.unwrap();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&config.output).unwrap();
    let mut quantities = Vec::new();
    while let Some(record) = reader.read_msg().unwrap() {
        let record = match record {
            AnyMessage::PriceLevelUpdate(record) => {
                let record = PriceLevelUpdateDomain::try_from_decoder(record).unwrap();
                (record.symbol, record.quantity)
            }
            AnyMessage::Snapshot(record) => {
                let record = SnapshotDomain::try_from_decoder(record).unwrap();
                (record.symbol, record.bids[0].quantity)
            }
            _ => panic!("unexpected SBE message"),
        };
        quantities.push(record);
    }
    assert_eq!(
        quantities,
        vec![
            ("a".into(), 10),
            ("a".into(), 12),
            ("a".into(), 13),
            ("a".into(), 20),
            ("a".into(), 23),
            ("b".into(), 5)
        ]
    );
    let captured = std::fs::read_to_string(temp.path().join("capture.jsonl")).unwrap();
    let events = parse_jsonl(&captured).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, FixtureEvent::SessionStart { .. }))
            .count(),
        2
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, FixtureEvent::PingFrame { bytes, .. } if bytes == &[1,2,3]))
    );
    let replay_config = Config {
        output: temp.path().join("replay.sbe"),
        ..config.clone()
    };
    let replayed = replay(&replay_config, events, TestProtocol::default)
        .await
        .unwrap();
    assert_eq!(replayed.pipeline.workers[0].unsync_marks, 1);
    assert_eq!(
        std::fs::read(&config.output).unwrap(),
        std::fs::read(&replay_config.output).unwrap()
    );
}

fn data(event: OrderBookEvent, at: i64) -> WorkerMessage {
    WorkerMessage::MarketData(MarketData {
        received_instant: None,
        market: MarketId::from("m"),
        connection: ConnectionId(0),
        received_at: DateTime::UNIX_EPOCH,
        exchange_ts: DateTime::from_timestamp_millis(at).unwrap(),
        event,
    })
}
fn snapshot(qty: i64, at: i64, comparable: bool) -> WorkerMessage {
    data(
        OrderBookEvent::Snapshot {
            order_book: OrderBookId::from("a"),
            timestamp_comparable: comparable,
            bids: vec![(Price(1), Qty(qty))],
            asks: vec![],
        },
        at,
    )
}
fn changes(changes: Vec<QuantityChange>) -> WorkerMessage {
    data(
        OrderBookEvent::LevelUpdates(
            changes
                .into_iter()
                .map(|change| LevelUpdate {
                    order_book: OrderBookId::from("a"),
                    side: Side::Buy,
                    price: Price(1),
                    change,
                })
                .collect(),
        ),
        200,
    )
}
async fn worker_run(
    messages: Vec<WorkerMessage>,
) -> (
    anyhow::Result<engineering_challenge::core::worker::WorkerTotals>,
    Vec<OutputRecord>,
    Metrics,
) {
    let metrics = Metrics::default();
    let (tx, rx) = mpsc::channel(messages.len());
    let (writer, mut records) = mpsc::channel(32);
    for message in messages {
        tx.send(message).await.unwrap();
    }
    drop(tx);
    let result = Worker::new(0)
        .run(
            rx,
            writer,
            Duration::from_secs(1),
            metrics.register(Component::Worker(0)),
        )
        .await;
    let mut output = vec![];
    while let Some(batch) = records.recv().await {
        output.extend(batch);
    }
    (result, output, metrics)
}

#[tokio::test]
async fn quantities_are_absolute_in_output_and_repeat_changes_use_staged_values() {
    let (result, records, _) = worker_run(vec![
        add_market(),
        changes(vec![QuantityChange::Add(999)]),
        snapshot(10, 100, true),
        changes(vec![
            QuantityChange::Add(2),
            QuantityChange::Add(-5),
            QuantityChange::Set(Qty(4)),
            QuantityChange::Add(-4),
        ]),
    ])
    .await;
    let totals = result.unwrap();
    assert_eq!(totals.dropped_unsynced, 1);
    let quantities: Vec<_> = records
        .into_iter()
        .filter_map(|record| match record {
            OutputRecord::LevelUpdate { qty, .. } => Some(qty.0),
            _ => None,
        })
        .collect();
    assert_eq!(quantities, [12, 7, 4, 0]);
}

#[tokio::test]
async fn invalid_event_fails_without_partial_output() {
    for invalid in [
        vec![QuantityChange::Add(1), QuantityChange::Add(-100)],
        vec![QuantityChange::Set(Qty(i64::MAX)), QuantityChange::Add(1)],
        vec![QuantityChange::Set(Qty(-1))],
    ] {
        let (result, records, _) = worker_run(vec![
            add_market(),
            snapshot(10, 100, true),
            changes(invalid),
        ])
        .await;
        assert!(result.is_err());
        assert_eq!(records.len(), 1, "no partial invalid event reaches output");
    }
}

#[tokio::test]
async fn snapshot_timestamp_comparison_is_explicit() {
    for comparable in [false, true] {
        let (result, records, _) = worker_run(vec![
            add_market(),
            snapshot(10, 200, true),
            snapshot(20, 100, comparable),
        ])
        .await;
        assert_eq!(result.unwrap().older_full_books, u64::from(comparable));
        assert_eq!(records.len(), if comparable { 1 } else { 2 });
    }
}

#[test]
fn late_acknowledgement_produces_a_protocol_response() {
    let mut protocol = TestProtocol::default();
    protocol.start_session();
    protocol.subscribe(&[books()], false);
    protocol.unsubscribe(&[books()]);
    let ids = engineering_challenge::core::connection::ConnectionIds::default();
    let result = protocol.decode(
        &Message::Binary(br#"{"kind":"ack","sid":22}"#.to_vec().into()),
        DecodeContext {
            connection: ConnectionId(0),
            received_at: Utc::now(),
            ids: &ids,
            max_frame_bytes: 1024,
        },
    );
    assert_eq!(
        result.outgoing,
        vec![Message::Text("cancel-late-ack".into())]
    );
}

#[test]
fn registry_accepts_opaque_listing_ids() {
    let mut registry = engineering_challenge::core::registry::Registry::default();
    let listing = Listing {
        recovery: engineering_challenge::model::RecoveryEligibility::Eligible,
        listing_id: ListingId::from("ticker/uuid"),
        status: ListingStatus::Desired,
        market: Some(books().market),
        order_books: books().order_books.to_vec(),
        created_at: None,
    };
    assert_eq!(registry.reconcile(vec![listing]).len(), 1);
    assert_eq!(registry.len(), 1);
}

#[test]
fn core_has_no_concrete_exchange_dependencies_outside_startup() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut paths: Vec<_> = std::fs::read_dir(root.join("src/core"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "rs") && path.file_name().unwrap() != "app.rs"
        })
        .collect();
    paths.push(root.join("src/exchange/protocol.rs"));
    for path in paths {
        let text = std::fs::read_to_string(&path).unwrap();
        // Check identifier tokens so grouped imports cannot evade the boundary.
        let has_adapter = text
            .lines()
            .map(|line| line.split("//").next().unwrap_or_default())
            .any(|line| {
                line.split(|ch: char| !ch.is_alphanumeric() && ch != '_')
                    .any(|token| token == "polymarket")
            });
        assert!(
            !has_adapter,
            "concrete exchange leaked into {}",
            path.file_name().unwrap().to_string_lossy()
        );
    }
}

// Poll real loopback I/O without letting Tokio auto-advance the paused clock.
async fn drive<F: std::future::Future>(future: F) -> F::Output {
    use futures_util::FutureExt;
    tokio::pin!(future);
    for _ in 0..5000 {
        if let Some(value) = future.as_mut().now_or_never() {
            return value;
        }
        std::thread::sleep(Duration::from_millis(1));
        tokio::task::yield_now().await;
    }
    panic!("loopback I/O did not complete");
}

#[tokio::test]
async fn queued_membership_control_liveness_and_first_book_retry_use_shared_loop() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let protocol = TestProtocol {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        ..TestProtocol::default()
    };
    let (tx, mut messages) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let context = Arc::new(BookConnectionContext {
        recovery: None,
        workers: engineering_challenge::core::routing::Workers::from(vec![tx]),
        decode_log: Arc::new(DecodeErrorLog::open(&temp.path().join("decode.jsonl")).unwrap()),
        open_limiter: Arc::new(RateLimiter::per_second(1000.)),
        capture: None,
        cancel: cancel.clone(),
        max_frame_bytes: 1024,
        silence_limit: Duration::from_secs(15),
        metrics: Metrics::default(),
    });
    let (commands, rx) = mpsc::unbounded_channel();
    let old = MarketBooks {
        generation: 0,
        market: MarketId::from("old"),
        order_books: [OrderBookId::from("old-a"), OrderBookId::from("old-b")],
    };
    commands
        .send(ConnectionCommand::Subscribe(vec![old.clone()]))
        .unwrap();
    let connection = tokio::spawn(run_book_connection(ConnectionId(0), rx, context, protocol));
    let (tcp, _) = listener.accept().await.unwrap();
    // The handshake is deliberately held until the desired membership changes.
    commands
        .send(ConnectionCommand::Unsubscribe(vec![old]))
        .unwrap();
    commands
        .send(ConnectionCommand::Subscribe(vec![books()]))
        .unwrap();
    let mut server = tokio_tungstenite::accept_async(tcp).await.unwrap();
    let subscribed: Value = serde_json::from_str(&next_text(&mut server).await).unwrap();
    assert_eq!(
        subscribed["books"],
        serde_json::to_value(vec![books()]).unwrap()
    );
    tokio::time::pause();
    // Control traffic alone keeps the socket live for eight silence windows,
    // while missing baselines use the protocol's refresh operation at most
    // ten times. Removed pre-handshake membership must never be requested.
    let mut requests = 0;
    for step in 0..12 {
        drive(server.send(Message::Ping(vec![step].into())))
            .await
            .unwrap();
        loop {
            match drive(server.next()).await.unwrap().unwrap() {
                Message::Pong(bytes) => {
                    assert_eq!(bytes.as_ref(), &[step]);
                    break;
                }
                Message::Text(text) => {
                    let requested: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(requested["op"], "refresh");
                    let mut requested_books: Vec<_> = requested["books"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|book| book.as_str().unwrap())
                        .collect();
                    requested_books.sort();
                    assert_eq!(requested_books, ["a", "b"]);
                    requests += 1;
                    assert!(requests <= 10, "missing-book retry budget exceeded");
                }
                other => panic!("unexpected collector message: {other:?}"),
            }
        }
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(requests, 10);
    assert!(
        messages.try_recv().is_err(),
        "control frames never become market data or unsync resets"
    );
    commands
        .send(ConnectionCommand::Unsubscribe(vec![books()]))
        .unwrap();
    let removed: Value = serde_json::from_str(
        &drive(server.next())
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(removed["op"], "leave");
    assert!(matches!(
        drive(server.next()).await.unwrap().unwrap(),
        Message::Close(_)
    ));
    drive(server.flush()).await.unwrap();
    cancel.cancel();
    drive(connection).await.unwrap().unwrap();
}

#[tokio::test]
async fn fatal_quantity_error_flushes_prior_sbe_and_reports_failure() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp, "invalid");
    let pipeline = Pipeline::start(&config).unwrap();
    for message in [
        add_market(),
        snapshot(10, 100, true),
        changes(vec![QuantityChange::Add(1), QuantityChange::Add(-100)]),
    ] {
        pipeline.workers().send(message).await.unwrap();
    }
    assert!(pipeline.finish().await.is_err());
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&config.output).unwrap();
    assert!(matches!(
        reader.read_msg().unwrap(),
        Some(AnyMessage::Snapshot(_))
    ));
    assert!(reader.read_msg().unwrap().is_none());
}
