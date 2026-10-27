//! Wire-level timing and lifecycle races for delayed missing-book recovery.
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use engineering_challenge::core::book_connection::{
    BookConnectionContext, ConnectionCommand, run_book_connection,
};
use engineering_challenge::core::decode_log::DecodeErrorLog;
use engineering_challenge::core::metrics::Metrics;
use engineering_challenge::core::rate_limit::RateLimiter;
use engineering_challenge::core::routing::Workers;
use engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol;
use engineering_challenge::model::{
    ConnectionId, MarketBooks, MarketId, OrderBookEvent, OrderBookId, WorkerMessage,
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

async fn pump() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    std::thread::sleep(Duration::from_millis(1));
}

async fn io<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = pump() => assert!(Instant::now() < deadline, "loopback operation stalled"),
        }
    }
}

async fn step(duration: Duration) {
    tokio::time::advance(duration).await;
    for _ in 0..20 {
        pump().await;
    }
}

fn market(id: &str, assets: [&str; 2]) -> MarketBooks {
    MarketBooks {
        generation: 0,
        market: MarketId::from(id),
        order_books: assets.map(OrderBookId::from),
    }
}

fn book(asset: &str) -> Message {
    Message::Text(
        json!({"event_type":"book", "market":if asset.starts_with('2') {"b"} else {"a"},
        "asset_id":asset,"timestamp":"1792777248021","bids":[],"asks":[]})
        .to_string()
        .into(),
    )
}

struct Rig {
    listener: TcpListener,
    socket: WebSocketStream<TcpStream>,
    commands: mpsc::UnboundedSender<ConnectionCommand>,
    input: mpsc::Receiver<WorkerMessage>,
    task: JoinHandle<anyhow::Result<()>>,
    cancel: CancellationToken,
    pings: usize,
    recovery: mpsc::Receiver<engineering_challenge::core::recovery::RecoveryState>,
    _temp: tempfile::TempDir,
}

impl Rig {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (sender, input) = mpsc::channel(64);
        let temp = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        let (recovery_tx, recovery) = mpsc::channel(1);
        let ctx = Arc::new(BookConnectionContext {
            recovery: Some(recovery_tx),
            workers: Workers::from(vec![sender]),
            decode_log: Arc::new(DecodeErrorLog::open(&temp.path().join("decode.jsonl")).unwrap()),
            open_limiter: Arc::new(RateLimiter::per_second(1000.0)),
            capture: None,
            cancel: cancel.clone(),
            max_frame_bytes: 1 << 20,
            silence_limit: Duration::from_secs(30),
            metrics: Metrics::default(),
        });
        let (commands, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_book_connection(
            ConnectionId(1),
            receiver,
            ctx,
            PolymarketProtocol::new(format!("ws://{}", listener.local_addr().unwrap())),
        ));
        commands
            .send(ConnectionCommand::Subscribe(vec![
                market("a", ["101", "102"]),
                market("b", ["201", "202"]),
            ]))
            .unwrap();
        let (tcp, _) = io(listener.accept()).await.unwrap();
        let socket = io(accept_async(tcp)).await.unwrap();
        let mut rig = Self {
            listener,
            socket,
            commands,
            input,
            task,
            cancel,
            pings: 0,
            recovery,
            _temp: temp,
        };
        assert_eq!(rig.wire().await["type"], "market");
        for asset in ["101", "201", "202"] {
            io(rig.socket.send(book(asset))).await.unwrap();
            assert!(matches!(
                io(rig.input.recv()).await,
                Some(WorkerMessage::MarketData(_))
            ));
        }
        rig
    }

    async fn wire(&mut self) -> Value {
        loop {
            let message = io(self.socket.next()).await.unwrap().unwrap();
            if let Message::Text(text) = message {
                if text == "PING" {
                    self.pings += 1;
                    io(self.socket.send(Message::Text("PONG".into())))
                        .await
                        .unwrap();
                } else {
                    return serde_json::from_str(&text).unwrap();
                }
            } else {
                panic!("unexpected wire message: {message:?}");
            }
        }
    }

    async fn unsubscribe_gap(&mut self) {
        step(Duration::from_secs(5)).await;
        let retry = self.wire().await;
        assert_eq!(retry["operation"], "unsubscribe");
        assert_eq!(retry["assets_ids"], json!(["102"]));
    }

    async fn finish(mut self) {
        self.cancel.cancel();
        io(async {
            while let Some(message) = self.socket.next().await {
                match message {
                    Ok(Message::Close(_)) => {
                        let _ = self.socket.flush().await;
                        break;
                    }
                    Err(tokio_tungstenite::tungstenite::Error::Io(error))
                        if error.kind() == std::io::ErrorKind::ConnectionReset =>
                    {
                        break;
                    }
                    Err(error) => panic!("unexpected shutdown error: {error}"),
                    Ok(_) => {}
                }
            }
        })
        .await;
        io(self.task).await.unwrap().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn retry_waits_five_seconds_keeps_reading_and_requires_a_post_subscribe_book() {
    let mut rig = Rig::start().await;
    rig.unsubscribe_gap().await;
    // An in-flight book during the gap cannot satisfy readiness or cancel subscribe.
    io(rig.socket.send(book("102"))).await.unwrap();
    io(rig.socket.send(book("101"))).await.unwrap();
    let Some(WorkerMessage::MarketData(data)) = io(rig.input.recv()).await else {
        panic!("healthy book must still route")
    };
    assert!(
        matches!(data.event, OrderBookEvent::Snapshot { ref order_book, .. } if order_book.0.as_ref() == "101")
    );
    assert!(rig.input.try_recv().is_err());
    step(Duration::from_millis(4999)).await;
    assert!(
        rig.socket.next().now_or_never().is_none(),
        "no subscribe before five seconds"
    );
    step(Duration::from_millis(1)).await;
    let resumed = rig.wire().await;
    assert_eq!(resumed["operation"], "subscribe");
    assert_eq!(resumed["assets_ids"], json!(["102"]));
    assert!(rig.pings >= 2, "heartbeat must continue through the gap");
    io(rig.socket.send(book("102"))).await.unwrap();
    assert!(matches!(
        io(rig.input.recv()).await,
        Some(WorkerMessage::MarketData(_))
    ));
    rig.finish().await;
}

#[tokio::test(start_paused = true)]
async fn removal_cancels_delayed_subscribe_while_other_markets_stay_connected() {
    let mut rig = Rig::start().await;
    rig.unsubscribe_gap().await;
    rig.commands
        .send(ConnectionCommand::Unsubscribe(vec![market(
            "a",
            ["101", "102"],
        )]))
        .unwrap();
    assert_eq!(rig.wire().await["operation"], "unsubscribe");
    step(Duration::from_secs(5)).await;
    // Read/respond to the regular heartbeat, without any subscription resurrection.
    let message = io(rig.socket.next()).await.unwrap().unwrap();
    assert_eq!(message, Message::Text("PING".into()));
    io(rig.socket.send(Message::Text("PONG".into())))
        .await
        .unwrap();
    for _ in 0..20 {
        pump().await;
    }
    assert!(rig.socket.next().now_or_never().is_none());
    rig.finish().await;
}

#[tokio::test(start_paused = true)]
async fn reconnect_during_unsubscribe_restores_membership_and_retry_budget() {
    let mut rig = Rig::start().await;
    rig.unsubscribe_gap().await;
    io(rig.socket.close(None)).await.unwrap();
    let (tcp, _) = io(rig.listener.accept()).await.unwrap();
    rig.socket = io(accept_async(tcp)).await.unwrap();
    let subscription = rig.wire().await;
    let mut assets: Vec<_> = subscription["assets_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assets.sort_unstable();
    assert_eq!(assets, ["101", "102", "201", "202"]);
    while rig.input.try_recv().is_ok() {}
    for asset in ["101", "201", "202"] {
        io(rig.socket.send(book(asset))).await.unwrap();
        io(rig.input.recv()).await.unwrap();
    }
    rig.unsubscribe_gap().await;
    rig.finish().await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_gap_does_not_wait_for_subscribe_deadline() {
    let mut rig = Rig::start().await;
    rig.unsubscribe_gap().await;
    let now = tokio::time::Instant::now();
    rig.finish().await;
    assert_eq!(tokio::time::Instant::now(), now);
}

#[tokio::test(start_paused = true)]
async fn exhausted_round_reports_once_then_retries_after_cooldown_without_http() {
    let mut rig = Rig::start().await;
    for _ in 0..10 {
        rig.unsubscribe_gap().await;
        step(Duration::from_secs(5)).await;
        assert_eq!(rig.wire().await["operation"], "subscribe");
    }
    assert!(
        rig.recovery.try_recv().is_err(),
        "final send is not exhaustion"
    );
    step(Duration::from_secs(5)).await;
    let report = rig.recovery.try_recv().unwrap();
    assert_eq!(report.market, MarketId::from("a"));
    assert_eq!(report.missing, [OrderBookId::from("102")]);
    for _ in 0..5 {
        step(Duration::from_secs(10)).await;
        while let Some(Some(Ok(message))) = rig.socket.next().now_or_never() {
            assert_eq!(
                message,
                Message::Text("PING".into()),
                "no retry during cooldown"
            );
            io(rig.socket.send(Message::Text("PONG".into())))
                .await
                .unwrap();
        }
    }
    step(Duration::from_secs(10)).await;
    assert_eq!(rig.wire().await["operation"], "unsubscribe");
    step(Duration::from_secs(5)).await;
    assert_eq!(rig.wire().await["operation"], "subscribe");
    io(rig.socket.send(book("102"))).await.unwrap();
    io(rig.input.recv()).await.unwrap();
    let recovered = rig.recovery.try_recv().unwrap();
    assert!(recovered.missing.is_empty());
    rig.finish().await;
}

#[tokio::test(start_paused = true)]
async fn disconnected_admission_attaches_after_initial_subscription() {
    let mut rig = Rig::start().await;
    rig.commands
        .send(ConnectionCommand::Unsubscribe(vec![
            market("a", ["101", "102"]),
            market("b", ["201", "202"]),
        ]))
        .unwrap();
    assert_eq!(rig.wire().await["operation"], "unsubscribe");
    let _ = io(rig.socket.next()).await;
    let _ = io(rig.socket.flush()).await;
    let mut replacement = market("a", ["101", "102"]);
    replacement.generation = 7;
    rig.commands
        .send(ConnectionCommand::Subscribe(vec![replacement]))
        .unwrap();
    // The paused clock must release both the close deadline and open limiter.
    step(Duration::from_secs(3)).await;
    let (tcp, _) = io(rig.listener.accept()).await.unwrap();
    rig.socket = io(accept_async(tcp)).await.unwrap();
    assert_eq!(rig.wire().await["type"], "market");
    while let Some(message) = rig.input.recv().now_or_never().flatten() {
        if matches!(message, WorkerMessage::SourceAttached { generation: 7, .. }) {
            rig.finish().await;
            return;
        }
    }
    let message = io(rig.input.recv()).await.unwrap();
    assert!(matches!(
        message,
        WorkerMessage::SourceAttached { generation: 7, .. }
    ));
    rig.finish().await;
}
