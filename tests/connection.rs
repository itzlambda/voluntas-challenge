//! A book connection against a local fake WebSocket server: subscription
//! messages, routing, and the reconnect ordering rules (SPEC section 8).

use engineering_challenge::core::metrics::{Component, Metrics};
use std::sync::Arc;
use std::time::Duration;

use engineering_challenge::core::book_connection::{
    BookConnectionContext, ConnectionCommand, run_book_connection,
};
use engineering_challenge::core::decode_log::DecodeErrorLog;
use engineering_challenge::core::rate_limit::RateLimiter;
use engineering_challenge::core::routing::{Workers, worker_index};
use engineering_challenge::exchange::polymarket::lifecycle::run_lifecycle_connection;
use engineering_challenge::model::{
    ConnectionId, MarketBooks, MarketId, OrderBookEvent, OrderBookId, UnsyncReason, WorkerMessage,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{WebSocketStream, accept_async};
use tokio_util::sync::CancellationToken;

const MARKET: &str = "0x26f92ac5a446296cf4b454e14694c3aaeb313cb84d8e258eff0d0dc491c91113";
const STEP: Duration = Duration::from_secs(10);
const SILENCE: Duration = Duration::from_secs(6);

type Server = WebSocketStream<tokio::net::TcpStream>;

/// The next JSON message from the client, skipping `PING`.
async fn next_json(server: &mut Server) -> Value {
    timeout(STEP, read_json(server))
        .await
        .expect("client message")
}

/// Like `next_json` without a timeout, for tests on a paused clock, where a
/// timeout would expire while bytes are still in flight.
async fn read_json(server: &mut Server) -> Value {
    loop {
        let message = server.next().await.unwrap().unwrap();
        if let Message::Text(text) = message
            && text.as_str() != "PING"
        {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

fn book_frame(asset: &str) -> String {
    format!(
        r#"{{"event_type":"book","market":"{MARKET}","asset_id":"{asset}","timestamp":"1792777248021",
        "bids":[{{"price":"0.4","size":"10"}}],"asks":[{{"price":"0.6","size":"5"}}]}}"#
    )
}

async fn next_message(rx: &mut mpsc::Receiver<WorkerMessage>) -> WorkerMessage {
    timeout(STEP, rx.recv())
        .await
        .expect("worker message")
        .expect("channel open")
}

#[tokio::test]
async fn subscribes_routes_and_resyncs_after_reconnect() {
    check_reconnect(false).await;
}

#[tokio::test]
async fn slow_consumer_closes_reconnect_with_the_same_subscriptions() {
    check_reconnect(true).await;
}

async fn check_reconnect(slow_consumer: bool) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());

    let (workers, mut inputs): (Vec<_>, Vec<_>) = (0..2).map(|_| mpsc::channel(64)).unzip();
    let temp = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let ctx = Arc::new(BookConnectionContext {
        recovery: None,
        workers: Workers::from(workers),
        decode_log: Arc::new(DecodeErrorLog::open(&temp.path().join("decode.jsonl")).unwrap()),
        open_limiter: Arc::new(RateLimiter::per_second(1000.0)),
        capture: None,
        cancel: cancel.clone(),
        max_frame_bytes: 1 << 20,
        silence_limit: Duration::from_secs(30),
        metrics: Metrics::default(),
    });
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(run_book_connection(
        ConnectionId(3),
        commands_rx,
        ctx,
        engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::new(url.clone()),
    ));

    let market = MarketId::from(MARKET);
    let books = [OrderBookId::from("101"), OrderBookId::from("102")];
    commands
        .send(ConnectionCommand::Subscribe(vec![MarketBooks {
            generation: 0,
            market: market.clone(),
            order_books: books.clone(),
        }]))
        .unwrap();
    let worker = worker_index(&market, 2);

    // First session: subscription with both assets, then data.
    let (tcp, _) = timeout(STEP, listener.accept()).await.unwrap().unwrap();
    let mut server = accept_async(tcp).await.unwrap();
    let subscription = next_json(&mut server).await;
    assert_eq!(subscription["type"], "market");
    assert_eq!(subscription["initial_dump"], true);
    let mut assets: Vec<&str> = subscription["assets_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assets.sort_unstable();
    assert_eq!(assets, ["101", "102"]);

    server
        .send(Message::Text(book_frame("101").into()))
        .await
        .unwrap();
    let WorkerMessage::MarketData(data) = next_message(&mut inputs[worker]).await else {
        panic!("expected market data");
    };
    assert_eq!(data.connection, ConnectionId(3));
    assert!(
        matches!(data.event, OrderBookEvent::Snapshot { ref order_book, .. } if order_book.0.as_ref() == "101")
    );

    // Every worker gets MarkUnsynced after the data read before the gap.
    server
        .send(Message::Text(book_frame("102").into()))
        .await
        .unwrap();
    if slow_consumer {
        server
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Again,
                reason: "slow consumer: send buffer full".into(),
            })))
            .await
            .unwrap();
    }
    let server = if slow_consumer {
        Some(server)
    } else {
        drop(server);
        None
    };
    assert!(matches!(
        next_message(&mut inputs[worker]).await,
        WorkerMessage::MarketData(_)
    ));
    for input in &mut inputs {
        assert_eq!(
            next_message(input).await,
            WorkerMessage::MarkUnsynced {
                connection: ConnectionId(3),
                reason: if slow_consumer {
                    UnsyncReason::RemoteClose
                } else {
                    UnsyncReason::IoError
                }
            }
        );
    }

    drop(server);

    // It reconnects with the same ID and resubscribes the whole asset set.
    let (tcp, _) = timeout(STEP, listener.accept()).await.unwrap().unwrap();
    let mut server = accept_async(tcp).await.unwrap();
    let resubscription = next_json(&mut server).await;
    assert_eq!(resubscription["assets_ids"], subscription["assets_ids"]);

    if slow_consumer {
        // Repeated 1013 closes preserve placement and the full subscription.
        for _ in 0..2 {
            server
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Again,
                    reason: "slow consumer: send buffer full".into(),
                })))
                .await
                .unwrap();
            for input in &mut inputs {
                assert_eq!(
                    next_message(input).await,
                    WorkerMessage::MarkUnsynced {
                        connection: ConnectionId(3),
                        reason: UnsyncReason::RemoteClose,
                    }
                );
            }
            let (tcp, _) = timeout(STEP, listener.accept()).await.unwrap().unwrap();
            server = accept_async(tcp).await.unwrap();
            assert_eq!(next_json(&mut server).await, subscription);
        }
    }

    // Removing the last market unsubscribes and closes the socket.
    commands
        .send(ConnectionCommand::Unsubscribe(vec![MarketBooks {
            generation: 0,
            market: market.clone(),
            order_books: books.clone(),
        }]))
        .unwrap();
    let unsubscribe = next_json(&mut server).await;
    assert_eq!(unsubscribe["operation"], "unsubscribe");
    assert_eq!(unsubscribe["assets_ids"].as_array().unwrap().len(), 2);

    cancel.cancel();
    timeout(STEP, task).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn lifecycle_reconnects_and_resubscribes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let (events, _events_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(run_lifecycle_connection(
        0,
        url,
        Arc::new(RateLimiter::per_second(1000.0)),
        events,
        cancel.clone(),
        Metrics::default().register(Component::Connection(
            engineering_challenge::core::metrics::ConnectionKind::MarketDiscovery,
        )),
    ));
    let (tcp, _) = timeout(STEP, listener.accept()).await.unwrap().unwrap();
    let mut server = accept_async(tcp).await.unwrap();
    let subscription = next_json(&mut server).await;
    assert_eq!(subscription["assets_ids"], serde_json::json!([]));

    drop(server);
    let (tcp, _) = timeout(STEP, listener.accept()).await.unwrap().unwrap();
    let mut server = accept_async(tcp).await.unwrap();
    assert_eq!(next_json(&mut server).await, subscription);

    cancel.cancel();
    let totals = timeout(STEP, task).await.unwrap().unwrap();
    assert_eq!(totals.sessions, 2);
}

/// A synthetic interruption: the test server holds everything, including
/// PONG, for less than the silence limit. The connection must stay up and
/// receive the held data. Only silence beyond the limit reconnects. Scaled
/// down: a 6 s limit instead of 30 s.
#[tokio::test]
async fn reconnects_only_after_the_full_no_frame_deadline() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (workers, mut inputs): (Vec<_>, Vec<_>) = (0..1).map(|_| mpsc::channel(64)).unzip();
    let temp = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let ctx = Arc::new(BookConnectionContext {
        recovery: None,
        workers: Workers::from(workers),
        decode_log: Arc::new(DecodeErrorLog::open(&temp.path().join("decode.jsonl")).unwrap()),
        open_limiter: Arc::new(RateLimiter::per_second(1000.0)),
        capture: None,
        cancel: cancel.clone(),
        max_frame_bytes: 1 << 20,
        silence_limit: SILENCE,
        metrics: Metrics::default(),
    });
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(run_book_connection(
        ConnectionId(1),
        commands_rx,
        ctx,
        engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::new(url.clone()),
    ));
    let market = MarketId::from(MARKET);
    commands
        .send(ConnectionCommand::Subscribe(vec![MarketBooks {
            generation: 0,
            market,
            order_books: [OrderBookId::from("101"), OrderBookId::from("102")],
        }]))
        .unwrap();

    let (tcp, _) = listener.accept().await.unwrap();
    let mut server = accept_async(tcp).await.unwrap();
    read_json(&mut server).await;
    server
        .send(Message::Text(book_frame("101").into()))
        .await
        .unwrap();
    assert!(matches!(
        inputs[0].recv().await,
        Some(WorkerMessage::MarketData(_))
    ));

    // The pause: nothing for 4 s, not even PONG. Then the held data.
    tokio::time::sleep(Duration::from_secs(4)).await;
    server
        .send(Message::Text(book_frame("102").into()))
        .await
        .unwrap();
    server.send(Message::Text("PONG".into())).await.unwrap();
    match inputs[0].recv().await {
        Some(WorkerMessage::MarketData(_)) => {}
        other => panic!("expected the held data, not a reconnect: {other:?}"),
    }

    // Once the full no-frame deadline expires, the connection reconnects.
    assert_eq!(
        timeout(Duration::from_secs(30), inputs[0].recv())
            .await
            .expect("stall detected"),
        Some(WorkerMessage::MarkUnsynced {
            connection: ConnectionId(1),
            reason: UnsyncReason::Stalled
        })
    );
    let (tcp, _) = listener.accept().await.unwrap();
    let mut server = accept_async(tcp).await.unwrap();
    assert_eq!(
        read_json(&mut server).await["assets_ids"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    cancel.cancel();
    task.await.unwrap().unwrap();
}

/// Successful control frames keep the socket healthy, and inbound traffic
/// does not suppress the collector's ten-second text heartbeat.
#[tokio::test]
async fn control_frames_reset_silence_and_heartbeats_continue_while_receiving() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sender, mut input) = mpsc::channel(64);
    let temp = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let ctx = Arc::new(BookConnectionContext {
        recovery: None,
        workers: Workers::from(vec![sender]),
        decode_log: Arc::new(DecodeErrorLog::open(&temp.path().join("decode.jsonl")).unwrap()),
        open_limiter: Arc::new(RateLimiter::per_second(1000.0)),
        capture: None,
        cancel: cancel.clone(),
        max_frame_bytes: 1 << 20,
        silence_limit: SILENCE,
        metrics: Metrics::default(),
    });
    let (commands, receiver) = mpsc::unbounded_channel();
    let task = tokio::spawn(run_book_connection(
        ConnectionId(1),
        receiver,
        ctx,
        engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::new(format!(
            "ws://{}",
            listener.local_addr().unwrap()
        )),
    ));
    commands
        .send(ConnectionCommand::Subscribe(vec![MarketBooks {
            generation: 0,
            market: MarketId::from(MARKET),
            order_books: [OrderBookId::from("101"), OrderBookId::from("102")],
        }]))
        .unwrap();
    let (tcp, _) = listener.accept().await.unwrap();
    let mut server = accept_async(tcp).await.unwrap();
    next_json(&mut server).await;
    for asset in ["101", "102"] {
        server
            .send(Message::Text(book_frame(asset).into()))
            .await
            .unwrap();
        assert!(matches!(
            next_message(&mut input).await,
            WorkerMessage::MarketData(_)
        ));
    }
    let started = tokio::time::Instant::now();
    let mut traffic = tokio::time::interval(Duration::from_millis(50));
    let mut pings = 0;
    timeout(Duration::from_secs(14), async {
        loop {
            tokio::select! {
                _ = traffic.tick() => {
                    server.send(Message::Pong(vec![1].into())).await.unwrap();
                }
                frame = server.next() => {
                    match frame.unwrap().unwrap() {
                        Message::Text(text) if text == "PING" => {
                            pings += 1;
                            if pings == 2 { break; }
                        }
                        other => panic!("unexpected collector message: {other:?}"),
                    }
                }
                message = input.recv() => panic!("control traffic must prevent reset: {message:?}"),
            }
        }
    })
    .await
    .expect("heartbeat continues during traffic");
    assert!(started.elapsed() >= Duration::from_secs(9));
    cancel.cancel();
    task.await.unwrap().unwrap();
}
