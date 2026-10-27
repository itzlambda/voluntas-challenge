mod common;

use std::sync::Arc;
use std::time::Duration;

use engineering_challenge::core::book_connection::{
    BookConnectionContext, ConnectionCommand, run_book_connection,
};
use engineering_challenge::core::capture::Capture;
use engineering_challenge::core::config::Config;
use engineering_challenge::core::decode_log::DecodeErrorLog;
use engineering_challenge::core::fixture::{FixtureEvent, ProtocolCommand};
use engineering_challenge::core::metrics::Metrics;
use engineering_challenge::core::pipeline::{Pipeline, replay};
use engineering_challenge::core::rate_limit::RateLimiter;
use engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol;
use engineering_challenge::model::{
    ConnectionId, MarketBooks, MarketId, OrderBookId, WorkerMessage,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

async fn subscription(socket: &mut WebSocketStream<TcpStream>) {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match message {
            Message::Text(text) if text == "PING" => {
                socket.send(Message::Text("PONG".into())).await.unwrap();
            }
            Message::Text(text) => {
                let message: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(message["type"], "market");
                return;
            }
            _ => panic!("expected subscription"),
        }
    }
}
fn snapshot(asset: &str, qty: i64, timestamp: &str) -> Value {
    json!({"event_type":"book","market":"m","asset_id":asset,"bids":[{"price":"0.5","size":qty.to_string()}],"asks":[],"timestamp":timestamp})
}
fn change(qty: i64, timestamp: &str) -> Value {
    json!({"event_type":"price_change","market":"m","price_changes":[{"asset_id":"a","side":"BUY","price":"0.5","size":qty.to_string()}],"timestamp":timestamp})
}

#[tokio::test(start_paused = true)]
async fn delayed_retry_capture_ignores_gap_books_and_restores_them_on_request_or_reconnect() {
    for reconnect in [false, true] {
        let books = MarketBooks {
            generation: 0,
            market: MarketId::from("m"),
            order_books: [OrderBookId::from("a"), OrderBookId::from("b")],
        };
        let command = |command| FixtureEvent::ProtocolCommand {
            connection: 0,
            command,
        };
        let frame = |asset, qty| FixtureEvent::Frame {
            connection: 0,
            received_at: chrono::DateTime::from_timestamp_millis(1000).unwrap(),
            text: snapshot(asset, qty, "100").to_string(),
        };
        let mut events = vec![
            FixtureEvent::AddMarket {
                generation: 0,
                market: "m".into(),
                order_books: vec!["a".into(), "b".into()],
                connection: 0,
                standby: None,
            },
            FixtureEvent::SessionStart { connection: 0 },
            command(ProtocolCommand::Subscribe {
                books: vec![books.clone()],
                initial: true,
            }),
            frame("a", 10),
            command(ProtocolCommand::PrepareBooks {
                books: vec![OrderBookId::from("b")],
            }),
            frame("b", 11),
        ];
        if reconnect {
            events.extend([
                FixtureEvent::MarkUnsynced {
                    connection: 0,
                    reason: engineering_challenge::model::UnsyncReason::RemoteClose,
                },
                FixtureEvent::SessionStart { connection: 0 },
                command(ProtocolCommand::Subscribe {
                    books: vec![books],
                    initial: true,
                }),
            ]);
        } else {
            events.push(command(ProtocolCommand::RequestBooks {
                books: vec![OrderBookId::from("b")],
            }));
        }
        events.extend([frame("b", 20), FixtureEvent::Advance { millis: 1 }]);
        let text = events
            .iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        for run in common::check(common::fixture(&text)).await {
            assert_eq!(
                run.records["b"].len(),
                1,
                "in-flight gap baseline must be ignored"
            );
            assert!(
                matches!(&run.records["b"][0], common::Record::Snapshot { bids, .. } if bids == &vec![(500_000, 20_000_000)])
            );
        }
    }
}

#[tokio::test]
async fn polymarket_capture_with_control_frames_and_decode_fault_replays_and_matches_oracle() {
    tokio::time::timeout(Duration::from_secs(20), check_capture())
        .await
        .unwrap();
}

async fn check_capture() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let config = Config {
        workers: 1,
        output: temp.path().join("live.sbe"),
        decode_error_log: temp.path().join("decode.jsonl"),
        ..Config::default()
    };
    let pipeline = Pipeline::start(&config).unwrap();
    let books = MarketBooks {
        generation: 0,
        market: MarketId::from("m"),
        order_books: [OrderBookId::from("a"), OrderBookId::from("b")],
    };
    pipeline
        .workers()
        .send(WorkerMessage::AddMarket {
            generation: 0,
            market: books.market.clone(),
            order_books: books.order_books.to_vec(),
            sources: engineering_challenge::model::MarketSources::single(ConnectionId(0)),
        })
        .await
        .unwrap();
    let (capture, file) = Capture::create(&temp.path().join("capture.jsonl")).unwrap();
    capture.record(&FixtureEvent::AddMarket {
        generation: 0,
        market: "m".into(),
        order_books: vec!["a".into(), "b".into()],
        connection: 0,
        standby: None,
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let protocol = PolymarketProtocol::new(format!("ws://{}", listener.local_addr().unwrap()));
    let cancel = CancellationToken::new();
    let metrics = Metrics::default();
    let ctx = Arc::new(BookConnectionContext {
        recovery: None,
        workers: pipeline.workers().clone(),
        decode_log: Arc::new(DecodeErrorLog::open(&config.decode_error_log).unwrap()),
        open_limiter: Arc::new(RateLimiter::per_second(1000.)),
        capture: Some(capture.clone()),
        cancel: cancel.clone(),
        max_frame_bytes: 1 << 20,
        silence_limit: Duration::from_secs(180),
        metrics: metrics.clone(),
    });
    let (commands, rx) = mpsc::unbounded_channel();
    commands
        .send(ConnectionCommand::Subscribe(vec![books]))
        .unwrap();
    let connection = tokio::spawn(run_book_connection(ConnectionId(0), rx, ctx, protocol));
    let (tcp, _) = listener.accept().await.unwrap();
    let mut first = accept_async(tcp).await.unwrap();
    subscription(&mut first).await;
    first.send(Message::Ping(vec![7].into())).await.unwrap();
    loop {
        match first.next().await.unwrap().unwrap() {
            Message::Pong(bytes) => {
                assert_eq!(bytes.as_ref(), &[7]);
                break;
            }
            Message::Text(text) if text == "PING" => {
                first.send(Message::Text("PONG".into())).await.unwrap();
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
    first
        .send(Message::Text(
            json!([snapshot("a", 10, "100"), snapshot("b", 11, "100")])
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let malformed = json!({"event_type":"book","market":"m","asset_id":"b","timestamp":"101","bids":[{"price":"bad","size":"1"}],"asks":[]});
    first
        .send(Message::Text(
            json!([change(12, "101"), malformed]).to_string().into(),
        ))
        .await
        .unwrap();
    loop {
        match first.next().await.unwrap().unwrap() {
            Message::Close(_) => {
                let _ = first.flush().await;
                break;
            }
            Message::Text(text) if text == "PING" => {
                first.send(Message::Text("PONG".into())).await.unwrap();
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
    let (tcp, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut second = accept_async(tcp).await.unwrap();
    subscription(&mut second).await;
    second.send(Message::Pong(vec![8].into())).await.unwrap();
    second
        .send(Message::Text(
            json!([snapshot("a", 20, "102"), snapshot("b", 11, "102")])
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    second
        .send(Message::Text(change(23, "103").to_string().into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if metrics
                .render()
                .unwrap()
                .text
                .lines()
                .any(|line| line == "collector_events_total 6")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    connection.await.unwrap().unwrap();
    drop(commands);
    capture.record(&FixtureEvent::Advance { millis: 1 });
    drop(capture);
    file.finish().unwrap();
    let live = pipeline.finish().await.unwrap();
    assert_eq!(live.workers[0].unsync_marks, 1);
    let events =
        common::fixture(&std::fs::read_to_string(temp.path().join("capture.jsonl")).unwrap());
    assert!(
        events
            .iter()
            .any(|event| matches!(event, FixtureEvent::PingFrame { .. }))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, FixtureEvent::PongFrame { .. }))
    );
    common::audit_live_sbe(&events, &config.output);
    let replay_config = Config {
        output: temp.path().join("replayed.sbe"),
        ..config.clone()
    };
    let replayed = replay(&replay_config, events.clone(), PolymarketProtocol::default)
        .await
        .unwrap();
    assert_eq!(replayed.pipeline.workers[0].unsync_marks, 1);
    assert_eq!(
        std::fs::read(&config.output).unwrap(),
        std::fs::read(&replay_config.output).unwrap()
    );
    for run in common::check(events).await {
        assert_eq!(
            common::sum(&run.totals, |worker| worker.unsync_marks),
            run.workers as u64
        );
        assert_eq!(run.decode_log.len(), 1);
        assert!(run.records.contains_key("a"));
    }
}
