use std::collections::HashSet;
use std::time::Duration;

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use fake_exchange::{config::Config, server::Server};

fn config() -> Config {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut config = Config::parse_from(["fake-exchange"]);
    config.ws_port = 0;
    config.gamma_port = 0;
    config.rate = 0.0;
    config.repeat_book_rate = 0.0;
    config.noise_rate = 0.0;
    config
}
async fn send(socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn receive(socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> Value {
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(frame.to_text().unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_socket_subscription_semantics() {
    let server = Server::start(config()).await.unwrap();
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/ws/market", server.ws_addr))
            .await
            .unwrap();
    send(
        &mut socket,
        json!({"type":"market","assets_ids":["2","3","4"],"initial_dump":true}),
    )
    .await;
    let first = receive(&mut socket).await;
    assert_eq!(first.as_array().unwrap().len(), 3);
    send(
        &mut socket,
        json!({"operation":"subscribe","assets_ids":["2"]}),
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), socket.next())
            .await
            .is_err()
    );
    send(
        &mut socket,
        json!({"operation":"subscribe","assets_ids":["5"]}),
    )
    .await;
    assert_eq!(receive(&mut socket).await["asset_id"], "5");
    send(
        &mut socket,
        json!({"operation":"unsubscribe","assets_ids":["2"]}),
    )
    .await;
    send(
        &mut socket,
        json!({"operation":"subscribe","assets_ids":["2"]}),
    )
    .await;
    assert_eq!(receive(&mut socket).await["asset_id"], "2");
    socket.send(Message::Text("PING".into())).await.unwrap();
    assert_eq!(
        socket.next().await.unwrap().unwrap().to_text().unwrap(),
        "PONG"
    );
    socket.close(None).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gamma_keyset_cap_and_closed_lookup() {
    let mut config = config();
    config.markets = 237;
    let server = Server::start(config).await.unwrap();
    let http = reqwest::Client::new();
    let mut cursor: Option<String> = None;
    let mut found = HashSet::new();
    let mut pages = 0;
    loop {
        let mut url = reqwest::Url::parse(&format!(
            "http://{}/markets/keyset?closed=false&limit=10000&active=false",
            server.gamma_addr
        ))
        .unwrap();
        if let Some(cursor) = &cursor {
            url.query_pairs_mut().append_pair("after_cursor", cursor);
        }
        let page: Value = http.get(url).send().await.unwrap().json().await.unwrap();
        let markets = page["markets"].as_array().unwrap();
        assert!(markets.len() <= 100);
        if pages == 0 {
            assert_eq!(markets.len(), 100);
        }
        for row in markets {
            assert_eq!(row["closed"], false);
            assert!(found.insert(row["id"].as_str().unwrap().to_owned()));
        }
        pages += 1;
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(found.len(), 237);
    let page: Value = http
        .get(format!(
            "http://{}/markets/keyset?id=1&id=237&closed=any",
            server.gamma_addr
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["markets"].as_array().unwrap().len(), 2);
    let status = http
        .get(format!(
            "http://{}/markets/keyset?closed=any",
            server.gamma_addr
        ))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 422);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_queue_overflow_sends_1013() {
    let mut config = config();
    config.markets = 1;
    config.rate = 100_000.0;
    config.outbound_capacity = 1;
    config.entries_min = 1;
    config.entries_max = 1;
    config.mirror_probability = 0.0;
    let server = Server::start(config).await.unwrap();
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{}", server.ws_addr))
        .await
        .unwrap();
    send(
        &mut socket,
        json!({"type":"market","assets_ids":["2","3"],"initial_dump":true}),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let code = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = socket.next().await {
            if let Message::Close(Some(frame)) = message.unwrap() {
                return u16::from(frame.code);
            }
        }
        0
    })
    .await
    .unwrap();
    assert_eq!(code, 1013);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_hint_and_resolution_close() {
    let mut config = config();
    config.markets = 1;
    config.new_markets_per_second = 10.0;
    config.resolutions_per_second = 5.0;
    let server = Server::start(config).await.unwrap();
    let (mut lifecycle, _) = tokio_tungstenite::connect_async(format!("ws://{}", server.ws_addr))
        .await
        .unwrap();
    send(
        &mut lifecycle,
        json!({"type":"market","assets_ids":[],"custom_feature_enabled":true}),
    )
    .await;
    let (mut books, _) = tokio_tungstenite::connect_async(format!("ws://{}", server.ws_addr))
        .await
        .unwrap();
    send(
        &mut books,
        json!({"type":"market","assets_ids":["2","3"],"initial_dump":true}),
    )
    .await;
    assert!(receive(&mut books).await.is_array());
    let hint = receive(&mut lifecycle).await;
    assert_eq!(hint["event_type"], "new_market");
    let close = tokio::time::timeout(Duration::from_secs(3), books.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Close(Some(close)) = close else {
        panic!("expected resolution close");
    };
    assert_eq!(close.reason, "all subscribed assets resolved");
    let page: Value = reqwest::get(format!(
        "http://{}/markets/keyset?id=1&closed=any",
        server.gamma_addr
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(page["markets"][0]["closed"], true);
    let page: Value = reqwest::get(format!(
        "http://{}/markets/keyset?id=1&closed=false",
        server.gamma_addr
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert!(page["markets"].as_array().unwrap().is_empty());
    server.shutdown().await.unwrap();
}

fn fixed(text: &Value) -> i64 {
    let text = text.as_str().unwrap();
    let (integer, fraction) = text.split_once('.').unwrap_or((text, ""));
    integer.parse::<i64>().unwrap() * 1_000_000 + format!("{fraction:0<6}").parse::<i64>().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_entry_hashes_match_every_intermediate_book() {
    use fake_exchange::book::{Asset, Levels};
    let mut config = config();
    config.markets = 1;
    config.cores = 1;
    config.rate = 100.0;
    config.mirror_probability = 1.0;
    let server = Server::start(config).await.unwrap();
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{}", server.ws_addr))
        .await
        .unwrap();
    send(
        &mut socket,
        json!({"type":"market","assets_ids":["2","3"],"initial_dump":true}),
    )
    .await;
    let snapshot = receive(&mut socket).await;
    let mut books = std::collections::HashMap::new();
    for book in snapshot.as_array().unwrap() {
        let asset = book["asset_id"].as_str().unwrap().parse::<u64>().unwrap();
        books.insert(
            asset,
            Asset {
                asset,
                market: 1,
                created_ms: 0,
                levels: Levels {
                    bids: book["bids"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|l| (fixed(&l["price"]), fixed(&l["size"])))
                        .collect(),
                    asks: book["asks"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|l| (fixed(&l["price"]), fixed(&l["size"])))
                        .collect(),
                },
            },
        );
    }
    let mut last_time = 0;
    for _ in 0..30 {
        let frame = receive(&mut socket).await;
        let time = frame["timestamp"].as_str().unwrap().parse::<i64>().unwrap();
        assert!(time >= last_time);
        last_time = time;
        assert_eq!(frame["event_type"], "price_change");
        let entries = frame["price_changes"].as_array().unwrap();
        assert!(entries.len().is_multiple_of(2));
        for entry in entries {
            let asset = entry["asset_id"].as_str().unwrap().parse::<u64>().unwrap();
            let book = books.get_mut(&asset).unwrap();
            let side = if entry["side"] == "BUY" { 0 } else { 1 };
            let price = fixed(&entry["price"]);
            let size = fixed(&entry["size"]);
            assert!(price > 0 && price < 1_000_000 && price % 10_000 == 0 && size >= 0);
            book.levels.set(side, price, size);
            assert_eq!(entry["hash"], book.hash(time).unwrap());
            if let (Some((bid, _)), Some((ask, _))) = (
                book.levels.bids.last_key_value(),
                book.levels.asks.first_key_value(),
            ) {
                assert!(bid < ask);
            }
            assert!(
                book.levels
                    .bids
                    .values()
                    .chain(book.levels.asks.values())
                    .all(|s| *s > 0)
            );
        }
    }
    socket.close(None).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn healthy_hot_socket_keeps_up_on_one_runtime_thread() {
    let mut config = config();
    config.cores = 1;
    config.markets = 1;
    config.rate = 100_000.0;
    config.outbound_capacity = 128;
    config.entries_min = 1;
    config.entries_max = 1;
    config.mirror_probability = 0.0;
    let server = Server::start(config).await.unwrap();
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{}", server.ws_addr))
        .await
        .unwrap();
    send(
        &mut socket,
        json!({"type":"market","assets_ids":["2","3"],"initial_dump":true}),
    )
    .await;
    assert!(receive(&mut socket).await.is_array());
    for _ in 0..1000 {
        assert_eq!(receive(&mut socket).await["event_type"], "price_change");
    }
    socket.close(None).await.unwrap();
    server.shutdown().await.unwrap();
}
