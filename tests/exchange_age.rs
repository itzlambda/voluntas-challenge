//! Receipt-age sampling follows decoded update messages, not frames or levels.

use chrono::{DateTime, Duration};
use engineering_challenge::core::connection::{ConnectionIds, FrameTotals, handle_frame};
use engineering_challenge::core::decode_log::DecodeErrorLog;
use engineering_challenge::core::routing::Workers;
use engineering_challenge::model::{ConnectionId, MarketId, OrderBookId};
use serde_json::json;
use tokio::sync::mpsc;

#[tokio::test]
async fn samples_live_messages_once_and_preserves_read_timestamp() {
    let mut ids = ConnectionIds::default();
    ids.add(
        &MarketId::from("m"),
        &[OrderBookId::from("a"), OrderBookId::from("b")],
    );
    let (tx, mut rx) = mpsc::channel(32);
    let workers = Workers::from(vec![tx]);
    let temp = tempfile::tempdir().unwrap();
    let log = DecodeErrorLog::open(&temp.path().join("decode.jsonl")).unwrap();
    let received = DateTime::UNIX_EPOCH + Duration::microseconds(1_010_123);
    let change = |timestamp: &str, asset: &str| {
        json!({
            "event_type": "price_change", "market": "m", "timestamp": timestamp,
            "price_changes": [
                {"asset_id": asset, "side": "BUY", "price": "0.4", "size": "2"},
                {"asset_id": "b", "side": "SELL", "price": "0.6", "size": "3"}
            ]
        })
    };
    let snapshot = json!({"event_type":"book", "market":"m", "asset_id":"a",
        "timestamp":"1", "bids":[], "asks":[]});
    let mut malformed = change("1000", "a");
    malformed["price_changes"][1]["side"] = json!("invalid");
    let unknown = json!({"event_type":"price_change", "market":"unknown",
        "timestamp":"1000", "price_changes":[]});
    let empty = json!({"event_type":"price_change", "market":"m",
        "timestamp":"1000", "price_changes":[]});
    let mut totals = FrameTotals::default();
    let mut protocol =
        engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::default();
    for text in [
        json!([snapshot.clone(), change("1000", "a"), change("1020", "a")]).to_string(),
        snapshot.to_string(),
        unknown.to_string(),
        empty.to_string(),
        malformed.to_string(),
        "PONG".into(),
        // A trailing syntax error discards the entire frame, including its samples.
        format!("[{},", change("1000", "a")),
    ] {
        handle_frame(
            &mut protocol,
            &tokio_tungstenite::tungstenite::Message::Text(text.into()),
            ConnectionId(1),
            received,
            std::time::Instant::now(),
            &ids,
            &workers,
            1 << 20,
            &log,
            &mut totals,
            |_| {},
        )
        .await
        .unwrap();
    }
    assert_eq!(totals.exchange_to_receive.count, 1);
    assert_eq!(totals.negative_exchange_ages, 1);
    assert!((totals.exchange_to_receive.sum - 0.010123).abs() < 1e-9);
    assert_eq!(totals.decode_errors, 2);
    let mut updates = 0;
    while let Ok(message) = rx.try_recv() {
        if let engineering_challenge::model::WorkerMessage::MarketData(data) = message {
            assert_eq!(data.received_at, received);
            if let engineering_challenge::model::OrderBookEvent::LevelUpdates(levels) = data.event {
                assert_eq!(levels.len(), 2);
                updates += 1;
            }
        }
    }
    assert_eq!(updates, 2);
}
