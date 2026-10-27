//! Market-channel WebSocket messages (SPEC sections 6 and 7).

use std::borrow::Cow;

use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;

use crate::model::{ListingId, MarketId, MarketLifecycleEvent, NewMarket, OrderBookId};

pub const MARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Heartbeat text sent every 10 s (`protocol::PING_INTERVAL`); the server answers `PONG`.
pub const PING: &str = "PING";
pub const PONG: &str = "PONG";

/// First message on an order book connection: custom features off.
pub fn book_subscription<'a>(assets: impl IntoIterator<Item = &'a OrderBookId>) -> String {
    let assets: Vec<&str> = assets.into_iter().map(|id| &*id.0).collect();
    json!({"type": "market", "assets_ids": assets, "initial_dump": true}).to_string()
}

/// First message on a lifecycle connection: no assets, custom features on,
/// so the socket receives lifecycle notifications.
pub fn lifecycle_subscription() -> String {
    json!({
        "type": "market",
        "assets_ids": [],
        "initial_dump": false,
        "custom_feature_enabled": true,
    })
    .to_string()
}

/// Adds assets on a live connection.
pub fn subscribe<'a>(assets: impl IntoIterator<Item = &'a OrderBookId>) -> String {
    operation("subscribe", assets)
}

/// Removes assets on a live connection.
pub fn unsubscribe<'a>(assets: impl IntoIterator<Item = &'a OrderBookId>) -> String {
    operation("unsubscribe", assets)
}

fn operation<'a>(operation: &str, assets: impl IntoIterator<Item = &'a OrderBookId>) -> String {
    let assets: Vec<&str> = assets.into_iter().map(|id| &*id.0).collect();
    json!({"operation": operation, "assets_ids": assets}).to_string()
}

#[derive(Deserialize)]
struct LifecycleEvent<'a> {
    #[serde(borrow, default)]
    event_type: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct NewMarketEvent<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
    #[serde(borrow)]
    market: Cow<'a, str>,
    #[serde(borrow)]
    assets_ids: Vec<Cow<'a, str>>,
}

/// What a lifecycle frame held.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LifecycleFrame {
    pub events: Vec<MarketLifecycleEvent>,
    /// Recognized lifecycle events that could not be decoded.
    pub malformed: u32,
    /// Other events; dropped.
    pub ignored: u32,
}

/// Decodes additions and resolutions in frame order. The new-market
/// `active` flag is deliberately ignored (SPEC section 7).
pub fn decode_lifecycle_frame(text: &str) -> LifecycleFrame {
    let mut out = LifecycleFrame::default();
    let objects: Vec<&RawValue> = match text.trim_start().as_bytes().first() {
        Some(b'[') => serde_json::from_str(text).unwrap_or_default(),
        Some(b'{') => serde_json::from_str(text)
            .map(|o| vec![o])
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    if objects.is_empty() {
        out.ignored += 1;
        return out;
    }
    for object in objects {
        let kind = serde_json::from_str::<LifecycleEvent>(object.get())
            .ok()
            .and_then(|event| event.event_type);
        match kind.as_deref() {
            Some("new_market") => {
                let Ok(event) = serde_json::from_str::<NewMarketEvent>(object.get()) else {
                    out.malformed += 1;
                    continue;
                };
                let Ok(gamma_id) = event.id.parse::<u64>() else {
                    out.malformed += 1;
                    continue;
                };
                out.events.push(MarketLifecycleEvent::NewMarket(NewMarket {
                    market: MarketId::from(event.market.as_ref()),
                    listing_id: ListingId(gamma_id.to_string()),
                    order_books: event
                        .assets_ids
                        .iter()
                        .map(|id| OrderBookId::from(id.as_ref()))
                        .collect(),
                }));
            }
            Some("market_resolved") => {
                #[derive(Deserialize)]
                struct Resolution<'a> {
                    #[serde(borrow)]
                    market: Cow<'a, str>,
                }
                match serde_json::from_str::<Resolution>(object.get()) {
                    Ok(event) if !event.market.is_empty() => {
                        out.events
                            .push(MarketLifecycleEvent::Resolved(MarketId::from(
                                event.market.as_ref(),
                            )));
                    }
                    _ => out.malformed += 1,
                }
            }
            _ => out.ignored += 1,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_messages() {
        let books = [OrderBookId::from("11"), OrderBookId::from("12")];
        assert_eq!(
            book_subscription(&books),
            r#"{"assets_ids":["11","12"],"initial_dump":true,"type":"market"}"#
        );
        assert_eq!(
            subscribe(&books[..1]),
            r#"{"assets_ids":["11"],"operation":"subscribe"}"#
        );
        assert_eq!(
            unsubscribe(&books[1..]),
            r#"{"assets_ids":["12"],"operation":"unsubscribe"}"#
        );
        assert!(lifecycle_subscription().contains(r#""custom_feature_enabled":true"#));
    }

    #[test]
    fn decodes_new_market() {
        // Frame recorded from a live empty-asset subscription.
        let frame = r#"{"event_type":"new_market","id":"6164380","active":false,
            "market":"0x9a75879da7b451151eaffdbefc7d4c90e5d0abfb6f935faac218436491268b23",
            "assets_ids":["139333348134971982748983242661355363022","515340106024108428913004"],
            "timestamp":"1792885710857"}"#;
        let decoded = decode_lifecycle_frame(frame);
        assert_eq!(decoded.events.len(), 1);
        let MarketLifecycleEvent::NewMarket(market) = &decoded.events[0] else {
            panic!("expected new market");
        };
        assert_eq!(market.listing_id, ListingId::from("6164380"));
        assert_eq!(market.order_books.len(), 2);

        let mixed = decode_lifecycle_frame(
            r#"[{"event_type":"market_resolved"},{"event_type":"new_market","id":"x","market":"0x1","assets_ids":[]}]"#,
        );
        assert_eq!((mixed.ignored, mixed.malformed), (0, 2));
        assert_eq!(decode_lifecycle_frame("PONG").ignored, 1);
    }
    #[test]
    fn preserves_addition_and_resolution_order() {
        let frame = decode_lifecycle_frame(
            r#"[
            {"event_type":"market_resolved","market":"0x1"},
            {"event_type":"new_market","id":"1","market":"0x1","assets_ids":["11","12"]},
            {"event_type":"market_resolved","market":"0x1"},
            {"event_type":"market_resolved","market":""},
            {"event_type":"other"}
        ]"#,
        );
        assert_eq!(
            frame.events,
            vec![
                MarketLifecycleEvent::Resolved(MarketId::from("0x1")),
                MarketLifecycleEvent::NewMarket(NewMarket {
                    market: MarketId::from("0x1"),
                    listing_id: ListingId::from("1"),
                    order_books: vec![OrderBookId::from("11"), OrderBookId::from("12")],
                }),
                MarketLifecycleEvent::Resolved(MarketId::from("0x1")),
            ]
        );
        assert_eq!(frame.malformed, 1);
        assert_eq!(frame.ignored, 1);
    }
}
