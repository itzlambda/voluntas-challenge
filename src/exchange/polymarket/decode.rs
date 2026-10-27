//! Decoding market-channel frames into our order book events (SPEC section 6).
//!
//! A frame is one event object or an array of them. It is parsed in a single
//! pass: each event's fields are read leniently as the parser meets them, so
//! field order doesn't matter and events we don't use never fail. `book` and
//! `price_change` events are then checked strictly. Strings borrow from the
//! frame.

use std::borrow::Cow;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::Side;
use crate::exchange::polymarket::number::{parse_fixed6, parse_millis};
use crate::model::{
    ConnectionId, LevelUpdate, MarketData, OrderBookEvent, Price, Qty, SubscribedIds,
};

/// The result of decoding one frame from an order book connection.
#[derive(Debug, Default)]
pub struct DecodedFrame {
    /// Book events for subscribed markets, in frame order.
    pub events: Vec<MarketData>,
    /// Events or `price_change` entries for IDs this connection doesn't hold.
    pub unsubscribed: u32,
    /// Events that don't describe depth or aren't recognized; dropped.
    pub ignored: u32,
    /// `tick_size_change` events; logged by the caller, levels are unaffected.
    pub tick_size_changes: u32,
    /// Set when a recognized `book` or `price_change` failed to parse. That
    /// event is rejected whole and decoding stops; `events` holds the ones
    /// before it. Invalid JSON anywhere in the frame rejects the whole frame.
    pub error: Option<String>,
}

/// Decodes one text frame (not `PONG`, which the connection handles first).
pub fn decode_frame(
    text: &str,
    connection: ConnectionId,
    received_at: DateTime<Utc>,
    ids: &(impl SubscribedIds + ?Sized),
    max_frame_bytes: usize,
) -> DecodedFrame {
    if text.len() > max_frame_bytes {
        return DecodedFrame {
            error: Some(format!(
                "frame of {} bytes exceeds the {max_frame_bytes}-byte limit",
                text.len()
            )),
            ..DecodedFrame::default()
        };
    }

    // Only JSON objects and arrays can be book messages. Other text (for
    // example a server error string) is counted and dropped.
    let shape = match text.trim_start().as_bytes().first() {
        Some(b'[') => "array",
        Some(b'{') => "object",
        _ => {
            return DecodedFrame {
                ignored: 1,
                ..DecodedFrame::default()
            };
        }
    };

    let mut decoder = FrameDecoder {
        connection,
        received_at,
        ids,
        out: DecodedFrame::default(),
    };
    let mut json = serde_json::Deserializer::from_str(text);
    match (&mut decoder)
        .deserialize(&mut json)
        .and_then(|()| json.end())
    {
        Ok(()) => decoder.out,
        // Events decoded before the syntax error are discarded too.
        Err(error) => DecodedFrame {
            error: Some(format!("invalid JSON {shape}: {error}")),
            ..DecodedFrame::default()
        },
    }
}

/// Decodes each event of one frame as the parser reaches it.
struct FrameDecoder<'i, I: ?Sized> {
    connection: ConnectionId,
    received_at: DateTime<Utc>,
    ids: &'i I,
    out: DecodedFrame,
}

impl<I: SubscribedIds + ?Sized> FrameDecoder<'_, I> {
    /// Applies one array element or the frame's only object. `None` means the
    /// element wasn't a JSON object.
    fn on_event(&mut self, event: Option<RawEvent<'_>>) {
        let Some(event) = event else {
            self.out.ignored += 1;
            return;
        };
        let result = match event.event_type.as_deref() {
            Some("book") => self.decode_book(event),
            Some("price_change") => self.decode_price_change(event),
            Some("tick_size_change") => {
                self.out.tick_size_changes += 1;
                Ok(())
            }
            _ => {
                self.out.ignored += 1;
                Ok(())
            }
        };
        if let Err(error) = result {
            self.out.error = Some(error);
        }
    }

    fn decode_book(&mut self, event: RawEvent<'_>) -> Result<(), String> {
        let market = required(event.market, "book", "market")?;
        let asset_id = required(event.asset_id, "book", "asset_id")?;
        let timestamp = required(event.timestamp, "book", "timestamp")?;
        let bids = required(event.bids, "book", "bids")?;
        let asks = required(event.asks, "book", "asks")?;
        let exchange_ts = parse_millis(&timestamp)?;
        let bids = parse_levels(&bids)?;
        let asks = parse_levels(&asks)?;

        let (Some(market), Some(order_book)) =
            (self.ids.market(&market), self.ids.order_book(&asset_id))
        else {
            self.out.unsubscribed += 1;
            return Ok(());
        };
        self.out.events.push(MarketData {
            received_instant: None,
            market,
            connection: self.connection,
            received_at: self.received_at,
            exchange_ts,
            event: OrderBookEvent::Snapshot {
                timestamp_comparable: true,
                order_book,
                bids,
                asks,
            },
        });
        Ok(())
    }

    fn decode_price_change(&mut self, event: RawEvent<'_>) -> Result<(), String> {
        let market = required(event.market, "price_change", "market")?;
        let timestamp = required(event.timestamp, "price_change", "timestamp")?;
        let changes = required(event.price_changes, "price_change", "price_changes")?;
        let exchange_ts = parse_millis(&timestamp)?;
        let mut parsed = Vec::with_capacity(changes.len());
        for change in &changes {
            let asset_id = required(change.asset_id.as_deref(), "price_change", "asset_id")?;
            let side = match required(change.side.as_deref(), "price_change", "side")? {
                "BUY" => Side::Buy,
                "SELL" => Side::Sell,
                other => return Err(format!("unknown side {other:?}")),
            };
            let price = Price(parse_fixed6(required(
                change.price.as_deref(),
                "price_change",
                "price",
            )?)?);
            let qty = Qty(parse_fixed6(required(
                change.size.as_deref(),
                "price_change",
                "size",
            )?)?);
            parsed.push((asset_id, side, price, qty));
        }

        let Some(market) = self.ids.market(&market) else {
            self.out.unsubscribed += 1;
            return Ok(());
        };
        let mut updates = Vec::with_capacity(parsed.len());
        for (asset_id, side, price, qty) in parsed {
            match self.ids.order_book(asset_id) {
                Some(order_book) => updates.push(LevelUpdate {
                    order_book,
                    side,
                    price,
                    change: crate::model::QuantityChange::Set(qty),
                }),
                None => self.out.unsubscribed += 1,
            }
        }
        if !updates.is_empty() {
            self.out.events.push(MarketData {
                received_instant: None,
                market,
                connection: self.connection,
                received_at: self.received_at,
                exchange_ts,
                event: OrderBookEvent::LevelUpdates(updates),
            });
        }
        Ok(())
    }
}

/// A field that a `book` or `price_change` must carry with the right type.
fn required<T>(field: Option<T>, event: &str, name: &str) -> Result<T, String> {
    field.ok_or_else(|| format!("{event}: missing or invalid `{name}`"))
}

fn parse_levels(levels: &[Entry]) -> Result<Vec<(Price, Qty)>, String> {
    levels
        .iter()
        .map(|level| {
            let price = required(level.price.as_deref(), "book", "price")?;
            let size = required(level.size.as_deref(), "book", "size")?;
            Ok((Price(parse_fixed6(price)?), Qty(parse_fixed6(size)?)))
        })
        .collect()
}

/// The fields of one event object that any event type we use carries. A field
/// is `None` when it is missing or holds a different JSON type.
#[derive(Default)]
struct RawEvent<'a> {
    event_type: Option<Cow<'a, str>>,
    market: Option<Cow<'a, str>>,
    asset_id: Option<Cow<'a, str>>,
    timestamp: Option<Cow<'a, str>>,
    bids: Option<Vec<Entry<'a>>>,
    asks: Option<Vec<Entry<'a>>>,
    price_changes: Option<Vec<Entry<'a>>>,
}

/// A book level (`price`, `size`) or a `price_change` entry (also
/// `asset_id` and `side`).
#[derive(Default)]
struct Entry<'a> {
    asset_id: Option<Cow<'a, str>>,
    price: Option<Cow<'a, str>>,
    size: Option<Cow<'a, str>>,
    side: Option<Cow<'a, str>>,
}

impl<'de, I: SubscribedIds + ?Sized> DeserializeSeed<'de> for &mut FrameDecoder<'_, I> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

/// The frame itself: an array of events, or a single event object.
impl<'de, I: SubscribedIds + ?Sized> Visitor<'de> for &mut FrameDecoder<'_, I> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an event object or an array of them")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut events: A) -> Result<(), A::Error> {
        while events.next_element_seed(EventSeed(&mut *self))?.is_some() {}
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, fields: A) -> Result<(), A::Error> {
        let event = RawEventVisitor.visit_map(fields)?;
        self.on_event(event);
        Ok(())
    }
}

/// One array element. After a rejected event the rest are only skipped.
struct EventSeed<'d, 'i, I: ?Sized>(&'d mut FrameDecoder<'i, I>);

impl<'de, I: SubscribedIds + ?Sized> DeserializeSeed<'de> for EventSeed<'_, '_, I> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.0.out.error.is_some() {
            IgnoredAny::deserialize(deserializer)?;
            return Ok(());
        }
        let event = deserializer.deserialize_any(RawEventVisitor)?;
        self.0.on_event(event);
        Ok(())
    }
}

/// Implements the `Visitor` methods for JSON values other than strings,
/// arrays and objects, all yielding `$value`.
macro_rules! visit_scalars_as {
    ($value:expr) => {
        fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok($value)
        }
    };
}

/// Implements `visit_str`, `visit_seq` and `visit_map` (whichever the visitor
/// doesn't use) by consuming the value and yielding `$value`.
macro_rules! skip_as {
    (str, $value:expr) => {
        fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
            Ok($value)
        }
    };
    (seq, $value:expr) => {
        fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Self::Value, A::Error> {
            while items.next_element::<IgnoredAny>()?.is_some() {}
            Ok($value)
        }
    };
    (map, $value:expr) => {
        fn visit_map<A: MapAccess<'de>>(self, mut fields: A) -> Result<Self::Value, A::Error> {
            while fields.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
            Ok($value)
        }
    };
}

/// An event object or event-type array, or `None` for any other JSON value.
struct RawEventVisitor;

impl<'de> Visitor<'de> for RawEventVisitor {
    type Value = Option<RawEvent<'de>>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut fields: A) -> Result<Self::Value, A::Error> {
        let mut event = RawEvent::default();
        let mut seen = 0;
        while let Some(key) = fields.next_key::<Key>()? {
            let flag = 1 << key as u8;
            match key {
                Key::EventType => set_field(
                    &mut event.event_type,
                    &mut seen,
                    flag,
                    fields.next_value::<Text>()?.0,
                ),
                Key::Market => set_field(
                    &mut event.market,
                    &mut seen,
                    flag,
                    fields.next_value::<Text>()?.0,
                ),
                Key::AssetId => set_field(
                    &mut event.asset_id,
                    &mut seen,
                    flag,
                    fields.next_value::<Text>()?.0,
                ),
                Key::Timestamp => set_field(
                    &mut event.timestamp,
                    &mut seen,
                    flag,
                    fields.next_value::<Text>()?.0,
                ),
                Key::Bids => set_field(
                    &mut event.bids,
                    &mut seen,
                    flag,
                    fields.next_value_seed(Entries(EntryKind::Level))?,
                ),
                Key::Asks => set_field(
                    &mut event.asks,
                    &mut seen,
                    flag,
                    fields.next_value_seed(Entries(EntryKind::Level))?,
                ),
                Key::PriceChanges => set_field(
                    &mut event.price_changes,
                    &mut seen,
                    flag,
                    fields.next_value_seed(Entries(EntryKind::Change))?,
                ),
                Key::Price | Key::Size | Key::Side | Key::Other => {
                    fields.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(Some(event))
    }

    visit_scalars_as!(None);
    skip_as!(str, None);
    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Self::Value, A::Error> {
        let event_type = items.next_element::<Text>()?.and_then(|value| value.0);
        let mut extra = false;
        while items.next_element::<IgnoredAny>()?.is_some() {
            extra = true;
        }
        Ok((!extra).then_some(RawEvent {
            event_type,
            ..RawEvent::default()
        }))
    }
}

fn set_field<T>(field: &mut Option<T>, seen: &mut u16, flag: u16, value: Option<T>) {
    *field = if *seen & flag == 0 { value } else { None };
    *seen |= flag;
}

#[derive(Copy, Clone)]
enum EntryKind {
    Level,
    Change,
}

/// An array of level or change entries, or `None` for another shape.
struct Entries(EntryKind);

impl<'de> DeserializeSeed<'de> for Entries {
    type Value = Option<Vec<Entry<'de>>>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(EntriesVisitor(self.0))
    }
}

struct EntriesVisitor(EntryKind);

impl<'de> Visitor<'de> for EntriesVisitor {
    type Value = Option<Vec<Entry<'de>>>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Self::Value, A::Error> {
        let mut entries = Vec::with_capacity(items.size_hint().unwrap_or(0));
        let mut valid = true;
        while let Some(entry) = items.next_element_seed(MaybeEntry(self.0))? {
            match entry {
                Some(entry) if valid => entries.push(entry),
                Some(_) => {}
                None => valid = false,
            }
        }
        Ok(valid.then_some(entries))
    }

    visit_scalars_as!(None);
    skip_as!(str, None);
    skip_as!(map, None);
}

/// An entry object or positional array, or `None` for any other JSON value.
struct MaybeEntry(EntryKind);

impl<'de> DeserializeSeed<'de> for MaybeEntry {
    type Value = Option<Entry<'de>>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(EntryVisitor(self.0))
    }
}

struct EntryVisitor(EntryKind);

impl<'de> Visitor<'de> for EntryVisitor {
    type Value = Option<Entry<'de>>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut fields: A) -> Result<Self::Value, A::Error> {
        let mut entry = Entry::default();
        let mut seen = 0;
        while let Some(key) = fields.next_key::<Key>()? {
            let flag = 1 << key as u8;
            match key {
                Key::AssetId if matches!(self.0, EntryKind::Change) => set_field(
                    &mut entry.asset_id,
                    &mut seen,
                    flag,
                    fields.next_value::<Text>()?.0,
                ),
                Key::Price => set_field(
                    &mut entry.price,
                    &mut seen,
                    flag,
                    fields.next_value::<Text>()?.0,
                ),
                Key::Size => set_field(
                    &mut entry.size,
                    &mut seen,
                    flag,
                    fields.next_value::<Text>()?.0,
                ),
                Key::Side if matches!(self.0, EntryKind::Change) => set_field(
                    &mut entry.side,
                    &mut seen,
                    flag,
                    fields.next_value::<Text>()?.0,
                ),
                _ => {
                    fields.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(Some(entry))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = items.next_element::<Text>()? {
            values.push(value.0);
        }
        Ok(match self.0 {
            EntryKind::Level => <[_; 2]>::try_from(values).ok().map(|[price, size]| Entry {
                price,
                size,
                ..Entry::default()
            }),
            EntryKind::Change => {
                <[_; 4]>::try_from(values)
                    .ok()
                    .map(|[asset_id, price, size, side]| Entry {
                        asset_id,
                        price,
                        size,
                        side,
                    })
            }
        })
    }

    visit_scalars_as!(None);
    skip_as!(str, None);
}

/// A JSON string, or `None` for any other JSON value.
struct Text<'a>(Option<Cow<'a, str>>);

impl<'de> Deserialize<'de> for Text<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(TextVisitor)
    }
}

struct TextVisitor;

impl<'de> Visitor<'de> for TextVisitor {
    type Value = Text<'de>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_borrowed_str<E: de::Error>(self, text: &'de str) -> Result<Self::Value, E> {
        Ok(Text(Some(Cow::Borrowed(text))))
    }

    // Strings with escapes can't be borrowed from the frame.
    fn visit_str<E: de::Error>(self, text: &str) -> Result<Self::Value, E> {
        Ok(Text(Some(Cow::Owned(text.to_owned()))))
    }

    fn visit_string<E: de::Error>(self, text: String) -> Result<Self::Value, E> {
        Ok(Text(Some(Cow::Owned(text))))
    }

    visit_scalars_as!(Text(None));
    skip_as!(seq, Text(None));
    skip_as!(map, Text(None));
}

/// The object keys the decoder reads; every other key is skipped.
#[derive(Copy, Clone, Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum Key {
    EventType,
    Market,
    AssetId,
    Timestamp,
    Bids,
    Asks,
    PriceChanges,
    Price,
    Size,
    Side,
    #[serde(other)]
    Other,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::model::{MarketId, OrderBookId};

    #[derive(Default)]
    struct Ids {
        markets: HashSet<MarketId>,
        order_books: HashSet<OrderBookId>,
    }

    impl SubscribedIds for Ids {
        fn market(&self, id: &str) -> Option<MarketId> {
            self.markets.get(id).cloned()
        }
        fn order_book(&self, id: &str) -> Option<OrderBookId> {
            self.order_books.get(id).cloned()
        }
    }

    fn ids() -> Ids {
        Ids {
            markets: HashSet::from(["0xa1".into()]),
            order_books: HashSet::from(["11".into(), "12".into()]),
        }
    }

    fn decode(text: &str) -> DecodedFrame {
        decode_frame(text, ConnectionId(7), DateTime::UNIX_EPOCH, &ids(), 1 << 20)
    }

    #[test]
    fn malformed_singletons_fail_but_valid_unusable_headers_remain_ignored() {
        for text in [
            r#"{"event_type":"book","bids":[}"#,
            r#"{"event_type":"tick_size_change","extra":[}"#,
            r#"{"event_type":"last_trade_price"} trailing"#,
        ] {
            let out = decode(text);
            assert!(out.error.is_some(), "{text}");
            assert!(out.events.is_empty());
            assert_eq!(out.ignored, 0);
        }
        for text in [
            r#"{"event_type":7}"#,
            r#"{"event_type":"book","event_type":"price_change"}"#,
            r#"{"extra":{"nested":[1,2,3]}}"#,
        ] {
            let out = decode(text);
            assert!(out.error.is_none(), "{text}");
            assert!(out.events.is_empty());
            assert_eq!(out.ignored, 1);
        }
    }

    const BOOK: &str = r#"{"event_type":"book","market":"0xa1","asset_id":"11","timestamp":"1000",
        "hash":"x","bids":[{"price":"0.01","size":"5"},{"price":"0.02","size":"1.5"}],"asks":[]}"#;

    #[test]
    fn decodes_book_array_and_reuses_ids() {
        let ids = ids();
        let frame = format!("[{BOOK},{}]", BOOK.replace(r#""11""#, r#""12""#));
        let decoded = decode_frame(&frame, ConnectionId(7), DateTime::UNIX_EPOCH, &ids, 1 << 20);
        assert!(decoded.error.is_none());
        assert_eq!(decoded.events.len(), 2);
        let event = &decoded.events[0];
        assert_eq!(event.connection, ConnectionId(7));
        assert_eq!(event.exchange_ts.timestamp_millis(), 1000);
        let OrderBookEvent::Snapshot {
            order_book,
            bids,
            asks,
            ..
        } = &event.event
        else {
            panic!("expected a snapshot");
        };
        assert!(std::sync::Arc::ptr_eq(
            &order_book.0,
            &ids.order_books.get("11").unwrap().0
        ));
        assert_eq!(
            bids,
            &[
                (Price(10_000), Qty(5_000_000)),
                (Price(20_000), Qty(1_500_000))
            ]
        );
        assert!(asks.is_empty());
    }

    #[test]
    fn decodes_price_change_dropping_unsubscribed_entries() {
        let decoded = decode(
            r#"{"event_type":"price_change","market":"0xa1","timestamp":"2000","price_changes":[
            {"asset_id":"11","price":"0.5","size":"0","side":"BUY","hash":"h"},
            {"asset_id":"99","price":"0.5","size":"1","side":"SELL"},
            {"asset_id":"12","price":"0.6","size":"3","side":"SELL"}]}"#,
        );
        assert_eq!(decoded.unsubscribed, 1);
        let OrderBookEvent::LevelUpdates(updates) = &decoded.events[0].event else {
            panic!("expected updates");
        };
        assert_eq!(updates.len(), 2);
        assert_eq!(
            (updates[0].side, updates[0].change),
            (Side::Buy, crate::model::QuantityChange::Set(Qty(0)))
        );
        assert_eq!(
            (updates[1].side, updates[1].price),
            (Side::Sell, Price(600_000))
        );
    }

    #[test]
    fn counts_other_events_and_text() {
        let decoded = decode(
            r#"[{"event_type":"best_bid_ask","market":"0xa1"},{"event_type":"last_trade_price"},
            {"event_type":"tick_size_change","market":"0xa1"},{"event_type":"market_resolved"},{"no":"type"}]"#,
        );
        assert!(decoded.error.is_none());
        assert_eq!((decoded.ignored, decoded.tick_size_changes), (4, 1));
        assert_eq!(decode("INVALID OPERATION").ignored, 1);
        let unknown_market = decode(&BOOK.replace("0xa1", "0xb2"));
        assert_eq!(
            (unknown_market.events.len(), unknown_market.unsubscribed),
            (0, 1)
        );
    }

    #[test]
    fn rejects_bad_book_messages_whole() {
        let bad_price = BOOK.replace("1.5", "1.1234567");
        let decoded = decode(&format!("[{BOOK},{bad_price},{BOOK}]"));
        assert_eq!(decoded.events.len(), 1, "only the event before the failure");
        assert!(decoded.error.unwrap().contains("fractional"));

        for bad in [
            BOOK.replace(r#""timestamp":"1000""#, r#""timestamp":1000"#),
            BOOK.replace(r#""asks":[]"#, ""),
            BOOK.replace("\"5\"", "\"-5\""),
            r#"{"event_type":"price_change","market":"0xa1","timestamp":"1","price_changes":[
                {"asset_id":"11","price":"0.5","size":"1","side":"HOLD"}]}"#
                .to_string(),
            r#"{"event_type":"book""#.to_string(),
        ] {
            let decoded = decode(&bad);
            assert!(decoded.error.is_some(), "{bad}");
            assert!(decoded.events.is_empty());
        }

        let limited = decode_frame(BOOK, ConnectionId(1), DateTime::UNIX_EPOCH, &ids(), 10);
        assert!(limited.error.is_some());
    }

    #[test]
    fn unknown_events_and_wrong_typed_metadata_do_not_reconnect() {
        let decoded = decode(
            r#"[1e999,{"market":1e999,"bids":[null],"event_type":"unknown"},
            {"event_type":"tick_size_change","timestamp":1e999} ]"#,
        );
        assert!(decoded.error.is_none());
        assert!(decoded.events.is_empty());
        assert_eq!((decoded.ignored, decoded.tick_size_changes), (2, 1));
    }

    #[test]
    fn invalid_json_rejects_every_event_even_after_a_semantic_error() {
        for text in [
            format!("[{BOOK},{{]"),
            format!("[{BOOK},{{\"event_type\":\"book\"}},{{]"),
        ] {
            let decoded = decode(&text);
            assert!(decoded.error.is_some());
            assert!(decoded.events.is_empty());
            assert_eq!(
                decoded.unsubscribed + decoded.ignored + decoded.tick_size_changes,
                0
            );
        }
    }

    #[test]
    fn duplicate_fields_keep_the_existing_event_classification() {
        let duplicate_type = decode(r#"{"event_type":"book","event_type":"book"}"#);
        assert!(duplicate_type.error.is_none());
        assert_eq!(duplicate_type.ignored, 1);
        let duplicate_level = BOOK.replace("\"size\":\"5\"", "\"size\":\"5\",\"size\":\"6\"");
        let decoded = decode(&format!("[{BOOK},{duplicate_level},{BOOK}]"));
        assert!(decoded.error.is_some());
        assert_eq!(decoded.events.len(), 1);
        let duplicate_market = BOOK.replace(
            "\"market\":\"0xa1\"",
            "\"market\":\"0xa1\",\"market\":\"0xa1\"",
        );
        assert!(decode(&duplicate_market).error.is_some());
    }

    #[test]
    fn field_order_escapes_and_sequence_entries_remain_supported() {
        let decoded = decode(
            r#"{"bids":[["0.5","2"]],"asks":[],"timestamp":"1000",
            "asset_id":"1\u0031","market":"0x\u00611","event_type":"book"}"#,
        );
        assert!(decoded.error.is_none());
        let OrderBookEvent::Snapshot { bids, .. } = &decoded.events[0].event else {
            panic!("expected snapshot");
        };
        assert_eq!(bids, &[(Price(500_000), Qty(2_000_000))]);
        let decoded = decode(
            r#"{"price_changes":[["11","0.5","0","BUY"]],
            "timestamp":"1000","market":"0xa1","event_type":"price_change"}"#,
        );
        assert!(decoded.error.is_none());
        assert_eq!(decoded.events.len(), 1);
        let legacy_tick = decode(r#"[["tick_size_change"]]"#);
        assert_eq!(legacy_tick.tick_size_changes, 1);
    }
}
