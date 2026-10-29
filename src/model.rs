//! Our types (SPEC section 6). The core works only with these; exchange
//! specifics stop at the adapter.

use std::borrow::Borrow;
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::Side;

/// Fixed-point price; 1_000_000 = 1 USDC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(pub i64);

/// Fixed-point quantity; 1_000_000 = 1 share. 0 means the level is removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(pub i64);

/// Exchange market identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MarketId(pub Arc<str>);

/// Outcome order book identifier; written as the SBE `symbol`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrderBookId(pub Arc<str>);

/// Opaque exchange listing identifier, separate from market and book IDs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ListingId(pub String);

impl From<&str> for ListingId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl fmt::Display for ListingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A level's absolute quantity or signed change to its current quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityChange {
    Set(Qty),
    Add(i64),
}

impl QuantityChange {
    pub fn resolve(self, current: Qty) -> anyhow::Result<Qty> {
        let next = match self {
            Self::Set(qty) => qty.0,
            Self::Add(delta) => current
                .0
                .checked_add(delta)
                .ok_or_else(|| anyhow::anyhow!("quantity overflow"))?,
        };
        anyhow::ensure!(next >= 0, "negative resulting quantity");
        Ok(Qty(next))
    }
}

/// An admitted market and its two distinct outcome order books.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MarketBooks {
    #[serde(default, skip_serializing_if = "legacy_generation")]
    pub generation: u64,
    pub market: MarketId,
    pub order_books: [OrderBookId; 2],
}

/// Stable number of a connection slot. A reconnect keeps the same ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConnectionId(pub u16);

impl ConnectionId {
    /// Subscription placement assigns even IDs to primaries and odd IDs to replicas.
    pub fn pool(self) -> &'static str {
        if self.0 & 1 == 0 {
            "primary"
        } else {
            "replica"
        }
    }
}

/// One source for isolated replay, or two distinct mirrored live sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketSources {
    connections: [ConnectionId; 2],
    mirrored: bool,
}

impl MarketSources {
    pub fn single(connection: ConnectionId) -> Self {
        Self {
            connections: [connection; 2],
            mirrored: false,
        }
    }

    pub fn mirrored(a: ConnectionId, b: ConnectionId) -> anyhow::Result<Self> {
        anyhow::ensure!(a != b, "mirrored sources must use distinct connections");
        Ok(Self {
            connections: [a, b],
            mirrored: true,
        })
    }

    pub fn connections(&self) -> &[ConnectionId] {
        &self.connections[..if self.mirrored { 2 } else { 1 }]
    }

    pub fn is_mirrored(&self) -> bool {
        self.mirrored
    }
}

impl From<&str> for MarketId {
    fn from(id: &str) -> Self {
        Self(id.into())
    }
}

impl From<&str> for OrderBookId {
    fn from(id: &str) -> Self {
        Self(id.into())
    }
}

// Both hash and compare like the inner `str`, so sets can be queried by `&str`.
impl Borrow<str> for MarketId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for OrderBookId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for OrderBookId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// What an order book connection decodes a frame into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderBookEvent {
    /// A full order book from the exchange.
    Snapshot {
        order_book: OrderBookId,
        /// The exchange permits comparison with earlier depth event timestamps.
        timestamp_comparable: bool,
        bids: Vec<(Price, Qty)>,
        asks: Vec<(Price, Qty)>,
    },
    /// Level updates, in exchange order.
    LevelUpdates(Vec<LevelUpdate>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelUpdate {
    pub order_book: OrderBookId,
    pub side: Side,
    pub price: Price,
    pub change: QuantityChange,
}

/// An event tagged by the connection task before routing to the market's worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketData {
    /// Runtime-only monotonic receipt; never serialized or written to SBE.
    pub received_instant: Option<std::time::Instant>,
    pub market: MarketId,
    pub connection: ConnectionId,
    pub received_at: DateTime<Utc>,
    pub exchange_ts: DateTime<Utc>,
    pub event: OrderBookEvent,
}

/// What a lifecycle connection decodes a frame into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMarket {
    pub market: MarketId,
    pub listing_id: ListingId,
    pub order_books: Vec<OrderBookId>,
}

/// Lifecycle changes in their received order, including within array frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketLifecycleEvent {
    NewMarket(NewMarket),
    Resolved(MarketId),
}

/// Worker input (SPEC section 9). Controls and events share one channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerMessage {
    SourceAttached {
        market: MarketId,
        generation: u64,
        connection: ConnectionId,
    },
    MarketData(MarketData),
    /// Sent before the connection subscribes.
    AddMarket {
        generation: u64,
        market: MarketId,
        order_books: Vec<OrderBookId>,
        sources: MarketSources,
    },
    /// Sent before the connection unsubscribes.
    RemoveMarket(MarketId),
    /// Sent whenever data on a connection may have gaps.
    MarkUnsynced {
        connection: ConnectionId,
        reason: UnsyncReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsyncReason {
    /// Legacy single-source fixture value; live connections preserve the cause below.
    Disconnected,
    /// The server sent a close frame or ended the stream.
    RemoteClose,
    /// A transport or send operation failed.
    IoError,
    /// No frame arrived within the connection silence limit.
    Stalled,
    /// A recognized book message failed to parse.
    DecodeError,
    /// The exchange protocol reports missing messages.
    SequenceGap,
}

impl UnsyncReason {
    /// Fixed publisher-switch reason order, matching metrics exposition.
    pub fn switch_index(self) -> Option<usize> {
        match self {
            Self::RemoteClose => Some(0),
            Self::IoError => Some(1),
            Self::Stalled => Some(2),
            Self::DecodeError => Some(3),
            Self::SequenceGap => Some(4),
            Self::Disconnected => None,
        }
    }
}

/// Worker output to the writer.
///
/// We choose option A so backtests retain when the connection task first
/// observed a frame, before JSON parsing and worker queue delay.
/// Both local output timestamps use that receive-time interpretation:
///
/// | Interpretation | Update `backtest_timestamp` | Snapshot `received_at` |
/// | --- | --- | --- |
/// | A (chosen): receive time | Frame received by the connection task | Receive time of the latest message applied to the book |
/// | B: processing time | Worker applies the update | Worker captures the snapshot |
///
/// A snapshot therefore describes the freshness of its data, and a quiet book
/// keeps its previous timestamps even when a new periodic snapshot is emitted.
/// Option B would suit replay of when the worker made state available, including
/// our internal delay, but would require changing both local timestamp meanings.
/// Changing only the snapshot to capture time could place it after a queued
/// update's receive time even though the snapshot does not yet include that update.
/// In either option, exchange time comes from the message, with snapshots using
/// the latest applied message, including a repeat book with no level differences.
/// Replay preserves per-book file order, including when timestamps tie, since
/// timestamps alone do not define a total order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputRecord {
    LevelUpdate {
        /// None for authoritative-book corrections, which are excluded from latency.
        received_instant: Option<std::time::Instant>,
        order_book: OrderBookId,
        side: Side,
        price: Price,
        qty: Qty,
        /// Source frame receive time, encoded as `PriceLevelUpdate.backtest_timestamp`.
        received_at: DateTime<Utc>,
        exchange_ts: DateTime<Utc>,
    },
    Snapshot {
        order_book: OrderBookId,
        bids: Vec<(Price, Qty)>,
        asks: Vec<(Price, Qty)>,
        /// Receive time of the latest applied message, not snapshot capture time.
        received_at: DateTime<Utc>,
        exchange_ts: DateTime<Utc>,
    },
}

impl OutputRecord {
    pub fn order_book(&self) -> &OrderBookId {
        match self {
            Self::LevelUpdate { order_book, .. } | Self::Snapshot { order_book, .. } => order_book,
        }
    }
}

/// The IDs a connection is subscribed to. Decoders look IDs up here, so
/// events reuse the connection's shared copies instead of allocating.
pub trait SubscribedIds {
    fn market(&self, id: &str) -> Option<MarketId>;
    fn order_book(&self, id: &str) -> Option<OrderBookId>;
}

/// What a discovery listing says about one market, in exchange-neutral terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    pub recovery: RecoveryEligibility,
    /// The exchange's listing ID (Gamma `id`).
    pub listing_id: ListingId,
    pub status: ListingStatus,
    /// Set when the status is `Desired`.
    pub market: Option<MarketId>,
    pub order_books: Vec<OrderBookId>,
    pub created_at: Option<String>,
}

/// Explicit metadata facts for recovery; admission keeps its existing policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryEligibility {
    #[default]
    Unknown,
    Eligible,
    Ineligible,
}

/// Reconciliation subscribes only `Desired` rows. Every status is re-read on
/// each scan, so no status is final.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingStatus {
    /// Eligible: keep subscribed.
    Desired,
    /// Closed or archived.
    Closed,
    /// Not eligible for another reason (for example, not accepting orders).
    NotDesired,
    /// Out of scope (protocol v2).
    Excluded,
}

pub(crate) fn legacy_generation(generation: &u64) -> bool {
    *generation == 0
}
