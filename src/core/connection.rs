//! Connection-side handling shared by live connections and fixture replay:
//! the connection's subscribed IDs, and decode-then-route of one frame.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

use crate::core::decode_log::DecodeErrorLog;
use crate::core::metrics::{Distribution, Histogram};
use crate::core::routing::Workers;
use crate::core::telemetry;
use crate::exchange::protocol::{BookProtocol, ContinuityFault, DecodeContext, DecodedFrame};
use crate::model::{
    ConnectionId, MarketId, OrderBookEvent, OrderBookId, SubscribedIds, UnsyncReason, WorkerMessage,
};

/// A connection's own set of subscribed markets and order books. Events reuse
/// these shared IDs instead of allocating new strings.
#[derive(Debug, Default)]
pub struct ConnectionIds {
    markets: HashSet<MarketId>,
    order_books: HashSet<OrderBookId>,
}

impl ConnectionIds {
    pub fn add(&mut self, market: &MarketId, order_books: &[OrderBookId]) {
        self.markets.insert(market.clone());
        self.order_books.extend(order_books.iter().cloned());
    }

    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }

    pub fn order_books(&self) -> impl Iterator<Item = &OrderBookId> {
        self.order_books.iter()
    }

    pub fn pause_books(&mut self, books: &[OrderBookId]) {
        for book in books {
            self.order_books.remove(book);
        }
    }

    pub fn resume_books(&mut self, books: &[OrderBookId]) {
        self.order_books.extend(books.iter().cloned());
    }

    pub fn remove(&mut self, market: &MarketId, order_books: &[OrderBookId]) {
        self.markets.remove(market);
        for order_book in order_books {
            self.order_books.remove(order_book);
        }
    }
}

impl SubscribedIds for ConnectionIds {
    fn market(&self, id: &str) -> Option<MarketId> {
        self.markets.get(id).cloned()
    }

    fn order_book(&self, id: &str) -> Option<OrderBookId> {
        self.order_books.get(id).cloned()
    }
}

/// Frame counters per connection.
#[derive(Debug, Clone, PartialEq)]
pub struct FrameTotals {
    pub frames: u64,
    pub pongs: u64,
    pub events: u64,
    pub unsubscribed: u64,
    pub ignored: u64,
    pub decode_errors: u64,
    /// One sample per normalized level-update event with subscribed updates.
    pub exchange_to_receive: Histogram,
    pub negative_exchange_ages: u64,
}

impl Default for FrameTotals {
    fn default() -> Self {
        Self {
            frames: 0,
            pongs: 0,
            events: 0,
            unsubscribed: 0,
            ignored: 0,
            decode_errors: 0,
            exchange_to_receive: Histogram::new(Distribution::Exchange),
            negative_exchange_ages: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct FrameOutcome {
    pub fault: Option<UnsyncReason>,
    pub heartbeat_ack: bool,
    pub outgoing: Vec<Message>,
}

/// Decodes one WebSocket frame and sends its events to their workers in order.
/// `on_book` is called for each full book routed. An error means a worker is
/// gone, which is fatal.
#[allow(clippy::too_many_arguments)]
pub async fn handle_frame(
    protocol: &mut impl BookProtocol,
    frame: &Message,
    connection: ConnectionId,
    received_at: DateTime<Utc>,
    received_instant: std::time::Instant,
    ids: &ConnectionIds,
    workers: &Workers,
    max_frame_bytes: usize,
    decode_log: &DecodeErrorLog,
    totals: &mut FrameTotals,
    mut on_book: impl FnMut(&OrderBookId),
) -> anyhow::Result<FrameOutcome> {
    totals.frames += 1;
    let decoded = if frame.len() > max_frame_bytes {
        DecodedFrame {
            fault: Some(ContinuityFault {
                reason: UnsyncReason::DecodeError,
                detail: format!(
                    "frame of {} bytes exceeds the {max_frame_bytes}-byte limit",
                    frame.len()
                ),
            }),
            ..DecodedFrame::default()
        }
    } else {
        protocol.decode(
            frame,
            DecodeContext {
                connection,
                received_at,
                ids,
                max_frame_bytes,
            },
        )
    };
    totals.pongs += u64::from(decoded.heartbeat_ack);
    totals.events += decoded.events.len() as u64;
    totals.unsubscribed += u64::from(decoded.unsubscribed);
    totals.ignored += u64::from(decoded.ignored);
    let mut outcome = FrameOutcome {
        heartbeat_ack: decoded.heartbeat_ack,
        outgoing: decoded.outgoing,
        fault: None,
    };
    for mut event in decoded.events {
        event.connection = connection;
        event.received_at = received_at;
        event.received_instant = Some(received_instant);
        match &event.event {
            OrderBookEvent::Snapshot { order_book, .. } => on_book(order_book),
            // Snapshot timestamps can describe the last book change. Measure
            // receipt age only for normalized level-update events.
            OrderBookEvent::LevelUpdates(_) => {
                let age = event.received_at.signed_duration_since(event.exchange_ts);
                match age.to_std() {
                    Ok(age) => totals.exchange_to_receive.record(age.as_secs_f64()),
                    Err(_) => totals.negative_exchange_ages += 1,
                }
            }
        }
        workers.send(WorkerMessage::MarketData(event)).await?;
    }

    let Some(fault) = decoded.fault else {
        return Ok(outcome);
    };
    totals.decode_errors += u64::from(fault.reason == UnsyncReason::DecodeError);
    let error = fault.detail;
    let text = match frame {
        Message::Text(text) => text.to_string(),
        _ => format!("{frame:?}"),
    };
    let logged = decode_log.record(connection, received_at, &error, &text);
    telemetry::emit(
        if fault.reason == UnsyncReason::DecodeError {
            "decode_error"
        } else {
            "protocol_error"
        },
        json!({
            "connection": connection.0,
            "error": error,
            "logged": logged.as_ref().map_or_else(|e| e.to_string(), |()| "ok".into()),
        }),
    );
    let reset = WorkerMessage::MarkUnsynced {
        connection,
        reason: fault.reason,
    };
    workers.send(reset).await?;
    outcome.fault = Some(fault.reason);
    Ok(outcome)
}
