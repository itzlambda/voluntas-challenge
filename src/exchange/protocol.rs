//! Contract between a WebSocket protocol and shared book connection supervision.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::{Message, handshake::client::Request};

use crate::model::{
    ConnectionId, MarketBooks, MarketData, OrderBookId, SubscribedIds, UnsyncReason,
};

pub struct DecodeContext<'a> {
    pub connection: ConnectionId,
    pub received_at: DateTime<Utc>,
    pub ids: &'a dyn SubscribedIds,
    pub max_frame_bytes: usize,
}

#[derive(Debug)]
pub struct ContinuityFault {
    pub reason: UnsyncReason,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct DecodedFrame {
    /// Valid events before any fault. Never includes events after the fault.
    pub events: Vec<MarketData>,
    pub outgoing: Vec<Message>,
    pub heartbeat_ack: bool,
    pub unsubscribed: u32,
    pub ignored: u32,
    pub fault: Option<ContinuityFault>,
}

pub struct BookRequestPreparation {
    pub outgoing: Vec<Message>,
    pub delay: Duration,
}

/// One instance per connection task. Protocol state resets after every handshake.
/// The implementation owns no reconstructed order books or worker senders.
pub trait BookProtocol: Send + 'static {
    fn connect_request(&self) -> anyhow::Result<Request>;
    /// Clear protocol state after a handshake and before the initial subscription.
    fn start_session(&mut self);
    fn subscribe(&mut self, books: &[MarketBooks], initial: bool) -> Vec<Message>;
    fn unsubscribe(&mut self, books: &[MarketBooks]) -> Vec<Message>;
    /// Optionally unsubscribe before requesting books; the supervisor waits
    /// without blocking reads and excludes these outcomes during the gap.
    fn prepare_book_request(&mut self, _books: &[OrderBookId]) -> Option<BookRequestPreparation> {
        None
    }
    /// Request overdue first books after any preparation, retaining membership.
    fn request_books(&mut self, books: &[OrderBookId]) -> Vec<Message>;
    fn heartbeat_interval(&self) -> Option<Duration>;
    fn heartbeat(&mut self) -> Option<Message>;
    /// Return only valid events before a fault. Outgoing messages are discarded on fault.
    fn decode(&mut self, frame: &Message, context: DecodeContext<'_>) -> DecodedFrame;
}
