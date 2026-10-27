//! Polymarket book messages behind the shared WebSocket protocol contract.

use std::time::Duration;

use serde_json::json;
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, handshake::client::Request,
};

use crate::core::telemetry;
use crate::exchange::polymarket::{decode::decode_frame, ws};
use crate::exchange::protocol::{
    BookProtocol, BookRequestPreparation, ContinuityFault, DecodeContext, DecodedFrame,
};
use crate::model::{MarketBooks, OrderBookId, UnsyncReason};

pub const PING_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct PolymarketProtocol {
    url: String,
}

impl PolymarketProtocol {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl Default for PolymarketProtocol {
    fn default() -> Self {
        Self::new(ws::MARKET_WS_URL.to_owned())
    }
}

impl BookProtocol for PolymarketProtocol {
    fn connect_request(&self) -> anyhow::Result<Request> {
        Ok(self.url.as_str().into_client_request()?)
    }
    fn start_session(&mut self) {}
    fn subscribe(&mut self, books: &[MarketBooks], initial: bool) -> Vec<Message> {
        let assets = books.iter().flat_map(|market| &market.order_books);
        let text = if initial {
            ws::book_subscription(assets)
        } else {
            ws::subscribe(assets)
        };
        vec![Message::Text(text.into())]
    }
    fn unsubscribe(&mut self, books: &[MarketBooks]) -> Vec<Message> {
        vec![Message::Text(
            ws::unsubscribe(books.iter().flat_map(|market| &market.order_books)).into(),
        )]
    }
    fn prepare_book_request(&mut self, books: &[OrderBookId]) -> Option<BookRequestPreparation> {
        Some(BookRequestPreparation {
            outgoing: vec![Message::Text(ws::unsubscribe(books).into())],
            delay: Duration::from_secs(5),
        })
    }
    fn request_books(&mut self, books: &[OrderBookId]) -> Vec<Message> {
        vec![Message::Text(ws::subscribe(books).into())]
    }
    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(PING_INTERVAL)
    }
    fn heartbeat(&mut self) -> Option<Message> {
        Some(Message::Text(ws::PING.into()))
    }
    fn decode(&mut self, frame: &Message, context: DecodeContext<'_>) -> DecodedFrame {
        let Message::Text(text) = frame else {
            return DecodedFrame::default();
        };
        if text.as_str() == ws::PONG {
            return DecodedFrame {
                heartbeat_ack: true,
                ..DecodedFrame::default()
            };
        }
        let frame = decode_frame(
            text,
            context.connection,
            context.received_at,
            context.ids,
            context.max_frame_bytes,
        );
        if frame.tick_size_changes > 0 {
            telemetry::emit(
                "tick_size_change",
                json!({"connection": context.connection.0, "count": frame.tick_size_changes}),
            );
        }
        DecodedFrame {
            events: frame.events,
            unsubscribed: frame.unsubscribed,
            ignored: frame.ignored,
            fault: frame.error.map(|detail| ContinuityFault {
                reason: UnsyncReason::DecodeError,
                detail,
            }),
            ..DecodedFrame::default()
        }
    }
}
