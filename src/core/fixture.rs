//! The fixture format: JSON Lines of connection frames with their received
//! times, plus control and time-advance events. Tests replay fixtures on
//! Tokio's paused clock, so time moves only on `advance`.

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::model::{ConnectionId, MarketBooks, OrderBookId, UnsyncReason};
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FixtureEvent {
    SourceAttached {
        market: String,
        generation: u64,
        connection: u16,
    },
    SessionStart {
        connection: u16,
    },
    ProtocolCommand {
        connection: u16,
        command: ProtocolCommand,
    },
    BinaryFrame {
        connection: u16,
        received_at: DateTime<Utc>,
        bytes: Vec<u8>,
    },
    PingFrame {
        connection: u16,
        received_at: DateTime<Utc>,
        bytes: Vec<u8>,
    },
    PongFrame {
        connection: u16,
        received_at: DateTime<Utc>,
        bytes: Vec<u8>,
    },
    /// A raw text frame as received from the WebSocket.
    Frame {
        connection: u16,
        received_at: DateTime<Utc>,
        text: String,
    },
    /// The subscription manager places a market on a connection: `AddMarket`
    /// to the worker, then the connection subscribes.
    AddMarket {
        #[serde(default, skip_serializing_if = "crate::model::legacy_generation")]
        generation: u64,
        market: String,
        order_books: Vec<String>,
        connection: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        standby: Option<u16>,
    },
    /// `RemoveMarket` to the worker, then the connection unsubscribes.
    RemoveMarket {
        market: String,
    },
    /// A connection reports that its data may have gaps.
    MarkUnsynced {
        connection: u16,
        reason: UnsyncReason,
    },
    /// Moves the clock forward.
    Advance {
        millis: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ProtocolCommand {
    Subscribe {
        books: Vec<MarketBooks>,
        initial: bool,
    },
    Unsubscribe {
        books: Vec<MarketBooks>,
    },
    PrepareBooks {
        books: Vec<OrderBookId>,
    },
    RequestBooks {
        books: Vec<OrderBookId>,
    },
    Heartbeat,
}

impl FixtureEvent {
    pub fn from_frame(
        connection: ConnectionId,
        received_at: DateTime<Utc>,
        frame: &Message,
    ) -> Option<Self> {
        let connection = connection.0;
        Some(match frame {
            Message::Text(text) => Self::Frame {
                connection,
                received_at,
                text: text.to_string(),
            },
            Message::Binary(bytes) => Self::BinaryFrame {
                connection,
                received_at,
                bytes: bytes.to_vec(),
            },
            Message::Ping(bytes) => Self::PingFrame {
                connection,
                received_at,
                bytes: bytes.to_vec(),
            },
            Message::Pong(bytes) => Self::PongFrame {
                connection,
                received_at,
                bytes: bytes.to_vec(),
            },
            _ => return None,
        })
    }
}

/// Parses JSON Lines; blank lines and lines starting with `#` are skipped.
pub fn parse_jsonl(text: &str) -> anyhow::Result<Vec<FixtureEvent>> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
        .map(|(number, line)| {
            serde_json::from_str(line).with_context(|| format!("fixture line {}", number + 1))
        })
        .collect()
}

/// One JSON line, without the newline.
pub fn to_jsonl_line(event: &FixtureEvent) -> String {
    serde_json::to_string(event).expect("fixture events serialize")
}

#[cfg(test)]
mod tests {
    use crate::core::fixture::*;

    #[test]
    fn round_trips() {
        let text = r#"
# comment
{"type":"add_market","market":"0xa1","order_books":["11","12"],"connection":1}
{"type":"frame","connection":1,"received_at":"2026-10-25T12:00:00.000123Z","text":"PONG"}
{"type":"mark_unsynced","connection":1,"reason":"disconnected"}
{"type":"advance","millis":60000}
{"type":"remove_market","market":"0xa1"}
"#;
        let events = parse_jsonl(text).unwrap();
        assert_eq!(events.len(), 5);
        let FixtureEvent::Frame { received_at, .. } = &events[1] else {
            panic!("expected a frame");
        };
        assert_eq!(received_at.timestamp_subsec_micros(), 123);
        for event in &events {
            assert_eq!(&parse_jsonl(&to_jsonl_line(event)).unwrap()[0], event);
        }
        assert!(parse_jsonl(r#"{"type":"nope"}"#).is_err());
    }
    #[test]
    fn legacy_registration_keeps_its_json_shape() {
        let text = r#"{"type":"add_market","market":"m","order_books":["1","2"],"connection":7}"#;
        let events = parse_jsonl(text).unwrap();
        assert!(matches!(
            events[0],
            FixtureEvent::AddMarket { standby: None, .. }
        ));
        assert_eq!(to_jsonl_line(&events[0]), text);
    }

    #[test]
    fn mirrored_registration_round_trips() {
        let event = FixtureEvent::AddMarket {
            generation: 0,
            market: "m".into(),
            order_books: vec!["1".into(), "2".into()],
            connection: 7,
            standby: Some(8),
        };
        assert_eq!(parse_jsonl(&to_jsonl_line(&event)).unwrap(), vec![event]);
    }
}
