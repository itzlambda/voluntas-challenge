//! Lifecycle connections (SPEC section 7): market-channel sockets with no
//! assets and custom features on, receiving additions and resolutions.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::core::discovery::LifecycleEvent;
use crate::core::metrics::{EndReason, PUBLICATION_INTERVAL, Publisher};
use crate::core::rate_limit::RateLimiter;
use crate::core::reconnect_backoff::ReconnectBackoff;
use crate::core::telemetry;
use crate::exchange::polymarket::protocol::PING_INTERVAL;
use crate::exchange::polymarket::ws;
use crate::model::MarketLifecycleEvent;

use crate::core::book_connection::SILENCE_LIMIT;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LifecycleTotals {
    pub sessions: u64,
    pub new_markets: u64,
    pub resolutions: u64,
    pub malformed: u64,
}

pub async fn run_lifecycle_connection(
    index: usize,
    url: String,
    open_limiter: Arc<RateLimiter>,
    events: mpsc::UnboundedSender<LifecycleEvent>,
    cancel: CancellationToken,
    mut metrics: Publisher,
) -> LifecycleTotals {
    metrics.state.expected = true;
    metrics.publish();
    let mut publication = interval(PUBLICATION_INTERVAL);
    publication.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut totals = LifecycleTotals::default();
    let mut backoff = ReconnectBackoff::default();
    loop {
        let wait = open_limiter.wait();
        tokio::pin!(wait);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return totals,
                _ = publication.tick() => metrics.publish_if_due(),
                _ = &mut wait => break,
            }
        }
        // TCP_NODELAY, as on book connections.
        let connect = timeout(
            CONNECT_TIMEOUT,
            tokio_tungstenite::connect_async_with_config(&url, None, true),
        );
        tokio::pin!(connect);
        let socket = loop {
            tokio::select! {
                _ = publication.tick() => metrics.publish_if_due(),
                _ = cancel.cancelled() => return totals,
                result = &mut connect => break match result {
                    Ok(Ok((socket, _))) => Some(socket),
                    Ok(Err(error)) => {
                        telemetry::emit("lifecycle_connect_failed", json!({"index": index, "error": error.to_string()}));
                        None
                    }
                    Err(_) => None,
                },
            }
        };
        let started = Instant::now();
        if let Some(mut socket) = socket {
            let subscribed = socket
                .send(Message::Text(ws::lifecycle_subscription().into()))
                .await
                .is_ok();
            if subscribed {
                totals.sessions += 1;
                let _ = events.send(LifecycleEvent::Connected(index));
                metrics.state.connected = true;
                metrics.publish();
                let reason =
                    session(&mut socket, &events, &cancel, &mut totals, &mut metrics).await;
                metrics.state.connected = false;
                metrics.end_session(reason.end_reason());
                metrics.publish();
                let _ = events.send(LifecycleEvent::Disconnected(index));
                telemetry::emit(
                    "lifecycle_session_end",
                    json!({"index": index, "reason": reason.as_str(), "lasted_s": started.elapsed().as_secs_f64()}),
                );
            }
            let _ = timeout(CLOSE_TIMEOUT, socket.close(None)).await;
        }
        if cancel.is_cancelled() {
            return totals;
        }
        let delay = backoff.next_delay(started.elapsed());
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return totals,
                _ = publication.tick() => metrics.publish_if_due(),
                _ = &mut sleep => break,
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SessionEnd {
    Cancelled,
    PingFailed,
    Closed,
    Error,
    Silent,
}

impl SessionEnd {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::PingFailed => "ping send failed",
            Self::Closed => "closed",
            Self::Error => "error",
            Self::Silent => "silent",
        }
    }

    fn end_reason(self) -> EndReason {
        match self {
            Self::Cancelled => EndReason::Shutdown,
            Self::Closed => EndReason::RemoteClose,
            Self::Silent => EndReason::Silence,
            Self::PingFailed | Self::Error => EndReason::IoError,
        }
    }
}

async fn session(
    socket: &mut crate::core::book_connection::Socket,
    events: &mpsc::UnboundedSender<LifecycleEvent>,
    cancel: &CancellationToken,
    totals: &mut LifecycleTotals,
    metrics: &mut Publisher,
) -> SessionEnd {
    let mut publication = interval(PUBLICATION_INTERVAL);
    publication.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ping = interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_frame = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = publication.tick() => metrics.publish_if_due(),
            _ = cancel.cancelled() => return SessionEnd::Cancelled,
            _ = ping.tick() => {
                if socket.send(Message::Text(ws::PING.into())).await.is_err() {
                    return SessionEnd::PingFailed;
                }
            }
            message = socket.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    last_frame = Instant::now();
                    if text.as_str() == ws::PONG {
                        continue;
                    }
                    let frame = ws::decode_lifecycle_frame(&text);
                    totals.malformed += u64::from(frame.malformed);
                    metrics.state.decode_errors += u64::from(frame.malformed);
                    for event in frame.events {
                        let event = match event {
                            MarketLifecycleEvent::NewMarket(market) => {
                                totals.new_markets += 1;
                                LifecycleEvent::NewMarket(market)
                            }
                            MarketLifecycleEvent::Resolved(market) => {
                                totals.resolutions += 1;
                                LifecycleEvent::Resolved(market)
                            }
                        };
                        let _ = events.send(event);
                    }
                }
                Some(Ok(Message::Close(_))) | None => return SessionEnd::Closed,
                Some(Ok(_)) => last_frame = Instant::now(),
                Some(Err(_)) => return SessionEnd::Error,
            },
            _ = tokio::time::sleep_until(last_frame + SILENCE_LIMIT) => return SessionEnd::Silent,

        }
    }
}
