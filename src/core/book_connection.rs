//! An order book connection task (SPEC section 8): one WebSocket carrying
//! many markets. It is the only sender of its market data and of its
//! `MarkUnsynced`, and reads, decodes, routes and sends sequentially.

use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

use crate::core::capture::Capture;
use crate::core::connection::{ConnectionIds, FrameTotals, handle_frame};
use crate::core::decode_log::DecodeErrorLog;
use crate::core::fixture::{FixtureEvent, ProtocolCommand};
use crate::core::metrics::{Component, EndReason, Metrics, PUBLICATION_INTERVAL, Publisher};
use crate::core::rate_limit::RateLimiter;
use crate::core::reconnect_backoff::ReconnectBackoff;
use crate::core::routing::Workers;
use crate::core::telemetry;
use crate::exchange::protocol::BookProtocol;
use crate::model::{ConnectionId, MarketBooks, MarketId, OrderBookId, UnsyncReason, WorkerMessage};

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Reconnect after 30 seconds without a frame of any kind.
pub const SILENCE_LIMIT: Duration = Duration::from_secs(30);
/// Wait for a full book after the initial subscription and each retry.
const FIRST_BOOK_TIMEOUT: Duration = Duration::from_secs(5);
const FIRST_BOOK_RETRIES: u32 = 10;
const CHECK_INTERVAL: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// Asks a connection to change its subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionCommand {
    Subscribe(Vec<MarketBooks>),
    Unsubscribe(Vec<MarketBooks>),
}

/// Everything connection tasks share.
pub struct BookConnectionContext {
    pub recovery: Option<mpsc::Sender<crate::core::recovery::RecoveryState>>,
    pub workers: Workers,
    pub decode_log: Arc<DecodeErrorLog>,
    pub open_limiter: Arc<RateLimiter>,
    pub capture: Option<Capture>,
    pub cancel: CancellationToken,
    pub max_frame_bytes: usize,
    /// Reconnect after this long with nothing received ([`SILENCE_LIMIT`]).
    pub silence_limit: Duration,
    pub metrics: Metrics,
}

struct Pending {
    missing_since: Instant,
    continuous_reported: bool,
    cooldown: Option<Instant>,
    phase: BookWait,
    attempts: u32,
}

enum BookWait {
    Baseline { since: Instant },
    Resubscribe { at: Instant },
}

#[derive(Debug)]
enum SessionEnd {
    Cancelled,
    /// The last market was removed; the socket closes and the task idles.
    Emptied,
    Closed(Option<CloseCode>, String),
    Error(String),
    /// Nothing arrived beyond the configured silence limit.
    Stalled {
        /// Seconds since this connection's last frame of any kind.
        silent_s: f64,
        /// Seconds since the oldest unanswered PING.
        ping_age_s: f64,
    },
    ProtocolFault(UnsyncReason),
}

impl SessionEnd {
    fn describe(&self) -> String {
        match self {
            Self::Cancelled => "cancelled".into(),
            Self::Emptied => "emptied".into(),
            Self::Closed(code, reason) => match code {
                Some(code) => format!("closed {}: {reason}", u16::from(*code)),
                None => format!("closed: {reason}"),
            },
            Self::Error(error) => format!("error: {error}"),
            Self::Stalled {
                silent_s,
                ping_age_s,
            } => format!("stalled: silent {silent_s:.0} s, ping unanswered {ping_age_s:.0} s"),
            Self::ProtocolFault(reason) => format!("protocol fault: {reason:?}"),
        }
    }
}

struct BookConnection<P> {
    protocol: P,
    id: ConnectionId,
    ctx: Arc<BookConnectionContext>,
    ids: ConnectionIds,
    markets: HashMap<MarketId, [OrderBookId; 2]>,
    generations: HashMap<MarketId, u64>,
    attach_pending: HashSet<MarketId>,
    owners: HashMap<OrderBookId, MarketId>,
    reported: HashSet<MarketId>,
    dirty: HashMap<MarketId, crate::core::recovery::RecoveryState>,
    revision: u64,
    session_has_book: bool,
    pending: HashMap<OrderBookId, Pending>,
    totals: FrameTotals,
    metrics: Publisher,
    publication: tokio::time::Interval,
    sessions: u64,
}

/// Runs one connection until cancelled. Returns an error only when a worker
/// is gone, which is fatal.
pub async fn run_book_connection<P: BookProtocol>(
    id: ConnectionId,
    commands: mpsc::UnboundedReceiver<ConnectionCommand>,
    ctx: Arc<BookConnectionContext>,
    protocol: P,
) -> anyhow::Result<()> {
    let mut connection = BookConnection {
        protocol,
        id,
        ctx: ctx.clone(),
        ids: ConnectionIds::default(),
        markets: HashMap::new(),
        generations: HashMap::new(),
        attach_pending: HashSet::new(),
        owners: HashMap::new(),
        reported: HashSet::new(),
        dirty: HashMap::new(),
        revision: 0,
        session_has_book: false,
        pending: HashMap::new(),
        totals: FrameTotals::default(),
        metrics: ctx.metrics.register(Component::Connection(id)),
        publication: interval(PUBLICATION_INTERVAL),
        sessions: 0,
    };
    let result = connection.run(commands).await;
    connection.metrics.state.connected = false;
    connection.publish_metrics();
    connection.metrics.publish();
    result
}

impl<P: BookProtocol> BookConnection<P> {
    fn publish_metrics(&mut self) {
        self.flush_recovery();
        self.metrics
            .state
            .exchange
            .clone_from(&self.totals.exchange_to_receive);
        self.metrics.state.negative = self.totals.negative_exchange_ages;
        self.metrics.state.events = self.totals.events;
        self.metrics.state.socket_markets = self.markets.len() as u64;
        self.metrics.state.decode_errors = self.totals.decode_errors;
        self.metrics.state.expected = !self.ids.is_empty();
        self.metrics.publish_if_due();
    }

    async fn run(
        &mut self,
        mut commands: mpsc::UnboundedReceiver<ConnectionCommand>,
    ) -> anyhow::Result<()> {
        self.publication
            .set_missed_tick_behavior(MissedTickBehavior::Delay);
        self.publish_metrics();
        let mut backoff = ReconnectBackoff::default();
        loop {
            // Idle until there is something to subscribe to.
            while self.ids.is_empty() {
                backoff.reset();
                tokio::select! {
                    _ = self.publication.tick() => self.publish_metrics(),
                    _ = self.ctx.cancel.cancelled() => return Ok(()),
                    command = commands.recv() => match command {
                        Some(command) => { self.apply(command, None).await; }
                        None => return Ok(()),
                    },
                }
            }

            let session_start = Instant::now();
            let end = match self.connect(&mut commands).await {
                Some(socket) => {
                    self.sessions += 1;
                    for market in self.reported.clone() {
                        self.report_recovery(&market, "session_reset");
                    }
                    self.metrics.state.connected = true;
                    self.publish_metrics();
                    let result = self.session(socket, &mut commands).await;
                    self.metrics.state.connected = false;
                    let end = result?;
                    let reason = match &end {
                        SessionEnd::Cancelled => EndReason::Shutdown,
                        SessionEnd::Emptied => EndReason::Emptied,
                        SessionEnd::Closed(..) => EndReason::RemoteClose,
                        SessionEnd::Error(_) => EndReason::IoError,
                        SessionEnd::Stalled { .. } => EndReason::Silence,
                        SessionEnd::ProtocolFault(UnsyncReason::DecodeError) => {
                            EndReason::DecodeError
                        }
                        SessionEnd::ProtocolFault(_) => EndReason::ProtocolError,
                    };
                    self.metrics.end_session(reason);
                    self.publish_metrics();
                    end
                }
                None if self.ctx.cancel.is_cancelled() => SessionEnd::Cancelled,
                None => SessionEnd::Error("connect failed".into()),
            };
            let lasted = session_start.elapsed();
            match &end {
                SessionEnd::Cancelled => return Ok(()),
                SessionEnd::Emptied => {
                    backoff.reset();
                    continue;
                }
                _ => {}
            }

            // The reset goes on the same channels as this connection's data,
            // after everything it read before the gap.
            let reason = match end {
                SessionEnd::Closed(..) => Some(UnsyncReason::RemoteClose),
                SessionEnd::Error(_) => Some(UnsyncReason::IoError),
                // Already ordered before the local close.
                SessionEnd::Stalled { .. } | SessionEnd::ProtocolFault(_) => None,
                SessionEnd::Cancelled | SessionEnd::Emptied => None,
            };
            if let Some(reason) = reason {
                self.mark_unsynced(reason).await?;
            }
            telemetry::emit(
                "session_end",
                json!({
                    "connection": self.id.0,
                    "session": self.sessions,
                    "reason": end.describe(),
                    "lasted_s": lasted.as_secs_f64(),
                    "markets": self.markets.len(),
                }),
            );

            let resume = Instant::now() + backoff.next_delay(lasted);
            loop {
                tokio::select! {
                    _ = self.publication.tick() => self.publish_metrics(),
                    _ = self.ctx.cancel.cancelled() => return Ok(()),
                    _ = tokio::time::sleep_until(resume) => break,
                    command = commands.recv() => match command {
                        Some(command) => { self.apply(command, None).await; }
                        None => return Ok(()),
                    },
                }
            }
        }
    }

    /// Opens the socket and sends the subscription for the whole asset set.
    async fn connect(
        &mut self,
        commands: &mut mpsc::UnboundedReceiver<ConnectionCommand>,
    ) -> Option<Socket> {
        let limiter = self.ctx.open_limiter.clone();
        let wait = limiter.wait();
        tokio::pin!(wait);
        loop {
            tokio::select! {
                _ = self.ctx.cancel.cancelled() => return None,
                _ = self.publication.tick() => self.publish_metrics(),
                _ = &mut wait => break,
            }
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(self.ctx.max_frame_bytes))
            .max_frame_size(Some(self.ctx.max_frame_bytes));
        let request = match self.protocol.connect_request() {
            Ok(request) => request,
            Err(_) => {
                // Do not print authentication headers or signed request contents.
                telemetry::emit(
                    "connect_failed",
                    json!({"connection": self.id.0, "error": "invalid connection request"}),
                );
                return None;
            }
        };
        let connect =
            // TCP_NODELAY: we only send small PING and subscribe messages, so
            // Nagle's algorithm would only delay them.
            tokio_tungstenite::connect_async_with_config(request, Some(config), true);
        let connect = timeout(CONNECT_TIMEOUT, connect);
        tokio::pin!(connect);
        let mut socket = loop {
            tokio::select! {
                _ = self.publication.tick() => self.publish_metrics(),
                _ = self.ctx.cancel.cancelled() => return None,
                result = &mut connect => match result {
                    Ok(Ok((socket, _))) => break socket,
                    Ok(Err(error)) => {
                        telemetry::emit("connect_failed", json!({"connection": self.id.0, "error": error.to_string()}));
                        return None;
                    }
                    Err(_) => {
                        telemetry::emit("connect_failed", json!({"connection": self.id.0, "error": "timeout"}));
                        return None;
                    }
                },
            }
        };
        // Commands that arrived while connecting go into this subscription.
        while let Ok(command) = commands.try_recv() {
            self.apply(command, None).await;
        }
        self.protocol.start_session();
        // A previous session may have ended during a deliberate unsubscribe.
        for (market, books) in &self.markets {
            self.ids.add(market, books);
        }
        self.capture_session_start();
        let books = self.market_books();
        self.capture_protocol_command(|| ProtocolCommand::Subscribe {
            books: books.clone(),
            initial: true,
        });
        let outgoing = self.protocol.subscribe(&books, true);
        if !send_messages(&mut socket, outgoing).await {
            return None;
        }
        let now = Instant::now();
        let mut previous = std::mem::take(&mut self.pending);
        self.pending = self
            .ids
            .order_books()
            .map(|book| {
                let mut pending = previous.remove(book).unwrap_or(Pending {
                    missing_since: now,
                    continuous_reported: false,
                    cooldown: None,
                    phase: BookWait::Baseline { since: now },
                    attempts: 0,
                });
                pending.phase = BookWait::Baseline { since: now };
                pending.attempts = 0;
                (book.clone(), pending)
            })
            .collect();
        self.session_has_book = false;
        if !self.attach_sources().await {
            return None;
        }
        Some(socket)
    }

    async fn session(
        &mut self,
        mut socket: Socket,
        commands: &mut mpsc::UnboundedReceiver<ConnectionCommand>,
    ) -> anyhow::Result<SessionEnd> {
        let heartbeat_interval = self.protocol.heartbeat_interval();
        let mut ping = interval(
            heartbeat_interval
                .unwrap_or(Duration::from_secs(60))
                .max(Duration::from_millis(1)),
        );
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut check = interval(CHECK_INTERVAL);
        check.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut ping_outstanding: Option<Instant> = None;
        let mut last_frame = Instant::now();
        let mut resubscribe_at = None;

        let end = loop {
            tokio::select! {
                biased;
                _ = self.publication.tick() => self.publish_metrics(),
                _ = self.ctx.cancel.cancelled() => break SessionEnd::Cancelled,
                _ = ping.tick(), if heartbeat_interval.is_some() => {
                    self.capture_protocol_command(|| ProtocolCommand::Heartbeat);
                    if let Some(message) = self.protocol.heartbeat() {
                        if socket.send(message).await.is_err() { break SessionEnd::Error("ping send failed".into()); }
                        ping_outstanding.get_or_insert_with(Instant::now);
                    }
                }
                command = commands.recv() => {
                    let Some(command) = command else { break SessionEnd::Cancelled };
                    if !self.apply(command, Some(&mut socket)).await {
                        break SessionEnd::Error("send failed".into());
                    }
                    resubscribe_at = self.next_resubscribe();
                    if self.ids.is_empty() {
                        break SessionEnd::Emptied;
                    }
                }
                _ = async {
                    match resubscribe_at {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => pending().await,
                    }
                } => {
                    if !self.resume_missing_books(&mut socket).await {
                        break SessionEnd::Error("send failed".into());
                    }
                    resubscribe_at = self.next_resubscribe();
                }
                _ = check.tick() => {
                    let now = Instant::now();
                    let silent = now - last_frame;
                    // Suppress initial-book retries while frame arrival has stopped.
                    if silent < CHECK_INTERVAL * 6
                        && !self.request_missing_books(&mut socket, now).await
                    {
                        break SessionEnd::Error("send failed".into());
                    }
                    resubscribe_at = self.next_resubscribe();
                }
                message = socket.next() => {
                    // Capture application receipt immediately after the read, before decoding.
                    let received_instant = std::time::Instant::now();
                    let received_at = Utc::now();
                    if matches!(&message, Some(Ok(_))) {
                        last_frame = Instant::now();
                    }
                    let frame = match message {
                        Some(Ok(Message::Close(frame))) => {
                            let (code, reason) = frame
                                .map(|f| (Some(f.code), f.reason.to_string()))
                                .unwrap_or((None, String::new()));
                            break SessionEnd::Closed(code, reason);
                        }
                        Some(Ok(frame)) => frame,
                        Some(Err(error)) => break SessionEnd::Error(error.to_string()),
                        None => break SessionEnd::Closed(None, "stream ended".into()),
                    };
                    if let Some(capture) = &self.ctx.capture
                        && let Some(event) = FixtureEvent::from_frame(self.id, received_at, &frame)
                    {
                        capture.record(&event);
                    }
                    if matches!(frame, Message::Ping(_)) && socket.flush().await.is_err() {
                        break SessionEnd::Error("control response failed".into());
                    }
                    let mut recovered = Vec::new();
                    let pending = &mut self.pending;
                    let outcome = handle_frame(
                        &mut self.protocol,
                        &frame,
                        self.id,
                        received_at,
                        received_instant,
                        &self.ids,
                        &self.ctx.workers,
                        self.ctx.max_frame_bytes,
                        &self.ctx.decode_log,
                        &mut self.totals,
                        |book| {
                            if pending.remove(book).is_some() { recovered.push(book.clone()); }
                        },
                    )
                    .await?;
                    if !recovered.is_empty() { self.session_has_book = true; }
                    for book in recovered {
                        if let Some(market) = self.owners.get(&book).cloned()
                            && self.reported.contains(&market) { self.report_recovery(&market, "baseline"); }
                    }
                    if outcome.heartbeat_ack { ping_outstanding = None; }
                    if let Some(reason) = outcome.fault {
                        if let Some(capture) = &self.ctx.capture {
                            capture.record(&FixtureEvent::MarkUnsynced { connection: self.id.0, reason });
                        }
                        break SessionEnd::ProtocolFault(reason);
                    }
                    if !send_messages(&mut socket, outcome.outgoing).await {
                        break SessionEnd::Error("protocol response failed".into());
                    }
                }
                _ = tokio::time::sleep_until(last_frame + self.ctx.silence_limit) => {
                    let now = Instant::now();
                    break SessionEnd::Stalled {
                        silent_s: (now - last_frame).as_secs_f64(),
                        ping_age_s: ping_outstanding.map_or(0.0, |sent| (now - sent).as_secs_f64()),
                    };
                }

            }
        };

        // Invalidate first: graceful close must not delay standby promotion.
        if matches!(end, SessionEnd::Stalled { .. }) {
            self.mark_unsynced(UnsyncReason::Stalled).await?;
        }
        if !matches!(end, SessionEnd::Closed(..) | SessionEnd::Error(_)) {
            let _ = timeout(CLOSE_TIMEOUT, socket.close(None)).await;
        }
        Ok(end)
    }

    /// Applies a subscription change. With a socket, sends it; returns false
    /// if the send failed.
    async fn apply(&mut self, command: ConnectionCommand, socket: Option<&mut Socket>) -> bool {
        let (subscribing, changes) = match command {
            ConnectionCommand::Subscribe(changes) => (true, changes),
            ConnectionCommand::Unsubscribe(changes) => (false, changes),
        };
        let mut changed = Vec::new();
        let now = Instant::now();
        for MarketBooks {
            generation,
            market,
            order_books: books,
        } in changes
        {
            if subscribing {
                if self
                    .generations
                    .get(&market)
                    .is_some_and(|current| *current > generation)
                {
                    continue;
                }
                self.generations.insert(market.clone(), generation);
                if generation != 0 {
                    self.attach_pending.insert(market.clone());
                }
                for book in &books {
                    self.owners.insert(book.clone(), market.clone());
                }
                self.ids.add(&market, &books);
                for book in &books {
                    self.pending.insert(
                        book.clone(),
                        Pending {
                            missing_since: now,
                            continuous_reported: false,
                            cooldown: None,
                            phase: BookWait::Baseline { since: now },
                            attempts: 0,
                        },
                    );
                }
                changed.push(MarketBooks {
                    generation,
                    market: market.clone(),
                    order_books: books.clone(),
                });
                self.markets.insert(market, books);
            } else if self
                .generations
                .get(&market)
                .is_some_and(|current| *current == generation)
                && let Some(books) = self.markets.remove(&market)
            {
                self.generations.remove(&market);
                if let Some(capture) = &self.ctx.capture {
                    capture.record(&FixtureEvent::SourceAttached {
                        market: market.to_string(),
                        generation,
                        connection: self.id.0,
                    });
                }
                self.attach_pending.remove(&market);
                self.reported.remove(&market);
                self.dirty.remove(&market);
                for book in &books {
                    self.owners.remove(book);
                }
                self.ids.remove(&market, &books);
                for book in &books {
                    self.pending.remove(book);
                }
                changed.push(MarketBooks {
                    generation,
                    market,
                    order_books: books,
                });
            }
        }
        self.publish_metrics();
        let Some(socket) = socket else {
            return true;
        };
        if changed.is_empty() {
            return true;
        }
        let outgoing = if subscribing {
            self.capture_protocol_command(|| ProtocolCommand::Subscribe {
                books: changed.clone(),
                initial: false,
            });
            self.protocol.subscribe(&changed, false)
        } else {
            self.capture_protocol_command(|| ProtocolCommand::Unsubscribe {
                books: changed.clone(),
            });
            self.protocol.unsubscribe(&changed)
        };
        let sent = send_messages(socket, outgoing).await;
        if sent && subscribing {
            self.attach_sources().await
        } else {
            sent
        }
    }

    /// Starts a retry. Protocol-specific preparation can defer the actual
    /// request without blocking this connection's reads or heartbeats.
    async fn request_missing_books(&mut self, socket: &mut Socket, now: Instant) -> bool {
        let mut due: Vec<OrderBookId> = Vec::new();
        let mut max_attempts = 0;
        let mut reports = HashSet::new();
        for (book, pending) in &mut self.pending {
            if let Some(until) = pending.cooldown {
                if now < until {
                    continue;
                }
                pending.cooldown = None;
                pending.attempts = 0;
            }
            if let BookWait::Baseline { since } = pending.phase
                && pending.attempts == FIRST_BOOK_RETRIES
                && now - since >= FIRST_BOOK_TIMEOUT
            {
                pending.cooldown = Some(now + Duration::from_secs(60));
                if let Some(market) = self.owners.get(book) {
                    reports.insert(market.clone());
                }
                telemetry::emit(
                    "first_book_round_exhausted",
                    json!({"connection":self.id.0,"order_book":book,"missing_s":now.duration_since(pending.missing_since).as_secs_f64()}),
                );
                continue;
            }
            if let BookWait::Baseline { since } = pending.phase
                && pending.attempts < FIRST_BOOK_RETRIES
                && now - since >= FIRST_BOOK_TIMEOUT
            {
                pending.attempts += 1;
                max_attempts = max_attempts.max(pending.attempts);
                due.push(book.clone());
            }
        }
        for market in reports {
            self.report_recovery(&market, "round_exhausted");
        }
        if due.is_empty() {
            return true;
        }
        let fields =
            json!({"connection": self.id.0, "books": due.len(), "max_attempts": max_attempts});
        telemetry::emit("first_book_missing", fields);
        if let Some(preparation) = self.protocol.prepare_book_request(&due) {
            self.capture_protocol_command(|| ProtocolCommand::PrepareBooks { books: due.clone() });
            if !send_messages(socket, preparation.outgoing).await {
                return false;
            }
            self.ids.pause_books(&due);
            let at = Instant::now() + preparation.delay;
            for book in &due {
                if let Some(pending) = self.pending.get_mut(book) {
                    pending.phase = BookWait::Resubscribe { at };
                }
            }
            return true;
        }
        self.send_book_request(socket, due).await
    }

    fn next_resubscribe(&self) -> Option<Instant> {
        self.pending
            .values()
            .filter_map(|pending| match pending.phase {
                BookWait::Resubscribe { at } => Some(at),
                BookWait::Baseline { .. } => None,
            })
            .min()
    }

    async fn resume_missing_books(&mut self, socket: &mut Socket) -> bool {
        let now = Instant::now();
        let due = self
            .pending
            .iter()
            .filter_map(|(book, pending)| match pending.phase {
                BookWait::Resubscribe { at } if at <= now => Some(book.clone()),
                _ => None,
            })
            .collect();
        self.send_book_request(socket, due).await
    }

    async fn send_book_request(&mut self, socket: &mut Socket, due: Vec<OrderBookId>) -> bool {
        if due.is_empty() {
            return true;
        }
        self.capture_protocol_command(|| ProtocolCommand::RequestBooks { books: due.clone() });
        let outgoing = self.protocol.request_books(&due);
        let sent = send_messages(socket, outgoing).await;
        if sent {
            self.ids.resume_books(&due);
            let since = Instant::now();
            for book in &due {
                if let Some(pending) = self.pending.get_mut(book) {
                    pending.phase = BookWait::Baseline { since };
                }
            }
            if self.session_has_book {
                let mut reports = HashSet::new();
                for book in &due {
                    if let Some(pending) = self.pending.get_mut(book)
                        && !pending.continuous_reported
                        && since.duration_since(pending.missing_since) >= Duration::from_secs(180)
                    {
                        pending.continuous_reported = true;
                        if let Some(market) = self.owners.get(book) {
                            reports.insert(market.clone());
                        }
                    }
                }
                for market in reports {
                    self.report_recovery(&market, "continuous_missing");
                }
            }
            let exhausted = due
                .iter()
                .filter(|book| {
                    self.pending
                        .get(*book)
                        .is_some_and(|pending| pending.attempts == FIRST_BOOK_RETRIES)
                })
                .count();
            if exhausted > 0 {
                telemetry::emit(
                    "first_book_retry_budget_used",
                    json!({"connection": self.id.0, "books": exhausted}),
                );
            }
        }
        sent
    }

    fn market_books(&self) -> Vec<MarketBooks> {
        self.markets
            .iter()
            .map(|(market, books)| MarketBooks {
                generation: self.generations.get(market).copied().unwrap_or(0),
                market: market.clone(),
                order_books: books.clone(),
            })
            .collect()
    }

    async fn attach_sources(&mut self) -> bool {
        let markets: Vec<_> = self.attach_pending.iter().cloned().collect();
        for market in markets {
            let generation = self.generations.get(&market).copied().unwrap_or(0);
            if self
                .ctx
                .workers
                .send(WorkerMessage::SourceAttached {
                    market: market.clone(),
                    generation,
                    connection: self.id,
                })
                .await
                .is_err()
            {
                return false;
            }
            if let Some(capture) = &self.ctx.capture {
                capture.record(&FixtureEvent::SourceAttached {
                    market: market.to_string(),
                    generation,
                    connection: self.id.0,
                });
            }
            self.attach_pending.remove(&market);
        }
        true
    }

    fn report_recovery(&mut self, market: &MarketId, trigger: &'static str) {
        if self.ctx.recovery.is_none() {
            return;
        }
        let Some(books) = self.markets.get(market) else {
            return;
        };
        let missing: Vec<_> = books
            .iter()
            .filter(|book| self.pending.contains_key(*book))
            .cloned()
            .collect();
        let missing_s = missing
            .iter()
            .filter_map(|book| self.pending.get(book))
            .map(|pending| pending.missing_since.elapsed().as_secs_f64())
            .fold(0.0, f64::max);
        let complete = missing.is_empty();
        self.revision += 1;
        self.dirty.insert(
            market.clone(),
            crate::core::recovery::RecoveryState {
                market: market.clone(),
                generation: self.generations.get(market).copied().unwrap_or(0),
                connection: self.id,
                session: self.sessions,
                revision: self.revision,
                trigger,
                missing_s,
                missing,
            },
        );
        if complete {
            self.reported.remove(market);
        } else {
            self.reported.insert(market.clone());
        }
        self.flush_recovery();
    }

    fn flush_recovery(&mut self) {
        let Some(sender) = &self.ctx.recovery else {
            return;
        };
        self.dirty
            .retain(|_, report| sender.try_send(report.clone()).is_err());
    }

    fn capture_session_start(&self) {
        if let Some(capture) = &self.ctx.capture {
            capture.record(&FixtureEvent::SessionStart {
                connection: self.id.0,
            });
        }
    }

    /// Builds the command only when capture is enabled.
    fn capture_protocol_command(&self, command: impl FnOnce() -> ProtocolCommand) {
        if let Some(capture) = &self.ctx.capture {
            capture.record(&FixtureEvent::ProtocolCommand {
                connection: self.id.0,
                command: command(),
            });
        }
    }

    async fn mark_unsynced(&mut self, reason: UnsyncReason) -> anyhow::Result<()> {
        if let Some(capture) = &self.ctx.capture {
            capture.record(&FixtureEvent::MarkUnsynced {
                connection: self.id.0,
                reason,
            });
        }
        let now = Instant::now();
        for books in self.markets.values() {
            for book in books {
                self.pending.entry(book.clone()).or_insert(Pending {
                    missing_since: now,
                    continuous_reported: false,
                    cooldown: None,
                    phase: BookWait::Baseline { since: now },
                    attempts: 0,
                });
            }
        }
        let message = WorkerMessage::MarkUnsynced {
            connection: self.id,
            reason,
        };
        self.ctx
            .workers
            .send(message)
            .await
            .context("send MarkUnsynced")
    }
}

async fn send_messages(socket: &mut Socket, messages: Vec<Message>) -> bool {
    for message in messages {
        if socket.send(message).await.is_err() {
            return false;
        }
    }
    true
}
