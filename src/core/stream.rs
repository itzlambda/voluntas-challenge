//! Stream output mode: replaces the file writer with one hub task that routes
//! each record to the WebSocket clients subscribed to its order book.
//!
//! The hub encodes every record once, including when there are no subscribers.
//! Encoded bytes are shared with subscribers, or discarded if there are none. A
//! client whose queue is full is disconnected with close code 1013: updates
//! are deltas, so skipping one would silently corrupt that client's book.
//!
//! Protocol. The client sends text frames
//! `{"op":"subscribe","books":["<token id>",...]}` and
//! `{"op":"unsubscribe","books":[...]}`. The server sends binary frames, each
//! holding one or more complete SBE messages back to back, in the same
//! encoding and per-book order as file mode. A new subscriber joins
//! mid-stream, so it must ignore a book's updates until that book's first
//! snapshot.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::core::config::Config;
use crate::core::metrics::{PUBLICATION_INTERVAL, Publisher};
use crate::core::telemetry;
use crate::core::writer::{WriterTotals, reusable_update, with_message};
use crate::model::{OrderBookId, OutputRecord};

/// Largest binary frame a client task assembles from its queue.
const MAX_OUTBOUND_FRAME_BYTES: usize = 1 << 20;
/// Largest control message accepted from a client.
const MAX_INBOUND_MESSAGE_BYTES: usize = 1 << 20;
/// Pending control messages across all clients; a full queue slows only the
/// clients sending controls, never record routing.
const CONTROL_QUEUE: usize = 1024;
/// How long the hub waits for client tasks to close their sockets at shutdown.
const CLIENT_DRAIN_DEADLINE: Duration = Duration::from_secs(2);
/// How long a close frame may take to send.
const CLOSE_DEADLINE: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct StreamSettings {
    pub bind: SocketAddr,
    pub client_queue: usize,
    pub max_books: usize,
    pub max_clients: usize,
    pub allowed_origins: Arc<[String]>,
}

impl From<&Config> for StreamSettings {
    fn from(config: &Config) -> Self {
        Self {
            bind: config.stream_bind,
            client_queue: config.stream_client_queue,
            max_books: config.stream_max_books,
            max_clients: config.stream_max_clients,
            allowed_origins: config.stream_allowed_origins.clone().into(),
        }
    }
}

pub struct StreamHandle {
    pub address: SocketAddr,
    /// Resolves after the record channel closes and clients are told to go.
    pub done: oneshot::Receiver<anyhow::Result<WriterTotals>>,
}

/// Binds the listener and starts the hub on the current runtime. Totals
/// count all encoded output records, independently of subscribers.
pub fn spawn_stream(
    settings: StreamSettings,
    records: mpsc::Receiver<Vec<OutputRecord>>,
    metrics: Publisher,
) -> anyhow::Result<StreamHandle> {
    let listener = std::net::TcpListener::bind(settings.bind)
        .with_context(|| format!("bind stream listener {}", settings.bind))?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;
    let address = listener.local_addr()?;
    let (controls_tx, controls) = mpsc::channel(CONTROL_QUEUE);
    let stop = CancellationToken::new();
    let tracker = TaskTracker::new();
    tracker.spawn(accept_loop(
        listener,
        controls_tx,
        settings.clone(),
        stop.clone(),
        tracker.clone(),
    ));
    let (done_tx, done) = oneshot::channel();
    let failure = metrics.failure_token();
    tokio::spawn(async move {
        let guard = failure.drop_guard();
        let result = Hub::new(settings.max_books)
            .run(records, controls, metrics)
            .await;
        if result.is_ok() {
            guard.disarm();
        }
        // Dropping the hub closed every client queue; each task now sends 1001.
        stop.cancel();
        tracker.close();
        let _ = tokio::time::timeout(CLIENT_DRAIN_DEADLINE, tracker.wait()).await;
        let _ = done_tx.send(result);
    });
    Ok(StreamHandle { address, done })
}

type ClientId = u64;

#[derive(Debug)]
enum Control {
    Join {
        id: ClientId,
        frames: mpsc::Sender<Bytes>,
        kick: oneshot::Sender<Kick>,
    },
    Subscribe {
        id: ClientId,
        books: Vec<OrderBookId>,
    },
    Unsubscribe {
        id: ClientId,
        books: Vec<OrderBookId>,
    },
    Leave {
        id: ClientId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kick {
    SlowConsumer,
    TooManyBooks,
    Shutdown,
}

impl Kick {
    fn close_frame(self) -> CloseFrame {
        let (code, reason) = match self {
            Self::SlowConsumer => (CloseCode::Again, "slow consumer"),
            Self::TooManyBooks => (CloseCode::Policy, "too many books"),
            Self::Shutdown => (CloseCode::Away, "shutting down"),
        };
        CloseFrame {
            code,
            reason: reason.into(),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::SlowConsumer => "slow_consumer",
            Self::TooManyBooks => "too_many_books",
            Self::Shutdown => "shutdown",
        }
    }
}

struct Client {
    frames: mpsc::Sender<Bytes>,
    kick: Option<oneshot::Sender<Kick>>,
    books: HashSet<OrderBookId>,
}

struct Hub {
    max_books: usize,
    clients: HashMap<ClientId, Client>,
    subscribers: HashMap<OrderBookId, Vec<ClientId>>,
    totals: WriterTotals,
}

impl Hub {
    fn new(max_books: usize) -> Self {
        Self {
            max_books,
            clients: HashMap::new(),
            subscribers: HashMap::new(),
            totals: WriterTotals::default(),
        }
    }

    async fn run(
        mut self,
        mut records: mpsc::Receiver<Vec<OutputRecord>>,
        mut controls: mpsc::Receiver<Control>,
        mut metrics: Publisher,
    ) -> anyhow::Result<WriterTotals> {
        let mut publish = tokio::time::interval(PUBLICATION_INTERVAL);
        publish.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Subscription changes first, so a fresh subscriber's records
                // are not routed past it.
                biased;
                Some(control) = controls.recv() => self.control(control),
                batch = records.recv() => {
                    let Some(batch) = batch else { break };
                    self.totals.queue_peak = self.totals.queue_peak.max(records.len() + 1);
                    let started = std::time::Instant::now();
                    for record in batch {
                        self.route(record)?;
                    }
                    self.totals.encode_time += started.elapsed();
                    metrics.state.busy_seconds = self.totals.encode_time.as_secs_f64();
                }
                _ = publish.tick() => {
                    metrics.state.internal.clone_from(&self.totals.receive_to_encode);
                    metrics.publish();
                }
            }
        }
        for client in self.clients.values_mut() {
            if let Some(kick) = client.kick.take() {
                let _ = kick.send(Kick::Shutdown);
            }
        }
        metrics
            .state
            .internal
            .clone_from(&self.totals.receive_to_encode);
        metrics.publish();
        Ok(self.totals)
    }

    fn route(&mut self, record: OutputRecord) -> anyhow::Result<()> {
        let ids = self.subscribers.get(record.order_book());
        let received_instant = match &record {
            OutputRecord::LevelUpdate {
                received_instant, ..
            } => *received_instant,
            OutputRecord::Snapshot { .. } => None,
        };
        let snapshot = matches!(record, OutputRecord::Snapshot { .. });
        // Snapshots and updates are interleaved per book, so the reused
        // update's allocation would rarely survive; a fresh one is simpler.
        let bytes = with_message(record, &mut reusable_update(), |message| {
            let mut buffer = vec![0; message.encoded_length_with_header()?];
            let written = message.encode(&mut buffer)?;
            anyhow::ensure!(written == buffer.len(), "SBE encoded length mismatch");
            Ok(Bytes::from(buffer))
        })
        .context("encode stream record")?;
        if snapshot {
            self.totals.snapshots += 1;
        } else {
            self.totals.level_updates += 1;
        }
        self.totals.bytes += bytes.len() as u64;
        if let Some(received_instant) = received_instant {
            self.totals
                .receive_to_encode
                .record(received_instant.elapsed().as_secs_f64());
        }
        let Some(ids) = ids else {
            return Ok(());
        };
        let mut slow = Vec::new();
        for id in ids {
            let Some(client) = self.clients.get(id) else {
                continue;
            };
            match client.frames.try_send(bytes.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    slow.push((*id, Some(Kick::SlowConsumer)))
                }
                // The task is gone; its Leave is on the way.
                Err(mpsc::error::TrySendError::Closed(_)) => slow.push((*id, None)),
            }
        }
        for (id, kick) in slow {
            self.remove(id, kick);
        }
        Ok(())
    }

    fn control(&mut self, control: Control) {
        match control {
            Control::Join { id, frames, kick } => {
                self.clients.insert(
                    id,
                    Client {
                        frames,
                        kick: Some(kick),
                        books: HashSet::new(),
                    },
                );
            }
            Control::Subscribe { id, books } => {
                let Some(client) = self.clients.get_mut(&id) else {
                    return;
                };
                let added: Vec<_> = books
                    .into_iter()
                    .filter(|book| client.books.insert(book.clone()))
                    .collect();
                if client.books.len() > self.max_books {
                    self.remove(id, Some(Kick::TooManyBooks));
                    return;
                }
                for book in added {
                    self.subscribers.entry(book).or_default().push(id);
                }
            }
            Control::Unsubscribe { id, books } => {
                let Some(client) = self.clients.get_mut(&id) else {
                    return;
                };
                for book in books {
                    if client.books.remove(&book) {
                        unsubscribe(&mut self.subscribers, &book, id);
                    }
                }
            }
            Control::Leave { id } => self.remove(id, None),
        }
    }

    /// Forgets a client. With a kick, its task closes the socket with that reason.
    fn remove(&mut self, id: ClientId, kick: Option<Kick>) {
        let Some(mut client) = self.clients.remove(&id) else {
            return;
        };
        for book in &client.books {
            unsubscribe(&mut self.subscribers, book, id);
        }
        if let Some(kick) = kick {
            telemetry::emit(
                "stream_client_kicked",
                json!({"client":id,"reason":kick.label(),"books":client.books.len()}),
            );
            if let Some(sender) = client.kick.take() {
                let _ = sender.send(kick);
            }
        }
    }
}

fn unsubscribe(
    subscribers: &mut HashMap<OrderBookId, Vec<ClientId>>,
    book: &OrderBookId,
    id: ClientId,
) {
    if let Some(ids) = subscribers.get_mut(book) {
        ids.retain(|other| *other != id);
        if ids.is_empty() {
            subscribers.remove(book);
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    controls: mpsc::Sender<Control>,
    settings: StreamSettings,
    stop: CancellationToken,
    tracker: TaskTracker,
) {
    let permits = Arc::new(Semaphore::new(settings.max_clients));
    let mut next_id: ClientId = 0;
    loop {
        let (socket, peer) = tokio::select! {
            _ = stop.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    // Usually descriptor exhaustion; back off instead of spinning.
                    telemetry::emit("stream_accept_failed", json!({"error":error.to_string()}));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            drop(socket);
            continue;
        };
        next_id += 1;
        let id = next_id;
        let controls = controls.clone();
        let stop = stop.clone();
        let settings = settings.clone();
        tracker.spawn(async move {
            let _permit = permit;
            if let Err(error) = serve_client(id, socket, controls.clone(), settings, stop).await {
                telemetry::emit(
                    "stream_client_error",
                    json!({"client":id,"peer":peer.to_string(),"error":format!("{error:#}")}),
                );
            }
            // Harmless if the hub already removed this client.
            let _ = controls.send(Control::Leave { id }).await;
        });
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
enum Request {
    Subscribe { books: Vec<String> },
    Unsubscribe { books: Vec<String> },
}

async fn serve_client(
    id: ClientId,
    socket: TcpStream,
    controls: mpsc::Sender<Control>,
    settings: StreamSettings,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    socket.set_nodelay(true)?;
    let config = WebSocketConfig::default().max_message_size(Some(MAX_INBOUND_MESSAGE_BYTES));
    use tokio_tungstenite::tungstenite::handshake::server::{
        ErrorResponse, Request as HandshakeRequest, Response as HandshakeResponse,
    };
    let origins = settings.allowed_origins;
    #[allow(
        clippy::result_large_err,
        reason = "tungstenite requires an unboxed HTTP error response"
    )]
    let callback = move |request: &HandshakeRequest,
                         response: HandshakeResponse|
          -> Result<HandshakeResponse, ErrorResponse> {
        let mut headers = request.headers().get_all("origin").iter();
        let allowed = match headers.next() {
            None => true,
            Some(origin) => {
                headers.next().is_none()
                    && (origins.is_empty()
                        || origin
                            .to_str()
                            .is_ok_and(|value| origins.iter().any(|allowed| allowed == value)))
            }
        };
        if allowed {
            Ok(response)
        } else {
            let mut error = ErrorResponse::new(Some("origin not allowed".into()));
            *error.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::FORBIDDEN;
            Err(error)
        }
    };
    let ws = tokio::select! {
        _ = stop.cancelled() => return Ok(()),
        ws = tokio::time::timeout(Duration::from_secs(5),
            tokio_tungstenite::accept_hdr_async_with_config(socket, callback, Some(config))) => {
            ws.context("WebSocket handshake timeout")?.context("WebSocket handshake")?
        }
    };
    let (frames_tx, mut frames) = mpsc::channel(settings.client_queue);
    let (kick_tx, mut kick) = oneshot::channel();
    if controls
        .send(Control::Join {
            id,
            frames: frames_tx,
            kick: kick_tx,
        })
        .await
        .is_err()
    {
        return Ok(());
    }
    let (mut sink, mut source) = ws.split();
    loop {
        tokio::select! {
            // The hub sends a kick before dropping the queue, so checking it
            // first reports the real reason. A dropped sender also means the
            // hub is gone.
            biased;
            kicked = &mut kick => {
                let kicked = kicked.unwrap_or(Kick::Shutdown);
                close(&mut sink, &mut source, kicked.close_frame()).await;
                return Ok(());
            }
            first = frames.recv() => {
                let Some(first) = first else {
                    // Unreachable in practice: the kick above arrives first.
                    close(&mut sink, &mut source, Kick::Shutdown.close_frame()).await;
                    return Ok(());
                };
                let send = sink.send(Message::Binary(gather(first, &mut frames)));
                tokio::pin!(send);
                tokio::select! {
                    sent = &mut send => sent.context("send records")?,
                    kicked = &mut kick => {
                        let kicked = kicked.unwrap_or(Kick::Shutdown);
                        if kicked != Kick::Shutdown {
                            // Kicked while blocked on a stuck socket: drop it
                            // without a close frame, as a frame may be half written.
                            return Ok(());
                        }
                        send.await.context("send records")?;
                        close(&mut sink, &mut source, kicked.close_frame()).await;
                        return Ok(());
                    }
                }
            }
            inbound = source.next() => {
                let text = match inbound {
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                    Some(Ok(Message::Binary(_))) => {
                        close(&mut sink, &mut source, CloseFrame { code: CloseCode::Unsupported, reason: "text requests only".into() }).await;
                        return Ok(());
                    }
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    // Browsers vanish without a close handshake routinely.
                    Some(Err(WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => return Ok(()),
                    Some(Err(error)) => return Err(error).context("read request"),
                };
                let control = match serde_json::from_str::<Request>(&text) {
                    Ok(Request::Subscribe { books }) => Control::Subscribe { id, books: book_ids(books) },
                    Ok(Request::Unsubscribe { books }) => Control::Unsubscribe { id, books: book_ids(books) },
                    Err(_) => {
                        close(&mut sink, &mut source, CloseFrame { code: CloseCode::Policy, reason: "invalid request".into() }).await;
                        return Ok(());
                    }
                };
                if controls.send(control).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

fn book_ids(books: Vec<String>) -> Vec<OrderBookId> {
    books
        .into_iter()
        .map(|book| OrderBookId(book.into()))
        .collect()
}

/// Sends whatever is already queued with `first`, without waiting for more:
/// one message alone when idle, a coalesced frame when the client lags.
fn gather(first: Bytes, frames: &mut mpsc::Receiver<Bytes>) -> Bytes {
    let Ok(second) = frames.try_recv() else {
        return first;
    };
    let mut frame = Vec::with_capacity(first.len() * 2);
    frame.extend_from_slice(&first);
    frame.extend_from_slice(&second);
    while frame.len() < MAX_OUTBOUND_FRAME_BYTES {
        match frames.try_recv() {
            Ok(next) => frame.extend_from_slice(&next),
            Err(_) => break,
        }
    }
    Bytes::from(frame)
}

/// Sends a close frame, then waits briefly for the client's reply so the
/// socket is not dropped with unread input, which would reset it and could
/// discard the close frame before the client reads it.
async fn close(
    sink: &mut futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>,
    source: &mut futures_util::stream::SplitStream<WebSocketStream<TcpStream>>,
    frame: CloseFrame,
) {
    let _ = tokio::time::timeout(CLOSE_DEADLINE, async {
        if sink.send(Message::Close(Some(frame))).await.is_ok() {
            while let Some(Ok(message)) = source.next().await {
                if message.is_close() {
                    break;
                }
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::core::metrics::{Component, Metrics};
    use crate::model::{Price, Qty};
    use crate::util::dtos::challenge_orderbook::{
        AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
    };
    use crate::util::{AlignedSbeSchema, OrderbookSchema};

    type Client = WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

    fn settings(client_queue: usize, max_books: usize) -> StreamSettings {
        StreamSettings {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            client_queue,
            max_books,
            max_clients: 64,
            allowed_origins: Arc::from([]),
        }
    }

    fn update(book: &str, price: i64) -> OutputRecord {
        OutputRecord::LevelUpdate {
            received_instant: Some(std::time::Instant::now()),
            order_book: OrderBookId(book.into()),
            side: Side::Buy,
            price: Price(price),
            qty: Qty(5),
            received_at: chrono::DateTime::UNIX_EPOCH,
            exchange_ts: chrono::DateTime::UNIX_EPOCH,
        }
    }

    fn snapshot(book: &str, levels: i64) -> OutputRecord {
        OutputRecord::Snapshot {
            order_book: OrderBookId(book.into()),
            bids: (1..=levels).map(|price| (Price(price), Qty(1))).collect(),
            asks: Vec::new(),
            received_at: chrono::DateTime::UNIX_EPOCH,
            exchange_ts: chrono::DateTime::UNIX_EPOCH,
        }
    }

    /// Splits a binary frame into (symbol, price or snapshot level count).
    fn decode(frame: &[u8]) -> Vec<(String, i64)> {
        let mut offset = 0;
        let mut out = Vec::new();
        while offset < frame.len() {
            let message = OrderbookSchema::try_decode(frame, offset).unwrap();
            offset += OrderbookSchema::encoded_length(&message).unwrap();
            out.push(match message {
                AnyMessage::PriceLevelUpdate(decoder) => {
                    let update = PriceLevelUpdateDomain::try_from_decoder(decoder).unwrap();
                    (update.symbol, update.price_level)
                }
                AnyMessage::Snapshot(decoder) => {
                    let snapshot = SnapshotDomain::try_from_decoder(decoder).unwrap();
                    (snapshot.symbol, snapshot.bids.len() as i64)
                }
                _ => panic!("unknown message"),
            });
        }
        out
    }

    async fn connect(address: SocketAddr, books: &[&str]) -> Client {
        let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
            .await
            .unwrap();
        let request = json!({"op":"subscribe","books":books}).to_string();
        client.send(Message::text(request)).await.unwrap();
        client
    }

    /// Reads binary frames until `count` messages arrived.
    async fn receive(client: &mut Client, count: usize) -> Vec<(String, i64)> {
        let mut out = Vec::new();
        while out.len() < count {
            match client.next().await.unwrap().unwrap() {
                Message::Binary(frame) => out.extend(decode(&frame)),
                other => panic!("unexpected {other:?}"),
            }
        }
        out
    }

    async fn close_code(client: &mut Client) -> Option<CloseCode> {
        loop {
            match client.next().await {
                Some(Ok(Message::Close(frame))) => return frame.map(|frame| frame.code),
                Some(Ok(_)) => continue,
                Some(Err(WsError::ConnectionClosed)) | None => return None,
                Some(Err(error)) => panic!("read failed: {error}"),
            }
        }
    }

    fn start(settings: StreamSettings) -> (StreamHandle, mpsc::Sender<Vec<OutputRecord>>) {
        let (records, rx) = mpsc::channel(1024);
        let metrics = Metrics::default().register(Component::Writer);
        (spawn_stream(settings, rx, metrics).unwrap(), records)
    }

    /// Subscribes `client` to its own `probe` book and waits until the probe
    /// routes. The hub handles one client's requests in order, so every
    /// earlier request from it has then been applied.
    async fn sync(records: &mpsc::Sender<Vec<OutputRecord>>, client: &mut Client, probe: &str) {
        let request = json!({"op":"subscribe","books":[probe]}).to_string();
        client.send(Message::text(request)).await.unwrap();
        loop {
            records.send(vec![update(probe, -1)]).await.unwrap();
            if let Ok(Some(Ok(Message::Binary(frame)))) =
                tokio::time::timeout(Duration::from_millis(20), client.next()).await
            {
                assert!(decode(&frame).iter().all(|(symbol, _)| symbol == probe));
                // Drain any further probes already in flight.
                while let Ok(Some(Ok(_))) =
                    tokio::time::timeout(Duration::from_millis(20), client.next()).await
                {
                }
                let request = json!({"op":"unsubscribe","books":[probe]}).to_string();
                client.send(Message::text(request)).await.unwrap();
                return;
            }
        }
    }

    #[tokio::test]
    async fn encodes_and_records_latency_without_clients() {
        let (stream, records) = start(settings(64, 16));
        records
            .send(vec![snapshot("A", 2), update("A", 1), update("B", 3)])
            .await
            .unwrap();
        drop(records);
        let totals = stream.done.await.unwrap().unwrap();
        assert_eq!(totals.snapshots, 1);
        assert_eq!(totals.level_updates, 2);
        assert_eq!(totals.receive_to_encode.count, 2);
        assert!(totals.bytes > 0);
    }

    #[tokio::test]
    async fn routes_only_subscribed_books_in_order() {
        let (stream, records) = start(settings(64, 16));
        let mut a = connect(stream.address, &["A"]).await;
        let mut b = connect(stream.address, &["A", "B"]).await;
        sync(&records, &mut a, "probe-a").await;
        sync(&records, &mut b, "probe-b").await;
        records
            .send(vec![
                snapshot("A", 2),
                snapshot("C", 9),
                update("B", 3),
                update("A", 1),
            ])
            .await
            .unwrap();
        assert_eq!(receive(&mut a, 2).await, [("A".into(), 2), ("A".into(), 1)]);
        assert_eq!(
            receive(&mut b, 3).await,
            [("A".into(), 2), ("B".into(), 3), ("A".into(), 1)]
        );

        a.send(Message::text(r#"{"op":"unsubscribe","books":["A"]}"#))
            .await
            .unwrap();
        sync(&records, &mut a, "probe-a2").await;
        records.send(vec![update("A", 7)]).await.unwrap();
        assert_eq!(receive(&mut b, 1).await, [("A".into(), 7)]);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), a.next())
                .await
                .is_err()
        );

        drop(records);
        // Read both at once: each server task waits only briefly for a reply.
        let codes = tokio::join!(close_code(&mut a), close_code(&mut b));
        assert_eq!(codes, (Some(CloseCode::Away), Some(CloseCode::Away)));
        let totals = stream.done.await.unwrap().unwrap();
        // The unsubscribed "C" snapshot was encoded but sent to neither client.
        assert_eq!(totals.snapshots, 2);
    }

    #[tokio::test]
    async fn disconnects_a_client_that_cannot_keep_up() {
        let (stream, records) = start(settings(1, 16));
        let mut slow = connect(stream.address, &["A"]).await;
        sync(&records, &mut slow, "probe").await;
        // About 64 MB of snapshots, far beyond socket buffers, while the
        // client reads nothing.
        for _ in 0..64 {
            records.send(vec![snapshot("A", 60_000)]).await.unwrap();
        }
        let mut received = 0;
        let ended = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match slow.next().await {
                    Some(Ok(Message::Binary(frame))) => received += decode(&frame).len(),
                    Some(Ok(Message::Close(frame))) => {
                        return frame.map(|frame| frame.code);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => return None,
                }
            }
        })
        .await
        .expect("slow client was not disconnected");
        assert!(received < 64, "received every snapshot: {received}");
        assert!(
            matches!(ended, None | Some(CloseCode::Again)),
            "{ended:?} after {received}"
        );
        drop(records);
        stream.done.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn enforces_client_limit_and_releases_capacity_after_disconnect() {
        let mut config = settings(16, 2);
        config.max_clients = 1;
        let (stream, records) = start(config);
        let first = connect(stream.address, &[]).await;
        assert!(
            tokio_tungstenite::connect_async(format!("ws://{}", stream.address))
                .await
                .is_err()
        );
        drop(first);
        let next = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok((client, _)) =
                    tokio_tungstenite::connect_async(format!("ws://{}", stream.address)).await
                {
                    break client;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(next);
        drop(records);
        stream.done.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rejects_unlisted_origins_and_accepts_allowed_or_native_clients() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut config = settings(16, 2);
        config.allowed_origins = vec!["https://demo.example.com".to_owned()].into();
        let (stream, records) = start(config);
        for origin in ["https://other.example.com", "null"] {
            let mut request = format!("ws://{}", stream.address)
                .into_client_request()
                .unwrap();
            request
                .headers_mut()
                .insert("Origin", origin.parse().unwrap());
            match tokio_tungstenite::connect_async(request).await {
                Err(WsError::Http(response)) => assert_eq!(response.status(), 403),
                other => panic!("expected Origin rejection: {other:?}"),
            }
        }
        let mut request = format!("ws://{}", stream.address)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("Origin", "https://demo.example.com".parse().unwrap());
        let (allowed, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let native = connect(stream.address, &[]).await;
        drop(allowed);
        drop(native);
        drop(records);
        stream.done.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rejects_too_many_books_and_bad_requests() {
        let (stream, _records) = start(settings(16, 2));
        let mut greedy = connect(stream.address, &["A", "B", "C"]).await;
        assert_eq!(close_code(&mut greedy).await, Some(CloseCode::Policy));
        let mut confused = connect(stream.address, &[]).await;
        confused
            .send(Message::text(r#"{"op":"replay"}"#))
            .await
            .unwrap();
        assert_eq!(close_code(&mut confused).await, Some(CloseCode::Policy));
    }
}
