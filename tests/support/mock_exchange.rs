use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

pub const STEP: Duration = Duration::from_secs(8);
pub const MARKET_A: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
pub const MARKET_B: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
pub const MARKET_C: &str = "0x0000000000000000000000000000000000000000000000000000000000000002";
pub const BASE_MS: i64 = 1_790_000_000_000;

pub fn scratch() -> tempfile::TempDir {
    let root = std::env::temp_dir().join("submission-fake-e2e");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::Builder::new()
        .prefix("collector-")
        .tempdir_in(root)
        .unwrap()
}

pub fn listing(id: u64, market: &str, assets: [&str; 2]) -> Value {
    json!({"id": id.to_string(), "conditionId": market,
        "clobTokenIds": serde_json::to_string(&assets).unwrap(), "version": "v1",
        "active": true, "closed": false, "archived": false, "enableOrderBook": true,
        "acceptingOrders": true, "createdAt": "2026-10-01T00:00:00Z"})
}

pub fn hint(id: u64, market: &str, assets: [&str; 2]) -> Value {
    json!({"event_type": "new_market", "id": id.to_string(), "market": market,
        "assets_ids": assets})
}

#[derive(Clone, Debug)]
pub enum Event {
    Client { socket: usize, value: Value },
    Ping(usize),
    Closed(usize),
}

enum Command {
    Send(Value, oneshot::Sender<()>),
    Drop,
}

type Sockets = Arc<Mutex<HashMap<usize, mpsc::UnboundedSender<Command>>>>;

/// Both endpoints bind only to loopback. No client request is forwarded.
pub struct MockExchange {
    pub ws_url: String,
    pub gamma_url: String,
    pub catalog: Arc<Mutex<Vec<Value>>>,
    pub requests: Arc<Mutex<Vec<String>>>,
    sockets: Sockets,
    events: mpsc::UnboundedReceiver<Event>,
    backlog: VecDeque<Event>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl MockExchange {
    pub async fn start(catalog: Vec<Value>) -> Self {
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", ws.local_addr().unwrap());
        let gamma_url = format!("http://{}", http.local_addr().unwrap());
        let catalog = Arc::new(Mutex::new(catalog));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let sockets: Sockets = Arc::default();
        let cancel = CancellationToken::new();
        let (events_tx, events) = mpsc::unbounded_channel();
        let ws_task = {
            let cancel = cancel.clone();
            let sockets = sockets.clone();
            tokio::spawn(async move {
                let mut children = JoinSet::new();
                let mut next_id = 0;
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        Some(result) = children.join_next() => result.unwrap(),
                        accepted = ws.accept() => {
                            let (tcp, _) = accepted.unwrap();
                            tcp.set_nodelay(true).unwrap();
                            let id = next_id;
                            next_id += 1;
                            let events = events_tx.clone();
                            let sockets = sockets.clone();
                            let cancel = cancel.clone();
                            children.spawn(async move {
                                let mut ws = accept_async(tcp).await.unwrap();
                                let (tx, mut rx) = mpsc::unbounded_channel();
                                sockets.lock().unwrap().insert(id, tx);
                                loop {
                                    tokio::select! {
                                        _ = cancel.cancelled() => break,
                                        command = rx.recv() => match command {
                                            Some(Command::Send(value, done)) => {
                                                let text = if value == "PONG" { "PONG".to_owned() } else { value.to_string() };
                                                ws.send(Message::Text(text.into())).await.unwrap();
                                                let _ = done.send(());
                                            }
                                            Some(Command::Drop) | None => break,
                                        },
                                        message = ws.next() => match message {
                                            Some(Ok(Message::Text(text))) if text.as_str() == "PING" => {
                                                let _ = events.send(Event::Ping(id));
                                            }
                                            Some(Ok(Message::Text(text))) => {
                                                let value = serde_json::from_str(&text).unwrap();
                                                let _ = events.send(Event::Client { socket: id, value });
                                            }
                                            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                                            _ => {}
                                        },
                                    }
                                }
                                sockets.lock().unwrap().remove(&id);
                                let _ = events.send(Event::Closed(id));
                            });
                        }
                    }
                }
                while let Some(result) = children.join_next().await {
                    result.unwrap();
                }
            })
        };
        let http_task = {
            let cancel = cancel.clone();
            let catalog = catalog.clone();
            let requests = requests.clone();
            tokio::spawn(async move {
                'accept: loop {
                    let accepted = tokio::select! {
                        _ = cancel.cancelled() => break,
                        accepted = http.accept() => accepted.unwrap(),
                    };
                    let mut tcp = accepted.0;
                    let mut request = Vec::new();
                    loop {
                        let mut bytes = [0; 2048];
                        let count = tokio::select! {
                            _ = cancel.cancelled() => return,
                            result = tcp.read(&mut bytes) => result.unwrap(),
                        };
                        // Request cancellation during collector shutdown can close an
                        // accepted socket before its headers are sent.
                        if count == 0 {
                            continue 'accept;
                        }
                        request.extend_from_slice(&bytes[..count]);
                        assert!(request.len() < 16_384, "bounded HTTP request");
                        if request.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8(request).unwrap();
                    let target = request.split_whitespace().nth(1).unwrap().to_owned();
                    let url = reqwest::Url::parse(&format!("http://localhost{target}")).unwrap();
                    assert_eq!(url.path(), "/markets/keyset");
                    requests.lock().unwrap().push(target);
                    let ids: Vec<String> = url
                        .query_pairs()
                        .filter(|(k, _)| k == "id")
                        .map(|(_, v)| v.into_owned())
                        .collect();
                    let rows: Vec<Value> = catalog
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|row| {
                            if ids.is_empty() {
                                row["closed"] == false
                            } else {
                                ids.iter().any(|id| row["id"] == *id)
                            }
                        })
                        .cloned()
                        .collect();
                    let body = json!({"markets": rows, "next_cursor": null}).to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    tcp.write_all(response.as_bytes()).await.unwrap();
                    tcp.shutdown().await.unwrap();
                }
            })
        };
        Self {
            ws_url,
            gamma_url,
            catalog,
            requests,
            sockets,
            events,
            backlog: VecDeque::new(),
            cancel,
            tasks: vec![ws_task, http_task],
        }
    }

    pub async fn send(&self, socket: usize, value: Value) {
        let (done, rx) = oneshot::channel();
        self.sockets
            .lock()
            .unwrap()
            .get(&socket)
            .unwrap()
            .send(Command::Send(value, done))
            .unwrap();
        within(rx).await.unwrap();
        pump().await;
    }

    pub fn drop_socket(&self, socket: usize) {
        self.sockets
            .lock()
            .unwrap()
            .get(&socket)
            .unwrap()
            .send(Command::Drop)
            .unwrap();
    }

    pub async fn wait(&mut self, matches: impl Fn(&Event) -> bool) -> Event {
        let deadline = Instant::now() + STEP;
        loop {
            while let Ok(event) = self.events.try_recv() {
                self.backlog.push_back(event);
            }
            if let Some(index) = self.backlog.iter().position(&matches) {
                return self.backlog.remove(index).unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "fake exchange event deadline; queued: {:?}",
                self.backlog
            );
            pump().await;
        }
    }

    pub async fn subscription(&mut self, operation: &str, assets: &[&str]) -> usize {
        let event = self
            .wait(|event| {
                let Event::Client { value, .. } = event else {
                    return false;
                };
                let actual: BTreeSet<_> = value["assets_ids"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap())
                    .collect();
                let wanted: BTreeSet<_> = assets.iter().copied().collect();
                actual == wanted
                    && if operation == "initial" {
                        value["type"] == "market" && value["initial_dump"] == true
                    } else {
                        value["operation"] == operation
                    }
            })
            .await;
        let Event::Client { socket, .. } = event else {
            unreachable!()
        };
        socket
    }

    pub fn drain(&mut self) -> Vec<Event> {
        while let Ok(event) = self.events.try_recv() {
            self.backlog.push_back(event);
        }
        self.backlog.drain(..).collect()
    }

    pub async fn finish(mut self) {
        self.cancel.cancel();
        within(async {
            for task in self.tasks.drain(..) {
                task.await.unwrap();
            }
        })
        .await;
    }
}

impl Drop for MockExchange {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Keep the runtime runnable while loopback I/O progresses: paused Tokio must
/// never auto-advance health deadlines just because bytes are still in flight.
pub async fn pump() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    std::thread::sleep(Duration::from_millis(1));
}

pub async fn within<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + STEP;
    loop {
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = pump() => assert!(Instant::now() < deadline, "operation exceeded wall-clock deadline"),
        }
    }
}

pub async fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + STEP;
    while !condition() {
        assert!(Instant::now() < deadline, "condition deadline");
        pump().await;
    }
}

pub type Levels = BTreeMap<i64, i64>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Book {
    pub bids: Levels,
    pub asks: Levels,
}

impl Book {
    pub fn new(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Self {
        Self {
            bids: bids.iter().copied().collect(),
            asks: asks.iter().copied().collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Snapshot(Book, i64),
    Update(Side, i64, i64, i64),
}

pub type Records = BTreeMap<String, Vec<Record>>;

/// Server-owned prices/quantities are integers. Wire strings are generated
/// from these known values; no collector parser, reducer or fixture is reused.
#[derive(Default)]
pub struct Oracle {
    pub books: BTreeMap<String, Book>,
    pub expected: Records,
    timestamps: BTreeMap<String, i64>,
}

impl Oracle {
    pub fn full(&mut self, market: &str, asset: &str, book: Book, millis: i64) -> Value {
        let output = self.expected.entry(asset.into()).or_default();
        if let Some(old) = self.books.get(asset) {
            for (side, before, after) in [
                (Side::Buy, &old.bids, &book.bids),
                (Side::Sell, &old.asks, &book.asks),
            ] {
                let prices: BTreeSet<_> = before.keys().chain(after.keys()).copied().collect();
                for price in prices {
                    let qty = after.get(&price).copied().unwrap_or(0);
                    if before.get(&price).copied().unwrap_or(0) != qty {
                        output.push(Record::Update(side, price, qty, millis * 1000));
                    }
                }
            }
        } else {
            output.push(Record::Snapshot(book.clone(), millis * 1000));
        }
        let levels = |levels: &Levels| -> Vec<Value> {
            // Reverse input order to ensure the wire's order is not trusted.
            levels
                .iter()
                .rev()
                .map(|(&p, &q)| json!({"price": decimal(p), "size": decimal(q)}))
                .collect()
        };
        let frame = json!({"event_type": "book", "market": market, "asset_id": asset,
            "timestamp": millis.to_string(), "bids": levels(&book.bids), "asks": levels(&book.asks)});
        self.books.insert(asset.into(), book);
        self.timestamps.insert(asset.into(), millis * 1000);
        frame
    }

    pub fn changes(
        &mut self,
        market: &str,
        changes: &[(&str, Side, i64, i64)],
        millis: i64,
    ) -> Value {
        for &(asset, side, price, qty) in changes {
            let book = self.books.get_mut(asset).unwrap();
            let levels = if side == Side::Buy {
                &mut book.bids
            } else {
                &mut book.asks
            };
            if qty == 0 {
                levels.remove(&price);
            } else {
                levels.insert(price, qty);
            }
            self.expected
                .entry(asset.into())
                .or_default()
                .push(Record::Update(side, price, qty, millis * 1000));
            self.timestamps.insert(asset.into(), millis * 1000);
        }
        change_frame(market, changes, millis)
    }

    pub fn periodic(&mut self, asset: &str) {
        self.expected
            .entry(asset.into())
            .or_default()
            .push(Record::Snapshot(
                self.books[asset].clone(),
                self.timestamps[asset],
            ));
    }

    pub fn removed(&mut self, assets: &[&str]) {
        for asset in assets {
            if self.books.remove(*asset).is_some() {
                self.expected
                    .entry((*asset).into())
                    .or_default()
                    .push(Record::Snapshot(Book::default(), self.timestamps[*asset]));
            }
            self.timestamps.remove(*asset);
        }
    }

    pub fn check(&self, actual: &Records) {
        assert_eq!(
            *actual, self.expected,
            "every per-book record must match the server oracle"
        );
        let mut replayed: BTreeMap<String, Book> = BTreeMap::new();
        for (asset, records) in actual {
            for record in records {
                match record {
                    Record::Snapshot(book, _) => {
                        replayed.insert(asset.clone(), book.clone());
                    }
                    Record::Update(side, price, qty, _) => {
                        let book = replayed.get_mut(asset).expect("snapshot before updates");
                        let levels = if *side == Side::Buy {
                            &mut book.bids
                        } else {
                            &mut book.asks
                        };
                        if *qty == 0 {
                            levels.remove(price);
                        } else {
                            levels.insert(*price, *qty);
                        }
                    }
                }
            }
        }
        for (asset, server_book) in &self.books {
            assert_eq!(
                replayed.get(asset),
                Some(server_book),
                "final server book for {asset}"
            );
        }
    }
}

pub fn change_frame(market: &str, changes: &[(&str, Side, i64, i64)], millis: i64) -> Value {
    json!({"event_type": "price_change", "market": market, "timestamp": millis.to_string(),
        "price_changes": changes.iter().map(|(asset, side, price, qty)| json!({
            "asset_id": asset, "side": if *side == Side::Buy { "BUY" } else { "SELL" },
            "price": decimal(*price), "size": decimal(*qty)
        })).collect::<Vec<_>>()})
}

fn decimal(value: i64) -> String {
    format!("{}.{:06}", value / 1_000_000, value % 1_000_000)
}

pub fn read_sbe(path: &Path, received_bounds: (i64, i64)) -> Records {
    assert_eq!(
        std::fs::metadata(path).unwrap().len(),
        64 << 20,
        "one complete padded chunk"
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut output: Records = BTreeMap::new();
    while let Some(message) = reader.read_msg().unwrap() {
        let (symbol, record, received) = match message {
            AnyMessage::Snapshot(decoded) => {
                let m = SnapshotDomain::try_from_decoder(decoded).unwrap();
                let bids: Vec<_> = m
                    .bids
                    .into_iter()
                    .map(|l| (l.price_level, l.quantity))
                    .collect();
                let asks: Vec<_> = m
                    .asks
                    .into_iter()
                    .map(|l| (l.price_level, l.quantity))
                    .collect();
                assert!(bids.windows(2).all(|w| w[0].0 > w[1].0));
                assert!(asks.windows(2).all(|w| w[0].0 < w[1].0));
                (
                    m.symbol,
                    Record::Snapshot(
                        Book::new(&bids, &asks),
                        m.exchange_timestamp.timestamp_micros(),
                    ),
                    m.received_at.timestamp_micros(),
                )
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let m = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
                (
                    m.symbol,
                    Record::Update(
                        m.side,
                        m.price_level,
                        m.quantity,
                        m.exchange_timestamp.timestamp_micros(),
                    ),
                    m.backtest_timestamp.timestamp_micros(),
                )
            }
            _ => panic!("unexpected SBE record"),
        };
        assert!(
            (received_bounds.0..=received_bounds.1).contains(&received),
            "receive timestamp from actual run"
        );
        output.entry(symbol).or_default().push(record);
    }
    output
}
