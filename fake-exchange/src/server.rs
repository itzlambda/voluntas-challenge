use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, ensure};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use tokio_util::sync::CancellationToken;

use crate::book::{
    Asset, SCALE, WireBook, condition, initial_assets, now_ms, shard_for, write_change_header,
};
use crate::config::Config;
use crate::history::{Change, History, Lifecycle};

#[derive(Default)]
struct Stats {
    frames: AtomicU64,
    entries: AtomicU64,
    bytes: AtomicU64,
    generated: AtomicU64,
    max_depth: AtomicUsize,
    overflow: AtomicU64,
    connections: AtomicUsize,
    subscriptions: AtomicUsize,
    lag_us: Vec<AtomicU64>,
}

#[derive(Clone)]
struct Frame {
    bytes: Arc<[u8]>,
    entries: usize,
}
impl Frame {
    fn json(value: &Value, entries: usize) -> anyhow::Result<Self> {
        Ok(Self {
            bytes: serde_json::to_vec(value)?.into(),
            entries,
        })
    }
}

#[derive(Clone)]
enum Outbound {
    Data(Frame),
    Resolved([u64; 2]),
}

#[derive(Clone)]
struct Client {
    id: u64,
    output: mpsc::Sender<Outbound>,
    close: watch::Sender<bool>,
    overflowed: Arc<AtomicBool>,
    stats: Arc<Stats>,
}
impl Client {
    fn send(&self, frame: Outbound) {
        if self.overflowed.load(Ordering::Relaxed) {
            return;
        }
        match self.output.try_send(frame) {
            Ok(()) => {
                self.stats.max_depth.fetch_max(
                    self.output.max_capacity() - self.output.capacity(),
                    Ordering::Relaxed,
                );
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !self.overflowed.swap(true, Ordering::Relaxed) {
                    self.close.send_replace(true);
                    self.stats.overflow.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

enum ShardCommand {
    Subscribe {
        assets: Vec<u64>,
        client: Client,
        dump: bool,
        reply: oneshot::Sender<Vec<Vec<u8>>>,
    },
    Unsubscribe {
        assets: Vec<u64>,
        connection: u64,
    },
    Insert {
        id: u64,
        time: i64,
        reply: oneshot::Sender<()>,
    },
    Resolve(u64),
    CatalogWeight(f64),
}
type ShardTx = mpsc::Sender<ShardCommand>;

enum CatalogCommand {
    Page {
        query: Vec<(String, String)>,
        reply: oneshot::Sender<(u16, Value)>,
    },
    Lifecycle(Client),
    Disconnect(u64),
}
type CatalogTx = mpsc::Sender<CatalogCommand>;

pub struct Server {
    pub ws_addr: SocketAddr,
    pub gamma_addr: SocketAddr,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<anyhow::Result<()>>>,
    history: Option<std::path::PathBuf>,
}

impl Server {
    pub async fn start(config: Config) -> anyhow::Result<Self> {
        config.validate()?;
        if let Some(dir) = &config.history {
            fs::create_dir_all(dir)?;
            ensure!(
                fs::read_dir(dir)?.next().is_none(),
                "history directory must be empty"
            );
            fs::write(
                dir.join("manifest.json"),
                serde_json::to_vec(
                    &json!({"version":1,"shards":config.cores,"seed":config.seed,"started_ms":now_ms()}),
                )?,
            )?;
        }
        let ws = TcpListener::bind((Ipv4Addr::LOCALHOST, config.ws_port)).await?;
        let http = TcpListener::bind((Ipv4Addr::LOCALHOST, config.gamma_port)).await?;
        let ws_addr = ws.local_addr()?;
        let gamma_addr = http.local_addr()?;
        let config = Arc::new(config);
        let stats = Arc::new(Stats {
            lag_us: (0..config.cores).map(|_| AtomicU64::new(0)).collect(),
            ..Stats::default()
        });
        let cancel = CancellationToken::new();
        let mut tasks = Vec::new();
        let mut shards = Vec::new();
        let mut ready = Vec::new();
        let created = now_ms();
        let total_weight = (1..=config.markets)
            .map(|id| weight(id, config.zipf))
            .sum::<f64>();
        for index in 0..config.cores {
            let (tx, rx) = mpsc::channel(4096);
            shards.push(tx);
            let (ready_tx, ready_rx) = oneshot::channel();
            ready.push(ready_rx);
            let config = config.clone();
            let stats = stats.clone();
            let cancel = cancel.clone();
            tasks.push(tokio::spawn(async move {
                let mut shard = Shard::new(index, config, stats, total_weight)?;
                for id in 1..=shard.config.markets {
                    if shard_for(id, shard.config.cores) == index {
                        shard.insert(id, created)?;
                    }
                }
                shard.reweight();
                let _ = ready_tx.send(());
                shard.run(rx, cancel).await
            }));
        }
        for response in ready {
            response.await.context("shard failed to initialize")?;
        }
        let shards = Arc::new(shards);
        let (catalog_tx, catalog_rx) = mpsc::channel(4096);
        tasks.push(tokio::spawn(catalog(
            config.clone(),
            shards.clone(),
            catalog_rx,
            cancel.clone(),
            created,
        )));
        tasks.push(tokio::spawn(ws_accept(
            ws,
            config.clone(),
            shards,
            catalog_tx.clone(),
            stats.clone(),
            cancel.clone(),
        )));
        tasks.push(tokio::spawn(http_accept(http, catalog_tx, cancel.clone())));
        tasks.push(tokio::spawn(report(stats, cancel.clone())));
        Ok(Self {
            ws_addr,
            gamma_addr,
            cancel,
            tasks,
            history: config.history.clone(),
        })
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.cancel.cancel();
        let mut failure = None;
        for task in self.tasks {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure.get_or_insert(error);
                }
                Err(error) => {
                    failure.get_or_insert(error.into());
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        if let Some(dir) = self.history {
            fs::write(
                dir.join("complete.json"),
                serde_json::to_vec(&json!({"finished_ms":now_ms()}))?,
            )?;
        }
        Ok(())
    }
}

fn weight(id: u64, exponent: f64) -> f64 {
    (id as f64).powf(-exponent)
}

struct Market {
    assets: [Asset; 2],
    last_ms: i64,
    subscribers: [HashMap<u64, Client>; 2],
    clients: Vec<Client>,
    wire: [WireBook; 2],
}

struct Shard {
    index: usize,
    config: Arc<Config>,
    stats: Arc<Stats>,
    rng: fastrand::Rng,
    markets: HashMap<u64, Market>,
    weighted: Vec<(f64, u64)>,
    total_weight: f64,
    initial_weight: f64,
    history: Option<History>,
    frame_bytes: Vec<u8>,
    hash_bytes: Vec<u8>,
}

impl Shard {
    fn new(
        index: usize,
        config: Arc<Config>,
        stats: Arc<Stats>,
        initial_weight: f64,
    ) -> anyhow::Result<Self> {
        let history = config
            .history
            .as_ref()
            .map(|d| History::create(d, index))
            .transpose()?;
        Ok(Self {
            index,
            rng: fastrand::Rng::with_seed(config.seed.wrapping_add(index as u64)),
            config,
            stats,
            markets: HashMap::new(),
            weighted: Vec::new(),
            total_weight: 0.0,
            initial_weight,
            history,
            frame_bytes: Vec::with_capacity(8192),
            hash_bytes: Vec::with_capacity(8192),
        })
    }
    fn insert(&mut self, id: u64, time: i64) -> anyhow::Result<()> {
        let levels = if id.is_multiple_of(20) {
            0
        } else {
            self.rng
                .usize(self.config.levels_min..=self.config.levels_max)
        };
        let assets = initial_assets(id, time, levels, &mut self.rng);
        if let Some(history) = &mut self.history {
            for asset in &assets {
                history.asset(asset)?;
            }
        }
        self.markets.insert(
            id,
            Market {
                wire: std::array::from_fn(|i| WireBook::new(&assets[i])),
                clients: Vec::new(),
                assets,
                last_ms: time,
                subscribers: std::array::from_fn(|_| HashMap::new()),
            },
        );
        Ok(())
    }
    fn reweight(&mut self) {
        self.weighted.clear();
        self.total_weight = 0.0;
        let mut ids: Vec<_> = self.markets.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            self.total_weight += weight(id, self.config.zipf);
            self.weighted.push((self.total_weight, id));
        }
    }
    fn choose(&mut self) -> Option<u64> {
        if self.weighted.is_empty() {
            return None;
        }
        let sample = self.rng.f64() * self.total_weight;
        self.weighted
            .get(self.weighted.partition_point(|(sum, _)| *sum <= sample))
            .map(|(_, id)| *id)
    }
    fn command(&mut self, command: ShardCommand) -> anyhow::Result<()> {
        match command {
            ShardCommand::Subscribe {
                assets,
                client,
                dump,
                reply,
            } => {
                let mut books = Vec::new();
                for asset in assets {
                    if let Some(market) = self.markets.get_mut(&(asset / 2)) {
                        let outcome = (asset % 2) as usize;
                        if market.subscribers[outcome]
                            .insert(client.id, client.clone())
                            .is_none()
                        {
                            if !market.clients.iter().any(|c| c.id == client.id) {
                                market.clients.push(client.clone());
                            }
                            if dump {
                                market.last_ms = market.last_ms.max(now_ms());
                                let mut bytes = Vec::with_capacity(8192);
                                market.wire[outcome].write_book(
                                    &market.assets[outcome],
                                    market.last_ms,
                                    &mut bytes,
                                    &mut self.hash_bytes,
                                );
                                books.push(bytes);
                            }
                        }
                    }
                }
                let _ = reply.send(books);
            }
            ShardCommand::Unsubscribe { assets, connection } => {
                for asset in assets {
                    if let Some(market) = self.markets.get_mut(&(asset / 2)) {
                        market.subscribers[(asset % 2) as usize].remove(&connection);
                        if !market
                            .subscribers
                            .iter()
                            .any(|s| s.contains_key(&connection))
                        {
                            market.clients.retain(|c| c.id != connection);
                        }
                    }
                }
            }
            ShardCommand::Insert { id, time, reply } => {
                self.insert(id, time)?;
                self.reweight();
                let _ = reply.send(());
            }
            ShardCommand::CatalogWeight(weight) => {
                self.initial_weight = weight;
            }
            ShardCommand::Resolve(id) => {
                if let Some(market) = self.markets.remove(&id) {
                    for client in &market.clients {
                        client.send(Outbound::Resolved([id * 2, id * 2 + 1]));
                    }
                    self.reweight();
                }
            }
        }
        Ok(())
    }
    fn generate(&mut self) -> anyhow::Result<usize> {
        let Some(id) = self.choose() else {
            return Ok(0);
        };
        let Some(market) = self.markets.get_mut(&id) else {
            return Ok(0);
        };
        market.last_ms = market.last_ms.max(now_ms());
        let time = market.last_ms;
        let target = self
            .rng
            .usize(self.config.entries_min..=self.config.entries_max);
        write_change_header(&mut self.frame_bytes, id, time);
        let mut changed = 0;
        for _ in 0..target {
            let outcome = self.rng.usize(0..2);
            let side = self.rng.u8(0..2);
            let tick = self.rng.i64(1..=49);
            let price = if side == 0 {
                tick * 10_000
            } else {
                (100 - tick) * 10_000
            };
            let mirrored = self.rng.f64() < self.config.mirror_probability;
            for mirror in 0..=usize::from(mirrored) {
                let which = if mirror == 0 { outcome } else { 1 - outcome };
                let side = if mirror == 0 { side } else { 1 - side };
                let price = if mirror == 0 { price } else { SCALE - price };
                let asset = &mut market.assets[which];
                let levels = if side == 0 {
                    &asset.levels.bids
                } else {
                    &asset.levels.asks
                };
                let previous = levels.get(&price).copied().unwrap_or(0);
                let mut size = if previous > 0 && self.rng.u8(0..5) == 0 {
                    0
                } else {
                    self.rng.i64(1..=1_000_000_000)
                };
                if size == previous {
                    size += 1;
                }
                asset.levels.set(side, price, size);
                if let Some(history) = &mut self.history {
                    history.change(Change {
                        time,
                        asset: asset.asset,
                        side,
                        price,
                        size,
                    })?;
                }
                market.wire[which].set(side, price, size);
                if !market.clients.is_empty() {
                    if changed > 0 {
                        self.frame_bytes.push(b',');
                    }
                    market.wire[which].write_entry(
                        &mut self.frame_bytes,
                        asset,
                        (side, price, size),
                        time,
                        &mut self.hash_bytes,
                    );
                }
                changed += 1;
            }
        }
        self.stats
            .generated
            .fetch_add(changed as u64, Ordering::Relaxed);
        if !market.clients.is_empty() {
            self.frame_bytes.extend_from_slice(b"]}");
            let frame = Frame {
                bytes: Arc::from(self.frame_bytes.as_slice()),
                entries: changed,
            };
            for client in &market.clients {
                client.send(Outbound::Data(frame.clone()));
            }
        }
        Ok(changed)
    }
    fn repeat(&mut self, noise: bool) -> anyhow::Result<()> {
        let Some(id) = self.choose() else {
            return Ok(());
        };
        let Some(market) = self.markets.get_mut(&id) else {
            return Ok(());
        };
        let outcome = self.rng.usize(0..2);
        if market.subscribers[outcome].is_empty() {
            return Ok(());
        }
        market.last_ms = market.last_ms.max(now_ms());
        let asset = &market.assets[outcome];
        let frame = if noise {
            let value = match self.rng.u8(0..3) {
                0 => {
                    json!({"event_type":"last_trade_price","market":condition(id),"asset_id":asset.asset.to_string(),"price":"0.500","size":"5","side":"BUY","timestamp":market.last_ms.to_string()})
                }
                1 => {
                    json!({"event_type":"best_bid_ask","market":condition(id),"asset_id":asset.asset.to_string(),"best_bid":"0.49","best_ask":"0.51","timestamp":market.last_ms.to_string()})
                }
                _ => {
                    json!({"event_type":"tick_size_change","market":condition(id),"asset_id":asset.asset.to_string(),"old_tick_size":"0.01","new_tick_size":"0.01","timestamp":market.last_ms.to_string()})
                }
            };
            Frame::json(&value, 0)?
        } else {
            self.frame_bytes.clear();
            market.wire[outcome].write_book(
                asset,
                market.last_ms,
                &mut self.frame_bytes,
                &mut self.hash_bytes,
            );
            Frame {
                bytes: Arc::from(self.frame_bytes.as_slice()),
                entries: 0,
            }
        };
        for client in market.subscribers[outcome].values() {
            client.send(Outbound::Data(frame.clone()));
        }
        Ok(())
    }
    async fn run(
        mut self,
        mut input: mpsc::Receiver<ShardCommand>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut previous = Instant::now();
        let (mut due, mut repeats, mut noise) = (0.0, 0.0, 0.0);
        let mut wake = previous;
        loop {
            tokio::select! {
                biased;
                _=cancel.cancelled()=>break,
                command=input.recv()=>match command {Some(command)=>self.command(command)?,None=>break},
                _=async {if wake>Instant::now() {tokio::time::sleep_until(wake).await}}=>{}
            }
            let start = Instant::now();
            let elapsed = (start - previous).as_secs_f64();
            previous = start;
            let share = if self.initial_weight > 0.0 {
                self.total_weight / self.initial_weight
            } else {
                0.0
            };
            let rate = self.config.rate * share;
            let repeat_rate = self.config.repeat_book_rate * share;
            let noise_rate = self.config.noise_rate * share;
            due += elapsed * rate;
            repeats += elapsed * repeat_rate;
            noise += elapsed * noise_rate;
            let deadline = start + Duration::from_micros(500);
            while due >= 1.0 {
                let count = self.generate()?;
                if count == 0 {
                    due = 0.0;
                    break;
                }
                due -= count as f64;
                if Instant::now() >= deadline {
                    break;
                }
            }
            while repeats >= 1.0 && Instant::now() < deadline {
                self.repeat(false)?;
                repeats -= 1.0;
            }
            while noise >= 1.0 && Instant::now() < deadline {
                self.repeat(true)?;
                noise -= 1.0;
            }
            let now = Instant::now();
            let spent = (now - previous).as_secs_f64();
            due += spent * rate;
            repeats += spent * repeat_rate;
            noise += spent * noise_rate;
            previous = now;
            let lag = if rate > 0.0 { due.max(0.0) / rate } else { 0.0 };
            self.stats.lag_us[self.index].store((lag * 1e6) as u64, Ordering::Relaxed);
            if due >= 1.0 || repeats >= 1.0 || noise >= 1.0 {
                wake = now;
            } else {
                let delay = [(due, rate), (repeats, repeat_rate), (noise, noise_rate)]
                    .into_iter()
                    .filter(|(_, rate)| *rate > 0.0)
                    .map(|(credit, rate)| ((1.0 - credit) / rate).max(0.0))
                    .fold(1.0, f64::min);
                wake = now + Duration::from_secs_f64(delay);
            }
            tokio::task::yield_now().await;
        }
        if let Some(history) = &mut self.history {
            history.flush()?;
        }
        Ok(())
    }
}

fn listing(id: u64, created: i64, closed: bool) -> Value {
    json!({"id":id.to_string(),"conditionId":condition(id),"clobTokenIds":format!("[\"{}\",\"{}\"]",id*2,id*2+1),
        "active":true,"closed":closed,"archived":false,"enableOrderBook":true,"acceptingOrders":!closed,"version":"v1",
        "createdAt":chrono::DateTime::from_timestamp_millis(created).map(|t|t.to_rfc3339()),
        "min_order_size":"5","tick_size":"0.01","neg_risk":false,"last_trade_price":"0.500",
        "orderMinSize":5,"orderPriceMinTickSize":0.01,"negRisk":false,"lastTradePrice":0.5})
}

fn page(rows: &BTreeMap<u64, (i64, bool)>, query: &[(String, String)]) -> (u16, Value) {
    let get = |name: &str| {
        query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, v)| v.as_str())
    };
    let ids: HashSet<u64> = query
        .iter()
        .filter(|(k, _)| k == "id")
        .filter_map(|(_, v)| v.parse().ok())
        .collect();
    if get("closed") == Some("any") && ids.is_empty() {
        return (422, json!({"error":"closed=any requires id"}));
    }
    let limit = get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 100);
    let cursor = get("after_cursor")
        .or_else(|| get("next_cursor"))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    let matching: Vec<_> = rows
        .range(..cursor)
        .rev()
        .filter(|(id, (_, closed))| {
            (ids.is_empty() || ids.contains(id))
                && match get("closed") {
                    Some("false") => !*closed,
                    Some("true") => *closed,
                    _ => true,
                }
        })
        .take(limit + 1)
        .collect();
    let more = matching.len() > limit;
    let selected: Vec<_> = matching.into_iter().take(limit).collect();
    let next = if more {
        selected.last().map(|(id, _)| id.to_string())
    } else {
        None
    };
    (
        200,
        json!({"markets":selected.iter().map(|(id,(time,closed))|listing(**id,*time,*closed)).collect::<Vec<_>>(),"next_cursor":next}),
    )
}

async fn catalog(
    config: Arc<Config>,
    shards: Arc<Vec<ShardTx>>,
    mut input: mpsc::Receiver<CatalogCommand>,
    cancel: CancellationToken,
    created: i64,
) -> anyhow::Result<()> {
    let mut rows: BTreeMap<_, _> = (1..=config.markets)
        .map(|id| (id, (created, false)))
        .collect();
    let mut lifecycle: HashMap<u64, Client> = HashMap::new();
    let mut log = config
        .history
        .as_ref()
        .map(|d| File::create(d.join("lifecycle.jsonl")).map(BufWriter::new))
        .transpose()?;
    let log_event =
        |log: &mut Option<BufWriter<File>>, id, time, event: &str| -> anyhow::Result<()> {
            if let Some(log) = log {
                serde_json::to_writer(
                    &mut *log,
                    &Lifecycle {
                        market: id,
                        time,
                        event: event.into(),
                    },
                )?;
                log.write_all(b"\n")?;
            }
            Ok(())
        };
    for id in 1..=config.markets {
        log_event(&mut log, id, created, "created")?;
    }
    let mut catalog_weight = (1..=config.markets)
        .map(|id| weight(id, config.zipf))
        .sum::<f64>();
    let mut next = config.markets + 1;
    let mut resolve_id = 1;
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous = Instant::now();
    let (mut creates, mut resolves) = (0.0, 0.0);
    loop {
        tokio::select! {
            biased;
            _=cancel.cancelled()=>break,
            command=input.recv()=>match command {
                Some(CatalogCommand::Page {query,reply})=>{let _=reply.send(page(&rows,&query));}
                Some(CatalogCommand::Lifecycle(client))=>{lifecycle.insert(client.id,client);}
                Some(CatalogCommand::Disconnect(id))=>{lifecycle.remove(&id);}
                None=>break,
            },
            _=interval.tick()=>{
                let time=Instant::now();let elapsed=(time-previous).as_secs_f64();previous=time;
                creates+=elapsed*config.new_markets_per_second;resolves+=elapsed*config.resolutions_per_second;
                for _ in 0..(creates as usize).min(100) {
                    let time=now_ms();let id=next;next+=1;
                    let (reply,rx)=oneshot::channel();
                    shards[shard_for(id,config.cores)].send(ShardCommand::Insert {id,time,reply}).await?;rx.await?;
                    rows.insert(id,(time,false));log_event(&mut log,id,time,"created")?;
                    catalog_weight+=weight(id,config.zipf);
                    for shard in shards.iter() {shard.send(ShardCommand::CatalogWeight(catalog_weight)).await?;}
                    let frame=Frame::json(&json!({"event_type":"new_market","id":id.to_string(),"market":condition(id),"assets_ids":[(id*2).to_string(),(id*2+1).to_string()],"active":true,"timestamp":time.to_string()}),0)?;
                    for client in lifecycle.values() {client.send(Outbound::Data(frame.clone()));}
                    creates-=1.0;
                }
                for _ in 0..(resolves as usize).min(100) {
                    if resolve_id>=next {resolves=0.0;break;}
                    if let Some((_,closed))=rows.get_mut(&resolve_id) {*closed=true;}
                    shards[shard_for(resolve_id,config.cores)].send(ShardCommand::Resolve(resolve_id)).await?;
                    log_event(&mut log,resolve_id,now_ms(),"resolved")?;
                    catalog_weight=(catalog_weight-weight(resolve_id,config.zipf)).max(0.0);
                    for shard in shards.iter() {shard.send(ShardCommand::CatalogWeight(catalog_weight)).await?;}
                    resolve_id+=1;resolves-=1.0;
                }
            }
        }
    }
    if let Some(log) = &mut log {
        log.flush()?;
    }
    Ok(())
}

async fn ws_accept(
    listener: TcpListener,
    config: Arc<Config>,
    shards: Arc<Vec<ShardTx>>,
    catalog: CatalogTx,
    stats: Arc<Stats>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    let mut id = 0;
    loop {
        tokio::select! {
            _=cancel.cancelled()=>break,
            result=listener.accept()=>{
                let (stream,_)=result?;stream.set_nodelay(true)?;id+=1;
                let config=config.clone();let shards=shards.clone();let catalog=catalog.clone();let stats=stats.clone();let cancel=cancel.clone();
                tasks.spawn(async move {
                    if let Err(error)=connection(stream,id,config,shards,catalog,stats,cancel).await {eprintln!("{}",json!({"event":"connection_error","error":error.to_string()}));}
                });
            }
            Some(_)=tasks.join_next()=>{}
        }
    }
    while let Some(result) = tasks.join_next().await {
        result?;
    }
    Ok(())
}

#[derive(Deserialize)]
struct Subscription {
    #[serde(default)]
    assets_ids: Vec<String>,
    operation: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    initial_dump: bool,
    #[serde(default)]
    custom_feature_enabled: bool,
}

async fn transmit(
    socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
    frame: Frame,
    stats: &Stats,
) -> anyhow::Result<()> {
    let len = frame.bytes.len();
    let text = tokio_tungstenite::tungstenite::Utf8Bytes::try_from(Bytes::from_owner(frame.bytes))?;
    socket.send(Message::Text(text)).await?;
    stats.frames.fetch_add(1, Ordering::Relaxed);
    stats
        .entries
        .fetch_add(frame.entries as u64, Ordering::Relaxed);
    stats.bytes.fetch_add(len as u64, Ordering::Relaxed);
    Ok(())
}

async fn transmit_batch(
    socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
    first: Frame,
    output: &mut mpsc::Receiver<Outbound>,
    pending: &mut Option<Outbound>,
    stats: &Stats,
) -> anyhow::Result<()> {
    let mut frame = first;
    let (mut frames, mut entries, mut bytes) = (0, 0, 0);
    loop {
        bytes += frame.bytes.len() as u64;
        entries += frame.entries as u64;
        frames += 1;
        let text =
            tokio_tungstenite::tungstenite::Utf8Bytes::try_from(Bytes::from_owner(frame.bytes))?;
        socket.feed(Message::Text(text)).await?;
        if frames >= 128 || bytes >= 256 * 1024 {
            break;
        }
        match output.try_recv() {
            Ok(Outbound::Data(next)) => frame = next,
            Ok(other) => {
                *pending = Some(other);
                break;
            }
            Err(_) => break,
        }
    }
    socket.flush().await?;
    stats.frames.fetch_add(frames, Ordering::Relaxed);
    stats.entries.fetch_add(entries, Ordering::Relaxed);
    stats.bytes.fetch_add(bytes, Ordering::Relaxed);
    Ok(())
}

async fn connection(
    stream: TcpStream,
    id: u64,
    config: Arc<Config>,
    shards: Arc<Vec<ShardTx>>,
    catalog: CatalogTx,
    stats: Arc<Stats>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut socket = tokio::select! {_=cancel.cancelled()=>return Ok(()), result=tokio::time::timeout(Duration::from_secs(10),tokio_tungstenite::accept_async_with_config(stream,Some(tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default().read_buffer_size(4096))))=>result??};
    let (tx, mut output) = mpsc::channel(config.outbound_capacity);
    let (close, mut closed) = watch::channel(false);
    let client = Client {
        id,
        output: tx,
        close,
        overflowed: Arc::new(AtomicBool::new(false)),
        stats: stats.clone(),
    };
    stats.connections.fetch_add(1, Ordering::Relaxed);
    let mut subscribed = HashSet::new();
    let mut pending_output = None;
    let mut resolved = HashSet::new();
    let heartbeat = tokio::time::sleep(Duration::from_secs(125));
    tokio::pin!(heartbeat);
    let result: anyhow::Result<()>=async {
        loop {
            tokio::select! {
                biased;
                _=cancel.cancelled()=>break,
                _=closed.changed()=>{
                    let _=tokio::time::timeout(Duration::from_secs(1),socket.close(Some(CloseFrame {code:CloseCode::Again,reason:"slow consumer".into()}))).await;
                    break;
                }
                _=&mut heartbeat=>break,
                message=socket.next()=>{
                    let Some(message)=message else {break;};
                    let Ok(message)=message else {break;};
                    match message {
                        Message::Text(text) if text=="PING"=>{
                            heartbeat.as_mut().reset(Instant::now()+Duration::from_secs(125));
                            transmit(&mut socket,Frame {bytes:Arc::from(&b"PONG"[..]),entries:0},&stats).await?;
                        }
                        Message::Text(text)=>{
                            let Ok(request)=serde_json::from_str::<Subscription>(&text) else {continue;};
                            let initial=request.kind.as_deref()==Some("market");
                            let subscribe=initial || request.operation.as_deref()==Some("subscribe");
                            let unsubscribe=request.operation.as_deref()==Some("unsubscribe");
                            if initial && request.custom_feature_enabled && request.assets_ids.is_empty() {catalog.send(CatalogCommand::Lifecycle(client.clone())).await?;}
                            if !subscribe && !unsubscribe {continue;}
                            let mut groups=vec![Vec::new();config.cores];
                            for asset in request.assets_ids.iter().filter_map(|id|id.parse::<u64>().ok()) {
                                if subscribe {
                                    if !subscribed.insert(asset) {continue;}
                                    resolved.remove(&asset);stats.subscriptions.fetch_add(1,Ordering::Relaxed);
                                } else {
                                    if !subscribed.remove(&asset) {continue;}
                                    resolved.remove(&asset);stats.subscriptions.fetch_sub(1,Ordering::Relaxed);
                                }
                                groups[shard_for(asset/2,config.cores)].push(asset);
                            }
                            let mut replies=Vec::new();
                            for (index,assets) in groups.into_iter().enumerate().filter(|(_,a)|!a.is_empty()) {
                                if subscribe {
                                    let (reply,rx)=oneshot::channel();
                                    shards[index].send(ShardCommand::Subscribe {assets,client:client.clone(),dump:!initial || request.initial_dump,reply}).await?;
                                    replies.push(rx);
                                } else {shards[index].send(ShardCommand::Unsubscribe {assets,connection:id}).await?;}
                            }
                            let mut books=Vec::new();for reply in replies {books.extend(reply.await?);}
                            if !books.is_empty() {
                                let array=initial || books.len()>1;
                                let mut bytes=Vec::new();if array {bytes.push(b'[');}
                                for (i,book) in books.iter().enumerate() {if i>0 {bytes.push(b',');}bytes.extend_from_slice(book);}
                                if array {bytes.push(b']');}
                                transmit(&mut socket,Frame {bytes:bytes.into(),entries:0},&stats).await?;
                            }
                        }
                        Message::Close(_)=>break,
                        Message::Ping(_)=>{socket.flush().await?;}
                        _=>{}
                    }
                }
                Some(frame)=async {match pending_output.take() {Some(frame)=>Some(frame),None=>output.recv().await}}=>match frame {
                    Outbound::Data(frame)=>{
                        tokio::select! {
                            _=cancel.cancelled()=>break,
                            _=closed.changed()=>{
                                let _=tokio::time::timeout(Duration::from_secs(1),socket.close(Some(CloseFrame {code:CloseCode::Again,reason:"slow consumer".into()}))).await;
                                break;
                            }
                            result=transmit_batch(&mut socket,frame,&mut output,&mut pending_output,&stats)=>result?,
                        }
                    }
                    Outbound::Resolved(assets)=>{
                        for asset in assets {if subscribed.contains(&asset) {resolved.insert(asset);}}
                        if !subscribed.is_empty() && subscribed.iter().all(|a|resolved.contains(a)) {
                            let _=tokio::time::timeout(Duration::from_secs(1),socket.close(Some(CloseFrame {code:CloseCode::Normal,reason:"all subscribed assets resolved".into()}))).await;
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }.await;
    stats.connections.fetch_sub(1, Ordering::Relaxed);
    stats
        .subscriptions
        .fetch_sub(subscribed.len(), Ordering::Relaxed);
    let mut groups = vec![Vec::new(); config.cores];
    for asset in subscribed {
        groups[shard_for(asset / 2, config.cores)].push(asset);
    }
    for (index, assets) in groups
        .into_iter()
        .enumerate()
        .filter(|(_, a)| !a.is_empty())
    {
        let _ = shards[index]
            .send(ShardCommand::Unsubscribe {
                assets,
                connection: id,
            })
            .await;
    }
    let _ = catalog.send(CatalogCommand::Disconnect(id)).await;
    result
}

async fn http_accept(
    listener: TcpListener,
    catalog: CatalogTx,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            _=cancel.cancelled()=>break,
            result=listener.accept()=>{
                let (stream,_)=result?;let catalog=catalog.clone();let cancel=cancel.clone();
                tasks.spawn(async move {
                    tokio::select! {
                        _=cancel.cancelled()=>{},
                        _=tokio::time::timeout(Duration::from_secs(10),http_request(stream,catalog))=>{},
                    }
                });
            }
            Some(_)=tasks.join_next()=>{}
        }
    }
    while let Some(result) = tasks.join_next().await {
        result?;
    }
    Ok(())
}
async fn http_request(mut stream: TcpStream, catalog: CatalogTx) -> anyhow::Result<()> {
    let mut request = Vec::new();
    let mut chunk = [0; 4096];
    while !request.windows(4).any(|b| b == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&chunk[..read]);
        ensure!(request.len() <= 65536, "HTTP header too large");
    }
    let text = std::str::from_utf8(&request)?;
    let mut first = text
        .lines()
        .next()
        .context("missing request line")?
        .split_whitespace();
    let method = first.next();
    let target = first.next().context("missing request target")?;
    let url = reqwest::Url::parse(&format!("http://localhost{target}"))?;
    let (status, body) = if method != Some("GET") || url.path() != "/markets/keyset" {
        (404, json!({"error":"not found"}))
    } else {
        let (reply, rx) = oneshot::channel();
        catalog
            .send(CatalogCommand::Page {
                query: url
                    .query_pairs()
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect(),
                reply,
            })
            .await?;
        rx.await?
    };
    let body = serde_json::to_vec(&body)?;
    let header = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        if status == 200 { "OK" } else { "Error" },
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn report(stats: Arc<Stats>, cancel: CancellationToken) -> anyhow::Result<()> {
    let mut previous = Instant::now();
    let mut interval =
        tokio::time::interval_at(previous + Duration::from_secs(10), Duration::from_secs(10));
    loop {
        tokio::select! {
            _=cancel.cancelled()=>break,
            _=interval.tick()=>{
                let now=Instant::now();let seconds=(now-previous).as_secs_f64();previous=now;
                eprintln!("{}",json!({"event":"stats","frames_per_second":stats.frames.swap(0,Ordering::Relaxed) as f64/seconds,
                    "entries_per_second":stats.entries.swap(0,Ordering::Relaxed) as f64/seconds,"bytes_per_second":stats.bytes.swap(0,Ordering::Relaxed) as f64/seconds,
                    "generated_changes_per_second":stats.generated.swap(0,Ordering::Relaxed) as f64/seconds,
                    "generator_lag_ms":stats.lag_us.iter().map(|n|n.load(Ordering::Relaxed) as f64/1000.0).collect::<Vec<_>>(),
                    "outbound_queue_depth_max":stats.max_depth.swap(0,Ordering::Relaxed),"closes_1013":stats.overflow.swap(0,Ordering::Relaxed),
                    "connections":stats.connections.load(Ordering::Relaxed),"subscriptions":stats.subscriptions.load(Ordering::Relaxed)}));
            }
        }
    }
    Ok(())
}
