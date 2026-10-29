//! Component-owned cumulative measurements. HTTP only reads cached exposition.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use prometheus::proto::{
    Bucket, Counter, Gauge, Histogram as ProtoHistogram, LabelPair, Metric, MetricFamily,
    MetricType,
};
use prometheus::{Encoder, TextEncoder};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::model::ConnectionId;

pub const PUBLICATION_INTERVAL: Duration = Duration::from_secs(1);
pub const STALE_AFTER: Duration = Duration::from_secs(15);
const EXCHANGE_BOUNDS: &[f64] = &[
    0., 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1., 2., 5., 10., 30., 60.,
];
const INTERNAL_BOUNDS: &[f64] = &[
    0., 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
    0.05, 0.1, 0.25, 0.5, 1., 2.5, 5., 10., 30., 60.,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distribution {
    Exchange,
    Internal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Histogram {
    distribution: Distribution,
    pub buckets: [u64; 23],
    pub count: u64,
    pub sum: f64,
}
impl Histogram {
    pub fn new(distribution: Distribution) -> Self {
        Self {
            distribution,
            buckets: [0; 23],
            count: 0,
            sum: 0.,
        }
    }
    pub fn bounds(&self) -> &'static [f64] {
        match self.distribution {
            Distribution::Exchange => EXCHANGE_BOUNDS,
            Distribution::Internal => INTERNAL_BOUNDS,
        }
    }
    pub fn record(&mut self, seconds: f64) {
        let bin = self.bounds().partition_point(|bound| *bound < seconds);
        self.buckets[bin] += 1;
        self.count += 1;
        self.sum += seconds;
    }
    pub fn add(&mut self, other: &Self) {
        for (a, b) in self.buckets.iter_mut().zip(other.buckets) {
            *a += b;
        }
        self.count += other.count;
        self.sum += other.sum;
    }
}
impl Default for Histogram {
    fn default() -> Self {
        Self::new(Distribution::Internal)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ConnectionKind {
    Orderbook,
    MarketDiscovery,
}
impl ConnectionKind {
    fn index(self) -> usize {
        match self {
            Self::Orderbook => 0,
            Self::MarketDiscovery => 1,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Orderbook => "orderbook",
            Self::MarketDiscovery => "market_discovery",
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum EndReason {
    RemoteClose,
    IoError,
    DecodeError,
    ProtocolError,
    Silence,
    Emptied,
    Shutdown,
}
impl EndReason {
    const ALL: [Self; 7] = [
        Self::RemoteClose,
        Self::IoError,
        Self::DecodeError,
        Self::ProtocolError,
        Self::Silence,
        Self::Emptied,
        Self::Shutdown,
    ];
    fn index(self) -> usize {
        self as usize
    }
    fn name(self) -> &'static str {
        match self {
            Self::RemoteClose => "remote_close",
            Self::IoError => "io_error",
            Self::DecodeError => "decode_error",
            Self::ProtocolError => "protocol_error",
            Self::Silence => "silence",
            Self::Emptied => "emptied",
            Self::Shutdown => "shutdown",
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum Component {
    Worker(usize),
    Writer,
    Discovery,
    Connection(ConnectionId),
    DiscoveryConnection,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub exchange: Histogram,
    pub internal: Histogram,
    pub negative: u64,
    pub events: u64,
    pub socket_markets: u64,
    pub decode_errors: u64,
    pub mismatches: u64,
    pub older_full_books: u64,
    pub older_full_book_lag: Histogram,
    pub desired_markets: u64,
    pub synced_markets: u64,
    pub standby_ready_markets: u64,
    pub publisher_switches: [u64; 5],
    pub desired_books: u64,
    pub synced_books: u64,
    pub connected: bool,
    pub expected: bool,
    pub session_ends: [u64; EndReason::ALL.len()],
    pub busy_seconds: f64,
    pub blocked_seconds: f64,
    pub last_scan: Option<f64>,
    pub initialized: bool,
}
impl Default for Snapshot {
    fn default() -> Self {
        Self {
            exchange: Histogram::new(Distribution::Exchange),
            internal: Histogram::default(),
            negative: 0,
            events: 0,
            socket_markets: 0,
            decode_errors: 0,
            mismatches: 0,
            older_full_books: 0,
            older_full_book_lag: Histogram::new(Distribution::Exchange),
            desired_markets: 0,
            synced_markets: 0,
            standby_ready_markets: 0,
            publisher_switches: [0; 5],
            desired_books: 0,
            synced_books: 0,
            connected: false,
            expected: false,
            session_ends: [0; EndReason::ALL.len()],
            busy_seconds: 0.,
            blocked_seconds: 0.,
            last_scan: None,
            initialized: true,
        }
    }
}
struct Published {
    component: Component,
    state: Snapshot,
    at: Option<(Instant, f64)>,
    retired: bool,
}
type Slot = Arc<Mutex<Published>>;
#[derive(Clone)]
pub struct Metrics {
    slots: Arc<Mutex<Vec<Slot>>>,
    failure: CancellationToken,
    started_at: f64,
}
impl Default for Metrics {
    fn default() -> Self {
        Self {
            slots: Arc::default(),
            failure: CancellationToken::new(),
            started_at: timestamp(),
        }
    }
}
pub struct Publisher {
    slot: Slot,
    pub state: Snapshot,
    next: Instant,
    failure: CancellationToken,
}
impl Publisher {
    pub fn publish(&mut self) {
        if let Ok(mut slot) = self.slot.try_lock() {
            slot.state.clone_from(&self.state);
            slot.at = Some((Instant::now(), timestamp()));
            self.next = Instant::now() + PUBLICATION_INTERVAL;
        }
    }
    pub fn failure_token(&self) -> CancellationToken {
        self.failure.clone()
    }
    pub fn is_due(&self) -> bool {
        Instant::now() >= self.next
    }
    pub fn publish_if_due(&mut self) {
        if Instant::now() >= self.next {
            self.publish();
        }
    }
    pub fn end_session(&mut self, reason: EndReason) {
        self.state.session_ends[reason.index()] += 1;
    }
}
impl Drop for Publisher {
    fn drop(&mut self) {
        let mut slot = self.slot.lock().unwrap_or_else(|e| e.into_inner());
        slot.state.clone_from(&self.state);
        slot.at = Some((Instant::now(), timestamp()));
        slot.retired = true;
    }
}
#[derive(Clone, Default)]
pub struct View {
    pub text: Arc<str>,
    pub sockets_text: Arc<str>,
    pub fresh: bool,
    pub initialized: bool,
    pub created: Option<Instant>,
}
impl Metrics {
    pub fn new(failure: CancellationToken) -> Self {
        Self {
            failure,
            ..Self::default()
        }
    }
    pub fn failure_token(&self) -> CancellationToken {
        self.failure.clone()
    }
    /// Register once per component task, never once per reconnect. Slots are not reused.
    pub fn register(&self, component: Component) -> Publisher {
        let slot = Arc::new(Mutex::new(Published {
            component,
            state: Snapshot::default(),
            at: None,
            retired: false,
        }));
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(slot.clone());
        Publisher {
            slot,
            state: Snapshot::default(),
            next: Instant::now(),
            failure: self.failure.clone(),
        }
    }
    pub fn render(&self) -> anyhow::Result<View> {
        let slots = self.slots.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let mut total = Snapshot::default();
        let mut connections = [[0u64; 2]; 2];
        let mut ends = [[0u64; EndReason::ALL.len()]; 2];
        let mut workers = Vec::new();
        let mut socket_events = Vec::new();
        let mut socket_connected = Vec::new();
        let mut socket_markets = Vec::new();
        let mut writer_busy = 0.;
        let mut oldest = None::<f64>;
        let mut fresh = !slots.is_empty();
        let mut initialized = !slots.is_empty();
        for slot in slots {
            let slot = slot.lock().unwrap_or_else(|e| e.into_inner());
            let s = &slot.state;
            total.exchange.add(&s.exchange);
            total.internal.add(&s.internal);
            total.negative += s.negative;
            total.events += s.events;
            total.decode_errors += s.decode_errors;
            total.mismatches += s.mismatches;
            total.older_full_books += s.older_full_books;
            total.older_full_book_lag.add(&s.older_full_book_lag);
            total.blocked_seconds += s.blocked_seconds;
            if let Component::Worker(id) = slot.component {
                workers.push((id, s.busy_seconds));
                for (total, switches) in total
                    .publisher_switches
                    .iter_mut()
                    .zip(s.publisher_switches)
                {
                    *total += switches;
                }
            }
            if let Component::Writer = slot.component {
                writer_busy += s.busy_seconds;
            }
            if let Component::Connection(id) = slot.component {
                let labels = vec![
                    ("socket_id", id.0.to_string()),
                    ("pool", id.pool().to_owned()),
                ];
                socket_events.push((labels.clone(), s.events as f64));
                if !slot.retired {
                    socket_connected.push((labels.clone(), f64::from(s.connected)));
                    socket_markets.push((labels, s.socket_markets as f64));
                }
            }
            let connection_kind = match slot.component {
                Component::Connection(_) => Some(ConnectionKind::Orderbook),
                Component::DiscoveryConnection => Some(ConnectionKind::MarketDiscovery),
                _ => None,
            };
            if let Some(kind) = connection_kind {
                let i = kind.index();
                for (a, b) in ends[i].iter_mut().zip(s.session_ends) {
                    *a += b;
                }
                if !slot.retired {
                    connections[i][0] += u64::from(s.expected);
                    connections[i][1] += u64::from(s.connected);
                }
            }
            if !slot.retired {
                total.desired_markets += s.desired_markets;
                total.synced_markets += s.synced_markets;
                total.standby_ready_markets += s.standby_ready_markets;
                total.desired_books += s.desired_books;
                total.synced_books += s.synced_books;
                initialized &= s.initialized;
                match slot.at {
                    Some((instant, at)) => {
                        fresh &= instant.elapsed() <= STALE_AFTER;
                        oldest = Some(oldest.map_or(at, |v| v.min(at)));
                    }
                    None => {
                        fresh = false;
                        initialized = false;
                    }
                }
            }
            if let Some(at) = s.last_scan {
                total.last_scan = Some(total.last_scan.map_or(at, |v| v.max(at)));
            }
        }
        let mut families = vec![
            histogram(
                "exchange_to_receive_seconds",
                "Exchange event age at application receipt; excludes negative ages and full books; includes clock offset.",
                &total.exchange,
            ),
            histogram(
                "receive_to_encode_seconds",
                "Monotonic receipt to encoding completion per emitted level record; excludes all snapshots and later flush.",
                &total.internal,
            ),
            scalar(
                "exchange_age_negative_total",
                "Price-change messages with negative exchange age.",
                MetricType::COUNTER,
                vec![(vec![], total.negative as f64)],
            ),
            scalar(
                "events_total",
                "Decoded data events; excludes heartbeat frames.",
                MetricType::COUNTER,
                vec![(vec![], total.events as f64)],
            ),
            scalar(
                "decode_errors_total",
                "Recognized messages or frames that failed decoding.",
                MetricType::COUNTER,
                vec![(vec![], total.decode_errors as f64)],
            ),
            scalar(
                "book_mismatches_total",
                "Accepted full books differing from an already-synchronized local book.",
                MetricType::COUNTER,
                vec![(vec![], total.mismatches as f64)],
            ),
            scalar(
                "older_full_books_skipped_total",
                "Full books skipped because their exchange timestamp predates applied depth in the current synchronized period.",
                MetricType::COUNTER,
                vec![(vec![], total.older_full_books as f64)],
            ),
            histogram(
                "older_full_book_lag_seconds",
                "Maximum applied depth timestamp minus skipped full-book timestamp; excludes accepted books.",
                &total.older_full_book_lag,
            ),
            scalar(
                "writer_blocked_seconds_total",
                "Worker-seconds waiting for writer channel capacity; can exceed wall time.",
                MetricType::COUNTER,
                vec![(vec![], total.blocked_seconds)],
            ),
            scalar(
                "markets",
                "Desired markets, markets with all publisher books synchronized, and markets also having a ready standby; states overlap.",
                MetricType::GAUGE,
                vec![
                    (
                        vec![("state", "desired".into())],
                        total.desired_markets as f64,
                    ),
                    (
                        vec![("state", "synced".into())],
                        total.synced_markets as f64,
                    ),
                    (
                        vec![("state", "standby_ready".into())],
                        total.standby_ready_markets as f64,
                    ),
                ],
            ),
            scalar(
                "books",
                "Desired and synchronized outcome books; states overlap.",
                MetricType::GAUGE,
                vec![
                    (
                        vec![("state", "desired".into())],
                        total.desired_books as f64,
                    ),
                    (vec![("state", "synced".into())], total.synced_books as f64),
                ],
            ),
        ];
        families.push(scalar(
            "publisher_switches_total",
            "Logical markets changing publisher connection after a connection failure.",
            MetricType::COUNTER,
            [
                "remote_close",
                "io_error",
                "silence",
                "decode_error",
                "sequence_gap",
            ]
            .into_iter()
            .zip(total.publisher_switches)
            .map(|(reason, count)| (vec![("reason", reason.into())], count as f64))
            .collect(),
        ));
        let mut connection_samples = Vec::new();
        let mut end_samples = Vec::new();
        for kind in [ConnectionKind::Orderbook, ConnectionKind::MarketDiscovery] {
            for (i, state) in ["expected", "connected"].iter().enumerate() {
                connection_samples.push((
                    vec![("kind", kind.name().into()), ("state", (*state).into())],
                    connections[kind.index()][i] as f64,
                ));
            }
            for reason in EndReason::ALL {
                end_samples.push((
                    vec![
                        ("kind", kind.name().into()),
                        ("reason", reason.name().into()),
                    ],
                    ends[kind.index()][reason.index()] as f64,
                ));
            }
        }
        families.push(scalar(
            "connections",
            "Expected and open connections by purpose; states overlap.",
            MetricType::GAUGE,
            connection_samples,
        ));
        families.push(scalar(
            "session_ends_total",
            "Established sessions ended by fixed reason; failed connection attempts excluded.",
            MetricType::COUNTER,
            end_samples,
        ));
        families.push(scalar("worker_busy_seconds_total", "Elapsed input handling time, excluding snapshot timers and output waits; not CPU utilization.", MetricType::COUNTER, workers.into_iter().map(|(id,value)| (vec![("worker",id.to_string())],value)).collect()));
        families.push(scalar(
            "writer_busy_seconds_total",
            "Elapsed record encoding and output write/flush time, excluding idle waits; not CPU utilization.",
            MetricType::COUNTER,
            vec![(vec![], writer_busy)],
        ));
        if let Some(at) = total.last_scan {
            families.push(scalar(
                "last_successful_scan_timestamp_seconds",
                "UTC completion time of last successful full discovery scan.",
                MetricType::GAUGE,
                vec![(vec![], at)],
            ));
        }
        if let Some(at) = oldest {
            families.push(scalar(
                "oldest_required_component_publication_timestamp_seconds",
                "Oldest UTC publication time among active required components.",
                MetricType::GAUGE,
                vec![(vec![], at)],
            ));
        }
        families.retain(|family| !family.get_metric().is_empty());
        let mut bytes = Vec::new();
        TextEncoder::new().encode(&families, &mut bytes)?;
        let socket_families = vec![
            scalar(
                "socket_events_total",
                "Cumulative decoded data events per upstream order-book socket, including full books; excludes heartbeats.",
                MetricType::COUNTER,
                socket_events,
            ),
            scalar(
                "socket_connected",
                "Whether an active order-book socket is connected.",
                MetricType::GAUGE,
                socket_connected,
            ),
            scalar(
                "socket_markets",
                "Logical markets subscribed on an active order-book socket.",
                MetricType::GAUGE,
                socket_markets,
            ),
            scalar(
                "socket_process_start_timestamp_seconds",
                "UTC start time of this metrics instance; identifies collector restarts in socket history.",
                MetricType::GAUGE,
                vec![(vec![], self.started_at)],
            ),
        ];
        let socket_families: Vec<_> = socket_families
            .into_iter()
            .filter(|family| !family.get_metric().is_empty())
            .collect();
        let mut socket_bytes = Vec::new();
        TextEncoder::new().encode(&socket_families, &mut socket_bytes)?;
        Ok(View {
            sockets_text: String::from_utf8(socket_bytes)?.into(),
            text: String::from_utf8(bytes)?.into(),
            fresh,
            initialized,
            created: Some(Instant::now()),
        })
    }
    pub async fn run(
        self,
        views: watch::Sender<View>,
        stop: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut tick = tokio::time::interval(PUBLICATION_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.cancelled() => { views.send_replace(self.render()?); return Ok(()); }
                _ = tick.tick() => { views.send_replace(self.render()?); }
            }
        }
    }
}
pub fn timestamp() -> f64 {
    Utc::now().timestamp_micros() as f64 / 1_000_000.
}
fn family(name: &str, help: &str, kind: MetricType) -> MetricFamily {
    let mut f = MetricFamily::default();
    f.set_name(format!("collector_{name}"));
    f.set_help(help.into());
    f.set_field_type(kind);
    f
}
type Samples = Vec<(Vec<(&'static str, String)>, f64)>;
fn scalar(name: &str, help: &str, kind: MetricType, samples: Samples) -> MetricFamily {
    let mut f = family(name, help, kind);
    let metrics = samples
        .into_iter()
        .map(|(labels, value)| {
            let mut m = Metric::default();
            m.set_label(
                labels
                    .into_iter()
                    .map(|(name, value)| {
                        let mut l = LabelPair::default();
                        l.set_name(name.into());
                        l.set_value(value);
                        l
                    })
                    .collect(),
            );
            if kind == MetricType::COUNTER {
                let mut c = Counter::default();
                c.set_value(value);
                m.set_counter(c);
            } else {
                let mut g = Gauge::default();
                g.set_value(value);
                m.set_gauge(g);
            }
            m
        })
        .collect();
    f.set_metric(metrics);
    f
}
fn histogram(name: &str, help: &str, data: &Histogram) -> MetricFamily {
    let mut f = family(name, help, MetricType::HISTOGRAM);
    let mut h = ProtoHistogram::default();
    h.set_sample_count(data.count);
    h.set_sample_sum(data.sum);
    let mut cumulative = 0;
    h.set_bucket(
        data.bounds()
            .iter()
            .enumerate()
            .map(|(i, bound)| {
                cumulative += data.buckets[i];
                let mut b = Bucket::default();
                b.set_upper_bound(*bound);
                b.set_cumulative_count(cumulative);
                b
            })
            .collect(),
    );
    let mut m = Metric::default();
    m.set_histogram(h);
    f.set_metric(vec![m]);
    f
}

#[cfg(test)]
mod tests {
    use crate::core::metrics::{Component, Metrics, STALE_AFTER, timestamp};
    use std::time::{Duration, Instant};

    #[test]
    fn a_frozen_component_invalidates_fresh_aggregates() {
        let metrics = Metrics::default();
        let mut worker = metrics.register(Component::Worker(0));
        worker.publish();
        assert!(metrics.render().unwrap().fresh);
        {
            let mut slot = worker.slot.lock().unwrap();
            slot.at = Some((
                Instant::now() - STALE_AFTER - Duration::from_secs(1),
                timestamp() - 16.,
            ));
        }
        assert!(!metrics.render().unwrap().fresh);
        worker.publish();
        assert!(metrics.render().unwrap().fresh);
    }

    #[test]
    fn idle_standby_slots_remain_required_until_retirement() {
        let metrics = Metrics::default();
        let mut worker = metrics.register(Component::Worker(0));
        worker.state.synced_markets = 1;
        worker.publish();
        let mut standby = metrics.register(Component::Connection(crate::model::ConnectionId(1)));
        assert!(!metrics.render().unwrap().fresh);
        standby.publish();
        let view = metrics.render().unwrap();
        assert!(view.fresh && view.initialized);
        assert!(
            view.text
                .contains("collector_markets{state=\"standby_ready\"} 0")
        );
        {
            let mut slot = standby.slot.lock().unwrap();
            slot.at = Some((
                Instant::now() - STALE_AFTER - Duration::from_secs(1),
                timestamp() - 16.,
            ));
        }
        assert!(!metrics.render().unwrap().fresh);
        drop(standby);
        assert!(metrics.render().unwrap().fresh);
    }
}
