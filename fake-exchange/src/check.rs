use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, ensure};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use serde::Serialize;
use serde_json::{Value, json};

use crate::book::Levels;
use crate::history::{self, Lifecycle};

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}
fn level_hash(side: u8, price: i64, size: i64) -> u128 {
    let key = mix(price as u64 ^ (u64::from(side) << 63));
    let a = mix(key ^ size as u64);
    let b = mix(key.wrapping_add(0x9e3779b97f4a7c15) ^ (size as u64).rotate_left(23));
    (u128::from(a) << 64) | u128::from(b)
}
#[derive(Default)]
struct Book {
    levels: Levels,
    fingerprint: u128,
}
impl Book {
    fn from_levels(levels: Levels) -> Self {
        let mut result = Self::default();
        for (side, entries) in [(0, &levels.bids), (1, &levels.asks)] {
            for (&price, &size) in entries {
                result.set(side, price, size);
            }
        }
        result
    }
    fn set(&mut self, side: u8, price: i64, size: i64) {
        let levels = if side == 0 {
            &self.levels.bids
        } else {
            &self.levels.asks
        };
        if let Some(old) = levels.get(&price) {
            self.fingerprint = self.fingerprint.wrapping_sub(level_hash(side, price, *old));
        }
        if size != 0 {
            self.fingerprint = self.fingerprint.wrapping_add(level_hash(side, price, size));
        }
        self.levels.set(side, price, size);
    }
}

struct State {
    fingerprint: u128,
    time_us: i64,
}
struct Truth {
    states: Vec<State>,
    created_us: i64,
    resolved_us: Option<i64>,
}

#[derive(Default, Serialize)]
pub struct Findings {
    pub count: u64,
    pub samples: Vec<Value>,
}
impl Findings {
    fn add(&mut self, sample: Value, count: u64) {
        self.count += count;
        if self.samples.len() < 20 {
            self.samples.push(sample);
        }
    }
}

#[derive(Default, Serialize)]
pub struct Summary {
    pub records: u64,
    pub snapshots: u64,
    pub updates: u64,
    pub retirements: u64,
    pub errors: Findings,
    pub gaps: Findings,
    pub coverage: Value,
    pub tail: Value,
    pub latency_ms: Value,
}
impl Summary {
    pub fn passed(&self, allow_gaps: bool) -> bool {
        self.errors.count == 0 && (allow_gaps || self.gaps.count == 0)
    }
}

#[derive(Default)]
struct Observed {
    book: Book,
    pointer: Option<usize>,
    pending: Option<(i64, i64)>,
    last_received_us: i64,
    last_exchange_us: i64,
    retired: bool,
    first_received_us: i64,
}

fn load_truth(dir: &Path) -> anyhow::Result<HashMap<u64, Truth>> {
    ensure!(
        dir.join("complete.json").is_file(),
        "history incomplete: shut server down with SIGINT first"
    );
    let manifest: Value = serde_json::from_reader(File::open(dir.join("manifest.json"))?)?;
    ensure!(manifest["version"] == 1, "unsupported history manifest");
    let shards = manifest["shards"].as_u64().context("missing shard count")?;
    let mut truth = HashMap::new();
    let mut books = HashMap::new();
    for asset in history::read_assets(dir)? {
        let book = Book::from_levels(asset.levels);
        ensure!(
            !truth.contains_key(&asset.asset),
            "duplicate asset in history"
        );
        truth.insert(
            asset.asset,
            Truth {
                states: vec![State {
                    fingerprint: book.fingerprint,
                    time_us: asset.created_ms * 1000,
                }],
                created_us: asset.created_ms * 1000,
                resolved_us: None,
            },
        );
        books.insert(asset.asset, book);
    }
    for shard in 0..shards {
        history::read_changes(&dir.join(format!("shard-{shard}.bin")), |change| {
            ensure!(
                change.price > 0
                    && change.price < 1_000_000
                    && change.price % 10_000 == 0
                    && change.size >= 0,
                "invalid history level"
            );
            let book = books
                .get_mut(&change.asset)
                .context("history change for unknown asset")?;
            let asset = truth.get_mut(&change.asset).context("missing asset")?;
            ensure!(
                asset
                    .states
                    .last()
                    .is_some_and(|s| s.time_us <= change.time * 1000),
                "history timestamp regression"
            );
            let old = book.fingerprint;
            book.set(change.side, change.price, change.size);
            ensure!(old != book.fingerprint, "history contains a no-op change");
            asset.states.push(State {
                fingerprint: book.fingerprint,
                time_us: change.time * 1000,
            });
            Ok(())
        })?;
    }
    for line in BufReader::new(File::open(dir.join("lifecycle.jsonl"))?).lines() {
        let event: Lifecycle = serde_json::from_str(&line?)?;
        if event.event == "resolved" {
            for asset in [event.market * 2, event.market * 2 + 1] {
                if let Some(truth) = truth.get_mut(&asset) {
                    truth.resolved_us = Some(event.time * 1000);
                }
            }
        }
    }
    Ok(truth)
}

fn error(summary: &mut Summary, asset: u64, message: &str, exchange_us: i64) {
    summary.errors.add(
        json!({"asset":asset.to_string(),"error":message,"exchange_timestamp_us":exchange_us}),
        1,
    );
}

fn settle(asset: u64, observed: &mut Observed, truth: &Truth, summary: &mut Summary) {
    let Some((exchange_us, _)) = observed.pending.take() else {
        return;
    };
    let Some(k) = observed.pointer else {
        return;
    };
    let found = truth
        .states
        .iter()
        .enumerate()
        .skip(k + 1)
        .find(|(_, s)| s.fingerprint == observed.book.fingerprint && s.time_us <= exchange_us);
    if let Some((j, state)) = found {
        let missed = j - k - 1;
        if missed > 0 {
            summary.gaps.add(
                json!({"asset":asset.to_string(),"from_state":k,"to_state":j,"changes":missed}),
                missed as u64,
            );
        }
        if state.time_us != exchange_us {
            error(
                summary,
                asset,
                "catch-up timestamp does not equal matching change time",
                exchange_us,
            );
        }
        observed.pointer = Some(j);
    } else {
        error(
            summary,
            asset,
            "updates match no later true state",
            exchange_us,
        );
    }
}

fn snapshot(
    asset: u64,
    levels: Levels,
    exchange_us: i64,
    received_us: i64,
    observed: &mut Observed,
    truth: &Truth,
    summary: &mut Summary,
) {
    settle(asset, observed, truth, summary);
    if observed.retired {
        error(summary, asset, "snapshot after retirement", exchange_us);
        return;
    }
    // The collector retires a published book with an empty snapshot carrying
    // its last data timestamps. A repeated unchanged book can advance those
    // timestamps without emitting SBE. Resolution is checked against history.
    if levels.bids.is_empty()
        && levels.asks.is_empty()
        && (!observed.book.levels.bids.is_empty() || !observed.book.levels.asks.is_empty())
        && observed.pointer.is_some()
        && truth
            .resolved_us
            .is_some_and(|resolved| exchange_us <= resolved)
        && exchange_us >= observed.last_exchange_us
        && received_us >= observed.last_received_us
    {
        observed.retired = true;
        summary.retirements += 1;
        return;
    }
    observed.book = Book::from_levels(levels);
    if let Some(k) = observed.pointer {
        let found =
            truth.states.iter().enumerate().skip(k).find(|(_, s)| {
                s.fingerprint == observed.book.fingerprint && s.time_us <= exchange_us
            });
        if let Some((j, _)) = found {
            if j > k {
                summary.gaps.add(
                    json!({"asset":asset.to_string(),"from_state":k,"to_state":j,"changes":j-k}),
                    (j - k) as u64,
                );
            }
            observed.pointer = Some(j);
        } else {
            error(
                summary,
                asset,
                "snapshot matches no current or later true state",
                exchange_us,
            );
        }
    } else {
        observed.pointer = truth
            .states
            .iter()
            .rposition(|s| s.time_us <= exchange_us && s.fingerprint == observed.book.fingerprint);
        if observed.pointer.is_none() {
            error(
                summary,
                asset,
                "first snapshot matches no true state at or before timestamp",
                exchange_us,
            );
        }
        observed.first_received_us = received_us;
    }
    observed.last_received_us = received_us;
    observed.last_exchange_us = exchange_us;
}

#[derive(Default)]
pub struct Latency {
    buckets: std::collections::BTreeMap<i64, u64>,
    count: u64,
    min: i64,
    max: i64,
}
impl Latency {
    pub fn add_us(&mut self, value: i64) {
        *self.buckets.entry(value.div_euclid(100)).or_default() += 1;
        if self.count == 0 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
        self.count += 1;
    }
    pub fn merge(&mut self, other: Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            self.min = other.min;
            self.max = other.max;
        } else {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
        self.count += other.count;
        for (bucket, count) in other.buckets {
            *self.buckets.entry(bucket).or_default() += count;
        }
    }
    pub fn summary(&self) -> Value {
        let percentile = |fraction: f64| {
            let threshold = ((self.count as f64 * fraction).ceil() as u64).max(1);
            let mut count = 0;
            for (&bucket, &n) in &self.buckets {
                count += n;
                if count >= threshold {
                    return Some(bucket as f64 / 10.0);
                }
            }
            None
        };
        json!({"count":self.count,"p50":percentile(0.50),"p99":percentile(0.99),"max":if self.count>0 {Some(self.max as f64/1000.0)} else {None},"min":if self.count>0 {Some(self.min as f64/1000.0)} else {None},"resolution_ms":0.1})
    }
}

pub fn check(sbe: &Path, dir: &Path, coverage_grace_secs: f64) -> anyhow::Result<Summary> {
    let truth = load_truth(dir)?;
    ensure!(
        fs::metadata(sbe)?.len().is_multiple_of(64 << 20),
        "SBE length is not a complete 64 MiB chunk"
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(sbe)?;
    let mut observed: HashMap<u64, Observed> = HashMap::new();
    let mut summary = Summary::default();
    let mut latency = Latency::default();
    let mut last_received = 0;
    while let Some(message) = reader.read_msg()? {
        summary.records += 1;
        match message {
            AnyMessage::Snapshot(decoder) => {
                let message = SnapshotDomain::try_from_decoder(decoder)?;
                let received = message.received_at.timestamp_micros();
                let exchange = message.exchange_timestamp.timestamp_micros();
                last_received = last_received.max(received);
                latency.add_us(received - exchange);
                summary.snapshots += 1;
                let asset = message
                    .symbol
                    .parse::<u64>()
                    .context("invalid asset symbol")?;
                let Some(truth) = truth.get(&asset) else {
                    error(&mut summary, asset, "asset absent from history", exchange);
                    continue;
                };
                let levels = Levels {
                    bids: message
                        .bids
                        .into_iter()
                        .map(|l| (l.price_level, l.quantity))
                        .collect(),
                    asks: message
                        .asks
                        .into_iter()
                        .map(|l| (l.price_level, l.quantity))
                        .collect(),
                };
                snapshot(
                    asset,
                    levels,
                    exchange,
                    received,
                    observed.entry(asset).or_default(),
                    truth,
                    &mut summary,
                );
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let message = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                let received = message.backtest_timestamp.timestamp_micros();
                let exchange = message.exchange_timestamp.timestamp_micros();
                last_received = last_received.max(received);
                latency.add_us(received - exchange);
                summary.updates += 1;
                let asset = message
                    .symbol
                    .parse::<u64>()
                    .context("invalid asset symbol")?;
                let Some(truth) = truth.get(&asset) else {
                    error(&mut summary, asset, "asset absent from history", exchange);
                    continue;
                };
                let observed = observed.entry(asset).or_default();
                if observed.retired {
                    error(&mut summary, asset, "update after retirement", exchange);
                    continue;
                }
                if observed
                    .pending
                    .is_some_and(|pair| pair != (exchange, received))
                {
                    settle(asset, observed, truth, &mut summary);
                }
                let Some(k) = observed.pointer else {
                    error(
                        &mut summary,
                        asset,
                        "first record must be a snapshot",
                        exchange,
                    );
                    continue;
                };
                observed.book.set(
                    match message.side {
                        Side::Buy => 0,
                        Side::Sell => 1,
                    },
                    message.price_level,
                    message.quantity,
                );
                observed.last_received_us = received;
                observed.last_exchange_us = exchange;
                if observed.pending.is_some() {
                    continue;
                }
                if let Some(state) = truth
                    .states
                    .get(k + 1)
                    .filter(|s| s.fingerprint == observed.book.fingerprint)
                {
                    if state.time_us != exchange {
                        error(
                            &mut summary,
                            asset,
                            "update timestamp does not equal change time",
                            exchange,
                        );
                    }
                    observed.pointer = Some(k + 1);
                } else {
                    observed.pending = Some((exchange, received));
                }
            }
            _ => anyhow::bail!("unknown SBE message"),
        }
    }
    let mut tails = Vec::new();
    let mut after_output = 0;
    let mut never = Vec::new();
    let mut ages = Latency::default();
    let grace = (coverage_grace_secs * 1e6) as i64;
    for (&asset, truth) in &truth {
        if let Some(observed) = observed.get_mut(&asset) {
            settle(asset, observed, truth, &mut summary);
            if let Some(k) = observed.pointer {
                let until = truth.states.partition_point(|s| s.time_us <= last_received);
                tails.push(until.saturating_sub(k + 1) as u64);
                after_output += truth.states.len().saturating_sub(until) as u64;
                ages.add_us(observed.first_received_us - truth.created_us);
            }
        }
        if observed.get(&asset).is_none_or(|o| o.pointer.is_none())
            && last_received > truth.created_us + grace
            && truth.resolved_us.unwrap_or(last_received) - truth.created_us >= grace
        {
            never.push(asset.to_string());
        }
    }
    tails.sort_unstable();
    let quantile = |p: f64| {
        tails
            .get(((tails.len().saturating_sub(1)) as f64 * p) as usize)
            .copied()
    };
    summary.tail = json!({"assets":tails.len(),"changes_before_last_received_record":tails.iter().sum::<u64>(),"p50":quantile(0.5),"p99":quantile(0.99),"max":tails.last(),"changes_after_output":after_output,"note":"unobserved trailing changes are reported separately from interior gaps"});
    summary.coverage = json!({"catalog_assets":truth.len(),"snapshotted_assets":observed.values().filter(|o|o.pointer.is_some()).count(),"eligible_without_snapshot":never.len(),"samples":never.into_iter().take(20).collect::<Vec<_>>(),"creation_to_first_snapshot_ms":ages.summary(),"grace_seconds":coverage_grace_secs});
    summary.latency_ms = latency.summary();
    if summary.records == 0 {
        summary
            .errors
            .add(json!({"error":"empty collector stream"}), 1);
    }
    Ok(summary)
}
