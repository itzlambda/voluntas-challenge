//! Reads a collector output file with the supplied `AlignedSbeReader` and
//! checks it: per book, every snapshot after the first must equal the book
//! rebuilt from the records before it, except exact input-proven replacement
//! baselines supplied with `--baselines`. Those also permit source timestamp
//! rollback at the replacement boundary.
//!
//! Usage: cargo run --release --example sbe_summary -- books.sbe

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, ensure};
use serde::Deserialize;

use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[derive(Default)]
struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    snapshots: u64,
    updates: u64,
    last_received_us: i64,
}

#[derive(Deserialize)]
struct Baseline {
    symbol: String,
    received_us: i64,
    bids: Vec<(i64, i64)>,
    asks: Vec<(i64, i64)>,
}
#[derive(Deserialize)]
struct Manifest {
    baselines: Vec<Baseline>,
}
type Baselines = HashMap<String, HashMap<i64, (BTreeMap<i64, i64>, BTreeMap<i64, i64>)>>;

fn manifest(text: &str) -> anyhow::Result<Baselines> {
    let parsed: Manifest = serde_json::from_str(text).context("baseline manifest")?;
    let mut out = Baselines::new();
    for baseline in parsed.baselines {
        ensure!(
            out.entry(baseline.symbol)
                .or_default()
                .insert(
                    baseline.received_us,
                    (
                        baseline.bids.into_iter().collect(),
                        baseline.asks.into_iter().collect()
                    )
                )
                .is_none(),
            "duplicate baseline marker"
        );
    }
    Ok(out)
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .context("usage: sbe_summary FILE [--baselines MANIFEST.json]")?;
    let baselines = match args.next() {
        None => Baselines::new(),
        Some(flag) => {
            ensure!(flag == "--baselines", "expected --baselines MANIFEST.json");
            let source = args.next().context("missing baseline manifest")?;
            ensure!(args.next().is_none(), "unexpected argument");
            manifest(&std::fs::read_to_string(source)?)?
        }
    };
    verify(&path, baselines)
}

fn verify(path: &str, mut baselines: Baselines) -> anyhow::Result<()> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut books: HashMap<String, Book> = HashMap::new();
    let (mut snapshots, mut updates, mut mismatches, mut regressions) = (0u64, 0u64, 0u64, 0u64);
    let (mut first_us, mut last_us) = (i64::MAX, i64::MIN);
    let mut unbaselined = 0u64;
    let mut validated_baselines = 0u64;
    let mut known_resets = 0u64;
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoder)?;
                snapshots += 1;
                let received = snapshot.received_at.timestamp_micros();
                let bids: BTreeMap<i64, i64> = snapshot
                    .bids
                    .iter()
                    .map(|l| (l.price_level, l.quantity))
                    .collect();
                let asks: BTreeMap<i64, i64> = snapshot
                    .asks
                    .iter()
                    .map(|l| (l.price_level, l.quantity))
                    .collect();
                let book = books.entry(snapshot.symbol.clone()).or_default();
                let baseline = baselines
                    .get_mut(&snapshot.symbol)
                    .and_then(|entries| entries.remove(&received));
                let is_baseline = baseline.is_some();
                if let Some((expected_bids, expected_asks)) = baseline {
                    ensure!(
                        bids == expected_bids && asks == expected_asks,
                        "raw baseline contents differ for {} at {}",
                        snapshot.symbol,
                        snapshot.received_at
                    );
                    validated_baselines += 1;
                    known_resets += u64::from(book.snapshots > 0);
                }
                // Only a book that already had a snapshot in this file can be
                // checked; one that starts with updates (for example after a
                // trimmed region) has no baseline yet.
                if book.snapshots == 0 && book.updates > 0 {
                    unbaselined += 1;
                } else if book.snapshots > 0
                    && !is_baseline
                    && (book.bids != bids || book.asks != asks)
                {
                    mismatches += 1;
                    if std::env::var_os("SBE_SUMMARY_VERBOSE").is_some() {
                        println!("differs: {} at {}", snapshot.symbol, snapshot.received_at);
                    }
                }
                if received < book.last_received_us && !is_baseline {
                    regressions += 1;
                }
                book.last_received_us = received;
                book.bids = bids;
                book.asks = asks;
                book.snapshots += 1;
                first_us = first_us.min(received);
                last_us = last_us.max(received);
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                updates += 1;
                let received = update.backtest_timestamp.timestamp_micros();
                let book = books.entry(update.symbol).or_default();
                let side = match update.side {
                    Side::Buy => &mut book.bids,
                    Side::Sell => &mut book.asks,
                };
                if update.quantity == 0 {
                    side.remove(&update.price_level);
                } else {
                    side.insert(update.price_level, update.quantity);
                }
                book.updates += 1;
                book.last_received_us = book.last_received_us.max(received);
                first_us = first_us.min(received);
                last_us = last_us.max(received);
            }
            _ => anyhow::bail!("unknown message"),
        }
    }
    let with_updates = books.values().filter(|b| b.updates > 0).count();
    let levels: usize = books.values().map(|b| b.bids.len() + b.asks.len()).sum();
    println!("file: {path}");
    println!(
        "books: {} ({with_updates} with level updates), levels held at end: {levels}",
        books.len()
    );
    println!("snapshots: {snapshots}, level updates: {updates}");
    if first_us <= last_us {
        println!(
            "received time span: {:.1} s",
            (last_us - first_us) as f64 / 1e6
        );
    }
    println!("snapshots differing without an input-proven baseline reset: {mismatches}");
    println!(
        "input-proven baselines checked: {validated_baselines} ({known_resets} repeat-book resets)"
    );
    println!("snapshot received times going backwards: {regressions}");
    println!("books whose first snapshot followed updates with no baseline: {unbaselined}");
    ensure!(
        baselines.values().all(HashMap::is_empty),
        "baseline manifest contains markers absent from SBE output"
    );
    anyhow::ensure!(
        mismatches == 0 && regressions == 0 && unbaselined == 0,
        "SBE verification failed: {mismatches} snapshot differences, {regressions} backwards snapshot receive times, {unbaselined} books without a baseline"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{Baselines, verify};
    use chrono::{TimeZone, Utc};
    use engineering_challenge::core::config::OUTPUT_CHUNK_BYTES;
    use engineering_challenge::util::AlignedSbeWriter;
    use engineering_challenge::util::dtos::challenge_orderbook::{
        SnapshotBidsEntryDomain, SnapshotDomain,
    };
    use std::collections::{BTreeMap, HashMap};

    #[test]
    fn changed_snapshot_requires_an_exact_input_proven_baseline() {
        check_replacement([1, 2]);
    }

    #[test]
    fn source_replacement_can_use_older_timestamps_only_with_a_proven_baseline() {
        check_replacement([2, 1]);
    }

    fn check_replacement(times: [i64; 2]) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("resets.sbe");
        let mut writer = AlignedSbeWriter::create(&path, OUTPUT_CHUNK_BYTES).unwrap();
        for (seconds, qty) in [(times[0], 10), (times[1], 20)] {
            let at = Utc.timestamp_opt(seconds, 0).single().unwrap();
            writer
                .write_msg(&SnapshotDomain {
                    received_at: at,
                    exchange_timestamp: at,
                    symbol: "11".into(),
                    bids: vec![SnapshotBidsEntryDomain {
                        price_level: 500000,
                        quantity: qty,
                    }],
                    asks: vec![],
                })
                .unwrap();
        }
        writer.flush().unwrap();
        drop(writer);
        let filename = path.to_str().unwrap();
        assert!(verify(filename, Baselines::new()).is_err());
        let make = |qty| {
            HashMap::from([(
                "11".to_owned(),
                HashMap::from([(
                    times[1] * 1_000_000,
                    (BTreeMap::from([(500000, qty)]), BTreeMap::new()),
                )]),
            )])
        };
        assert!(verify(filename, make(20)).is_ok());
        assert!(verify(filename, make(21)).is_err());
    }
}
