//! Read existing SBE output and classify snapshot transitions without accepting resets.
//! Usage: snapshot-transition-audit INPUT.sbe OUTPUT.json
use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, ensure};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};
use serde_json::{Value, json};

#[derive(Default)]
struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    snapshots: u64,
    updates: u64,
    previous_snapshot_record: u64,
    previous_snapshot_received_us: i64,
    previous_snapshot_empty: bool,
    previous_snapshot_updates: u64,
    last_received_us: i64,
}

fn changed_levels(before: &BTreeMap<i64, i64>, after: &BTreeMap<i64, i64>) -> usize {
    before.iter().filter(|(p, q)| after.get(p) != Some(q)).count()
        + after.keys().filter(|p| !before.contains_key(p)).count()
}

fn audit(path: &str) -> anyhow::Result<Value> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut books: HashMap<String, Book> = HashMap::new();
    let mut differences = Vec::new();
    let (mut records, mut snapshots, mut updates, mut empty_snapshots) = (0, 0, 0, 0);
    let (mut regressions, mut unbaselined) = (0, 0);
    let mut classes: BTreeMap<&str, u64> = BTreeMap::new();
    while let Some(message) = reader.read_msg()? {
        records += 1;
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoder)?;
                snapshots += 1;
                let received = snapshot.received_at.timestamp_micros();
                let bids: BTreeMap<i64, i64> = snapshot.bids.iter()
                    .map(|l| (l.price_level, l.quantity)).collect();
                let asks: BTreeMap<i64, i64> = snapshot.asks.iter()
                    .map(|l| (l.price_level, l.quantity)).collect();
                let empty = bids.is_empty() && asks.is_empty();
                empty_snapshots += u64::from(empty);
                let book = books.entry(snapshot.symbol.clone()).or_default();
                let intervening_updates = book.updates - book.previous_snapshot_updates;
                if book.snapshots == 0 && book.updates > 0 {
                    unbaselined += 1;
                } else if book.snapshots > 0 && (book.bids != bids || book.asks != asks) {
                    let class = if empty {
                        "nonempty_to_empty"
                    } else if book.previous_snapshot_empty && intervening_updates == 0 {
                        "empty_to_nonempty_without_updates"
                    } else {
                        "other"
                    };
                    *classes.entry(class).or_default() += 1;
                    differences.push(json!({
                        "symbol": snapshot.symbol, "record": records,
                        "received_us": received, "exchange_us": snapshot.exchange_timestamp.timestamp_micros(),
                        "class": class, "previous_snapshot_record": book.previous_snapshot_record,
                        "previous_snapshot_received_us": book.previous_snapshot_received_us,
                        "previous_snapshot_empty": book.previous_snapshot_empty,
                        "updates_since_previous_snapshot": intervening_updates,
                        "reconstructed_levels": book.bids.len() + book.asks.len(),
                        "snapshot_levels": bids.len() + asks.len(),
                        "changed_bid_levels": changed_levels(&book.bids, &bids),
                        "changed_ask_levels": changed_levels(&book.asks, &asks),
                        "same_received_as_previous_record_for_book": received == book.last_received_us,
                    }));
                }
                regressions += u64::from(received < book.last_received_us);
                book.last_received_us = received;
                book.bids = bids;
                book.asks = asks;
                book.snapshots += 1;
                book.previous_snapshot_record = records;
                book.previous_snapshot_received_us = received;
                book.previous_snapshot_empty = empty;
                book.previous_snapshot_updates = book.updates;
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                updates += 1;
                let book = books.entry(update.symbol).or_default();
                let side = match update.side { Side::Buy => &mut book.bids, Side::Sell => &mut book.asks };
                if update.quantity == 0 { side.remove(&update.price_level); }
                else { side.insert(update.price_level, update.quantity); }
                book.updates += 1;
                book.last_received_us = book.last_received_us.max(update.backtest_timestamp.timestamp_micros());
            }
            _ => anyhow::bail!("unknown SBE message"),
        }
    }
    Ok(json!({
        "records": records, "snapshots": snapshots, "updates": updates,
        "books": books.len(), "empty_snapshots": empty_snapshots,
        "snapshot_time_regressions": regressions, "books_without_initial_baseline": unbaselined,
        "difference_count": differences.len(), "classes": classes, "differences": differences,
        "scope": "Structural output classification only. Empty snapshots are not independently proven removal markers; fresh snapshots are not independently checked against raw input. No differences are waived."
    }))
}

fn self_test() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("snapshot-transition-audit-{}", std::process::id()));
    std::fs::create_dir(&dir)?;
    let file = dir.join("fixture.sbe");
    let mut writer = AlignedSbeWriter::create(&file, 64 << 20)?;
    for (index, quantity) in [10, 20, 0, 30, 30, 40, 40].into_iter().enumerate() {
        if index == 1 {
            writer.write_msg(&PriceLevelUpdateDomain {
                backtest_timestamp: "2026-01-01T00:00:00Z".parse()?,
                exchange_timestamp: "2026-01-01T00:00:00Z".parse()?,
                symbol: "fixture".into(), price_level: 500_000, quantity: 20, side: Side::Buy,
            })?;
        }
        writer.write_msg(&SnapshotDomain {
            received_at: "2026-01-01T00:00:00Z".parse()?,
            exchange_timestamp: "2026-01-01T00:00:00Z".parse()?,
            symbol: "fixture".into(),
            bids: if quantity == 0 { vec![] } else { vec![SnapshotBidsEntryDomain { price_level: 500_000, quantity }] },
            asks: Vec::<SnapshotAsksEntryDomain>::new(),
        })?;
    }
    writer.flush()?;
    drop(writer);
    let result = audit(file.to_str().context("fixture path")?)?;
    ensure!(result["snapshots"] == 7 && result["updates"] == 1, "fixture record counts");
    ensure!(result["difference_count"] == 3, "must retain all three differences");
    for class in ["nonempty_to_empty", "empty_to_nonempty_without_updates", "other"] {
        ensure!(result["classes"][class] == 1, "wrong classification: {class}");
    }
    std::fs::remove_dir_all(dir)?;
    println!("self-test passed: normal update, clear, fresh baseline, and unexplained difference");
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--self-test") { return self_test(); }
    ensure!(args.len() == 3, "usage: snapshot-transition-audit INPUT.sbe OUTPUT.json");
    let result = audit(&args[1])?;
    std::fs::write(&args[2], serde_json::to_vec_pretty(&result)?)?;
    println!("{}", json!({"snapshots": result["snapshots"], "updates": result["updates"],
        "difference_count": result["difference_count"], "classes": result["classes"],
        "empty_snapshots": result["empty_snapshots"]}));
    Ok(())
}
