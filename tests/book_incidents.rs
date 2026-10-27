#[allow(dead_code)]
mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

use engineering_challenge::Side;
use engineering_challenge::core::config::Config;
use engineering_challenge::core::pipeline::replay;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use serde_json::{Value, json};

use crate::common::{Record, check, fixture, read_records, sum};

type Levels = BTreeMap<i64, i64>;

fn expected_levels(value: &Value) -> Levels {
    serde_json::from_value::<Vec<(i64, i64)>>(value.clone())
        .unwrap()
        .into_iter()
        .collect()
}

#[tokio::test(start_paused = true)]
async fn absolute_refreshes_preserve_pre_refresh_state_and_emit_corrections() {
    for run in check(fixture(include_str!(
        "fixtures/book-incidents-regression.jsonl"
    )))
    .await
    {
        assert_eq!(sum(&run.totals, |w| w.mismatched_books), 2);
        assert_eq!(sum(&run.totals, |w| w.mismatched_levels), 2);
        assert_eq!(sum(&run.totals, |w| w.first_snapshots), 2);
        assert_eq!(sum(&run.totals, |w| w.level_updates), 5);
        let a = &run.records["11"];
        assert!(matches!(&a[4], Record::Snapshot { bids, asks, .. }
            if bids == &[(30_000, 5_100_000_000), (20_000, 9_000_000)]
            && asks == &[(970_000, 7_000_000)]));
        assert!(matches!(
            &a[5],
            Record::Update {
                side: Side::Buy,
                price: 30_000,
                qty: 6_100_000_000,
                ..
            }
        ));
        let b = &run.records["12"];
        assert!(matches!(&b[2], Record::Snapshot { bids, asks, .. }
            if bids == &[(180_000, 6_887_750_000)] && asks == &[(820_000, 8_000_000)]));
        assert!(matches!(
            &b[3],
            Record::Update {
                side: Side::Buy,
                price: 180_000,
                qty: 7_443_300_000,
                ..
            }
        ));
        // A later identical full book emits no correction. Trades are ignored,
        // same-time changes remain ordered, and a zero removes the level.
        assert_eq!(a.len(), 8);
        assert_eq!(b.len(), 5);
    }
}

#[tokio::test(start_paused = true)]
#[ignore = "requires the two local saved raw recordings and Python 3"]
async fn saved_raw_windows_match_decimal_before_every_refresh() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let scratch_dir = std::env::var_os("BOOK_INCIDENT_DIRECTORY")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/submission-replay"));
    let scratch = scratch_dir.as_path();
    std::fs::create_dir_all(scratch).unwrap();
    if std::env::var_os("BOOK_INCIDENT_DIRECTORY").is_none() {
        let generated = Command::new("python3")
            .arg(root.join("research/experiments/book-incident-replay.py"))
            .arg("--output-dir")
            .arg(scratch)
            .output()
            .unwrap();
        assert!(
            generated.status.success(),
            "{}",
            String::from_utf8_lossy(&generated.stderr)
        );
    }
    let windows: Vec<Value> =
        serde_json::from_slice(&std::fs::read(scratch.join("expectations.json")).unwrap()).unwrap();
    let expected_comparisons: usize = windows
        .iter()
        .map(|w| w["checkpoints"].as_array().unwrap().len())
        .sum();
    assert!(expected_comparisons > 0 && expected_comparisons < 1000);
    let mut reports = Vec::new();
    for workers in [1, 2, 4] {
        let mut actual_before = Vec::new();
        for window in &windows {
            let connection = window["connection"].as_str().unwrap();
            let events = fixture(
                &std::fs::read_to_string(scratch.join(window["fixture"].as_str().unwrap()))
                    .unwrap(),
            );
            let temp = tempfile::Builder::new()
                .prefix("book-incident-")
                .tempdir_in(scratch)
                .unwrap();
            let config = Config {
                workers,
                output: temp.path().join("books.sbe"),
                decode_error_log: temp.path().join("decode-errors.jsonl"),
                writer_queue: 1 << 20,
                ..Config::default()
            };
            let periodic = window["counts"]["periodic_snapshots"]
                .as_u64()
                .unwrap_or_else(|| {
                    window["books"].as_u64().unwrap()
                        * window["checkpoints"].as_array().unwrap().len() as u64
                });
            let expected_records = window["books"].as_u64().unwrap()
                + periodic
                + window["counts"]["applied_level_updates"].as_u64().unwrap()
                + window["checkpoints"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| p["differences"].as_array().unwrap().len() as u64)
                    .sum::<u64>()
                + window["counts"]["catch_up_levels"].as_u64().unwrap_or(0);
            // Neither channel can fill with this entire bounded fixture.
            // Workers therefore never await the plain writer thread while
            // the paused runtime can advance their recurring snapshot timers.
            assert!(expected_records < config.writer_queue as u64);
            assert!(
                events.len() + (window["counts"]["book"].as_u64().unwrap() as usize)
                    < config.worker_queue
            );
            let totals = replay(
                &config,
                events,
                engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::default,
            )
            .await
            .unwrap();
            assert_eq!(sum(&totals, |w| w.periodic_snapshots), periodic);
            assert_eq!(totals.frames.decode_errors, 0);
            assert_eq!(
                totals.frames.frames,
                window["counts"]["frames"].as_u64().unwrap()
            );
            let (records, bytes) = read_records(&config.output);
            assert_eq!(bytes, totals.pipeline.writer.bytes);
            let checkpoints = window["checkpoints"].as_array().unwrap();
            let different = checkpoints
                .iter()
                .filter(|p| !p["differences"].as_array().unwrap().is_empty())
                .count();
            assert_eq!(sum(&totals, |w| w.mismatched_books), different as u64);
            assert_eq!(
                sum(&totals, |w| w.mismatched_levels),
                checkpoints
                    .iter()
                    .map(|p| p["differences"].as_array().unwrap().len() as u64)
                    .sum::<u64>()
            );
            assert_eq!(
                sum(&totals, |w| w.first_snapshots),
                window["books"].as_u64().unwrap()
            );
            assert_eq!(
                sum(&totals, |w| w.level_updates),
                window["counts"]["applied_level_updates"].as_u64().unwrap()
            );
            assert_eq!(
                sum(&totals, |w| w.dropped_unknown + w.dropped_other_connection),
                0
            );
            for checkpoint in checkpoints {
                let asset = checkpoint["asset"].as_str().unwrap();
                let checkpoint_connection = checkpoint["connection"].as_str().unwrap_or(connection);
                let sequence = &records[asset];
                let ordinal = checkpoint["ordinal"].as_u64().unwrap() as usize;
                let (index, snapshot) = sequence
                    .iter()
                    .enumerate()
                    .filter(|(_, record)| matches!(record, Record::Snapshot { .. }))
                    .nth(ordinal)
                    .expect("periodic snapshot before incoming refresh");
                let Record::Snapshot {
                    bids,
                    asks,
                    received_us,
                    exchange_us,
                } = snapshot
                else {
                    unreachable!()
                };
                let bids: Levels = bids.iter().copied().collect();
                let asks: Levels = asks.iter().copied().collect();
                assert_eq!(
                    bids,
                    expected_levels(&checkpoint["before"]["bids"]),
                    "N={workers} {connection} line={} bids",
                    checkpoint["line"]
                );
                assert_eq!(
                    asks,
                    expected_levels(&checkpoint["before"]["asks"]),
                    "N={workers} {connection} line={} asks",
                    checkpoint["line"]
                );
                assert_eq!(
                    *received_us,
                    checkpoint["before_received_us"].as_i64().unwrap()
                );
                assert_eq!(
                    *exchange_us,
                    checkpoint["before_exchange_us"].as_i64().unwrap()
                );
                for (offset, difference) in checkpoint["differences"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    assert_eq!(
                        sequence[index + 1 + offset],
                        Record::Update {
                            side: if difference["side"] == "bid" {
                                Side::Buy
                            } else {
                                Side::Sell
                            },
                            price: difference["price"].as_i64().unwrap(),
                            qty: difference["incoming"].as_i64().unwrap(),
                            received_us: checkpoint["incoming_received_us"].as_i64().unwrap(),
                            exchange_us: checkpoint["incoming_exchange_us"].as_i64().unwrap(),
                        }
                    );
                }
                actual_before.push(json!({
                    "connection": checkpoint_connection, "line": checkpoint["line"], "asset": asset,
                    "before": {"bids": bids.into_iter().collect::<Vec<_>>(), "asks": asks.into_iter().collect::<Vec<_>>()},
                    "received_us": received_us, "exchange_us": exchange_us,
                }));
            }
            reports.push(json!({
                "workers": workers, "connection": connection,
                "frames": totals.frames.frames, "books": window["books"],
                "complete_before_books_compared": checkpoints.len(),
                "different_from_incoming": different, "rust_decimal_disagreements": 0,
                "first_snapshots": sum(&totals, |w| w.first_snapshots),
                "periodic_snapshots": sum(&totals, |w| w.periodic_snapshots),
                "absolute_updates": sum(&totals, |w| w.level_updates),
                "dropped_before_baseline": sum(&totals, |w| w.dropped_unsynced),
                "sbe_records": totals.pipeline.writer.snapshots + totals.pipeline.writer.level_updates,
                "sbe_message_bytes": bytes,
                "sbe_padded_bytes": std::fs::metadata(&config.output).unwrap().len(),
            }));
            // temp drops here: no SBE files accumulate between runs.
        }
        assert_eq!(actual_before.len(), expected_comparisons);
        std::fs::write(
            scratch.join(format!("rust-before-n{workers}.json")),
            serde_json::to_vec_pretty(&actual_before).unwrap(),
        )
        .unwrap();
    }
    std::fs::write(
        scratch.join("rust-summary.json"),
        serde_json::to_vec_pretty(&reports).unwrap(),
    )
    .unwrap();
    if let Some(path) = std::env::var_os("BOOK_INCIDENT_LIVE_SBE") {
        audit_live_output(Path::new(&path), &windows, scratch);
    }
    println!(
        "Compared {} complete pre-refresh Rust books with independent Decimal states across N=1,2,4",
        expected_comparisons * 3
    );
}

fn audit_live_output(path: &Path, windows: &[Value], scratch: &Path) {
    let checkpoints: BTreeMap<_, _> = windows
        .iter()
        .flat_map(|window| window["checkpoints"].as_array().unwrap())
        .filter(|p| !p["differences"].as_array().unwrap().is_empty())
        .map(|p| {
            (
                (
                    p["asset"].as_str().unwrap().to_owned(),
                    p["incoming_received_us"].as_i64().unwrap(),
                    p["incoming_exchange_us"].as_i64().unwrap(),
                ),
                p,
            )
        })
        .collect();
    let assets: BTreeSet<_> = checkpoints.keys().map(|key| key.0.as_str()).collect();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut held: BTreeMap<String, (Levels, Levels)> = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut reports = Vec::new();
    let mut scanned = 0u64;
    while let Some(message) = reader.read_msg().unwrap() {
        scanned += 1;
        match message {
            AnyMessage::Snapshot(decoded) => {
                if !assets.contains(decoded.symbol_as_str().unwrap()) {
                    continue;
                }
                let message = SnapshotDomain::try_from_decoder(decoded).unwrap();
                held.insert(
                    message.symbol,
                    (
                        message
                            .bids
                            .into_iter()
                            .map(|l| (l.price_level, l.quantity))
                            .collect(),
                        message
                            .asks
                            .into_iter()
                            .map(|l| (l.price_level, l.quantity))
                            .collect(),
                    ),
                );
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                if !assets.contains(decoded.symbol_as_str().unwrap()) {
                    continue;
                }
                let message = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
                let book = held
                    .get_mut(&message.symbol)
                    .expect("live symbol has initial snapshot");
                let key = (
                    message.symbol.clone(),
                    message.backtest_timestamp.timestamp_micros(),
                    message.exchange_timestamp.timestamp_micros(),
                );
                if let Some(checkpoint) = checkpoints.get(&key)
                    && seen.insert(key.clone())
                {
                    assert_eq!(
                        book.0,
                        expected_levels(&checkpoint["before"]["bids"]),
                        "live SBE before correction differs from raw Decimal bids"
                    );
                    assert_eq!(
                        book.1,
                        expected_levels(&checkpoint["before"]["asks"]),
                        "live SBE before correction differs from raw Decimal asks"
                    );
                    reports.push(json!({
                        "asset": message.symbol, "received_us": key.1, "exchange_us": key.2,
                        "live_before_equals_raw_before": true,
                        "bids": book.0.iter().map(|(&p, &q)| (p, q)).collect::<Vec<_>>(),
                        "asks": book.1.iter().map(|(&p, &q)| (p, q)).collect::<Vec<_>>(),
                    }));
                }
                let side = match message.side {
                    Side::Buy => &mut book.0,
                    Side::Sell => &mut book.1,
                };
                if message.quantity == 0 {
                    side.remove(&message.price_level);
                } else {
                    side.insert(message.price_level, message.quantity);
                }
            }
            _ => panic!("unexpected live SBE message"),
        }
    }
    assert_eq!(
        seen.len(),
        checkpoints.len(),
        "all selected incoming corrections found in live SBE"
    );
    std::fs::write(
        scratch.join("live-sbe-before.json"),
        serde_json::to_vec_pretty(&json!({
            "resource_confounded": true,
            "scanned_records": scanned,
            "selected_before_comparisons": reports.len(),
            "disagreements": 0,
            "comparisons": reports,
        }))
        .unwrap(),
    )
    .unwrap();
}
