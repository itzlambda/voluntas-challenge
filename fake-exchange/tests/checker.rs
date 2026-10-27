use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use engineering_challenge::Side;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use fake_exchange::book::{Asset, Levels};
use fake_exchange::history::{Change, History};

const BASE: i64 = 1_700_000_000_000;
fn time(ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(ms).unwrap()
}
fn history(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::write(path.join("manifest.json"), r#"{"version":1,"shards":1}"#).unwrap();
    fs::write(path.join("complete.json"), "{}").unwrap();
    fs::write(
        path.join("lifecycle.jsonl"),
        format!("{{\"market\":1,\"time\":{BASE},\"event\":\"created\"}}\n"),
    )
    .unwrap();
    let mut history = History::create(path, 0).unwrap();
    history
        .asset(&Asset {
            asset: 2,
            market: 1,
            created_ms: BASE,
            levels: Levels {
                bids: BTreeMap::from([(490_000, 1_000_000)]),
                asks: BTreeMap::new(),
            },
        })
        .unwrap();
    for (offset, price, size) in [
        (1, 490_000, 2_000_000),
        (2, 490_000, 3_000_000),
        (3, 480_000, 4_000_000),
    ] {
        history
            .change(Change {
                asset: 2,
                time: BASE + offset,
                side: 0,
                price,
                size,
            })
            .unwrap();
    }
    history.flush().unwrap();
}
fn snapshot(writer: &mut AlignedSbeWriter, ms: i64, levels: &[(i64, i64)]) {
    writer
        .write_msg(&SnapshotDomain {
            symbol: "2".into(),
            exchange_timestamp: time(ms),
            received_at: time(ms + 1),
            bids: levels
                .iter()
                .map(|&(price_level, quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: Vec::new(),
        })
        .unwrap();
}
fn update(writer: &mut AlignedSbeWriter, ms: i64, price: i64, size: i64) {
    writer
        .write_msg(&PriceLevelUpdateDomain {
            symbol: "2".into(),
            exchange_timestamp: time(ms),
            backtest_timestamp: time(ms + 1),
            price_level: price,
            quantity: size,
            side: Side::Buy,
        })
        .unwrap();
}
fn run(write: impl FnOnce(&mut AlignedSbeWriter)) -> fake_exchange::check::Summary {
    run_with_resolution(false, write)
}
fn run_with_resolution(
    resolved: bool,
    write: impl FnOnce(&mut AlignedSbeWriter),
) -> fake_exchange::check::Summary {
    let dir = tempfile::tempdir().unwrap();
    let hist = dir.path().join("history");
    history(&hist);
    if resolved {
        let path = hist.join("lifecycle.jsonl");
        let mut lifecycle = fs::read_to_string(&path).unwrap();
        lifecycle.push_str(&format!(
            "{{\"market\":1,\"time\":{},\"event\":\"resolved\"}}\n",
            BASE + 10
        ));
        fs::write(path, lifecycle).unwrap();
    }
    let sbe = dir.path().join("books.sbe");
    let mut writer = AlignedSbeWriter::create(&sbe, 64 << 20).unwrap();
    write(&mut writer);
    writer.flush().unwrap();
    fake_exchange::check::check(&sbe, &hist, 5.0).unwrap()
}
#[test]
fn clean_stream_passes() {
    let summary = run(|w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        update(w, BASE + 1, 490_000, 2_000_000);
        update(w, BASE + 2, 490_000, 3_000_000);
        update(w, BASE + 3, 480_000, 4_000_000);
        snapshot(w, BASE + 3, &[(480_000, 4_000_000), (490_000, 3_000_000)]);
    });
    assert!(
        summary.passed(false),
        "{}",
        serde_json::to_string(&summary).unwrap()
    );
    assert_eq!(summary.records, 5);
}
#[test]
fn skipped_update_reports_gap() {
    let summary = run(|w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        update(w, BASE + 2, 490_000, 3_000_000);
        update(w, BASE + 3, 480_000, 4_000_000);
    });
    assert!(summary.gaps.count > 0);
    assert_eq!(summary.errors.count, 0);
    assert!(!summary.passed(false));
    assert!(summary.passed(true));
}
#[test]
fn impossible_snapshot_is_error() {
    let summary = run(|w| snapshot(w, BASE, &[(490_000, 99_000_000)]));
    assert!(summary.errors.count > 0);
    assert!(!summary.passed(true));
}
#[test]
fn wrong_exchange_timestamp_is_error() {
    let summary = run(|w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        update(w, BASE + 2, 490_000, 2_000_000);
    });
    assert_eq!(summary.errors.count, 1);
    assert!(
        summary.errors.samples[0]["error"]
            .as_str()
            .unwrap()
            .contains("timestamp")
    );
}
#[test]
fn first_update_is_error() {
    let summary = run(|w| update(w, BASE + 1, 490_000, 2_000_000));
    assert!(summary.errors.count > 0);
}
#[test]
fn later_snapshot_reports_all_skipped_changes() {
    let summary = run(|w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        snapshot(w, BASE + 3, &[(480_000, 4_000_000), (490_000, 3_000_000)]);
    });
    assert_eq!(summary.gaps.count, 3);
    assert_eq!(summary.errors.count, 0);
}

#[test]
fn catch_up_diff_uses_one_timestamp_group() {
    let summary = run(|w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        update(w, BASE + 3, 490_000, 3_000_000);
        update(w, BASE + 3, 480_000, 4_000_000);
    });
    assert_eq!(summary.errors.count, 0);
    assert_eq!(summary.gaps.count, 2);
}

#[test]
fn initial_snapshot_chooses_latest_matching_past_state() {
    let summary = run(|w| {
        snapshot(w, BASE + 2, &[(490_000, 3_000_000)]);
        update(w, BASE + 3, 480_000, 4_000_000);
    });
    assert!(summary.passed(false));
}

#[test]
fn resolved_market_accepts_empty_retirement_snapshot() {
    let summary = run_with_resolution(true, |w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        update(w, BASE + 1, 490_000, 2_000_000);
        update(w, BASE + 2, 490_000, 3_000_000);
        update(w, BASE + 3, 480_000, 4_000_000);
        // Retirement preserves the most recent book's exchange and receive times.
        snapshot(w, BASE + 3, &[]);
    });
    assert!(
        summary.passed(false),
        "{}",
        serde_json::to_string(&summary).unwrap()
    );
    assert_eq!(summary.retirements, 1);
}

#[test]
fn open_market_rejects_empty_retirement_snapshot() {
    let summary = run(|w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        snapshot(w, BASE, &[]);
    });
    assert!(!summary.passed(false));
}

#[test]
fn retirement_rejects_exchange_time_after_resolution() {
    let summary = run_with_resolution(true, |w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        snapshot(w, BASE + 11, &[]);
    });
    assert!(!summary.passed(false));
}

#[test]
fn resolved_market_rejects_updates_after_retirement() {
    let summary = run_with_resolution(true, |w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        snapshot(w, BASE, &[]);
        update(w, BASE + 1, 490_000, 2_000_000);
    });
    assert!(!summary.passed(false));
}

#[test]
fn retirement_allows_metadata_from_unchanged_repeat_book() {
    let summary = run_with_resolution(true, |w| {
        snapshot(w, BASE, &[(490_000, 1_000_000)]);
        snapshot(w, BASE + 4, &[]);
    });
    assert!(summary.passed(false));
    assert_eq!(summary.retirements, 1);
}
