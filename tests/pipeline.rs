mod common;

use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use engineering_challenge::Side;
use engineering_challenge::core::config::Config;
use engineering_challenge::core::fixture::FixtureEvent;
use engineering_challenge::core::pipeline::replay;
use serde_json::Value;

use crate::common::{Record, check, fixed6, fixture, read_records, sum};

#[tokio::test(start_paused = true)]
async fn arbitrary_worker_counts_preserve_books_recovery_and_lifecycle() {
    for text in [
        include_str!("fixtures/basic.jsonl"),
        include_str!("fixtures/reconnect.jsonl"),
        include_str!("fixtures/lifecycle.jsonl"),
    ] {
        common::check_with_workers(fixture(text), &[3, 6, 32]).await;
    }
}

#[tokio::test(start_paused = true)]
async fn books_updates_diffs_and_periodic_snapshots() {
    let runs = check(fixture(include_str!("fixtures/basic.jsonl"))).await;
    for run in runs {
        assert_eq!(sum(&run.totals, |w| w.first_snapshots), 4);
        assert_eq!(sum(&run.totals, |w| w.periodic_snapshots), 12);
        assert_eq!(sum(&run.totals, |w| w.level_updates), 4);
        assert_eq!(sum(&run.totals, |w| w.mismatched_books), 1);
        assert_eq!(sum(&run.totals, |w| w.mismatched_levels), 5);
        assert_eq!(run.totals.pipeline.writer.snapshots, 16);
        assert_eq!(run.totals.pipeline.writer.level_updates, 9);
        assert_eq!(run.totals.frames.unsubscribed, 1);
        assert_eq!(run.records["101"].len(), 12);
        assert!(run.decode_log.is_empty());
        let records = &run.records["101"];
        assert!(
            matches!(&records[4], Record::Snapshot { received_us, exchange_us, .. }
            if *received_us == DateTime::parse_from_rfc3339("2026-10-25T12:00:05.000123Z").unwrap().timestamp_micros()
            && *exchange_us == 1_500_000)
        );
        assert!(matches!(
            &records[5],
            Record::Update {
                side: Side::Buy,
                price: 100_000,
                qty: 0,
                ..
            }
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn reconnect_preserves_order_and_separates_catch_up_from_mismatches() {
    for run in check(fixture(include_str!("fixtures/reconnect.jsonl"))).await {
        assert_eq!(sum(&run.totals, |w| w.first_snapshots), 4);
        assert_eq!(sum(&run.totals, |w| w.books_unsynced), 3);
        assert_eq!(sum(&run.totals, |w| w.dropped_unsynced), 3);
        assert_eq!(sum(&run.totals, |w| w.catch_up_levels), 3);
        assert_eq!(sum(&run.totals, |w| w.mismatched_books), 0);
        assert_eq!(sum(&run.totals, |w| w.level_updates), 3);
        assert_eq!(sum(&run.totals, |w| w.unsync_marks), run.workers as u64);
        assert_eq!(run.totals.frames.unsubscribed, 2);
        assert_eq!(run.totals.pipeline.writer.snapshots, 8);
        assert!(matches!(
            &run.records["101"][1],
            Record::Update { qty: 3_000_000, .. }
        ));
        assert!(matches!(
            &run.records["101"][2],
            Record::Update { qty: 5_000_000, .. }
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn removal_readd_and_old_connection_frames_start_a_fresh_book() {
    for run in check(fixture(include_str!("fixtures/lifecycle.jsonl"))).await {
        assert_eq!(sum(&run.totals, |w| w.first_snapshots), 4);
        assert_eq!(sum(&run.totals, |w| w.periodic_snapshots), 2);
        assert_eq!(sum(&run.totals, |w| w.dropped_unsynced), 2);
        assert_eq!(sum(&run.totals, |w| w.books_unsynced), 0);
        assert_eq!(sum(&run.totals, |w| w.level_updates), 1);
        assert_eq!(run.totals.frames.unsubscribed, 4);
        assert_eq!(run.totals.pipeline.writer.snapshots, 8);
        for symbol in ["101", "102"] {
            assert!(matches!(
                &run.records[symbol][1],
                Record::Snapshot { bids, asks, received_us, exchange_us }
                    if bids.is_empty() && asks.is_empty()
                    && *received_us == DateTime::parse_from_rfc3339("2026-10-25T12:00:02.000123Z").unwrap().timestamp_micros()
                    && *exchange_us == 1_200_000
            ));
        }
        assert!(
            matches!(&run.records["101"][2], Record::Snapshot { bids, asks, .. }
            if bids == &[(200_000, 4_000_000)] && asks.is_empty())
        );
    }
}

#[tokio::test(start_paused = true)]
async fn removal_clears_unsynced_books_once_and_skips_unpublished_books() {
    let events = fixture(
        r#"
{"type":"add_market","market":"0xa1","order_books":["101","102"],"connection":1}
{"type":"frame","connection":1,"received_at":"2026-10-25T12:00:02Z","text":"{\"event_type\":\"book\",\"market\":\"0xa1\",\"asset_id\":\"101\",\"bids\":[{\"price\":\"0.4\",\"size\":\"1\"}],\"asks\":[],\"timestamp\":\"1200\"}"}
{"type":"mark_unsynced","connection":1,"reason":"disconnected"}
{"type":"remove_market","market":"0xa1"}
{"type":"remove_market","market":"0xa1"}
{"type":"advance","millis":60010}
"#,
    );
    for run in check(events).await {
        assert_eq!(run.records.len(), 1);
        let records = &run.records["101"];
        assert_eq!(records.len(), 2);
        assert!(matches!(
            &records[1], Record::Snapshot { bids, asks, .. }
                if bids.is_empty() && asks.is_empty()
        ));
        assert_eq!(run.totals.pipeline.writer.snapshots, 2);
        assert_eq!(sum(&run.totals, |w| w.periodic_snapshots), 0);
        assert_eq!(sum(&run.totals, |w| w.books_held), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn malformed_book_logs_the_frame_and_unsyncs_only_its_connection() {
    let events = fixture(include_str!("fixtures/decode-error.jsonl"));
    let (bad_frame, bad_received_at) = events
        .iter()
        .find_map(|event| match event {
            FixtureEvent::Frame {
                text, received_at, ..
            } if text.contains("0.1234567") => Some((text.clone(), *received_at)),
            _ => None,
        })
        .unwrap();
    for run in check(events).await {
        assert_eq!(run.totals.frames.decode_errors, 1);
        assert_eq!(sum(&run.totals, |w| w.books_unsynced), 3);
        assert_eq!(sum(&run.totals, |w| w.periodic_skipped), 3);
        assert_eq!(sum(&run.totals, |w| w.dropped_unsynced), 3);
        assert_eq!(sum(&run.totals, |w| w.catch_up_levels), 1);
        assert_eq!(sum(&run.totals, |w| w.mismatched_books), 0);
        assert_eq!(run.decode_log.len(), 1);
        let log = &run.decode_log[0];
        assert_eq!(log["connection"], 1);
        assert_eq!(log["frame"], bad_frame);
        assert_eq!(
            DateTime::parse_from_rfc3339(log["received_at"].as_str().unwrap()).unwrap(),
            bad_received_at
        );
        assert!(log["error"].as_str().unwrap().contains("fractional"));
        assert!(log["logged_at"].as_str().is_some());
        assert!(matches!(
            &run.records["101"][2],
            Record::Update { qty: 5_000_000, .. }
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn periodic_snapshots_skip_unsynced_books_and_keep_the_original_cadence() {
    for run in check(fixture(include_str!("fixtures/periodic-resync.jsonl"))).await {
        assert_eq!(sum(&run.totals, |w| w.periodic_skipped), 5);
        assert_eq!(sum(&run.totals, |w| w.periodic_snapshots), 3);
        assert_eq!(sum(&run.totals, |w| w.catch_up_levels), 2);
        assert_eq!(sum(&run.totals, |w| w.mismatched_levels), 0);
        assert_eq!(run.totals.pipeline.writer.snapshots, 5);
        assert_eq!(run.records["101"].len(), 4);
        assert_eq!(run.records["102"].len(), 3);
    }
}

#[tokio::test(start_paused = true)]
async fn malformed_price_change_is_atomic_across_outcomes() {
    for run in check(fixture(include_str!(
        "fixtures/atomic-price-change-error.jsonl"
    )))
    .await
    {
        assert_eq!(run.totals.frames.decode_errors, 1);
        assert_eq!(sum(&run.totals, |w| w.books_unsynced), 2);
        assert_eq!(sum(&run.totals, |w| w.level_updates), 0);
        assert_eq!(sum(&run.totals, |w| w.catch_up_levels), 0);
        assert_eq!(sum(&run.totals, |w| w.dropped_unsynced), 1);
        assert_eq!(run.totals.pipeline.writer.level_updates, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn research_capture_replays_real_books_and_all_recorded_event_types() {
    let capture: Value = serde_json::from_str(include_str!(
        "../research/docs/polymarket-observed-events.json"
    ))
    .unwrap();
    let market = capture["market"]["conditionId"]
        .as_str()
        .unwrap()
        .to_owned();
    let assets: Vec<String> = capture["outcomes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|outcome| outcome["asset_id"].as_str().unwrap().to_owned())
        .collect();
    let received_at = DateTime::<Utc>::from_timestamp_millis(1_792_777_248_100).unwrap();
    let mut events = vec![
        FixtureEvent::AddMarket {
            generation: 0,
            market,
            order_books: assets.clone(),
            connection: 7,
            standby: None,
        },
        FixtureEvent::Frame {
            connection: 7,
            received_at,
            text: capture["first_message_raw"].as_str().unwrap().to_owned(),
        },
    ];
    let examples = &capture["first_example_of_each_type"];
    for (index, kind) in ["price_change", "best_bid_ask", "last_trade_price", "PONG"]
        .into_iter()
        .enumerate()
    {
        let text = if kind == "PONG" {
            examples[kind].as_str().unwrap().to_owned()
        } else {
            examples[kind].to_string()
        };
        events.push(FixtureEvent::Frame {
            connection: 7,
            received_at: received_at + Duration::milliseconds(index as i64 + 1),
            text,
        });
    }
    events.push(FixtureEvent::Advance { millis: 120_010 });
    for run in check(events).await {
        assert_eq!(run.totals.frames.frames, 5);
        assert_eq!(run.totals.frames.events, 3);
        assert_eq!(run.totals.frames.ignored, 2);
        assert_eq!(run.totals.frames.pongs, 1);
        assert_eq!(run.totals.frames.decode_errors, 0);
        assert_eq!(sum(&run.totals, |w| w.first_snapshots), 2);
        assert_eq!(sum(&run.totals, |w| w.periodic_snapshots), 4);
        assert_eq!(sum(&run.totals, |w| w.level_updates), 2);
        assert_eq!(sum(&run.totals, |w| w.mismatched_books), 0);
        assert_eq!(run.totals.pipeline.writer.snapshots, 6);
        assert_eq!(run.totals.pipeline.writer.level_updates, 2);
        for asset in &assets {
            assert_eq!(run.records[asset].len(), 4);
        }
    }
}

#[tokio::test]
async fn bounded_queues_drain_and_flush_on_replay_completion() {
    let events = fixture(include_str!("fixtures/basic.jsonl"))
        .into_iter()
        .take(2)
        .collect::<Vec<_>>();
    // The reference requires a final advance; the run below ends on queued data.
    let mut reference_events = events.clone();
    reference_events.push(FixtureEvent::Advance { millis: 1 });
    let expected_runs = tokio::time::timeout(StdDuration::from_secs(15), check(reference_events))
        .await
        .expect("reference replay completes");
    for expected in expected_runs {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            workers: expected.workers,
            worker_queue: 1,
            writer_queue: 1,
            output: temp.path().join("books.sbe"),
            decode_error_log: temp.path().join("decode-errors.jsonl"),
            ..Config::default()
        };
        let totals = tokio::time::timeout(
            StdDuration::from_secs(5),
            replay(
                &config,
                events.clone(),
                engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::default,
            ),
        )
        .await
        .expect("worker inputs close and writer flushes")
        .unwrap();
        let (records, bytes) = read_records(&config.output);
        assert_eq!(records, expected.records);
        assert_eq!(totals.pipeline.writer.snapshots, 2);
        assert_eq!(totals.pipeline.writer.bytes, bytes);
    }
}

#[test]
fn independent_decimal_parser_has_known_answers() {
    for (text, expected) in [
        ("0.022", 22_000),
        ("52438.01", 52_438_010_000),
        ("0.000001", 1),
        ("1.1234560000", 1_123_456),
        ("9223372036854.775807", i64::MAX),
    ] {
        assert_eq!(fixed6(text), Ok(expected));
    }
    for text in [
        "0.1234567",
        "-1",
        "1e3",
        "1.",
        ".1",
        "1.2.3",
        "9223372036854.775808",
    ] {
        assert!(fixed6(text).is_err(), "{text}");
    }
}

#[tokio::test]
#[should_panic(expected = "snapshot deadline")]
async fn rejects_fixture_deadline_ties() {
    let mut events = fixture(include_str!("fixtures/basic.jsonl"));
    events.truncate(2);
    events.push(FixtureEvent::Advance { millis: 60_000 });
    common::check_with_workers(events, &[1]).await;
}

#[tokio::test(start_paused = true)]
async fn explicit_sessions_suppress_only_the_matching_fault_reset() {
    use engineering_challenge::model::UnsyncReason;
    let mut events = vec![];
    for event in fixture(include_str!("fixtures/atomic-price-change-error.jsonl")) {
        if let FixtureEvent::Frame { text, .. } = &event
            && text.contains("1400")
        {
            events.push(FixtureEvent::SessionStart { connection: 1 });
        }
        let initial = matches!(event, FixtureEvent::AddMarket { .. });
        let malformed =
            matches!(&event, FixtureEvent::Frame { text, .. } if text.contains("0.1234567"));
        events.push(event);
        if initial {
            events.push(FixtureEvent::SessionStart { connection: 1 });
        }
        if malformed {
            events.push(FixtureEvent::MarkUnsynced {
                connection: 1,
                reason: UnsyncReason::DecodeError,
            });
            // A failed reconnect attempt produces another real reset.
            events.push(FixtureEvent::MarkUnsynced {
                connection: 1,
                reason: UnsyncReason::Disconnected,
            });
        }
    }
    for run in check(events).await {
        assert_eq!(
            sum(&run.totals, |worker| worker.unsync_marks),
            2 * run.workers as u64
        );
    }
}

#[tokio::test(start_paused = true)]
async fn paired_replay_updates_membership_on_both_sources() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        workers: 1,
        output: directory.path().join("paired"),
        decode_error_log: directory.path().join("decode.jsonl"),
        ..Config::default()
    };
    let add = |connection, standby| FixtureEvent::AddMarket {
        generation: 0,
        market: "0xa1".into(),
        order_books: vec!["101".into(), "102".into()],
        connection,
        standby: Some(standby),
    };
    let frame = |connection| FixtureEvent::Frame {
        connection,
        received_at: DateTime::UNIX_EPOCH,
        text: serde_json::json!({
            "event_type": "book", "market": "0xa1", "asset_id": "101",
            "bids": [], "asks": [], "timestamp": "1200"
        })
        .to_string(),
    };
    let totals = replay(
        &config,
        vec![
            add(8, 9),
            frame(8),
            frame(9),
            add(10, 11),
            frame(8),
            frame(9),
            frame(10),
            frame(11),
            FixtureEvent::RemoveMarket {
                market: "0xa1".into(),
            },
            frame(10),
            frame(11),
        ],
        engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::default,
    )
    .await
    .unwrap();
    assert_eq!(totals.frames.events, 4);
    assert_eq!(totals.frames.unsubscribed, 4);
    assert_eq!(totals.frames.decode_errors, 0);
    assert_eq!(
        totals.pipeline.writer.snapshots, 0,
        "neither source had both outcomes"
    );
}

#[tokio::test(start_paused = true)]
async fn paired_replay_rejects_duplicate_ids_and_ambiguous_legacy_resets() {
    for (standby, reset) in [(7, false), (8, true)] {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            workers: 1,
            output: directory.path().join("paired"),
            decode_error_log: directory.path().join("decode.jsonl"),
            ..Config::default()
        };
        let mut events = vec![FixtureEvent::AddMarket {
            generation: 0,
            market: "0xa1".into(),
            order_books: vec!["101".into(), "102".into()],
            connection: 7,
            standby: Some(standby),
        }];
        if reset {
            events.push(FixtureEvent::MarkUnsynced {
                connection: standby,
                reason: engineering_challenge::model::UnsyncReason::Disconnected,
            });
        }
        let error = replay(
            &config,
            events,
            engineering_challenge::exchange::polymarket::protocol::PolymarketProtocol::default,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if reset { "ambiguous" } else { "distinct" }),
            "{error:#}"
        );
    }
}
