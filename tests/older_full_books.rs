#[allow(dead_code)]
mod common;

use crate::common::{Record, check, sum};
use engineering_challenge::core::fixture::FixtureEvent;
use serde_json::{Value, json};

fn frame(event: Value, receive: u32) -> FixtureEvent {
    FixtureEvent::Frame {
        connection: 1,
        received_at: chrono::DateTime::from_timestamp(1_790_000_000 + i64::from(receive), 0)
            .unwrap(),
        text: event.to_string(),
    }
}
fn book(asset: &str, ts: u32, qty: u32) -> Value {
    json!({"event_type":"book","market":"m","asset_id":asset,"timestamp":ts.to_string(),
        "bids":[{"price":"0.4","size":qty.to_string()}],"asks":[]})
}
fn delta(ts: u32, qty: u32) -> Value {
    json!({"event_type":"price_change","market":"m","timestamp":ts.to_string(),
        "price_changes":[{"asset_id":"a","side":"BUY","price":"0.4","size":qty.to_string()}]})
}

#[tokio::test(start_paused = true)]
async fn full_book_filter_preserves_applied_max_metadata_and_reconnect_baseline() {
    let mut events = vec![FixtureEvent::AddMarket {
        generation: 0,
        market: "m".into(),
        order_books: vec!["a".into(), "b".into()],
        connection: 1,
        standby: None,
    }];
    for (i, event) in [
        book("a", 100, 1),
        book("b", 100, 1),
        delta(300, 3),
        delta(200, 2),      // Older absolute delta still applies.
        book("a", 250, 99), // Must compare against 300, not last source 200.
        book("b", 250, 4),  // Independent outcome maximum accepts this.
    ]
    .into_iter()
    .enumerate()
    {
        events.push(frame(event, i as u32));
    }
    events.push(FixtureEvent::Advance { millis: 60_001 });
    events.push(frame(book("a", 300, 5), 6)); // Equal accepted.
    events.push(frame(book("a", 400, 6), 7)); // Newer accepted.
    events.push(FixtureEvent::MarkUnsynced {
        connection: 1,
        reason: engineering_challenge::model::UnsyncReason::Disconnected,
    });
    events.push(frame(delta(900, 99), 8)); // Unsynced data cannot establish a maximum.
    events.push(frame(book("a", 50, 7), 9)); // Reconnect resets the comparison period.
    events.push(frame(book("a", 49, 99), 10));
    events.push(frame(book("a", 50, 8), 11));
    events.push(FixtureEvent::Advance { millis: 60_001 });
    for run in check(events).await {
        assert_eq!(sum(&run.totals, |w| w.older_full_books), 2);
        assert_eq!(sum(&run.totals, |w| w.mismatched_books), 4);
        assert_eq!(sum(&run.totals, |w| w.level_updates), 2);
        let a = &run.records["a"];
        // Rejection leaves both output timestamps and the original timer unchanged.
        assert!(
            matches!(&a[3], Record::Snapshot { bids, received_us, exchange_us, .. }
            if bids == &[(400_000, 2_000_000)] && *exchange_us == 200_000
                && *received_us == 1_790_000_003_000_000)
        );
        assert!(
            matches!(a.last().unwrap(), Record::Snapshot { bids, exchange_us, .. }
            if bids == &[(400_000, 8_000_000)] && *exchange_us == 50_000)
        );
    }
}

/// Private captures stay outside the repository. Convert them with the companion
/// script, then exercise decoding, routing, workers, writer and clean SBE reading.
#[tokio::test(start_paused = true)]
#[ignore = "requires OLDER_BOOK_FIXTURE containing a bounded raw capture fixture"]
async fn captured_depth_matches_independent_reference_and_clean_sbe() {
    let path = std::env::var("OLDER_BOOK_FIXTURE").unwrap();
    let events = common::fixture(&std::fs::read_to_string(path).unwrap());
    if let Ok(path) = std::env::var("OLDER_BOOK_LIVE_SBE") {
        common::audit_live_sbe(&events, std::path::Path::new(&path));
    }
    for run in check(events).await {
        assert_eq!(run.totals.frames.decode_errors, 0);
        println!(
            "workers={} frames={} skipped={} mismatches={} levels={}",
            run.workers,
            run.totals.frames.frames,
            sum(&run.totals, |w| w.older_full_books),
            sum(&run.totals, |w| w.mismatched_books),
            sum(&run.totals, |w| w.level_updates)
        );
        if let Ok(expected) = std::env::var("OLDER_BOOK_EXPECT_SKIPS") {
            assert_eq!(
                sum(&run.totals, |w| w.older_full_books),
                expected.parse::<u64>().unwrap()
            );
        }
        if let Ok(expected) = std::env::var("OLDER_BOOK_EXPECT_MISMATCHES") {
            assert_eq!(
                sum(&run.totals, |w| w.mismatched_books),
                expected.parse::<u64>().unwrap()
            );
        }
    }
}
