#![allow(dead_code)]
mod common;

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use common::{Record, Records, read_records};
use engineering_challenge::Side;
use engineering_challenge::core::config::Config;
use engineering_challenge::core::metrics::Metrics;
use engineering_challenge::core::pipeline::{Pipeline, PipelineTotals};
use engineering_challenge::model::{
    ConnectionId, LevelUpdate, MarketData, MarketSources, OrderBookEvent, Price, Qty, UnsyncReason,
    WorkerMessage,
};

struct Run {
    directory: tempfile::TempDir,
    pipeline: Pipeline,
    metrics: Metrics,
}

impl Run {
    async fn new() -> Self {
        Self::with_sources(MarketSources::mirrored(ConnectionId(0), ConnectionId(1)).unwrap()).await
    }

    async fn with_sources(sources: MarketSources) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            workers: 1,
            output: directory.path().join("books.sbe"),
            ..Config::default()
        };
        let metrics = Metrics::default();
        let pipeline = Pipeline::start_with_metrics(&config, &metrics).unwrap();
        let run = Self {
            directory,
            pipeline,
            metrics,
        };
        run.send(WorkerMessage::AddMarket {
            generation: 0,
            market: "m".into(),
            order_books: vec!["yes".into(), "no".into()],
            sources,
        })
        .await;
        run
    }

    async fn send(&self, message: WorkerMessage) {
        self.pipeline.workers().send(message).await.unwrap();
    }

    async fn event(&self, source: u16, time: i64, event: OrderBookEvent) {
        self.send(WorkerMessage::MarketData(MarketData {
            market: "m".into(),
            connection: ConnectionId(source),
            received_at: DateTime::<Utc>::from_timestamp_micros(time * 1000).unwrap(),
            exchange_ts: DateTime::<Utc>::from_timestamp_micros(time * 1000).unwrap(),
            received_instant: Some(Instant::now()),
            event,
        }))
        .await;
    }

    async fn book(&self, source: u16, symbol: &str, qty: i64, time: i64) {
        self.event(
            source,
            time,
            OrderBookEvent::Snapshot {
                timestamp_comparable: true,
                order_book: symbol.into(),
                bids: if qty == 0 {
                    vec![]
                } else {
                    vec![(Price(10), Qty(qty))]
                },
                asks: vec![],
            },
        )
        .await;
    }

    async fn update(&self, source: u16, symbol: &str, qty: i64, time: i64) {
        self.event(
            source,
            time,
            OrderBookEvent::LevelUpdates(vec![LevelUpdate {
                order_book: symbol.into(),
                side: Side::Buy,
                price: Price(10),
                change: engineering_challenge::model::QuantityChange::Set(Qty(qty)),
            }]),
        )
        .await;
    }

    async fn reset(&self, source: u16, reason: UnsyncReason) {
        self.send(WorkerMessage::MarkUnsynced {
            connection: ConnectionId(source),
            reason,
        })
        .await;
    }

    async fn coverage(&self, books: u64, markets: u64, standby: u64) {
        // Publication uses the real monotonic clock; timers use Tokio time.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        tokio::time::resume();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let expected = [
            ("collector_books{state=\"synced\"}", books),
            ("collector_markets{state=\"synced\"}", markets),
            ("collector_markets{state=\"standby_ready\"}", standby),
        ];
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let text = self.metrics.render().unwrap().text;
            let missing = expected
                .iter()
                .find(|(name, value)| !text.lines().any(|line| line == format!("{name} {value}")));
            if missing.is_none() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "coverage did not converge: {missing:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::pause();
    }

    async fn finish(self) -> (Records, PipelineTotals, String) {
        let totals = self.pipeline.finish().await.unwrap();
        let records = read_records(&self.directory.path().join("books.sbe")).0;
        let metrics = self.metrics.render().unwrap().text.to_string();
        (records, totals, metrics)
    }
}

fn snapshot(qty: i64, time: i64) -> Record {
    Record::Snapshot {
        bids: if qty == 0 { vec![] } else { vec![(10, qty)] },
        asks: vec![],
        received_us: time * 1000,
        exchange_us: time * 1000,
    }
}
fn update(qty: i64, time: i64) -> Record {
    Record::Update {
        side: Side::Buy,
        price: 10,
        qty,
        received_us: time * 1000,
        exchange_us: time * 1000,
    }
}

#[tokio::test(start_paused = true)]
async fn readiness_uses_one_complete_source_and_accepts_empty_baselines() {
    let run = Run::new().await;
    run.book(0, "yes", 10, 10).await;
    run.book(1, "no", 20, 20).await;
    run.coverage(1, 0, 0).await;
    run.update(0, "no", 999, 30).await; // No baseline on A's no outcome.
    run.update(1, "no", 21, 31).await; // Private until B is complete.
    run.book(1, "yes", 0, 40).await;
    run.coverage(2, 1, 0).await;
    run.book(0, "no", 100, 50).await;
    run.coverage(2, 1, 1).await;
    let (records, totals, _) = run.finish().await;
    assert_eq!(records["yes"], vec![snapshot(0, 40)]);
    assert_eq!(records["no"], vec![snapshot(21, 31)]);
    let worker = &totals.workers[0];
    assert_eq!(worker.dropped_unsynced, 1);
    assert_eq!(worker.first_snapshots, 2);
    assert_eq!(worker.publisher_switches, [0; 5]);
    assert_eq!(worker.books_held, 2);
    assert_eq!(worker.books_synced, 2);
    assert_eq!(totals.writer.receive_to_encode.count, 0);
}

#[tokio::test(start_paused = true)]
async fn failover_replaces_both_books_keeps_standby_private_and_does_not_fail_back() {
    let run = Run::new().await;
    run.book(0, "yes", 1, 10).await;
    run.book(0, "no", 2, 11).await;
    run.book(1, "yes", 7, 5).await; // Lower timestamps on B are eligible.
    run.book(1, "no", 8, 6).await;
    run.update(1, "yes", 9, 7).await;
    run.book(1, "yes", 10, 8).await; // Same-source standby mismatch.
    run.update(0, "yes", 3, 12).await;
    run.book(0, "yes", 4, 13).await; // Publisher correction, no latency sample.
    run.coverage(2, 1, 1).await;
    run.reset(0, UnsyncReason::RemoteClose).await;
    run.update(1, "yes", 11, 9).await;
    run.book(0, "yes", 100, 1).await; // Fresh session accepts older baseline.
    run.book(0, "no", 200, 2).await;
    run.update(0, "yes", 101, 3).await;
    run.update(1, "no", 12, 10).await;
    run.coverage(2, 1, 1).await;
    let (records, totals, text) = run.finish().await;
    assert_eq!(
        records["yes"],
        vec![
            snapshot(1, 10),
            update(3, 12),
            update(4, 13),
            snapshot(10, 8),
            update(11, 9)
        ]
    );
    assert_eq!(
        records["no"],
        vec![snapshot(2, 11), snapshot(8, 6), update(12, 10)]
    );
    assert_eq!(totals.workers[0].mismatched_books, 2);
    assert_eq!(totals.workers[0].publisher_switches, [1, 0, 0, 0, 0]);
    assert_eq!(totals.workers[0].catch_up_levels, 0);
    assert_eq!(totals.writer.receive_to_encode.count, 3);
    assert!(text.contains("collector_publisher_switches_total{reason=\"remote_close\"} 1"));
}

#[tokio::test(start_paused = true)]
async fn failure_before_complete_baseline_cannot_publish_stale_outcome() {
    let run = Run::new().await;
    run.book(0, "yes", 1, 10).await;
    run.reset(0, UnsyncReason::IoError).await;
    run.book(0, "no", 2, 11).await;
    run.book(1, "yes", 3, 12).await;
    run.coverage(1, 0, 0).await;
    run.book(1, "no", 4, 13).await;
    let (records, totals, _) = run.finish().await;
    assert_eq!(records["yes"], vec![snapshot(3, 12)]);
    assert_eq!(records["no"], vec![snapshot(4, 13)]);
    assert_eq!(totals.workers[0].publisher_switches, [0; 5]);
    assert_eq!(totals.workers[0].mismatched_books, 0);
}

#[tokio::test(start_paused = true)]
async fn both_down_same_source_recovery_is_a_snapshot_without_a_switch() {
    let run = Run::new().await;
    run.book(0, "yes", 1, 100).await;
    run.book(0, "no", 2, 101).await;
    run.reset(0, UnsyncReason::Stalled).await;
    run.reset(1, UnsyncReason::DecodeError).await;
    run.update(0, "yes", 999, 102).await;
    run.book(0, "yes", 5, 1).await;
    run.book(1, "no", 7, 2).await;
    run.coverage(1, 0, 0).await;
    run.book(0, "no", 6, 3).await;
    run.update(0, "yes", 8, 4).await;
    let (records, totals, _) = run.finish().await;
    assert_eq!(
        records["yes"],
        vec![snapshot(1, 100), snapshot(5, 1), update(8, 4)]
    );
    assert_eq!(records["no"], vec![snapshot(2, 101), snapshot(6, 3)]);
    assert_eq!(totals.workers[0].publisher_switches, [0; 5]);
    assert_eq!(totals.workers[0].mismatched_books, 0);
    assert_eq!(totals.writer.receive_to_encode.count, 1);
}

#[tokio::test(start_paused = true)]
async fn delayed_peer_readiness_retains_each_original_publisher_failure_reason() {
    for (index, reason) in [
        UnsyncReason::RemoteClose,
        UnsyncReason::IoError,
        UnsyncReason::Stalled,
        UnsyncReason::DecodeError,
        UnsyncReason::SequenceGap,
    ]
    .into_iter()
    .enumerate()
    {
        let run = Run::new().await;
        run.book(0, "yes", 1, 10).await;
        run.book(0, "no", 2, 11).await;
        run.book(1, "yes", 3, 12).await;
        run.reset(0, reason).await;
        run.reset(1, UnsyncReason::IoError).await;
        run.reset(0, UnsyncReason::DecodeError).await; // Must not replace original cause.
        run.book(1, "yes", 4, 1).await;
        run.book(1, "no", 5, 2).await;
        let (records, totals, text) = run.finish().await;
        let mut expected = [0; 5];
        expected[index] = 1;
        assert_eq!(totals.workers[0].publisher_switches, expected);
        for (reason_index, reason_name) in [
            "remote_close",
            "io_error",
            "silence",
            "decode_error",
            "sequence_gap",
        ]
        .into_iter()
        .enumerate()
        {
            assert!(text.contains(&format!(
                "collector_publisher_switches_total{{reason=\"{reason_name}\"}} {}",
                u64::from(reason_index == index)
            )));
        }
        assert_eq!(records["yes"], vec![snapshot(1, 10), snapshot(4, 1)]);
        assert_eq!(records["no"], vec![snapshot(2, 11), snapshot(5, 2)]);
    }
}

#[tokio::test(start_paused = true)]
async fn periodic_cadence_survives_switch_and_removal_keeps_last_publisher_stamps() {
    let run = Run::new().await;
    run.book(0, "yes", 1, 10).await;
    run.book(0, "no", 2, 11).await;
    run.coverage(2, 1, 0).await; // Timer started at t=0; now t=1.
    tokio::time::advance(Duration::from_secs(29)).await;
    run.book(1, "yes", 3, 20).await;
    run.book(1, "no", 4, 21).await;
    run.reset(0, UnsyncReason::IoError).await;
    run.book(1, "yes", 3, 22).await; // No diff still updates publication stamps.
    run.coverage(2, 1, 0).await; // t=31.
    tokio::time::advance(Duration::from_secs(29)).await;
    run.coverage(2, 1, 0).await; // Original deadline at t=60, not t=90.
    run.reset(1, UnsyncReason::Stalled).await;
    run.book(0, "yes", 50, 90).await; // Private partial recovery.
    run.coverage(1, 0, 0).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    run.coverage(1, 0, 0).await;
    run.send(WorkerMessage::RemoveMarket("m".into())).await;
    let (records, totals, _) = run.finish().await;
    assert_eq!(
        records["yes"],
        vec![
            snapshot(1, 10),
            snapshot(3, 20),
            snapshot(3, 22),
            snapshot(0, 22)
        ]
    );
    assert_eq!(
        records["no"],
        vec![
            snapshot(2, 11),
            snapshot(4, 21),
            snapshot(4, 21),
            snapshot(0, 21)
        ]
    );
    assert_eq!(totals.workers[0].periodic_snapshots, 2);
    assert_eq!(totals.workers[0].periodic_skipped, 2);
    assert_eq!(totals.workers[0].removal_snapshots, 2);
    assert_eq!(totals.workers[0].books_synced, 0);
    assert_eq!(totals.writer.receive_to_encode.count, 0);
}

#[tokio::test(start_paused = true)]
async fn full_book_filter_uses_each_sources_greatest_timestamp_and_resets_per_session() {
    let run = Run::new().await;
    run.book(0, "yes", 1, 100).await;
    run.book(0, "no", 2, 100).await;
    run.update(0, "yes", 3, 120).await;
    run.update(0, "yes", 4, 110).await; // Last stamp can regress, maximum cannot.
    run.book(0, "yes", 999, 119).await;
    run.book(0, "yes", 5, 120).await; // Equal accepted.
    run.book(1, "yes", 6, 1).await; // Independent source, much older than A.
    run.update(1, "yes", 7, 5).await;
    run.book(1, "yes", 999, 4).await;
    run.book(1, "no", 8, 2).await;
    run.reset(0, UnsyncReason::IoError).await;
    run.book(0, "yes", 9, 1).await;
    run.book(0, "no", 10, 1).await;
    run.reset(1, UnsyncReason::DecodeError).await;
    let (records, totals, text) = run.finish().await;
    assert_eq!(
        records["yes"],
        vec![
            snapshot(1, 100),
            update(3, 120),
            update(4, 110),
            update(5, 120),
            snapshot(7, 5),
            snapshot(9, 1)
        ]
    );
    assert_eq!(
        records["no"],
        vec![snapshot(2, 100), snapshot(8, 2), snapshot(10, 1)]
    );
    assert_eq!(totals.workers[0].older_full_books, 2);
    assert_eq!(totals.workers[0].mismatched_books, 1);
    assert!(text.contains("collector_older_full_books_skipped_total 2"));
    assert!(text.contains("collector_older_full_book_lag_seconds_count 2"));
}

#[tokio::test(start_paused = true)]
async fn standby_reset_leaves_publisher_coverage_and_output_unchanged() {
    let run = Run::new().await;
    run.book(0, "yes", 1, 10).await;
    run.book(0, "no", 2, 11).await;
    run.book(1, "yes", 3, 12).await;
    run.book(1, "no", 4, 13).await;
    run.coverage(2, 1, 1).await;
    run.reset(1, UnsyncReason::DecodeError).await;
    run.update(1, "yes", 99, 14).await;
    run.update(0, "yes", 5, 15).await;
    run.coverage(2, 1, 0).await;
    let (records, totals, _) = run.finish().await;
    assert_eq!(records["yes"], vec![snapshot(1, 10), update(5, 15)]);
    assert_eq!(records["no"], vec![snapshot(2, 11)]);
    assert_eq!(totals.workers[0].publisher_switches, [0; 5]);
    assert_eq!(totals.workers[0].dropped_unsynced, 1);
    assert_eq!(totals.writer.receive_to_encode.count, 1);
}

#[tokio::test(start_paused = true)]
async fn single_source_keeps_per_book_initialization_recovery_diffs_and_timestamps() {
    let run = Run::with_sources(MarketSources::single(ConnectionId(0))).await;
    run.book(0, "yes", 1, 10).await;
    run.coverage(1, 0, 0).await;
    run.reset(0, UnsyncReason::Disconnected).await;
    run.book(0, "yes", 2, 1).await; // Existing fixture recovery emits a diff.
    run.book(0, "yes", 2, 2).await; // No diff still advances removal metadata.
    run.book(0, "no", 3, 3).await;
    run.coverage(2, 1, 0).await;
    run.send(WorkerMessage::RemoveMarket("m".into())).await;
    let (records, totals, _) = run.finish().await;
    assert_eq!(
        records["yes"],
        vec![snapshot(1, 10), update(2, 1), snapshot(0, 2)]
    );
    assert_eq!(records["no"], vec![snapshot(3, 3), snapshot(0, 3)]);
    assert_eq!(totals.workers[0].first_snapshots, 2);
    assert_eq!(totals.workers[0].catch_up_levels, 1);
    assert_eq!(totals.workers[0].mismatched_books, 0);
    assert_eq!(totals.workers[0].books_unsynced, 1);
    assert_eq!(totals.writer.receive_to_encode.count, 0);
}

#[tokio::test]
async fn distributed_replicas_fail_over_per_market_and_keep_recovered_sources_private() {
    use engineering_challenge::core::routing::Workers;
    use engineering_challenge::core::subscriptions::{SpawnConnection, SubscriptionManager};
    use engineering_challenge::model::MarketBooks;
    use std::sync::{Arc, Mutex};

    async fn baseline(workers: &Workers, market: &MarketBooks, source: u16, qty: i64, time: i64) {
        for book in &market.order_books {
            workers
                .send(WorkerMessage::MarketData(MarketData {
                    market: market.market.clone(),
                    connection: ConnectionId(source),
                    received_at: DateTime::from_timestamp_millis(time).unwrap(),
                    exchange_ts: DateTime::from_timestamp_millis(time).unwrap(),
                    received_instant: None,
                    event: OrderBookEvent::Snapshot {
                        timestamp_comparable: true,
                        order_book: book.clone(),
                        bids: vec![(Price(10), Qty(qty))],
                        asks: vec![],
                    },
                }))
                .await
                .unwrap();
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        workers: 2,
        output: directory.path().join("books.sbe"),
        ..Config::default()
    };
    let pipeline = Pipeline::start(&config).unwrap();
    let workers = pipeline.workers().clone();
    let receivers = Arc::new(Mutex::new(Vec::new()));
    let keep = receivers.clone();
    let spawn: SpawnConnection = Box::new(move |id| {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        keep.lock().unwrap().push((id, receiver));
        sender
    });
    let mut manager = SubscriptionManager::new(workers.clone(), 2, spawn, None);
    let markets: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|id| MarketBooks {
            generation: 0,
            market: format!("0x{id}").as_str().into(),
            order_books: [
                format!("{id}-yes").as_str().into(),
                format!("{id}-no").as_str().into(),
            ],
        })
        .collect();
    manager.add(markets.clone()).await.unwrap();
    assert_eq!(manager.connections(), 4);
    // A and B share primary 0, but use replica 1 and replica 3 respectively.
    for (index, market) in markets.iter().enumerate() {
        baseline(
            &workers,
            market,
            [0, 0, 2][index],
            10 + index as i64,
            10 + index as i64,
        )
        .await;
        baseline(
            &workers,
            market,
            [1, 3, 1][index],
            20 + index as i64,
            20 + index as i64,
        )
        .await;
    }
    workers
        .send(WorkerMessage::MarkUnsynced {
            connection: ConnectionId(0),
            reason: UnsyncReason::IoError,
        })
        .await
        .unwrap();
    baseline(&workers, &markets[0], 0, 900, 30).await;
    baseline(&workers, &markets[1], 0, 901, 31).await;
    // Losing 1 affects A's publisher and C's standby, while B stays on 3.
    workers
        .send(WorkerMessage::MarkUnsynced {
            connection: ConnectionId(1),
            reason: UnsyncReason::RemoteClose,
        })
        .await
        .unwrap();
    drop(manager);
    drop(workers);
    let totals = pipeline.finish().await.unwrap();
    let (records, _) = read_records(&config.output);
    for book in &markets[0].order_books {
        assert_eq!(
            records[book.to_string().as_str()],
            vec![snapshot(10, 10), snapshot(20, 20), snapshot(900, 30)]
        );
    }
    for book in &markets[1].order_books {
        assert_eq!(
            records[book.to_string().as_str()],
            vec![snapshot(11, 11), snapshot(21, 21)]
        );
    }
    for book in &markets[2].order_books {
        assert_eq!(records[book.to_string().as_str()], vec![snapshot(12, 12)]);
    }
    assert_eq!(
        totals
            .workers
            .iter()
            .map(|w| w.publisher_switches[0])
            .sum::<u64>(),
        1
    );
    assert_eq!(
        totals
            .workers
            .iter()
            .map(|w| w.publisher_switches[1])
            .sum::<u64>(),
        2
    );
    assert_eq!(
        totals
            .workers
            .iter()
            .map(|w| w.removal_snapshots)
            .sum::<u64>(),
        0
    );
}

#[tokio::test(start_paused = true)]
async fn relative_updates_use_independent_source_images_and_gap_promotes_ready_peer() {
    use engineering_challenge::model::QuantityChange;
    let run = Run::new().await;
    run.book(0, "yes", 10, 10).await;
    run.book(0, "no", 20, 10).await;
    run.book(1, "yes", 100, 8).await;
    run.book(1, "no", 200, 8).await;
    for (source, symbol, delta, time) in [(0, "yes", 2, 11), (1, "yes", -3, 9), (1, "no", 5, 9)] {
        run.event(
            source,
            time,
            OrderBookEvent::LevelUpdates(vec![LevelUpdate {
                order_book: symbol.into(),
                side: Side::Buy,
                price: Price(10),
                change: QuantityChange::Add(delta),
            }]),
        )
        .await;
    }
    run.reset(0, UnsyncReason::SequenceGap).await;
    for (source, delta) in [(1, 4), (0, 999)] {
        run.event(
            source,
            12,
            OrderBookEvent::LevelUpdates(vec![LevelUpdate {
                order_book: "yes".into(),
                side: Side::Buy,
                price: Price(10),
                change: QuantityChange::Add(delta),
            }]),
        )
        .await;
    }
    let (records, totals, text) = run.finish().await;
    assert_eq!(
        records["yes"],
        vec![
            snapshot(10, 10),
            update(12, 11),
            snapshot(97, 9),
            update(101, 12)
        ]
    );
    assert_eq!(records["no"], vec![snapshot(20, 10), snapshot(205, 9)]);
    assert_eq!(totals.workers[0].dropped_unsynced, 1);
    assert_eq!(totals.workers[0].publisher_switches, [0, 0, 0, 0, 1]);
    assert!(text.contains("collector_publisher_switches_total{reason=\"sequence_gap\"} 1"));
}

#[tokio::test(start_paused = true)]
async fn readmission_requires_current_source_attachment_and_fresh_books() {
    let run = Run::new().await;
    run.send(WorkerMessage::RemoveMarket("m".into())).await;
    run.send(WorkerMessage::AddMarket {
        generation: 2,
        market: "m".into(),
        order_books: vec!["yes".into(), "no".into()],
        sources: MarketSources::mirrored(ConnectionId(0), ConnectionId(1)).unwrap(),
    })
    .await;
    run.book(0, "yes", 5, 10).await;
    run.book(0, "no", 5, 10).await;
    run.coverage(0, 0, 0).await;
    run.send(WorkerMessage::SourceAttached {
        market: "m".into(),
        generation: 1,
        connection: ConnectionId(0),
    })
    .await;
    run.book(0, "yes", 5, 11).await;
    run.book(0, "no", 5, 11).await;
    run.coverage(0, 0, 0).await;
    run.send(WorkerMessage::SourceAttached {
        market: "m".into(),
        generation: 2,
        connection: ConnectionId(0),
    })
    .await;
    run.update(0, "yes", 7, 12).await;
    run.coverage(0, 0, 0).await;
    run.book(0, "yes", 7, 13).await;
    run.book(0, "no", 7, 13).await;
    run.coverage(2, 1, 0).await;
    run.send(WorkerMessage::SourceAttached {
        market: "m".into(),
        generation: 2,
        connection: ConnectionId(0),
    })
    .await;
    run.coverage(2, 1, 0).await;
    let _ = run.finish().await;
}
