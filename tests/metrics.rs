use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::core::config::Config;
use engineering_challenge::core::metrics::{
    Component, ConnectionKind, Distribution, EndReason, Histogram, Metrics,
};
use engineering_challenge::core::metrics_http::MetricsServer;
use engineering_challenge::core::pipeline::Pipeline;
use engineering_challenge::model::{
    ConnectionId, LevelUpdate, MarketData, MarketId, MarketSources, OrderBookEvent, OrderBookId,
    Price, Qty, UnsyncReason, WorkerMessage,
};
use tokio_util::sync::CancellationToken;

fn value(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("missing {name}: {text}"))
        .parse()
        .unwrap()
}
async fn wait_for(metrics: &Metrics, name: &str, expected: f64) -> String {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let text = metrics.render().unwrap().text;
        if value(&text, name) == expected {
            return text.to_string();
        }
        assert!(
            Instant::now() < deadline,
            "{name} did not become {expected}: {text}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
#[test]
fn exposition_is_cumulative_and_has_only_allowed_families() {
    let metrics = Metrics::default();
    let mut input = metrics.register(Component::Connection(ConnectionKind::Orderbook));
    input.state.expected = true;
    input.state.connected = true;
    for age in [0., 0.005, 0.005001, 60., 61.] {
        input.state.exchange.record(age);
    }
    input.state.negative = 2;
    input.publish();
    let mut discovery = metrics.register(Component::Discovery);
    discovery.state.last_scan = Some(100.);
    discovery.publish();
    let mut worker = metrics.register(Component::Worker(0));
    worker.state.older_full_books = 2;
    worker.state.older_full_book_lag.record(0.008);
    worker.state.older_full_book_lag.record(0.006);
    worker.state.busy_seconds = 8.;
    worker.publish();
    let mut writer = metrics.register(Component::Writer);
    writer.state.busy_seconds = 1.25;
    writer.publish();
    let text = metrics.render().unwrap().text;
    assert_eq!(value(&text, "collector_writer_busy_seconds_total"), 1.25);
    assert_eq!(value(&text, "collector_older_full_books_skipped_total"), 2.);
    assert_eq!(
        value(&text, "collector_older_full_book_lag_seconds_count"),
        2.
    );
    assert!((value(&text, "collector_older_full_book_lag_seconds_sum") - 0.014).abs() < 1e-12);
    assert_eq!(
        value(
            &text,
            "collector_exchange_to_receive_seconds_bucket{le=\"0\"}"
        ),
        1.
    );
    assert_eq!(
        value(
            &text,
            "collector_exchange_to_receive_seconds_bucket{le=\"0.005\"}"
        ),
        2.
    );
    assert_eq!(
        value(
            &text,
            "collector_exchange_to_receive_seconds_bucket{le=\"60\"}"
        ),
        4.
    );
    assert_eq!(
        value(
            &text,
            "collector_exchange_to_receive_seconds_bucket{le=\"+Inf\"}"
        ),
        5.
    );
    assert_eq!(
        value(&text, "collector_exchange_to_receive_seconds_count"),
        5.
    );
    assert_eq!(value(&text, "collector_exchange_age_negative_total"), 2.);
    let names: BTreeSet<_> = text
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .map(|line| line.split_whitespace().next().unwrap())
        .collect();
    let expected: BTreeSet<_> = [
        "exchange_to_receive_seconds",
        "exchange_age_negative_total",
        "receive_to_encode_seconds",
        "markets",
        "books",
        "events_total",
        "connections",
        "session_ends_total",
        "publisher_switches_total",
        "decode_errors_total",
        "book_mismatches_total",
        "older_full_books_skipped_total",
        "older_full_book_lag_seconds",
        "worker_busy_seconds_total",
        "writer_busy_seconds_total",
        "writer_blocked_seconds_total",
        "last_successful_scan_timestamp_seconds",
        "oldest_required_component_publication_timestamp_seconds",
    ]
    .into_iter()
    .map(|name| format!("collector_{name}"))
    .collect();
    assert_eq!(names.len(), 18);
    assert_eq!(names, expected.iter().map(String::as_str).collect());
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("collector_markets{"))
            .collect::<Vec<_>>(),
        [
            "collector_markets{state=\"desired\"} 0",
            "collector_markets{state=\"synced\"} 0",
            "collector_markets{state=\"standby_ready\"} 0",
        ]
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("collector_publisher_switches_total{"))
            .collect::<Vec<_>>(),
        [
            "collector_publisher_switches_total{reason=\"remote_close\"} 0",
            "collector_publisher_switches_total{reason=\"io_error\"} 0",
            "collector_publisher_switches_total{reason=\"silence\"} 0",
            "collector_publisher_switches_total{reason=\"decode_error\"} 0",
            "collector_publisher_switches_total{reason=\"sequence_gap\"} 0",
        ]
    );
    for line in text.lines().filter(|line| !line.starts_with('#')) {
        let Some((name, rest)) = line.split_once('{') else {
            continue;
        };
        let labels = rest.split_once('}').unwrap().0;
        let keys: BTreeSet<_> = labels
            .split(',')
            .map(|label| label.split_once('=').unwrap().0)
            .collect();
        let allowed: BTreeSet<_> = match name {
            "collector_markets" | "collector_books" => ["state"].into_iter().collect(),
            "collector_connections" => ["kind", "state"].into_iter().collect(),
            "collector_session_ends_total" => ["kind", "reason"].into_iter().collect(),
            "collector_publisher_switches_total" => ["reason"].into_iter().collect(),
            "collector_worker_busy_seconds_total" => ["worker"].into_iter().collect(),
            "collector_exchange_to_receive_seconds_bucket"
            | "collector_receive_to_encode_seconds_bucket"
            | "collector_older_full_book_lag_seconds_bucket" => ["le"].into_iter().collect(),
            _ => panic!("unexpected labeled metric: {line}"),
        };
        assert_eq!(keys, allowed, "unexpected labels: {line}");
    }
}
#[test]
fn session_totals_survive_retirement_without_counting_snapshots_twice() {
    let metrics = Metrics::default();
    let mut first = metrics.register(Component::Connection(ConnectionKind::Orderbook));
    first.state.expected = true;
    first.state.connected = true;
    first.end_session(EndReason::RemoteClose);
    first.publish();
    first.publish();
    drop(first);
    let mut second = metrics.register(Component::Connection(ConnectionKind::Orderbook));
    second.state.expected = true;
    second.state.connected = true;
    second.end_session(EndReason::RemoteClose);
    second.publish();
    for _ in 0..3 {
        let text = metrics.render().unwrap().text;
        assert_eq!(
            value(
                &text,
                "collector_session_ends_total{kind=\"orderbook\",reason=\"remote_close\"}"
            ),
            2.
        );
        assert_eq!(
            value(
                &text,
                "collector_connections{kind=\"orderbook\",state=\"expected\"}"
            ),
            1.
        );
    }
    let empty = Metrics::default().render().unwrap();
    assert!(!empty.fresh);
    assert_eq!(value(&empty.text, "collector_events_total"), 0.);
}
#[test]
fn worker_switch_totals_survive_retirement_and_coverage_does_not() {
    let metrics = Metrics::default();
    let mut first = metrics.register(Component::Worker(0));
    first.state.publisher_switches = [1, 2, 3, 4, 0];
    first.state.standby_ready_markets = 7;
    first.state.synced_markets = 8;
    first.state.synced_books = 16;
    first.publish();
    first.publish();
    let mut second = metrics.register(Component::Worker(1));
    second.state.publisher_switches = [10, 20, 30, 40, 0];
    second.state.standby_ready_markets = 2;
    second.state.synced_markets = 3;
    second.state.synced_books = 6;
    second.publish();
    let assert_totals = |expected: [u64; 5], standby: u64, synced: u64, books: u64| {
        for _ in 0..3 {
            let text = metrics.render().unwrap().text;
            for (reason, count) in [
                "remote_close",
                "io_error",
                "silence",
                "decode_error",
                "sequence_gap",
            ]
            .into_iter()
            .zip(expected)
            {
                assert_eq!(
                    value(
                        &text,
                        &format!("collector_publisher_switches_total{{reason=\"{reason}\"}}")
                    ),
                    count as f64
                );
            }
            assert_eq!(
                value(&text, "collector_markets{state=\"standby_ready\"}"),
                standby as f64
            );
            assert_eq!(
                value(&text, "collector_markets{state=\"synced\"}"),
                synced as f64
            );
            assert_eq!(
                value(&text, "collector_books{state=\"synced\"}"),
                books as f64
            );
            assert_eq!(
                text.lines()
                    .filter(|line| line.starts_with("collector_books{"))
                    .count(),
                2
            );
        }
    };
    assert_totals([11, 22, 33, 44, 0], 9, 11, 22);
    first.state.publisher_switches = [2, 3, 4, 5, 0];
    first.publish();
    first.publish();
    assert_totals([12, 23, 34, 45, 0], 9, 11, 22);
    first.state.publisher_switches = [3, 4, 5, 6, 0];
    drop(first);
    assert_totals([13, 24, 35, 46, 0], 2, 3, 6);
    second.state.publisher_switches = [11, 21, 31, 41, 0];
    second.publish();
    assert_totals([14, 25, 36, 47, 0], 2, 3, 6);
    drop(second);
    assert_totals([14, 25, 36, 47, 0], 0, 0, 0);
}
#[tokio::test]
async fn live_coverage_recovers_and_snapshot_corrections_do_not_enter_latency() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        workers: 1,
        output: directory.path().join("books.sbe"),
        ..Config::default()
    };
    let metrics = Metrics::default();
    let mut desired = metrics.register(Component::Discovery);
    desired.state.desired_markets = 1;
    desired.state.desired_books = 2;
    desired.publish();
    let pipeline = Pipeline::start_with_metrics(&config, &metrics).unwrap();
    let books = [OrderBookId::from("a"), OrderBookId::from("b")];
    let market = MarketId::from("1");
    let conn = ConnectionId(0);
    pipeline
        .workers()
        .send(WorkerMessage::AddMarket {
            generation: 0,
            market: market.clone(),
            order_books: books.to_vec(),
            sources: MarketSources::single(conn),
        })
        .await
        .unwrap();
    let data = |event| {
        WorkerMessage::MarketData(MarketData {
            market: market.clone(),
            connection: conn,
            received_at: DateTime::<Utc>::MAX_UTC,
            received_instant: Some(Instant::now() - Duration::from_millis(5)),
            exchange_ts: DateTime::UNIX_EPOCH,
            event,
        })
    };
    let book = |index: usize, qty: i64| OrderBookEvent::Snapshot {
        timestamp_comparable: true,
        order_book: books[index].clone(),
        bids: vec![(Price(1), Qty(qty))],
        asks: vec![],
    };
    pipeline.workers().send(data(book(0, 1))).await.unwrap();
    let text = wait_for(&metrics, "collector_books{state=\"synced\"}", 1.).await;
    assert_eq!(value(&text, "collector_markets{state=\"synced\"}"), 0.);
    pipeline.workers().send(data(book(1, 1))).await.unwrap();
    wait_for(&metrics, "collector_markets{state=\"synced\"}", 1.).await;
    pipeline
        .workers()
        .send(data(OrderBookEvent::LevelUpdates(vec![
            LevelUpdate {
                order_book: books[0].clone(),
                side: Side::Buy,
                price: Price(1),
                change: engineering_challenge::model::QuantityChange::Set(Qty(2)),
            },
            LevelUpdate {
                order_book: books[1].clone(),
                side: Side::Buy,
                price: Price(1),
                change: engineering_challenge::model::QuantityChange::Set(Qty(2)),
            },
        ])))
        .await
        .unwrap();
    pipeline.workers().send(data(book(0, 3))).await.unwrap();
    wait_for(&metrics, "collector_book_mismatches_total", 1.).await;
    let mut newer = data(book(0, 3));
    if let WorkerMessage::MarketData(message) = &mut newer {
        message.exchange_ts = DateTime::from_timestamp(1, 0).unwrap();
    }
    pipeline.workers().send(newer).await.unwrap();
    pipeline.workers().send(data(book(0, 99))).await.unwrap();
    let text = wait_for(&metrics, "collector_older_full_books_skipped_total", 1.).await;
    assert_eq!(
        value(&text, "collector_older_full_book_lag_seconds_count"),
        1.
    );
    assert_eq!(
        value(&text, "collector_older_full_book_lag_seconds_sum"),
        1.
    );
    assert_eq!(value(&text, "collector_book_mismatches_total"), 1.);

    pipeline
        .workers()
        .send(WorkerMessage::MarkUnsynced {
            connection: conn,
            reason: UnsyncReason::Disconnected,
        })
        .await
        .unwrap();
    wait_for(&metrics, "collector_books{state=\"synced\"}", 0.).await;
    pipeline.workers().send(data(book(0, 4))).await.unwrap();
    pipeline.workers().send(data(book(1, 4))).await.unwrap();
    wait_for(&metrics, "collector_markets{state=\"synced\"}", 1.).await;
    pipeline
        .workers()
        .send(WorkerMessage::RemoveMarket(market))
        .await
        .unwrap();
    wait_for(&metrics, "collector_markets{state=\"synced\"}", 0.).await;
    let totals = pipeline.finish().await.unwrap();
    assert_eq!(totals.writer.receive_to_encode.count, 2);
    assert!(totals.writer.receive_to_encode.sum < 10.);
    assert_eq!(
        value(
            &metrics.render().unwrap().text,
            "collector_receive_to_encode_seconds_count"
        ),
        2.
    );
}
#[tokio::test]
async fn http_readiness_routes_and_shutdown_work_without_a_reader() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let metrics = Metrics::default();
    let mut component = metrics.register(Component::Worker(0));
    let fatal = CancellationToken::new();
    let mut server = MetricsServer::start("127.0.0.1:0".parse().unwrap(), metrics, fatal.clone())
        .await
        .unwrap();
    assert!(
        MetricsServer::start(server.address, Metrics::default(), CancellationToken::new())
            .await
            .is_err()
    );
    let base = format!("http://{}", server.address);
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    component.publish();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .unwrap()
            .status()
            == 200
        {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        client
            .post(format!("{base}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        405
    );
    assert_eq!(
        client
            .get(format!("{base}/missing"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client
            .get(format!("{base}/metrics"))
            .body("no")
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    let mut slow = tokio::net::TcpStream::connect(server.address)
        .await
        .unwrap();
    use tokio::io::AsyncWriteExt;
    slow.write_all(b"GET /metrics HTTP/1.1\r\nHost: ")
        .await
        .unwrap();
    component.state.events = 7;
    component.publish();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .contains("collector_events_total 7")
    );
    server.begin_shutdown();
    assert_eq!(
        client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    tokio::time::timeout(Duration::from_secs(2), server.finish())
        .await
        .unwrap()
        .unwrap();
    assert!(!fatal.is_cancelled());
}
#[test]
fn histogram_overflow_is_bounded() {
    let mut histogram = Histogram::new(Distribution::Internal);
    histogram.record(90.);
    assert_eq!(histogram.count, 1);
    assert_eq!(histogram.buckets[histogram.bounds().len()], 1);
}
