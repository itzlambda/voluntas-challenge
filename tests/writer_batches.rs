use engineering_challenge::core::metrics::{Component, Metrics};
use std::time::Duration;

use chrono::DateTime;
use engineering_challenge::Side;
use engineering_challenge::core::worker::Worker;
use engineering_challenge::core::writer::spawn_writer;
use engineering_challenge::model::{
    ConnectionId, LevelUpdate, MarketData, MarketId, MarketSources, OrderBookEvent, OrderBookId,
    OutputRecord, Price, Qty, WorkerMessage,
};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use tokio::sync::mpsc;

fn inputs() -> Vec<WorkerMessage> {
    let market = MarketId::from("market");
    let book = OrderBookId::from("11");
    let data = |event| {
        WorkerMessage::MarketData(MarketData {
            received_instant: Some(std::time::Instant::now()),
            market: market.clone(),
            connection: ConnectionId(7),
            received_at: DateTime::UNIX_EPOCH,
            exchange_ts: DateTime::UNIX_EPOCH,
            event,
        })
    };
    vec![
        WorkerMessage::AddMarket {
            generation: 0,
            market: market.clone(),
            order_books: vec![book.clone()],
            sources: MarketSources::single(ConnectionId(7)),
        },
        data(OrderBookEvent::Snapshot {
            timestamp_comparable: true,
            order_book: book.clone(),
            bids: vec![],
            asks: vec![],
        }),
        data(OrderBookEvent::LevelUpdates(
            [10, 20, 0]
                .into_iter()
                .map(|qty| LevelUpdate {
                    order_book: book.clone(),
                    side: Side::Buy,
                    price: Price(500_000),
                    change: engineering_challenge::model::QuantityChange::Set(Qty(qty)),
                })
                .collect(),
        )),
    ]
}

#[tokio::test(start_paused = true)]
async fn output_is_immediate_and_keeps_each_inputs_record_order() {
    let (input, messages) = mpsc::channel(4);
    let (writer, mut batches) = mpsc::channel(1);
    let task = tokio::spawn(Worker::new(0).run(
        messages,
        writer,
        Duration::from_secs(1),
        Metrics::default().register(Component::Worker(0)),
    ));
    let began = tokio::time::Instant::now();
    let mut messages = inputs().into_iter();
    input.send(messages.next().unwrap()).await.unwrap();
    input.send(messages.next().unwrap()).await.unwrap();
    // Leave the input open and send nothing else until the snapshot arrives.
    let first = tokio::time::timeout(Duration::from_secs(1), batches.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first.as_slice(), [OutputRecord::Snapshot { .. }]));
    assert_eq!(began.elapsed(), Duration::ZERO);

    input.send(messages.next().unwrap()).await.unwrap();
    let updates = tokio::time::timeout(Duration::from_secs(1), batches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(began.elapsed(), Duration::ZERO);
    let quantities: Vec<_> = updates
        .into_iter()
        .map(|record| match record {
            OutputRecord::LevelUpdate { qty, .. } => qty.0,
            _ => panic!("expected a level update"),
        })
        .collect();
    assert_eq!(quantities, [10, 20, 0]);
    drop(input);
    let totals = task.await.unwrap().unwrap();
    assert_eq!(totals.level_updates, 3);
    assert!(batches.recv().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn a_full_writer_queue_still_fails_after_the_stall_timeout() {
    let (input, messages) = mpsc::channel(4);
    for message in inputs() {
        input.send(message).await.unwrap();
    }
    drop(input);
    let (writer, mut batches) = mpsc::channel(1);
    let timeout = Duration::from_millis(123);
    let began = tokio::time::Instant::now();
    let error = Worker::new(0)
        .run(
            messages,
            writer,
            timeout,
            Metrics::default().register(Component::Worker(0)),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("writer channel full"));
    assert_eq!(began.elapsed(), timeout);
    assert!(matches!(
        batches.recv().await.unwrap().as_slice(),
        [OutputRecord::Snapshot { .. }]
    ));
    assert!(batches.recv().await.is_none());
}

#[tokio::test]
async fn writer_closure_is_fatal_only_when_there_is_output() {
    for count in [1, 2] {
        let (input, messages) = mpsc::channel(2);
        for message in inputs().into_iter().take(count) {
            input.send(message).await.unwrap();
        }
        drop(input);
        let (writer, batches) = mpsc::channel(1);
        drop(batches);
        let result = Worker::new(0)
            .run(
                messages,
                writer,
                Duration::from_secs(1),
                Metrics::default().register(Component::Worker(0)),
            )
            .await;
        if count == 1 {
            assert!(result.is_ok(), "an empty output must not be sent");
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("writer channel closed")
            );
        }
    }
}

#[tokio::test]
async fn writer_preserves_all_fields_across_symbols_and_batches() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("updates.sbe");
    let (sender, records) = mpsc::channel(1);
    let metrics = Metrics::default();
    let done = spawn_writer(path.clone(), records, metrics.register(Component::Writer)).unwrap();
    let expected = [
        (
            "70000000000000000000000000000000000000000000000000000000000000000000000000000",
            Side::Buy,
            100_001,
            900,
            1_000_001,
            900_001,
        ),
        ("1", Side::Sell, 200_002, 800, 1_000_002, 900_002),
        ("123456789012", Side::Buy, 300_003, 0, 1_000_003, 900_003),
        ("2", Side::Sell, 400_004, 700, 1_000_004, 900_004),
    ];
    for (index, batch) in expected.chunks(2).enumerate() {
        if index == 1 {
            sender
                .send(vec![OutputRecord::Snapshot {
                    order_book: OrderBookId::from("77"),
                    bids: vec![(Price(500_000), Qty(10))],
                    asks: vec![],
                    received_at: DateTime::from_timestamp_micros(2_000_001).unwrap(),
                    exchange_ts: DateTime::from_timestamp_micros(1_900_001).unwrap(),
                }])
                .await
                .unwrap();
        }
        sender
            .send(
                batch
                    .iter()
                    .map(|&(symbol, side, price, qty, received, exchange)| {
                        OutputRecord::LevelUpdate {
                            received_instant: Some(std::time::Instant::now()),
                            order_book: OrderBookId::from(symbol),
                            side,
                            price: Price(price),
                            qty: Qty(qty),
                            received_at: DateTime::from_timestamp_micros(received).unwrap(),
                            exchange_ts: DateTime::from_timestamp_micros(exchange).unwrap(),
                        }
                    })
                    .collect(),
            )
            .await
            .unwrap();
    }
    drop(sender);
    let totals = done.await.unwrap().unwrap();
    assert_eq!(totals.level_updates, expected.len() as u64);
    assert_eq!(totals.snapshots, 1);
    let busy = (totals.encode_time + totals.final_flush_time).as_secs_f64();
    assert!(busy > 0.);
    let text = metrics.render().unwrap().text;
    let published: f64 = text
        .lines()
        .find_map(|line| line.strip_prefix("collector_writer_busy_seconds_total "))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(published, busy);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut updates = 0;
    let mut snapshots = 0;
    let mut bytes = 0;
    while let Some(record) = reader.read_msg().unwrap() {
        match record {
            AnyMessage::PriceLevelUpdate(decoder) => {
                let message = PriceLevelUpdateDomain::try_from_decoder(decoder).unwrap();
                bytes += message.encoded_length_with_header().unwrap() as u64;
                let (symbol, side, price, qty, received, exchange) = expected[updates];
                assert_eq!(message.symbol, symbol);
                assert_eq!(message.side, side);
                assert_eq!(message.price_level, price);
                assert_eq!(message.quantity, qty);
                assert_eq!(message.backtest_timestamp.timestamp_micros(), received);
                assert_eq!(message.exchange_timestamp.timestamp_micros(), exchange);
                assert_eq!(snapshots, usize::from(updates >= 2));
                updates += 1;
            }
            AnyMessage::Snapshot(decoder) => {
                assert_eq!(updates, 2);
                let message = SnapshotDomain::try_from_decoder(decoder).unwrap();
                bytes += message.encoded_length_with_header().unwrap() as u64;
                assert_eq!(message.symbol, "77");
                assert_eq!(message.received_at.timestamp_micros(), 2_000_001);
                assert_eq!(message.exchange_timestamp.timestamp_micros(), 1_900_001);
                assert_eq!(message.bids.len(), 1);
                assert_eq!(message.bids[0].price_level, 500_000);
                assert_eq!(message.bids[0].quantity, 10);
                assert!(message.asks.is_empty());
                snapshots += 1;
            }
            _ => panic!("unexpected SBE message type"),
        }
    }
    assert_eq!(updates, expected.len());
    assert_eq!(snapshots, 1);
    assert_eq!(bytes, totals.bytes);
}
