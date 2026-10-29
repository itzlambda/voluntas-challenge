//! The writer (SPEC section 5): one OS thread owns the output file. It
//! converts each `OutputRecord` into the supplied domain type and calls
//! `AlignedSbeWriter::write_msg`, which flushes whenever a chunk fills.

use std::path::PathBuf;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot};

use crate::Side;
use crate::core::config::OUTPUT_CHUNK_BYTES;
use crate::core::metrics::{Histogram, PUBLICATION_INTERVAL, Publisher};
use crate::model::OutputRecord;
use crate::util::AlignedSbeWriter;
use crate::util::dtos::SbeEncode;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct WriterTotals {
    pub snapshots: u64,
    pub level_updates: u64,
    /// Encoded message bytes, without chunk padding.
    pub bytes: u64,
    /// Level-update receive age at SBE encoding completion, including any
    /// chunk rotation inside write_msg. This is not fsync or file visibility.
    pub receive_to_encode: Histogram,
    pub encode_time: std::time::Duration,
    pub final_flush_time: std::time::Duration,
    /// Queued batches plus the batch being processed, sampled on receipt.
    pub queue_peak: usize,
}

/// Starts the writer thread. File writes block, so this is a plain OS thread
/// rather than a Tokio task. The result arrives on the returned receiver
/// after the channel closes and the final chunk is flushed.
pub fn spawn_writer(
    path: PathBuf,
    records: mpsc::Receiver<Vec<OutputRecord>>,
    metrics: Publisher,
) -> anyhow::Result<oneshot::Receiver<anyhow::Result<WriterTotals>>> {
    let writer = AlignedSbeWriter::create(&path, OUTPUT_CHUNK_BYTES)
        .with_context(|| format!("create output {}", path.display()))?;
    let (done_tx, done_rx) = oneshot::channel();
    let failure = metrics.failure_token();
    std::thread::Builder::new()
        .name("writer".into())
        .spawn(move || {
            let guard = failure.drop_guard();
            let result = (|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .context("create writer timer runtime")?;
                run(writer, records, metrics, runtime)
            })();
            if result.is_ok() {
                guard.disarm();
            }
            let _ = done_tx.send(result);
        })
        .context("spawn writer thread")?;
    Ok(done_rx)
}

fn run(
    mut writer: AlignedSbeWriter,
    mut records: mpsc::Receiver<Vec<OutputRecord>>,
    mut metrics: Publisher,
    runtime: tokio::runtime::Runtime,
) -> anyhow::Result<WriterTotals> {
    let mut totals = WriterTotals::default();
    // Keep the symbol's allocation across updates instead of allocating a String
    // per record. write_msg encodes synchronously into its own buffer, so this
    // writer-owned message can be reused as soon as the call returns.
    let mut reusable_update = reusable_update();
    loop {
        if metrics.is_due() {
            metrics.state.internal.clone_from(&totals.receive_to_encode);
            metrics.publish();
        }
        let batch = match runtime
            .block_on(async { tokio::time::timeout(PUBLICATION_INTERVAL, records.recv()).await })
        {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(_) => continue,
        };
        totals.queue_peak = totals.queue_peak.max(records.len() + 1);
        for record in batch {
            let received_instant = match &record {
                OutputRecord::LevelUpdate {
                    received_instant, ..
                } => *received_instant,
                // Periodic snapshots retain the most recent exchange event's
                // receive time; their age does not measure processing latency.
                OutputRecord::Snapshot { .. } => None,
            };
            let started = std::time::Instant::now();
            write_record(&mut writer, &mut reusable_update, record, &mut totals)?;
            totals.encode_time += started.elapsed();
            metrics.state.busy_seconds = totals.encode_time.as_secs_f64();
            if let Some(received_instant) = received_instant {
                totals
                    .receive_to_encode
                    .record(received_instant.elapsed().as_secs_f64());
            }
            if metrics.is_due() {
                metrics.state.internal.clone_from(&totals.receive_to_encode);
                metrics.publish();
            }
        }
    }
    let started = std::time::Instant::now();
    writer.flush().context("flush output")?;
    totals.final_flush_time = started.elapsed();
    metrics.state.busy_seconds = (totals.encode_time + totals.final_flush_time).as_secs_f64();
    metrics.state.internal.clone_from(&totals.receive_to_encode);
    metrics.publish();
    Ok(totals)
}

fn write_record(
    writer: &mut AlignedSbeWriter,
    reusable_update: &mut PriceLevelUpdateDomain,
    record: OutputRecord,
    totals: &mut WriterTotals,
) -> anyhow::Result<()> {
    let snapshot = matches!(record, OutputRecord::Snapshot { .. });
    let len = with_message(record, reusable_update, |message| {
        let len = message.encoded_length_with_header()?;
        writer.write_msg(message)?;
        Ok(len)
    })
    .context(if snapshot {
        "write snapshot"
    } else {
        "write price level update"
    })?;
    if snapshot {
        totals.snapshots += 1;
    } else {
        totals.level_updates += 1;
    }
    totals.bytes += len as u64;
    Ok(())
}

/// A level update whose fields, including the symbol allocation, are
/// overwritten by [`with_message`] for each record.
pub(crate) fn reusable_update() -> PriceLevelUpdateDomain {
    PriceLevelUpdateDomain {
        backtest_timestamp: chrono::DateTime::UNIX_EPOCH,
        exchange_timestamp: chrono::DateTime::UNIX_EPOCH,
        price_level: 0,
        quantity: 0,
        side: Side::Buy,
        symbol: String::new(),
    }
}

/// Converts `record` into the supplied SBE domain type and passes it to
/// `encode`. Both output modes use this, so they encode identically.
pub(crate) fn with_message<R>(
    record: OutputRecord,
    reusable_update: &mut PriceLevelUpdateDomain,
    encode: impl FnOnce(&dyn SbeEncode) -> anyhow::Result<R>,
) -> anyhow::Result<R> {
    match record {
        OutputRecord::LevelUpdate {
            order_book,
            side,
            price,
            qty,
            received_at,
            exchange_ts,
            ..
        } => {
            // Receive time by design; see OutputRecord and SPEC section 5.
            reusable_update.backtest_timestamp = received_at;
            reusable_update.exchange_timestamp = exchange_ts;
            reusable_update.price_level = price.0;
            reusable_update.quantity = qty.0;
            reusable_update.side = side;
            reusable_update.symbol.clear();
            reusable_update.symbol.push_str(&order_book.0);
            encode(reusable_update)
        }
        OutputRecord::Snapshot {
            order_book,
            bids,
            asks,
            received_at,
            exchange_ts,
        } => {
            let message = SnapshotDomain {
                received_at,
                exchange_timestamp: exchange_ts,
                bids: bids
                    .into_iter()
                    .map(|(price, qty)| SnapshotBidsEntryDomain {
                        price_level: price.0,
                        quantity: qty.0,
                    })
                    .collect(),
                asks: asks
                    .into_iter()
                    .map(|(price, qty)| SnapshotAsksEntryDomain {
                        price_level: price.0,
                        quantity: qty.0,
                    })
                    .collect(),
                symbol: order_book.0.to_string(),
            };
            encode(&message)
        }
    }
}
