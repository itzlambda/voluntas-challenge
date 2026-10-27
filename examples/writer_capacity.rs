//! Standalone writer capacity check. SBE only on stdout; measurements on stderr.
use engineering_challenge::core::metrics::{Component, Metrics};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, ensure};
use chrono::Utc;
use engineering_challenge::Side;
use engineering_challenge::core::writer::spawn_writer;
use engineering_challenge::model::{OrderBookId, OutputRecord, Price, Qty};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use serde_json::json;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args.next().unwrap_or_else(|| "/dev/stdout".into());
    if first == "--verify" {
        let path = args.next().context("missing output file")?;
        let count: usize = args
            .next()
            .context("missing expected record count")?
            .parse()?;
        ensure!(args.next().is_none(), "unexpected argument");
        return verify(&path, count);
    }
    let output = PathBuf::from(first);
    let count: usize = args
        .next()
        .unwrap_or_else(|| "2000000".into())
        .parse()
        .context("record count")?;
    ensure!(
        count > 0 && args.next().is_none(),
        "usage: writer_capacity [OUTPUT [RECORDS]]"
    );
    let (sender, input) = mpsc::channel(65536);
    let done = spawn_writer(
        output,
        input,
        Metrics::default().register(Component::Writer),
    )?;
    let asset = OrderBookId::from(
        "70000000000000000000000000000000000000000000000000000000000000000000000000000",
    );
    let began = Instant::now();
    let at = Utc::now();
    sender
        .send(vec![OutputRecord::Snapshot {
            order_book: asset.clone(),
            bids: Vec::new(),
            asks: Vec::new(),
            received_at: at,
            exchange_ts: at,
        }])
        .await?;
    for index in 0..count {
        let received_at = Utc::now();
        sender
            .send(vec![OutputRecord::LevelUpdate {
                received_instant: Some(std::time::Instant::now()),
                order_book: asset.clone(),
                side: Side::Buy,
                price: Price(500000),
                qty: Qty(index as i64 + 1),
                received_at,
                exchange_ts: received_at,
            }])
            .await?;
    }
    drop(sender);
    let totals = done.await??;
    let elapsed = began.elapsed();
    ensure!(
        totals.snapshots == 1 && totals.level_updates == count as u64,
        "writer count mismatch"
    );
    eprintln!(
        "WRITER {}",
        json!({"records":count+1,"encoded_bytes":totals.bytes,"elapsed_s":elapsed.as_secs_f64(),"MB_per_s":totals.bytes as f64/elapsed.as_secs_f64()/1_000_000.0,"encode_and_chunk_write_s":totals.encode_time.as_secs_f64(),"final_flush_s":totals.final_flush_time.as_secs_f64(),"writer_queue_peak":totals.queue_peak})
    );
    Ok(())
}

fn verify(path: &str, count: usize) -> anyhow::Result<()> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let first = reader.read_msg()?.context("missing first snapshot")?;
    let AnyMessage::Snapshot(decoder) = first else {
        anyhow::bail!("first record must be a snapshot");
    };
    let snapshot = SnapshotDomain::try_from_decoder(decoder)?;
    ensure!(
        snapshot.bids.is_empty() && snapshot.asks.is_empty(),
        "initial book must be empty"
    );
    let mut updates = 0usize;
    while let Some(record) = reader.read_msg()? {
        let AnyMessage::PriceLevelUpdate(decoder) = record else {
            anyhow::bail!("unexpected record");
        };
        let update = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
        updates += 1;
        ensure!(
            update.symbol == snapshot.symbol
                && update.side == Side::Buy
                && update.price_level == 500000
                && update.quantity == updates as i64,
            "output differs from counter oracle"
        );
    }
    ensure!(updates == count, "output update count differs from oracle");
    eprintln!("VERIFIED {{\"snapshots\":1,\"updates\":{updates}}}");
    Ok(())
}
