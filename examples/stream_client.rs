//! Subscribes to a collector running with `--output-mode stream`, rebuilds
//! the requested books, and prints one JSON summary when the server closes
//! the connection or `SECONDS` elapse. A reference for stream clients: every
//! binary frame holds complete SBE messages back to back, and a book's updates
//! are ignored until its first snapshot.
//!
//! Usage: cargo run --release --example stream_client -- ws://127.0.0.1:9100 SECONDS BOOK...

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeSchema, OrderbookSchema};

#[derive(Default)]
struct Book {
    synced: bool,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    snapshots: u64,
    updates: u64,
    /// Updates that arrived before the first snapshot, as a new subscriber expects.
    skipped: u64,
    /// Snapshots that differed from the book rebuilt from earlier records.
    mismatches: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let (Some(url), Some(seconds)) = (args.next(), args.next()) else {
        bail!("usage: stream_client URL SECONDS BOOK...");
    };
    let seconds: u64 = seconds.parse().context("SECONDS")?;
    let books: Vec<String> = args.collect();
    let (mut socket, _) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("connect {url}"))?;
    let request = serde_json::json!({"op":"subscribe","books":books});
    socket.send(Message::text(request.to_string())).await?;

    let mut state: HashMap<String, Book> = HashMap::new();
    let mut frames = 0u64;
    let mut close = None;
    let deadline = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(deadline);
    loop {
        let message = tokio::select! {
            _ = &mut deadline => {
                let _ = socket.close(None).await;
                break;
            }
            message = socket.next() => message,
        };
        match message {
            Some(Ok(Message::Binary(frame))) => {
                frames += 1;
                apply(&frame, &mut state)?;
            }
            Some(Ok(Message::Close(frame))) => {
                close = frame.map(|frame| format!("{} {}", u16::from(frame.code), frame.reason));
                break;
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error).context("read"),
            None => break,
        }
    }
    let summary: BTreeMap<_, _> = state
        .iter()
        .map(|(symbol, book)| {
            (
                symbol.clone(),
                serde_json::json!({
                    "snapshots": book.snapshots,
                    "updates": book.updates,
                    "skipped_before_snapshot": book.skipped,
                    "snapshot_mismatches": book.mismatches,
                    "bids": book.bids.len(),
                    "asks": book.asks.len(),
                }),
            )
        })
        .collect();
    println!(
        "{}",
        serde_json::json!({"frames":frames,"close":close,"books":summary})
    );
    Ok(())
}

fn apply(frame: &[u8], state: &mut HashMap<String, Book>) -> anyhow::Result<()> {
    let mut offset = 0;
    while offset < frame.len() {
        let message = OrderbookSchema::try_decode(frame, offset)?;
        offset += OrderbookSchema::encoded_length(&message)?;
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoder)?;
                let book = state.entry(snapshot.symbol).or_default();
                let bids: BTreeMap<_, _> = snapshot
                    .bids
                    .iter()
                    .map(|level| (level.price_level, level.quantity))
                    .collect();
                let asks: BTreeMap<_, _> = snapshot
                    .asks
                    .iter()
                    .map(|level| (level.price_level, level.quantity))
                    .collect();
                // Replacement snapshots after a source switch may differ by
                // design; periodic ones should not.
                if book.synced && (book.bids != bids || book.asks != asks) {
                    book.mismatches += 1;
                }
                book.bids = bids;
                book.asks = asks;
                book.synced = true;
                book.snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                let book = state.entry(update.symbol).or_default();
                if !book.synced {
                    book.skipped += 1;
                    continue;
                }
                let side = match update.side {
                    Side::Buy => &mut book.bids,
                    Side::Sell => &mut book.asks,
                };
                if update.quantity == 0 {
                    side.remove(&update.price_level);
                } else {
                    side.insert(update.price_level, update.quantity);
                }
                book.updates += 1;
            }
            _ => bail!("unknown message"),
        }
    }
    Ok(())
}
