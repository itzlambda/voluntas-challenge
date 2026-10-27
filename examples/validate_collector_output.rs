//! Structural validation for Polymarket output; no upstream completeness claim.
use std::collections::HashSet;

use anyhow::{Context, ensure};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

fn main() -> anyhow::Result<()> {
    let file = std::env::args().nth(1).context("SBE path required")?;
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&file)?;
    let mut books = HashSet::new();
    let (mut snapshots, mut updates, mut levels) = (0_u64, 0_u64, 0_u64);
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoder) => {
                let s = SnapshotDomain::try_from_decoder(decoder)?;
                ensure!(!s.symbol.is_empty(), "empty snapshot symbol");
                ensure!(
                    s.bids
                        .windows(2)
                        .all(|w| w[0].price_level > w[1].price_level),
                    "unordered/duplicate bids"
                );
                ensure!(
                    s.asks
                        .windows(2)
                        .all(|w| w[0].price_level < w[1].price_level),
                    "unordered/duplicate asks"
                );
                for b in &s.bids {
                    ensure!(
                        (0..=1_000_000).contains(&b.price_level) && b.quantity > 0,
                        "invalid bid"
                    );
                }
                for a in &s.asks {
                    ensure!(
                        (0..=1_000_000).contains(&a.price_level) && a.quantity > 0,
                        "invalid ask"
                    );
                }
                levels += (s.bids.len() + s.asks.len()) as u64;
                books.insert(s.symbol);
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                ensure!(books.contains(&u.symbol), "update before first baseline");
                ensure!(
                    (0..=1_000_000).contains(&u.price_level) && u.quantity >= 0,
                    "invalid update"
                );
                updates += 1;
            }
            _ => anyhow::bail!("unexpected SBE message"),
        }
    }
    println!(
        "{{\"status\":\"verified_structure\",\"books\":{},\"snapshots\":{},\"updates\":{},\"snapshot_levels\":{},\"scope\":\"complete supplied-reader decoding, baseline presence, sorted unique positive snapshot levels; no cross-source depth or update-completeness proof\"}}",
        books.len(),
        snapshots,
        updates,
        levels
    );
    Ok(())
}
