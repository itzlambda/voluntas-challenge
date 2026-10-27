use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};

use crate::book::Asset;

pub const MAGIC: &[u8; 8] = b"FXHIST01";

#[derive(Clone, Copy, Debug)]
pub struct Change {
    pub time: i64,
    pub asset: u64,
    pub side: u8,
    pub price: i64,
    pub size: i64,
}

pub struct History {
    changes: BufWriter<File>,
    assets: BufWriter<File>,
}

impl History {
    pub fn create(dir: &Path, shard: usize) -> anyhow::Result<Self> {
        let mut changes = BufWriter::new(File::create(dir.join(format!("shard-{shard}.bin")))?);
        changes.write_all(MAGIC)?;
        Ok(Self {
            changes,
            assets: BufWriter::new(File::create(dir.join(format!("assets-{shard}.jsonl")))?),
        })
    }
    pub fn asset(&mut self, asset: &Asset) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.assets, asset)?;
        self.assets.write_all(b"\n")?;
        Ok(())
    }
    pub fn change(&mut self, c: Change) -> anyhow::Result<()> {
        self.changes.write_all(&c.time.to_le_bytes())?;
        self.changes.write_all(&c.asset.to_le_bytes())?;
        self.changes.write_all(&[c.side, 0, 0, 0, 0, 0, 0, 0])?;
        self.changes.write_all(&c.price.to_le_bytes())?;
        self.changes.write_all(&c.size.to_le_bytes())?;
        Ok(())
    }
    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.changes.flush()?;
        self.assets.flush()?;
        Ok(())
    }
}

pub fn read_changes(
    path: &Path,
    mut visit: impl FnMut(Change) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut input = BufReader::new(File::open(path)?);
    let mut magic = [0; 8];
    input.read_exact(&mut magic)?;
    ensure!(&magic == MAGIC, "invalid history version");
    let mut bytes = [0; 40];
    loop {
        let first = input.read(&mut bytes[..1])?;
        if first == 0 {
            break;
        }
        input
            .read_exact(&mut bytes[1..])
            .context("truncated history change")?;
        let number = |start| -> [u8; 8] { std::array::from_fn(|i| bytes[start + i]) };
        ensure!(
            bytes[16] <= 1 && bytes[17..24] == [0; 7],
            "invalid change side/reserved bytes"
        );
        visit(Change {
            time: i64::from_le_bytes(number(0)),
            asset: u64::from_le_bytes(number(8)),
            side: bytes[16],
            price: i64::from_le_bytes(number(24)),
            size: i64::from_le_bytes(number(32)),
        })?;
    }
    Ok(())
}

pub fn read_assets(dir: &Path) -> anyhow::Result<Vec<Asset>> {
    let mut assets = Vec::new();
    let mut paths = fs::read_dir(dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    for path in paths {
        if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("assets-"))
            && path.extension().is_some_and(|n| n == "jsonl")
        {
            for line in BufReader::new(File::open(path)?).lines() {
                assets.push(serde_json::from_str(&line?)?);
            }
        }
    }
    ensure!(!assets.is_empty(), "no asset table in history");
    Ok(assets)
}

#[derive(Serialize, Deserialize)]
pub struct Lifecycle {
    pub market: u64,
    pub event: String,
    pub time: i64,
}
