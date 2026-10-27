use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha1::{Digest, Sha1};

pub const SCALE: i64 = 1_000_000;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |t| t.as_millis() as i64)
}

pub fn decimal(value: i64) -> String {
    let mut bytes = Vec::with_capacity(24);
    write_decimal(&mut bytes, value);
    String::from_utf8_lossy(&bytes).into_owned()
}

pub fn condition(id: u64) -> String {
    format!("0x{id:064x}")
}

pub fn shard_for(id: u64, shards: usize) -> usize {
    let hash = condition(id).bytes().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x100000001b3)
    });
    (hash % shards as u64) as usize
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Levels {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

impl Levels {
    pub fn set(&mut self, side: u8, price: i64, size: i64) {
        let levels = if side == 0 {
            &mut self.bids
        } else {
            &mut self.asks
        };
        if size == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, size);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Asset {
    pub asset: u64,
    pub market: u64,
    pub created_ms: i64,
    pub levels: Levels,
}

pub fn write_integer(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
}

pub fn write_decimal(out: &mut Vec<u8>, value: i64) {
    let magnitude = value.unsigned_abs();
    if value < 0 {
        out.push(b'-');
    }
    out.extend_from_slice(
        itoa::Buffer::new()
            .format(magnitude / SCALE as u64)
            .as_bytes(),
    );
    let mut fraction = magnitude % SCALE as u64;
    if fraction == 0 {
        return;
    }
    out.push(b'.');
    let mut divisor = 100_000;
    while divisor > 0 {
        out.push(b'0' + (fraction / divisor) as u8);
        fraction %= divisor;
        if fraction == 0 {
            break;
        }
        divisor /= 10;
    }
}

fn write_condition(out: &mut Vec<u8>, id: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.extend_from_slice(b"0x");
    let start = out.len();
    out.resize(start + 64, b'0');
    for i in 0..16 {
        out[start + 63 - i] = HEX[((id >> (i * 4)) & 15) as usize];
    }
}

fn write_level(out: &mut Vec<u8>, price: i64, size: i64) {
    out.extend_from_slice(br#"{"price":""#);
    write_decimal(out, price);
    out.extend_from_slice(br#"","size":""#);
    write_decimal(out, size);
    out.extend_from_slice(br#""}"#);
}

fn write_levels<'a>(out: &mut Vec<u8>, levels: impl Iterator<Item = (&'a i64, &'a i64)>) {
    out.push(b'[');
    for (i, (&price, &size)) in levels.enumerate() {
        if i > 0 {
            out.push(b',');
        }
        write_level(out, price, size);
    }
    out.push(b']');
}

fn write_header(out: &mut Vec<u8>, asset: &Asset, timestamp: i64) -> usize {
    out.clear();
    out.extend_from_slice(br#"{"market":""#);
    write_condition(out, asset.market);
    out.extend_from_slice(br#"","asset_id":""#);
    out.extend_from_slice(itoa::Buffer::new().format(asset.asset).as_bytes());
    out.extend_from_slice(br#"","timestamp":""#);
    write_integer(out, timestamp);
    out.extend_from_slice(br#"","hash":""#);
    let offset = out.len();
    out.extend_from_slice(br#"","bids":"#);
    offset
}

const HASH_TAIL: &[u8] =
    br#","min_order_size":"5","tick_size":"0.01","neg_risk":false,"last_trade_price":"0.500"}"#;

fn write_hex(out: &mut Vec<u8>, digest: impl IntoIterator<Item = u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 15) as usize]);
    }
}

fn write_book(out: &mut Vec<u8>, preimage: &[u8], hash_offset: usize) {
    out.extend_from_slice(br#"{"event_type":"book","#);
    out.extend_from_slice(&preimage[1..hash_offset]);
    write_hex(out, Sha1::digest(preimage));
    out.extend_from_slice(&preimage[hash_offset..]);
}

fn write_entry(out: &mut Vec<u8>, asset: &Asset, side: u8, price: i64, size: i64, preimage: &[u8]) {
    out.extend_from_slice(br#"{"asset_id":""#);
    out.extend_from_slice(itoa::Buffer::new().format(asset.asset).as_bytes());
    out.extend_from_slice(br#"","price":""#);
    write_decimal(out, price);
    out.extend_from_slice(br#"","size":""#);
    write_decimal(out, size);
    out.extend_from_slice(br#"","side":""#);
    out.extend_from_slice(if side == 0 { b"BUY" } else { b"SELL" });
    out.extend_from_slice(br#"","hash":""#);
    write_hex(out, Sha1::digest(preimage));
    out.extend_from_slice(br#"","best_bid":""#);
    write_decimal(
        out,
        asset.levels.bids.last_key_value().map_or(0, |(p, _)| *p),
    );
    out.extend_from_slice(br#"","best_ask":""#);
    write_decimal(
        out,
        asset
            .levels
            .asks
            .first_key_value()
            .map_or(SCALE, |(p, _)| *p),
    );
    out.extend_from_slice(br#""}"#);
}

impl Asset {
    fn preimage(&self, timestamp: i64, scratch: &mut Vec<u8>) -> usize {
        let offset = write_header(scratch, self, timestamp);
        write_levels(scratch, self.levels.bids.iter());
        scratch.extend_from_slice(br#","asks":"#);
        write_levels(scratch, self.levels.asks.iter().rev());
        scratch.extend_from_slice(HASH_TAIL);
        offset
    }
    pub fn hash(&self, timestamp: i64) -> anyhow::Result<String> {
        let mut scratch = Vec::with_capacity(8192);
        self.preimage(timestamp, &mut scratch);
        let mut hex = Vec::with_capacity(40);
        write_hex(&mut hex, Sha1::digest(&scratch));
        Ok(String::from_utf8(hex)?)
    }
    pub fn book(&self, timestamp: i64) -> anyhow::Result<Value> {
        let mut scratch = Vec::with_capacity(8192);
        let mut bytes = Vec::with_capacity(8192);
        self.write_book(timestamp, &mut bytes, &mut scratch);
        Ok(serde_json::from_slice(&bytes)?)
    }
    pub fn entry(&self, side: u8, price: i64, size: i64, timestamp: i64) -> anyhow::Result<Value> {
        let mut scratch = Vec::with_capacity(8192);
        let mut bytes = Vec::with_capacity(256);
        self.preimage(timestamp, &mut scratch);
        write_entry(&mut bytes, self, side, price, size, &scratch);
        Ok(serde_json::from_slice(&bytes)?)
    }
    pub fn write_book(&self, timestamp: i64, out: &mut Vec<u8>, scratch: &mut Vec<u8>) {
        let offset = self.preimage(timestamp, scratch);
        write_book(out, scratch, offset);
    }
}

struct EncodedSide {
    bytes: Vec<u8>,
    lengths: [u16; 100],
}

impl Default for EncodedSide {
    fn default() -> Self {
        Self {
            bytes: Vec::with_capacity(2048),
            lengths: [0; 100],
        }
    }
}

impl EncodedSide {
    fn set(&mut self, index: usize, price: i64, size: i64, scratch: &mut Vec<u8>) {
        let offset = self.lengths[..index]
            .iter()
            .map(|n| usize::from(*n))
            .sum::<usize>();
        let previous = usize::from(self.lengths[index]);
        scratch.clear();
        if size != 0 {
            write_level(scratch, price, size);
            scratch.push(b',');
        }
        self.bytes
            .splice(offset..offset + previous, scratch.iter().copied());
        self.lengths[index] = scratch.len() as u16;
    }
    fn write(&self, out: &mut Vec<u8>) {
        out.push(b'[');
        if !self.bytes.is_empty() {
            out.extend_from_slice(&self.bytes[..self.bytes.len() - 1]);
        }
        out.push(b']');
    }
}

#[derive(Default)]
pub(crate) struct WireBook {
    bids: EncodedSide,
    asks: EncodedSide,
    level: Vec<u8>,
}

impl WireBook {
    pub(crate) fn new(asset: &Asset) -> Self {
        let mut wire = Self {
            level: Vec::with_capacity(48),
            ..Self::default()
        };
        for (&price, &size) in &asset.levels.bids {
            wire.set(0, price, size);
        }
        for (&price, &size) in &asset.levels.asks {
            wire.set(1, price, size);
        }
        wire
    }
    pub(crate) fn set(&mut self, side: u8, price: i64, size: i64) {
        let (levels, index) = if side == 0 {
            (&mut self.bids, price / 10_000)
        } else {
            (&mut self.asks, (SCALE - price) / 10_000)
        };
        levels.set(index as usize, price, size, &mut self.level);
    }
    fn preimage(&self, asset: &Asset, timestamp: i64, scratch: &mut Vec<u8>) -> usize {
        let offset = write_header(scratch, asset, timestamp);
        self.bids.write(scratch);
        scratch.extend_from_slice(br#","asks":"#);
        self.asks.write(scratch);
        scratch.extend_from_slice(HASH_TAIL);
        offset
    }
    pub(crate) fn write_book(
        &self,
        asset: &Asset,
        timestamp: i64,
        out: &mut Vec<u8>,
        scratch: &mut Vec<u8>,
    ) {
        let offset = self.preimage(asset, timestamp, scratch);
        write_book(out, scratch, offset);
    }
    pub(crate) fn write_entry(
        &self,
        out: &mut Vec<u8>,
        asset: &Asset,
        level: (u8, i64, i64),
        timestamp: i64,
        scratch: &mut Vec<u8>,
    ) {
        let (side, price, size) = level;
        self.preimage(asset, timestamp, scratch);
        write_entry(out, asset, side, price, size, scratch);
    }
}

pub(crate) fn write_change_header(out: &mut Vec<u8>, market: u64, time: i64) {
    out.clear();
    out.extend_from_slice(br#"{"event_type":"price_change","market":""#);
    write_condition(out, market);
    out.extend_from_slice(br#"","timestamp":""#);
    write_integer(out, time);
    out.extend_from_slice(br#"","price_changes":["#);
}

pub fn initial_assets(id: u64, time: i64, levels: usize, rng: &mut fastrand::Rng) -> [Asset; 2] {
    std::array::from_fn(|outcome| {
        let mut book = Levels::default();
        for i in 0..levels {
            book.set(0, (49 - i as i64) * 10_000, rng.i64(1..=1000) * SCALE);
            book.set(1, (51 + i as i64) * 10_000, rng.i64(1..=1000) * SCALE);
        }
        Asset {
            asset: id * 2 + outcome as u64,
            market: id,
            created_ms: time,
            levels: book,
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::book::{WireBook, initial_assets};

    #[test]
    fn cached_levels_match_full_serialization_after_mutations() {
        let mut rng = fastrand::Rng::with_seed(42);
        let mut asset = initial_assets(1, 0, 15, &mut rng)[0].clone();
        let mut wire = WireBook::new(&asset);
        let (mut expected, mut actual, mut scratch) = (Vec::new(), Vec::new(), Vec::new());
        for timestamp in 1..=500 {
            let side = rng.u8(0..2);
            let tick = rng.i64(1..=49);
            let price = if side == 0 {
                tick * 10_000
            } else {
                (100 - tick) * 10_000
            };
            let size = if rng.bool() {
                0
            } else {
                rng.i64(1..=1_000_000_000)
            };
            asset.levels.set(side, price, size);
            wire.set(side, price, size);
            asset.preimage(timestamp, &mut expected);
            wire.preimage(&asset, timestamp, &mut actual);
            assert_eq!(actual, expected);
            actual.clear();
            wire.write_book(&asset, timestamp, &mut actual, &mut scratch);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&actual).unwrap(),
                asset.book(timestamp).unwrap()
            );
            actual.clear();
            wire.write_entry(
                &mut actual,
                &asset,
                (side, price, size),
                timestamp,
                &mut scratch,
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&actual).unwrap(),
                asset.entry(side, price, size, timestamp).unwrap()
            );
        }
    }
}
