use fake_exchange::book::{Asset, Levels, SCALE, decimal, initial_assets, shard_for};
use std::collections::BTreeMap;

#[test]
fn python_sha1_vector() {
    let asset = Asset {
        asset: 2,
        market: 1,
        created_ms: 0,
        levels: Levels {
            bids: BTreeMap::from([(100_000, 2_500_000), (490_000, 3 * SCALE)]),
            asks: BTreeMap::from([(900_000, 4_125_000), (510_000, 7 * SCALE)]),
        },
    };
    assert_eq!(
        asset.hash(1_700_000_000_123).unwrap(),
        "0df56fbea236471f0f4ce762731044b425d22ada"
    );
    let frame = asset.book(1_700_000_000_123).unwrap();
    assert_eq!(frame["bids"][0]["price"], "0.1");
    assert_eq!(frame["asks"][0]["price"], "0.9");
    assert_eq!(frame["hash"], asset.hash(1_700_000_000_123).unwrap());
}

#[test]
fn initial_books_are_sorted_uncrossed_and_fixed_six() {
    let mut rng = fastrand::Rng::with_seed(19);
    for levels in 0..=49 {
        for asset in initial_assets(7, 123, levels, &mut rng) {
            assert_eq!(asset.levels.bids.len(), levels);
            assert_eq!(asset.levels.asks.len(), levels);
            if levels > 0 {
                assert!(
                    asset.levels.bids.last_key_value().unwrap().0
                        < asset.levels.asks.first_key_value().unwrap().0
                );
            }
            for (&p, &s) in asset.levels.bids.iter().chain(&asset.levels.asks) {
                assert!(p > 0 && p < SCALE && p % 10_000 == 0 && s > 0);
                assert!(decimal(s).split('.').nth(1).is_none_or(|v| v.len() <= 6));
            }
        }
    }
    let mut book = Levels::default();
    book.set(0, 100_000, 2);
    book.set(0, 100_000, 3);
    assert_eq!(book.bids.len(), 1);
    book.set(0, 100_000, 0);
    assert!(book.bids.is_empty());
    assert_eq!(decimal(123_456_789), "123.456789");
    assert_eq!(decimal(SCALE), "1");
    for id in 1..100 {
        assert!(shard_for(id, 4) < 4);
    }
}

#[test]
fn direct_bytes_match_original_json_builders() {
    use serde::Serialize;
    use serde_json::{Value, json};
    use sha1::{Digest, Sha1};
    #[derive(Serialize)]
    struct OriginalHashBook {
        market: String,
        asset_id: String,
        timestamp: String,
        hash: String,
        bids: Vec<Value>,
        asks: Vec<Value>,
        min_order_size: String,
        tick_size: String,
        neg_risk: bool,
        last_trade_price: String,
    }
    let original_decimal = |value: i64| {
        let mut text = format!("{}.{:06}", value / SCALE, value % SCALE);
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
        text
    };
    let mut rng = fastrand::Rng::with_seed(9182);
    let mut scratch = Vec::new();
    let mut bytes = Vec::new();
    for levels in [0, 1, 5, 15, 49] {
        for mut asset in initial_assets(rng.u64(1..1_000_000), 0, levels, &mut rng) {
            for size in asset
                .levels
                .bids
                .values_mut()
                .chain(asset.levels.asks.values_mut())
            {
                *size = rng.i64(1..=1_000_000_000);
            }
            let timestamp = rng.i64(1_700_000_000_000..1_800_000_000_000);
            let make = |(&p, &s)| json!({"price":original_decimal(p),"size":original_decimal(s)});
            let preimage = OriginalHashBook {
                market: fake_exchange::book::condition(asset.market),
                asset_id: asset.asset.to_string(),
                timestamp: timestamp.to_string(),
                hash: String::new(),
                bids: asset.levels.bids.iter().map(make).collect(),
                asks: asset.levels.asks.iter().rev().map(make).collect(),
                min_order_size: "5".into(),
                tick_size: "0.01".into(),
                neg_risk: false,
                last_trade_price: "0.500".into(),
            };
            let digest = Sha1::digest(serde_json::to_vec(&preimage).unwrap());
            let original_hash = digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            assert_eq!(asset.hash(timestamp).unwrap(), original_hash);
            let mut original = serde_json::to_value(preimage).unwrap();
            original["hash"] = original_hash.clone().into();
            original["event_type"] = "book".into();
            bytes.clear();
            asset.write_book(timestamp, &mut bytes, &mut scratch);
            assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), original);
            let entry = asset.entry(0, 490_000, 5_123_456, timestamp).unwrap();
            assert_eq!(
                entry,
                json!({"asset_id":asset.asset.to_string(),"side":"BUY","price":"0.49","size":"5.123456","hash":original_hash,
                "best_bid":asset.levels.bids.last_key_value().map_or_else(||"0".into(),|(p,_)|original_decimal(*p)),
                "best_ask":asset.levels.asks.first_key_value().map_or_else(||"1".into(),|(p,_)|original_decimal(*p))})
            );
        }
    }
}

#[test]
fn decimal_formatter_handles_fixed_six_boundaries() {
    for (value, expected) in [
        (0, "0"),
        (1, "0.000001"),
        (100, "0.0001"),
        (100_000, "0.1"),
        (1_000_001, "1.000001"),
        (-1, "-0.000001"),
        (i64::MAX, "9223372036854.775807"),
        (i64::MIN, "-9223372036854.775808"),
    ] {
        assert_eq!(decimal(value), expected);
    }
}
