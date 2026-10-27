# Observed Polymarket market events

This is a live observation, captured on October 23, 2026, from 17:40:48 to
17:41:17 UTC. The command requested a 30-second capture of both outcome assets
for market `5906195`, "LoL: Gen.G vs Invictus Gaming (BO1) - Worlds Group Stage."
Its outcome labels are `Gen.G` and `Invictus Gaming`.

The [capture summary and complete examples](polymarket-observed-events.json)
contain the subscription, original initial message text, and the first decoded
example of each observed event type. Example objects retain all received fields
and levels. The full capture remains available locally at
`/tmp/polymarket-events.jsonl` until that file is replaced or removed.

## Messages received

One connection received 7,230 application messages. One JSON message contained
an array of two events, so the event count differs from the message count:

- `book`: 520 full outcome-book snapshots.
- `price_change`: 5,881 messages containing price-level updates.
- `best_bid_ask`: 568 messages containing the best prices and spread.
- `last_trade_price`: 259 trade notifications.
- `PONG`: 3 plain-text heartbeat replies.

The first message was a JSON array containing two `book` objects, one per
subscribed asset. The remaining JSON messages were individual objects.
Later full snapshots arrived without another subscription request.

This capture does not establish fixed delivery rates or ordering across event
types. The [upstream specification](https://docs.polymarket.com/asyncapi.json)
also defines `tick_size_change`, `new_market`, and `market_resolved`; none
appeared in this capture. Custom features were enabled for the subscription.

## How the collector uses the fields

The raw `market` field contains the condition ID. `asset_id` selects an outcome
book. Prices and quantities are decimal strings. The server's `timestamp`
is expressed in Unix milliseconds.

- `book` supplies `bids`, `asks`, `hash`, `timestamp`, `tick_size`, and
  `last_trade_price` in this capture. Each bid or ask has a `price` and `size`.
  Replace both sides of that asset's local book, including removing old levels
  absent from the new snapshot.
- `price_change` supplies a `price_changes` array. The first observed message
  contains changes for both outcome assets. Each change has `asset_id`,
  `price`, `size`, `side`, `hash`, `best_bid`, and `best_ask`. `BUY` selects
  bids and `SELL` selects asks. Set the quantity to `size`; zero removes
  the level.
- `best_bid_ask` supplies `best_bid`, `best_ask`, and `spread` for one asset.
  These fields alone cannot maintain deeper levels.
- `last_trade_price` supplies the executed `price`, `size`, `side`,
  `fee_rate_bps`, and `transaction_hash` for one asset. The collector does not
  subtract this trade size from the book separately, because book messages
  already describe book changes.
- `PONG` is a heartbeat response, not a JSON market event.

The event payloads and `received_at` are distinct: `received_at` is the local
time when the capture command handles a message. The wrapper's `connection`
counts connection sessions, not exchange event sequence numbers.

## Repeat the capture

```sh
cargo run --quiet --bin stream-events -- --seconds 30 > /tmp/polymarket-events.jsonl
jq '.raw | fromjson?' /tmp/polymarket-events.jsonl
```

The command discovers one eligible market by default, so a later run may
select a different market. Use `--market 3` to select the third eligible
market. The command uses the same WebSocket transport and heartbeat logic as
`watch-books` and retries interrupted connections. Press Ctrl+C to stop early.

To inspect the saved examples without connecting again:

```sh
jq '.first_example_of_each_type.price_change' docs/polymarket-observed-events.json
jq '.first_example_of_each_type.book' docs/polymarket-observed-events.json
jq -r '.first_message_raw' docs/polymarket-observed-events.json | jq .
```
