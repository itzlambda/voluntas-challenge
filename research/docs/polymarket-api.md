# Polymarket API reference for the collector

Last checked against official documentation: 2026-10-23.

See [market discovery examples](polymarket-market-discovery.md) for the four listing
methods, request and response shapes, and the proposed market retirement policy.

This is a project reference, not a complete copy of Polymarket's documentation.
It covers public prediction-market discovery and order-book collection. Trading,
wallet operations, reference-price feeds, and perpetual futures are outside its scope.
The linked official pages and specifications are authoritative when this summary differs.

## Official documentation

- [Documentation home](https://docs.polymarket.com/)
- [Documentation index](https://docs.polymarket.com/llms.txt)
- [Prediction API reference index](https://docs.polymarket.com/_llms/en/api-reference/predictions.txt)
- [Direct API integration and authentication](https://docs.polymarket.com/getting-started/api)
- [Market discovery](https://docs.polymarket.com/market-data/discover-markets)
- [Market details and status](https://docs.polymarket.com/market-data/market-details)
- [Market WebSocket reference](https://docs.polymarket.com/api-reference/wss/market)
- [Protocol V2 API migration](https://docs.polymarket.com/migrate/polymarket-v2/api-integrations)
- [Prediction changelog](https://docs.polymarket.com/changelog/predictions)

Machine-readable specifications:

- [Gamma REST OpenAPI](https://docs.polymarket.com/api-spec/gamma-openapi.yaml)
- [CLOB REST OpenAPI](https://docs.polymarket.com/api-spec/clob-openapi.yaml)
- [Market WebSocket AsyncAPI](https://docs.polymarket.com/asyncapi.json)

The documentation index exposes Markdown versions of individual pages with a
`.md` suffix. SDK examples can contain normalized fields that differ from raw REST
and WebSocket payloads. This reference uses raw wire fields unless stated otherwise.

## Services and authentication

The collector uses these production services:

- Gamma REST: `https://gamma-api.polymarket.com`, for events and market metadata.
- CLOB REST: `https://clob.polymarket.com`, for order-book snapshots.
- Market WebSocket: `wss://ws-subscriptions-clob.polymarket.com/ws/market`, for
  public snapshots, price-level updates, and lifecycle events.

Public market discovery, public REST books, and the market WebSocket require no
API credentials. This collector therefore needs no trading account, wallet
private key, funded balance, or CLOB API key for these interfaces.

The separate `/ws/user` channel and private trading operations require
authentication. Their credentials are outside this collector's scope.

Source: [API integration](https://docs.polymarket.com/getting-started/api).

## Events, markets, and asset IDs

An event groups one or more markets. Each binary market has outcome assets,
typically YES and NO. Each asset has its own bid and ask book.

- Gamma `id` identifies a market in the metadata API.
- `conditionId` identifies the market condition. WebSocket messages call this `market`.
- An outcome's asset ID identifies a book. REST calls it `token_id`;
  WebSocket messages call it `asset_id`, and subscriptions use `assets_ids`.
- `slug` and question text describe a market. They are not subscription identifiers.

Current documentation distinguishes two protocol versions:

- `version: "v1"`: asset IDs come from `clobTokenIds`, a JSON-encoded array string.
- `version: "v2"`: asset IDs come from `positionIds`, an array of decimal strings.

The selected ID remains the value for REST `token_id` and WebSocket `assets_ids`
in both versions. The outcome labels come from the JSON-encoded `outcomes` array
and correspond by index. Unsupported versions and mismatched arrays need explicit
handling rather than guessing an outcome ID. IDs remain strings throughout.

Sources: [Markets and events](https://docs.polymarket.com/concepts/markets-events),
[Protocol V2 identifiers](https://docs.polymarket.com/migrate/polymarket-v2/api-integrations).

## Market discovery and lifecycle

Gamma provides two market listing interfaces:

- `GET /markets/keyset?closed=false&limit=100` returns an object with `markets`
  and an optional `next_cursor`. The next request supplies that value as
  `after_cursor`. A missing cursor marks the last page. The maximum limit is 100.
  `offset` is invalid on this endpoint. A deployment without keyset support can
  return HTTP 503.
- `GET /markets?closed=false&limit=100&offset=0` returns an array and uses offset
  pagination. This is a separate interface with a different response shape.

The keyset listing is suitable for scanning a changing catalog. An open-market
listing alone does not establish that a market currently accepts orders.

Relevant metadata fields include `active`, `closed`, `acceptingOrders`, and
`enableOrderBook`. The project's proposed eligibility rule is an active, open
market that accepts orders, has an enabled book, and has valid outcome IDs.
This rule is a collector policy, not a single documented API filter.

Example public requests:

```sh
curl --fail-with-body --get 'https://gamma-api.polymarket.com/markets/keyset' \
	--data-urlencode 'closed=false' \
	--data-urlencode 'limit=100'

curl --fail-with-body 'https://gamma-api.polymarket.com/markets/<MARKET_ID>'
```

Sources: [Keyset listing](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination),
[Offset listing](https://docs.polymarket.com/api-reference/markets/list-markets),
[Market status](https://docs.polymarket.com/market-data/market-details).

## REST order books

- `GET /book?token_id=<ASSET_ID>` returns one book.
- `POST /books` accepts a JSON array of objects containing `token_id` and returns
  multiple books. This POST reads data; it does not place orders.
- A book contains `asset_id`, `market`, `timestamp`, `hash`, `bids`, and `asks`.
  Levels contain decimal-string `price` and `size` fields. Additional metadata
  includes `tick_size`, `min_order_size`, `neg_risk`, and `last_trade_price`.
- HTTP 404 from `/book` means no order book exists for that asset.

```sh
curl --fail-with-body --get 'https://clob.polymarket.com/book' \
	--data-urlencode 'token_id=<ASSET_ID>'

curl --fail-with-body 'https://clob.polymarket.com/books' \
	-H 'Content-Type: application/json' \
	--data '[{"token_id":"<ASSET_ID>"}]'
```

Placeholders represent IDs returned by market discovery.

Sources: [Single book](https://docs.polymarket.com/api-reference/market-data/get-order-book),
[Batch books](https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body).

## Market WebSocket

The initial subscription is a JSON text message:

```json
{
	"type": "market",
	"assets_ids": ["<YES_ASSET_ID>", "<NO_ASSET_ID>"],
	"initial_dump": true,
	"custom_feature_enabled": true
}
```

`initial_dump` defaults to true. `custom_feature_enabled` enables lifecycle and
best-price events. Optional `level` accepts 1, 2, or 3, defaulting to 2;
the schema leaves their depth semantics unspecified.

Subscription changes:

```json
{
	"operation": "subscribe",
	"assets_ids": ["<NEW_ASSET_ID>"],
	"custom_feature_enabled": true
}
```

```json
{
	"operation": "unsubscribe",
	"assets_ids": ["<REMOVED_ASSET_ID>"]
}
```

The heartbeat is text `PING` every 10 seconds, answered with text `PONG`.
WebSocket control frames are separate.

Relevant events:

- `book`: full `bids` and `asks` for `asset_id`, with `market`, `timestamp`, and
  `hash`. Snapshots can recur.
- `price_change`: contains `market`, `timestamp`, and `price_changes`. Each entry
  has `asset_id`, `price`, `side`, and replacement aggregate `size`. `BUY` means
  bids; `SELL` means asks. Zero deletes a level. Quantities are not additive.
- `tick_size_change`: an asset's price increment changes.
- `new_market` and `market_resolved`: lifecycle notifications.
- `last_trade_price` and `best_bid_ask`: trade and best-price notifications,
  not complete depth updates.

Book and price-change timestamps are string-encoded Unix milliseconds.
Neither schema documents sequence numbers. Hashes alone do not prove continuity.

Source for subscription payloads, heartbeats, and event fields:
[Market channel](https://docs.polymarket.com/api-reference/wss/market).

## Combos and negative risk

Polymarket Combos combine multiple outcome selections and use an RFQ flow.
Their quoting integration is separate from the public CLOB market channel.
A regular CLOB market can be eligible as a Combo leg without itself being a Combo.

Negative risk relates mutually exclusive outcomes within an event. The
`negRisk` flag does not identify a Combo. Excluding every negative-risk market
would therefore also exclude regular markets.

For this project, excluding Combo products while retaining regular CLOB markets
is an interpretation of the challenge's Kalshi MVE exclusion. The challenge does
not define an equivalent Polymarket filter.

Sources: [Combos](https://docs.polymarket.com/trading/combos/overview),
[Negative risk](https://docs.polymarket.com/concepts/negative-risk),
[Challenge specification](https://voluntas.io/engineering-challenge).

## Rate limits and service interruptions

The documented IP limits include 300 requests per 10 seconds for Gamma `/markets`,
1,500 per 10 seconds for CLOB `/book`, and 500 per 10 seconds for `/books`.
General and combined limits also apply. These figures do not establish a limit
for `/markets/keyset` or WebSocket subscriptions.

Cloudflare can delay or queue requests when limits are exceeded. HTTP timeouts
and growing latency can therefore matter even without an immediate rejection.
The market-channel schema does not specify a maximum number of assets per
connection. A fixed capacity would be a project setting requiring live validation.

Sources: [Rate limits](https://docs.polymarket.com/api-reference/rate-limits),
[Matching engine interruptions](https://docs.polymarket.com/trading/matching-engine).

## Collector decisions and unresolved checks

These are implementation considerations for this repository, not API guarantees:

- The subscription catalog contains outcome asset IDs. Both assets of a binary
  market have books. A stable output symbol needs to distinguish those assets.
- The proposed book representation uses exact decimal or fixed-point values.
  Binary floating-point price keys can prevent matching updates to existing levels.
- The output schema scales prices and quantities by 1,000,000. Schema version 2
  adds an explicit `side` to `PriceLevelUpdate`: `Buy` (wire value 0) updates a
  bid and `Sell` (wire value 1) updates an ask. Use the shared `Side` type.
- A received `book` replaces that asset's state. A `price_change` replaces or
  deletes the indicated levels. Every entry in the envelope needs processing.
- A new connection needs a fresh snapshot before its book is considered valid.
  REST snapshots taken while a stream is active require an ordering strategy.
- Reception time and exchange time are separate values. The supplied SBE
  helpers use microsecond timestamps, while the documented feed timestamps use
  milliseconds. Conversion does not add exchange-time precision.
- Periodic Gamma reconciliation can detect new and closed markets even when
  lifecycle notifications are missed. Lack of trades alone does not prove closure.
- Recovery needs reconnect backoff, subscription restoration, and a replacement
  snapshot. Redundant sources also require a defined failover policy.
- The challenge requires multiple connections, deltas, a full snapshot every
  minute, the existing encoding helpers, and clean SIGINT shutdown.
- Live validation still needs to establish message framing, useful asset counts
  per socket, initial snapshots after dynamic subscription, and lifecycle-event
  coverage. This document records documentation review, not an end-to-end feed test.

Repository references: [example entry point](../../src/main.rs),
[output schema](../../sbe_schemas/engineering_challenge/challenge_orderbook.yaml),
[challenge requirements](https://voluntas.io/engineering-challenge).
