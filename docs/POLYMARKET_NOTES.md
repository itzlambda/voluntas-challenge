# Polymarket behavior notes

Exchange behavior we measured while building the collector, between
2026-10-23 and 2026-10-27. Much of it is observed, not documented by
Polymarket, and live behavior can change. Re-measure before relying on an
exact number.

## Gamma metadata can be wrong or stale

The most important caveat: Gamma's market metadata does not always match the
order book. The collector cannot trust any single field, which is why it
re-checks eligibility and keeps retrying missing books.

- **Markets listed as tradable with no order book.** Some markets were open
  and `acceptingOrders=true` in Gamma, yet the WebSocket returned an empty
  `[]` for their assets (not a `book` with empty sides), both initially and
  after resubscription, on fresh sockets. `GET /book` returned 404 "No
  orderbook exists". In the same 512-asset subscription, the other 510 assets
  received books normally, so it was these markets, not the socket. Our last
  validation run still had two such markets.
- **Markets that closed while we subscribed.** Others had closed during the
  scan, so they stayed in the desired set with no book until a later
  eligibility check removed them.
- **Stale status fields on closed markets.** All 903 closed markets listed as
  `proposed` or `disputed` had already resolved, 2,841 closed markets still
  showed `acceptingOrders=true`, and `resolvedBy` was set on some unresolved
  markets.
- **Finished markets look open until resolution.** `closed=true` is set at
  resolution, not when trading stops. A five-minute market showed open and
  accepting orders until it switched straight to closed, about two minutes
  after its scheduled end.
- **Cached listings.** Gamma listings are CDN-cached for up to 300 seconds
  unless the query is made unique.
- **Unreliable lifecycle flags.** The `active` flag on `new_market` was false
  for 136 markets Gamma showed as eligible, and a few announced IDs returned
  404 from Gamma.

## Edge cases the collector handles

Each row is a Polymarket behavior we hit, what the collector does about it,
and where to look in the code.

### Subscriptions and first books

| Edge case | Handling | Code |
|---|---|---|
| Subscribing again returns no new book. | Unsubscribe, wait five seconds, subscribe. | `core/book_connection.rs` |
| A first book never arrives; the socket answers `[]`. | Five-second window per outcome, up to ten retries per round, 60-second pause between rounds. Markets still incomplete are re-checked in Gamma and removed only if ineligible. Other markets on the socket are unaffected. | `core/book_connection.rs`, `core/recovery.rs`, `core/discovery.rs` |
| `[]` versus a `book` with empty sides. | An empty `book` is a valid baseline. `[]` carries no book and initializes nothing. | `exchange/polymarket/decode.rs`, `core/worker.rs` |
| A market's two outcome books arrive separately. | Each book is handled on its own. A source is ready only when both outcomes have a baseline. | `core/worker.rs` |
| Data arrives for an outcome before its baseline. | Ignored until the full book arrives. | `core/worker.rs` |

### Book messages

| Edge case | Handling | Code |
|---|---|---|
| Full books arrive unprompted, many per minute. | Treated as input: compared with the held book, only differences written, any difference counted as a mismatch. | `core/worker.rs` |
| A full book replays depth older than updates already applied. | Rejected while synced; counted in `collector_older_full_books_skipped_total`. | `core/worker.rs` |
| A full book arrives before its matching `price_change`. | Quantities are absolute, so the later update just sets the same value again. | `core/worker.rs` |
| Equal or regressing timestamps. | Applied in arrival order; readers replay per-book file order. | `core/worker.rs` |
| One `price_change` holds changes for both outcomes. | Routed whole by its `market` (condition ID), so both outcomes reach the same worker. | `core/routing.rs` |
| A frame is one object or an array (the initial dump is an array of books). | Routed per event, not per frame. | `exchange/polymarket/decode.rs` |
| Prices and sizes are decimal strings; timestamps are millisecond strings. | Parsed exactly into fixed point; more than six decimals is an error; never floats. | `exchange/polymarket/number.rs` |
| A recognized book message fails to parse. | Rejected whole, logged to a debug file, the socket resets and resyncs. | `exchange/polymarket/decode.rs`, `core/decode_log.rs` |
| Text that is not JSON, plus `tick_size_change`, `best_bid_ask`, `last_trade_price`. | Counted and dropped; tick-size changes are logged. Book sockets keep custom features off to avoid extra traffic. | `exchange/polymarket/decode.rs` |
| Trade notifications overlap depth updates. | Not subtracted; absolute updates and full books already include their effect. | `exchange/polymarket/decode.rs` |
| Timestamps and hashes are not unique across feeds. | The two feeds are never compared, merged, or deduplicated. | `core/worker.rs` |

### Connections

| Edge case | Handling | Code |
|---|---|---|
| Sockets close about 125 seconds after the last `PING`. | `PING` every 10 seconds on every socket, even when busy. | `exchange/polymarket/protocol.rs` |
| `1013: slow consumer` closes, even at 100 markets per socket. | Reconnect with the same subscriptions and switch to the standby copy. Second copies are spread across sockets so one busy socket fails over to many. | `core/book_connection.rs`, `core/subscriptions.rs` |
| A socket stops delivering without closing. | Reset after 30 seconds with no frame of any kind. | `core/book_connection.rs` |
| Reconnect storms. | Connection opens are rate-limited (20 per second by default). | `core/rate_limit.rs` |

### Discovery

| Edge case | Handling | Code |
|---|---|---|
| One lifecycle socket misses announcements and never replays them. | Three lifecycle sockets, plus a full scan every 15 minutes and after any interval with none connected. | `exchange/polymarket/lifecycle.rs`, `core/discovery.rs` |
| `market_resolved` almost never arrives. | Removal relies on the full scan; the event is still applied when it does arrive. | `core/registry.rs` |
| Announced market IDs arrive out of order. | No "IDs above the last seen" bookmarks; the scan is the source of truth. | `core/registry.rs` |
| The announcement's `active` flag is wrong, or the ID is not in Gamma yet. | Ignore the flag; confirm in Gamma before subscribing; unconfirmed IDs are rechecked after five seconds without expiring. | `core/discovery.rs` |
| Gamma ignores `active`, `archived`, and `acceptingOrders` filters. | Eligibility is checked locally; missing `acceptingOrders` counts as false. | `exchange/polymarket/gamma.rs` |
| Listings are CDN-cached for up to 300 seconds. | A fresh cache-busting query parameter per scan pass and per lookup. | `exchange/polymarket/gamma.rs` |
| Page size is capped at 100 and deep offsets fail. | Keyset pagination with `next_cursor`. | `exchange/polymarket/gamma.rs` |
| A scan is not an atomic snapshot; markets open and close mid-scan. | Pages are buffered and applied only when the scan completes; recent lifecycle events override older pages; a failed scan changes nothing. | `core/registry.rs` |
| Omitting `version` returns v1 only, implicitly. | Request `version=v1` explicitly. | `exchange/polymarket/gamma.rs` |
| Some rows have no tokens, or three or five. | Eligibility requires exactly two distinct outcome IDs. | `exchange/polymarket/gamma.rs` |
| Gamma and the CLOB rate-limit by delaying requests. | All Gamma calls share one request budget (10 per second by default). | `core/rate_limit.rs` |

## Order-book WebSocket

- **Every `book` and `price_change` carries `market`** (the condition ID), so
  messages can be routed without a lookup. One `price_change` can hold
  changes for both outcomes, and the initial dump is an array of `book`
  objects.
- **Subscribing a new asset on a live socket** returned a full `book` 10 of
  10 times, within 22–72 ms.
- **Subscribing again to an asset already subscribed returns nothing**
  (0 of 10). Unsubscribe then subscribe returns a fresh book (35 of 35).
- **A market with no orders** gets a `book` with empty bids and asks.
- **Full books arrive unprompted**, up to 520 in 30 seconds on one active
  market. Some arrive ahead of the matching `price_change`, and some replay
  depth older than updates already sent.
- **Without `PING`, sockets close** about 125 seconds after the last one.
  Polymarket documents a 10-second interval but no timeout.
- **Book format**, across 8,339 books: bids ascending, asks descending, one
  entry per price, no zero sizes, at most six decimals. A market's two
  outcome books do not always arrive together.
- **Capacity:** one socket accepted 20,000 markets, then was closed with
  `1013: slow consumer`. Up to 16,000 sockets were admitted from one IP with
  no per-IP limit seen. `1013` also occurred at 100 markets per socket on a
  busy AWS run, so no safe per-socket count is known.
- When every subscribed asset resolves, the server closes the socket with
  reason `all subscribed assets resolved`.
- **Redundant sockets** for one book received the same updates in the same
  order when healthy, with arrival spread up to about 65 ms.
- **Timestamp and hash are not unique:** distinct updates shared both, so
  redundant feeds cannot be merged or deduplicated by them.

## Lifecycle events

- A socket subscribed with `assets_ids: []` and
  `custom_feature_enabled: true` receives `new_market` for markets it is not
  subscribed to.
- **One socket is not enough.** In 65 minutes, three sockets together
  delivered all 519 new markets; individually they got 519, 510, and 518.
  Missed events are not replayed after a reconnect.
- **Delay from creation to `new_market`:** 30 s median, 100 s p95, 122 s
  maximum.
- **`market_resolved` never arrived** on these sockets: 0 of 401
  resolutions. It cannot be relied on for removals.
- **Market IDs do not arrive in order**, and are not strictly ordered by
  creation time, so "fetch IDs above the last one seen" misses markets.
- **The event's `active` flag is unreliable:** false for 136 markets Gamma
  showed as eligible. A few announced IDs returned 404 from Gamma.
- New markets averaged 0.14 per second, in bursts of up to 142 per minute.

## Gamma API

- **Only `closed` works as a server-side filter.** `active`, `archived`,
  `acceptingOrders`, and `enableOrderBook` are silently ignored, so
  eligibility must be checked locally.
- **Omitting `version` returns v1 only.** Request `version=v1&version=v2` for
  both.
- **Listings are CDN-cached for up to 300 seconds.** `Cache-Control: no-cache`
  does not help; a unique query parameter does.
- **Use the keyset endpoint for full scans.** `limit` is capped at 100 rows.
  `/markets` rejects offsets past 100,000 and can skip rows as markets close.
  A full open-v1 scan is about 2,500 pages, five minutes, and 1.8 GB.
- **The events listing is not complete:** it omitted 306 eligible markets
  that the market scan found.
- **`closed=true` is set at resolution, not when trading stops.** Before
  resolution, a finished market still looks open and accepting orders.
- Markets start trading almost at creation: 94% within five minutes, median
  about 20 seconds.
- Closures run about 2,000–4,000 per hour, mostly sports.
- v1 carried nearly all volume: 253,740 tradable v1 markets against 123 v2
  (2026-10-25). Polymarket tentatively plans to put new markets on v2 from
  2026-11-02.
- Resolution data comes from `data-api.polymarket.com`, which returned 403
  without a browser-like User-Agent.

## Host and network

- Long receive pauses (up to 119 s) seen on our first test host were later
  attributed to that host or its network, not the exchange. Sockets on one
  host can still share delivery pauses.
- The AWS London host showed no broad shared pause in an hour-long run, but
  one episode delivered updates for 54 books about 20 seconds late.
