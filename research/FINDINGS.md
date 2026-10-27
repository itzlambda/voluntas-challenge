# Key findings

A quick reference to the most useful facts from our research, measured
2026-10-23 to 2026-10-25 unless stated otherwise. Each item links to the
report with its method and limits. Live behavior can change; re-measure
before relying on an exact number.

## Lifecycle WebSocket (`new_market`, `market_resolved`)

From [lifecycle WebSocket coverage](docs/polymarket-lifecycle-ws.md) (65 min, 3 empty-asset sockets)
and [empty-asset subscriptions](docs/polymarket-empty-assets.md):

- A socket subscribed with `assets_ids: []` and `custom_feature_enabled: true`
  receives `new_market` for markets it isn't subscribed to. This is observed,
  not documented.
- **Redundancy is necessary.** Together the 3 sockets delivered 519 of 519 new
  eligible v1 markets; individually they delivered 519, 510 and 518. Missed
  events were not replayed after a reconnect.
- **Delay from creation to `new_market`:** 30 s median, 100 s p95, 122 s maximum.
- **`market_resolved` never arrived on empty sockets:** 0 of 401 resolutions
  during the window. It cannot be used as a removal signal from them.
- **Archived test-host receive stalls:** 5 gaps in 65 min, up to 119 s,
  were observed on the original host. Later host and network controls
  undermined the initial Polymarket-side explanation. These observations
  explain the conservative silence timeout, but do not establish an exchange
  delivery cycle or lossless recovery. See the [incident archive](archive/test-host-connectivity/README.md).
- **Market IDs don't arrive in order,** and IDs aren't strictly ordered by
  creation time. A "fetch IDs above the last one seen" catch-up would miss
  markets.
- **The event's `active` flag is unreliable:** it was false for 136 markets that
  Gamma showed as eligible.
- `new_market` carries `id`, `market` (condition ID) and `assets_ids`: enough to
  subscribe without a REST lookup. 7 of 534 hinted IDs returned 404 from Gamma.
- Event rate was 0.14 new markets/s on average, in bursts of up to 142 per minute.
  An earlier short test saw 3–4/s.
- Five abnormal `1006` disconnects in 65 min across 3 sockets. Reconnect took
  about 1.2 s.

## Subscription behavior and book format

From [subscription behavior](docs/polymarket-subscription-behavior.md):

- **Subscribing a new asset on a live connection** delivered a full `book`
  10 of 10 times, in 22–72 ms.
- **Subscribing again to an asset already subscribed** delivered **no** new
  book (0 of 10). To get a fresh book, unsubscribe and then subscribe: that
  worked 35 of 35 times, back to back or 1 s apart. On busy markets, a few old
  updates arrived in between and cannot be told apart from new ones.
- **A market with no orders** receives a `book` with empty bids and asks.
- **Without `PING`,** connections closed about 125 s after the last one (local
  code `1006`). Polymarket documents a 10 s `PING` interval but no timeout.
- **Format across 8,339 books:** bids ascending, asks descending, one entry per
  price, no zero sizes, at most 6 decimals, and no book older than an earlier
  `price_change`. A market's two outcome books did not always arrive together.

## Market lifecycle and resolution

From [resolution lag](docs/polymarket-resolution-lag.md),
[resolution tail](docs/polymarket-resolution-tail.md) and
[updated-at polling](docs/polymarket-updated-at.md):

- **Gamma sets `closed=true` at resolution, not when trading stops.** In 1,673
  sampled v1 markets, resolution was at or before `closedTime`; there was never
  a positive gap.
- **Before resolution, a market still looks open.** A 5-minute BTC up/down
  market showed open and accepting orders until it switched straight to closed
  and resolved, about 2 minutes after its scheduled end.
- **Closed but unresolved was zero** in a six-hour census of 14,182 closures. That
  window is dominated by sports and automatic resolutions, so rare disputed or
  high-value cases may not show up. The docs define `closed` as "resolved or
  otherwise closed".
- **No maximum resolution time is documented.** A normal disputed resolution
  takes about 4–6 days. The longest verified gap in a 24,873-market sample was
  2 h 37 min.
- **Gamma status fields can be stale:**
  - All 903 closed markets listed as `proposed` or `disputed` had already resolved.
  - 2,841 closed markets still showed `acceptingOrders=true`.
  - `resolvedBy` is set on some unresolved markets.
- **Closures:** about 49,000–99,000 per day (about 2,000–4,000 per hour), 94%
  of them sports.
- Only 107 of about 242,000–254,000 open v1 markets (0.04%) weren't tradable
  (`acceptingOrders` or `enableOrderBook` false). Some have sat inactive for
  about 81–310 days.
- **Markets start trading almost at creation.** In about 90,000 seed records,
  94% accepted orders within 5 min of creation, with a median of about 20 s.
  (Sampled from the saved seed in this session; not in a separate report.)
- **Gamma listings are CDN-cached for up to 300 s.** `Cache-Control: no-cache`
  doesn't refresh them; a unique query parameter does.
- **Resolution data** (`/v2/tokens`, `/v2/resolutions`) is served by
  `data-api.polymarket.com`, not the CLOB host. It returned HTTP 403 (error
  1010) without a browser User-Agent.
- Condition-level `/v2/resolutions` lookups can return v2 timestamps for a v1
  market. Use `question_id`, or token lookups restricted to `module=v1_ctf`.

## Gamma discovery API

From [market discovery](market-discovery.md),
[correctness review](docs/polymarket-lifecycle-correctness.md) and
[protocol adoption](docs/polymarket-protocol-adoption.md):

- A full `closed=false&version=v1` keyset scan takes about 2,540 pages (100 per
  page), takes about 5 min, and returns about 1.8 GB (about 7 KB per market).
- **Only `closed` works as a server-side filter.** `active`, `archived`,
  `acceptingOrders` and `enableOrderBook` filters were silently ignored, so
  eligibility must be checked locally.
- Omitting `version` returns v1 only. Request `version=v1&version=v2` for both.
- **v1 is nearly all the volume:** 253,740 tradable v1 markets against 123 v2.
  Polymarket tentatively plans to put new markets on v2 from 2026-11-02.
- Markets with equal timestamps aren't ordered by ID within a timestamp-sorted
  listing.
- `closed=any` is rejected without an `id` filter (HTTP 422).
- **2026-10-26, full-scan endpoint choice:** requesting `limit=1000` returned
  only 100 rows on both `/markets` and `/markets/keyset`. `/markets` rejected
  offset 100,000 with HTTP 422 and directed the caller to keyset. Offset
  pagination can also skip rows as earlier open markets close. The
  [recorded decision](docs/polymarket-market-discovery.md#decision-confirmed-on-2026-10-26)
  explains why we use keyset, including its own limitation that a live walk
  is not an atomic snapshot.
- **2026-10-26, events are not a complete replacement:** an exhausted event
  keyset walk took 220 pages versus 2,393 market pages, but omitted 306
  markets that remained eligible on direct recheck. Of these, 304 belonged
  to open parents missing from event listings, one had a closed parent, and
  one had no parent link. See the
  [full comparison and counterexamples](docs/polymarket-market-discovery.md#full-coverage-check-on-2026-10-26).
  Event grouping can accelerate candidate discovery, but a direct market
  scan is still needed for complete coverage.

## Order-book WebSocket

From [observed events](docs/polymarket-observed-events.md),
[subscription capacity](docs/polymarket-subscription-capacity.md) and
[redundancy](docs/polymarket-redundancy.md):

- **Every `book` and `price_change` carries `market` (the condition ID).** One
  `price_change` can contain changes for both outcomes, and the initial dump is
  an array of `book` objects.
- **The exchange sends full `book` snapshots again without being asked:** 520
  in 30 s for one active market.
- **One socket accepted 20,000 markets** (40,000 assets), then the server closed
  it with `1013: slow consumer`. No sustainable maximum per socket was found;
  100 markets per socket is a provisional setting.
- **Up to 16,000 sockets were admitted from one IP**, with no per-IP limit
  observed. 14,000 passed the final health check.
- 14,000 top-volume markets produced about 9,700 events/s and about 6 MB/s.
- The server closes a socket with reason `all subscribed assets resolved` once
  every subscribed asset has resolved.
- **Ten redundant sockets for one book** received the same updates in the same
  order when healthy. Arrival spread across sockets was up to about 65 ms. All
  10 sockets went silent together for about 60 s once.
- **Timestamp and hash are not unique:** distinct updates shared both, so
  redundant feeds cannot be merged or deduplicated by them.

## Processing cost (earlier Rust implementation)

From the earlier implementation's single-book and large-catalog reports, now
in Git history (`git show 76e65b8:research/docs/polymarket-single-book-performance.md`
and `polymarket-large-catalog.md`):

- Parsing, applying and encoding one frame took about 3.4 µs, on one hot book
  with typed borrowing JSON parsing. The earlier `serde_json::Value` parsing
  took 11.1 µs.
- A 400,000-book registry with no populated levels used about 262 MiB RSS.
- Neither measurement covers a realistic multi-book cold-cache workload.

## Output format

From the scaffold in `src/util/`:

- **`AlignedSbeReader::open` reads 64 MiB chunks**, so output must use 64 MiB
  chunks. Each flush pads to a full chunk.
- The reader uses positioned reads, so it needs a regular file, not a pipe.
- About 60% of each `PriceLevelUpdate` is the 77-character asset ID used as
  `symbol` (estimated from the schema, not measured).
