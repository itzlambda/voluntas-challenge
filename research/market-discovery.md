# Market discovery and lifecycle design

The initial inventory is implemented by `Gamma::seed` and the main command.
Incremental lifecycle polling remains design work. The proposal was checked
against public documentation and bounded HTTP observations starting 2026-10-24.

The agreed scope is **protocol v1 only**. The seed explicitly requests
`version=v1`; future opening and closure polls must use the same selection.
The [correctness follow-up](docs/polymarket-lifecycle-correctness.md) also found
closure-time ties whose IDs were not descending. Future incremental traversal
must consume complete timestamp boundaries using the server cursor.

The proposed normal operation is a full Gamma catalog scan at bootstrap,
followed by two shared listings ordered by `updatedAt`, split by closed status.
The [updated-time investigation](docs/polymarket-updated-at.md) supersedes the
earlier activation-time/closure-time proposal and establishes that cache
freshness and observation ordering need explicit handling. WebSocket hints
can accelerate checks. This is a recommendation based on the available
interfaces, not a measured claim that this is the fastest possible
implementation or a documented lossless change feed.

[On-chain discovery](on-chain-discovery.md) is a separate future research item.
The current design adds no blockchain or contract dependency.

## Initial inventory

Use Gamma's market endpoint directly. Events are presentation groups and are
not needed to discover outcome books.

```http
GET https://gamma-api.polymarket.com/markets/keyset?version=v1&closed=false&limit=100&order=id&ascending=true
```

Follow each response's `next_cursor` using `after_cursor`, with the same filters
and ordering. Stop when the cursor is absent. The documented maximum page size
is 100. Reuse one HTTP client with compression and connection pooling. Decode
only required fields and release each page after merging it. Page fetching can
overlap processing, but the next request depends on the previous cursor.
Parallel offsets or fabricated cursors do not provide the same traversal.

Use immutable IDs for traversal rather than changing volume rankings. The
current seed retains the full raw API object for each tradable market, plus
typed market and condition IDs, protocol version, and outcome IDs and labels.
It indexes asset IDs back to their market and deduplicates by market ID.
Metadata can be reduced later if memory measurements justify it.

Eligibility is a predicate over independent fields, not a single status enum:

```text
active == true
closed == false
archived == false
acceptingOrders == true
enableOrderBook == true
two valid, distinct outcome asset IDs
```

Use `clobTokenIds` for the selected CTF/v1 markets. Protocol v2 is outside the
current challenge scope. Unknown or incomplete records
need confirmation rather than an inferred trading status. Neither zero volume
nor an empty book excludes a tradable market. This is a regular order-book
market catalog; Combo/RFQ positions are not added from their separate APIs.
Negative-risk event grouping alone does not make a constituent market a Combo.

A cursor scan of a changing catalog is not an atomic exchange snapshot. Markets
can change behind the cursor. Completion means the traversal finished, with
eventual consistency supplied by subsequent reconciliation.

Sources: [Gamma keyset reference](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination),
[market discovery and protocol identifiers](https://docs.polymarket.com/market-data/discover-markets).

### Server-side tradability filters

Checked on 2026-10-25: the current market listing references document
`closed`, but do not list filters for `active`, `archived`, `acceptingOrders`,
or `enableOrderBook`. These response fields do not imply matching query
parameters exist.

Bounded live requests to both `/markets` and `/markets/keyset`, ordered by
ascending ID with `closed=false`, returned market `559651` with `active=true`,
`archived=false`, `acceptingOrders=true`, and `enableOrderBook=true` even when
requesting each opposite value separately with `active=false`, `archived=true`,
`accepting_orders=false`, or `enable_order_book=false`. The keyset endpoint
also ignored `acceptingOrders=false` and `enableOrderBook=false`.
These observations show those tested query parameters did not enforce the
requested predicate. HTTP 200 alone is not proof of filter support.

Both endpoints also returned `acceptingOrders=false` for market `3358951`
when explicitly requesting `accepting_orders=true` or `acceptingOrders=true`,
using `order=acceptingOrders,id&ascending=true&limit=3&closed=false`.
For example:

```sh
curl -sG 'https://gamma-api.polymarket.com/markets/keyset' \
  --data-urlencode 'closed=false' \
  --data-urlencode 'accepting_orders=true' \
  --data-urlencode 'order=acceptingOrders,id' \
  --data-urlencode 'ascending=true' \
  --data-urlencode 'limit=3' |
  jq '.markets[] | {id, closed, acceptingOrders}'
```

The observed first row was
`{"id":"3358951","closed":false,"acceptingOrders":false}`.
As a positive control, `closed=true&order=id&ascending=true&limit=3`
returned closed markets `12`, `17`, and `18` on both endpoints.

Use the documented `closed=false` server filter, then apply the full eligibility
predicate locally before inserting a market into the catalog. Unknown status
is excluded. Keep full metadata only for retained tradable markets.

References: [keyset query parameters](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination),
[offset listing query parameters](https://docs.polymarket.com/api-reference/markets/list-markets).

## Incremental discovery after bootstrap

The [current two-listing design](docs/polymarket-updated-at.md) queries
`version=v1&closed=false` and `version=v1&closed=true`, each ordered by
`updatedAt,id` descending. Reapply the full eligibility predicate to every
observation. Each poll starts at the head and fetches as many pages as needed
to cover its previous completed boundary plus overlap, including complete
timestamp ties. Apply rows by market ID and revision so stale cached reads
cannot overwrite newer status. Advance a boundary only after traversal succeeds.

Observed listing responses were cacheable for 300 seconds. Request no-cache
headers did not refresh one tested cache hit; a unique query parameter did.
Freshness must be checked as part of polling. That workaround is empirical,
not a documented API guarantee. Reconciliation remains a separate repair path
for unproven lifecycle timestamp coverage and delayed publication.

A historical two-page check validated activation-time ordering for 20 distinct
open records. `startDate` is not a documented activation-event timestamp. Coverage
of null fields, reopenings, late indexing, pauses, and corrected timestamps
remains unverified, so the design does not promise that repair scans will
never be needed. Repeated full scans are not part of the normal polling loop.

## Optional WebSocket hints

The market WebSocket supports `new_market` and `market_resolved` with
`custom_feature_enabled=true`.

These are additional messages on the ordinary public market channel. Connect
to `wss://ws-subscriptions-clob.polymarket.com/ws/market` and subscribe using
outcome asset IDs:

```json
{
  "type": "market",
  "assets_ids": ["<YES_ASSET_ID>", "<NO_ASSET_ID>"],
  "custom_feature_enabled": true
}
```

The IDs are outcome IDs, not Gamma market IDs or condition IDs. No separate
`new_market` subscription topic is documented. This subscription also receives
ordinary book traffic when asset IDs are supplied. A subsequent
[empty-asset experiment](docs/polymarket-empty-assets.md) confirmed that
`assets_ids: []` with custom features enabled receives new-market notifications
without book data. This is observed behavior rather than a documented global
catalog guarantee, and global resolution coverage remains unverified.

- A `new_market` message queues that market ID for a Gamma eligibility check.
  Creation does not prove that order taking has started. Coalesce repeated hints
  by market ID and fetch up to 100 pending IDs in one request.
- A resolution message for a known condition provides an additional reason to
  retire it if still tracked. Trading closure is the actual removal criterion;
  the collector must not wait for resolution. Preserve terminal resolution
  observations so an older in-flight HTTP response cannot re-add the market.
- A non-trading market can become tradable later. Reconcile open candidates,
  including IDs below the newest observed ID. An ID high-water mark alone is
  insufficient for activation and reopening.

The observed empty-asset mode lets the discovery listener start before the
initial scan, without choosing an anchor book. Merge pending hints after
bootstrap. This reduces the gap between initial discovery and steady operation.
Hints should schedule work without blocking the receiver on HTTP requests.

The documented stream has no catalog replay cursor or contiguous lifecycle
sequence. Prior experiments observed new-market notifications beyond the assets
subscribed on a socket, but do not prove complete global coverage. Do not assume
that one small subscription receives every resolution. When book sockets exist,
their lifecycle notifications can feed the same catalog owner. Discovery-only
operation must remain correct through REST reconciliation without those sockets.

Resolution is also later than the end of trading in some cases. WebSocket
resolution messages cannot be the only closure detector. The checked public
market-channel specification documents neither a `market_closed` event nor
an `acceptingOrders` change event. REST trading-status reconciliation is the
closure mechanism in this design; lifecycle notifications only accelerate it.

Sources: [market-stream lifecycle payloads](https://docs.polymarket.com/market-data/realtime-data),
[subscription options](https://docs.polymarket.com/api-reference/wss/market).
Prior observations are in [the redundancy findings](docs/polymarket-redundancy.md).

## Detect closed markets without sweeping the active inventory

The primary proposed closure detector is a
[shared recent-closures listing](docs/polymarket-recent-closures.md), ordered by
descending `closedTime` and ID. Poll from the head, paginate through an overlap
window, and match returned closed IDs against the local registry. A bounded
two-page check validated this query for 20 closed records. Normal closure
detection therefore does not require repeatedly checking every open market.

This detects explicit `closed=true` transitions. Temporary order-taking pauses
with `closed=false` require separate treatment. The API has no documented bound
on indexing delay or closure-time corrections, so this is not a guaranteed
lossless change feed. The overlap and occasional repair policy remain design
decisions.

## Reconciliation and targeted closure confirmation

Use a full open-market scan at startup and as an exceptional repair mechanism
during recovery. Upsert records as pages arrive, including
status changes for known markets. Only after a successful complete scan identify
previously tracked markets that were absent. Confirm ambiguous IDs in batches,
including both closed and open markets:

```http
GET https://gamma-api.polymarket.com/markets/keyset?closed=any&limit=100&id=123&id=456
```

Each identifier filter accepts at most 100 values. A successful response with
explicit non-trading status can remove that market from the tradable set.
`acceptingOrders=false` can mean trading has stopped even before resolution.
The same eligibility predicate is reevaluated for every confirmed status record.
A confirmed non-trading state removes the market from the subscription set,
without requiring a resolved outcome. A temporary stop is not terminal
resolution. Retain enough metadata to recognize a later reopening.

An absent row, incomplete status, timeout, partial scan, or failed request is
unknown, not proof of closure. Keep the last confirmed record, expose its
freshness, and retry. Process resolution observations before applying older
HTTP results. Expiration dates, quiet feeds, and socket resets are not closure
evidence.

At the planning size of 200,000 open rows, a 100-row endpoint requires roughly
2,000 page requests for a full pass. A second full status sweep over the same
inventory would duplicate much of that work. After bootstrap, prefer the two
shared incremental listings plus targeted checks for ambiguous candidates.

Use a shared request budget across all discovery work. Normal operation uses
the opening and closure polls, with optional coalesced WebSocket hints. Full
repair scans are exceptional and non-overlapping. Actual freshness and recovery
coverage need measurement. A newest-ID poll is another optional accelerator. No documented
`updated_since` filter or durable catalog change feed was found in the checked
market endpoints.

The published Gamma `/markets` allowance is 300 requests per ten seconds;
`/markets/keyset` has no separate published allowance. Keep below listing limits,
measure throttling and latency, and back off rather than assuming that allowance
is available throughput. A complete 200,000-row pass every minute alone would
require about 33.3 requests per second before confirmations or retries.

Sources: [batch identifiers and closed=any](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination),
[trading-status field meanings](https://docs.polymarket.com/market-data/market-details),
[published rate limits](https://docs.polymarket.com/api-reference/rate-limits).
Intervals and budgeting above are proposed settings, not benchmark results.

## Observed API checks

[Saved results](experiments/market-discovery-results.json) contain response shapes
and IDs from seven successful public requests, not a full-catalog crawl:

- Two consecutive ascending Gamma keyset pages each returned 100 distinct rows
  with no overlap. A descending request also returned 100 rows.
- The sampled Gamma responses contained all five eligibility flags and used
  protocol v1. These checks do not validate live protocol v2 behavior.
- A request to the offset endpoint with `limit=500` returned 100 rows. Do not
  assume that requesting a larger limit increases page size.
- A 100-ID lookup with `closed=any` returned all 100 IDs, including one known
  closed market. The same lookup with `closed=false` returned 99 and omitted
  the closed market.
- CLOB `/simplified-markets` returned 1,000 rows, of which 941 were closed.
  It exposed outcome tokens, `version`, and `accepting_orders`, but no
  `enableOrderBook` or Gamma market ID. Its documented query only specifies a
  cursor. Its smaller records do not establish a complete, efficiently filtered
  tradable catalog, so it is not the primary source for this design.

Source for the alternative: [simplified-market reference](https://docs.polymarket.com/api-reference/markets/get-simplified-markets).

## Boundary of the first milestone

The proposed component owns market metadata and emits additions, changed
metadata, and removals with reasons. It does not own order books, decide socket
batch sizes, or write SBE. The first implementation should demonstrate initial
pagination and correct lifecycle transitions before those components are added.

Before relying on lifecycle hints, validate their subscription scope and
activation timing. Before choosing final refresh intervals, measure a complete
catalog traversal and the volume of missing-ID confirmations. These are open
checks, not reasons to delay the design-only phase.
