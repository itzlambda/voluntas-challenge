# Discover recent closures with a shared listing

**Superseded normal-path proposal:** see the later
[updatedAt investigation](polymarket-updated-at.md) for the recommended general
change polls and observed five-minute cache behavior. The closure-time query
below remains useful for targeted lifecycle observations.

Checked on 2026-10-25. The proposed steady-state closure detector polls a
shared listing of closed markets, then matches returned market IDs against the
local registry. It does not repeatedly request every tracked market's status.

**Correctness update:** the [follow-up review](polymarket-lifecycle-correctness.md)
confirmed descending closure times in larger samples but found that IDs within
equal closure times were not descending. Follow the API cursor and consume the
whole boundary timestamp bucket; do not stop on an assumed timestamp/ID tuple
order. The agreed scope is now v1 only, selected explicitly below.

The public Gamma endpoint accepted:

```http
GET https://gamma-api.polymarket.com/markets/keyset?version=v1&closed=true&limit=100&order=closedTime,id&ascending=false
```

A bounded live check fetched two consecutive ten-row pages. All 20 records
reported `closed=true`, all had `closedTime`, their closure times were in
descending order, and the pages had no overlapping IDs. The separate
`order=closedTime` and `order=updatedAt` probes also returned HTTP 200.
This validates the queried ordering and pagination on the sample, not a
durable or complete change-feed guarantee.

Source: [Gamma keyset parameters](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination).
[Saved two-page observation](../experiments/polymarket-recent-closures-results.json).

## Proposed polling semantics

Each poll starts at the newest page without a cursor. Within that poll, follow
`next_cursor` until the chosen overlap window before the previous processed
closure-time frontier is covered. Include the entire timestamp bucket at the
boundary and deduplicate closure observations by market ID. A previously seen
ID alone is not a safe stopping condition.

For each explicit closed record, check registry membership by ID. Retire it
only if tracked. Closures for unrelated markets need no additional HTTP lookup.
In normal operation, the amount of listing work depends on closure activity
and the overlap window rather than the number of tracked open markets.

Advance the completed frontier only after the necessary pages succeed. After
a failed request or disconnect, retry from the head with overlap. Keep normal
duplicate observations harmless. Bootstrap and recovery must merge the closed
listing with the initial open inventory without allowing an older in-flight
open response to overwrite a newer closure observation.

## Limits to establish before adoption

- This detects `closed=true`. It does not detect every temporary
  `acceptingOrders=false` transition while `closed=false`, or every reopening.
- The documentation exposes `closedTime` but does not provide a bound on
  indexing delay, backdating, or corrections. A finite overlap window therefore
  reduces missed-record risk without proving lossless discovery.
- Coverage of records with absent closure times, tie behavior, cursor stability
  during concurrent closures, and the meaning of `closedTime` relative to actual
  order-taking cessation need further investigation.
- `updatedAt` ordering is another candidate, but ordinary metadata updates can
  bring old closed markets to the front. It is not a closure-only event stream.
- Full inventory scans remain a repair tool for startup, recovery, and occasional
  reconciliation. Their frequency must be chosen separately from this primary
  closure poll, rather than continuously sweeping the tracked inventory.

No collector code was implemented. WebSocket resolution notifications remain
optional additional hints; the design does not wait for resolution to detect
an explicit closed status.
