# Incremental opening and closure discovery

**Superseded normal-path proposal:** the later
[updatedAt investigation](polymarket-updated-at.md) recommends two updated-time
listings, explicit cache handling, revision checks, and reconciliation. The
activation/closure-specific query observations below remain useful evidence.

Design only, checked on 2026-10-25. The proposed normal operation is one full
open-catalog bootstrap followed by two shared incremental listings. It does
not continuously sweep or query every tracked market.

**Correctness update:** the [2026-10-25 follow-up](polymarket-lifecycle-correctness.md)
found that omitting `version` excludes tradable protocol v2 markets. It also
found closure rows with equal timestamps whose IDs were ascending despite
`ascending=false`. The agreed scope is now v1 only, selected explicitly below.
ID ordering must not be used as an early-stop assumption.

## Listings

For possible recent trading activations:

```http
GET https://gamma-api.polymarket.com/markets/keyset?version=v1&closed=false&limit=100&order=acceptingOrdersTimestamp,id&ascending=false
```

For recent explicit closures:

```http
GET https://gamma-api.polymarket.com/markets/keyset?version=v1&closed=true&limit=100&order=closedTime,id&ascending=false
```

`startDate` is documented as market timing within the expected schedule, not
as a durable record of every transition into accepting orders. The API also
exposes `acceptingOrdersTimestamp`, which is a more relevant candidate for the
opening query. Its name alone does not establish transition coverage or
reopening semantics.

The live API accepted ordering by `startDate,id`, `acceptingOrdersTimestamp,id`,
`createdAt,id`, and `updatedAt,id`. Two consecutive ten-row activation pages
contained 20 distinct open records. All had activation timestamps, and their
timestamp and numeric-ID pairs were in descending order. Several records
shared the same activation second, making ID tie handling necessary.
The newest `updatedAt` results included markets created days earlier; that
ordering mixes ordinary metadata activity with lifecycle changes.

[Saved API observations](../experiments/polymarket-recent-openings-results.json).
Sources: [market query ordering](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination),
[activation timestamp field](https://docs.polymarket.com/api-reference/markets/list-markets),
[market timing](https://docs.polymarket.com/market-data/market-details).

## Proposed polling contract

1. Record bootstrap start time before beginning the full inventory scan. Retain
   that boundary for both incremental views, so changes during the scan are
   revisited after bootstrap. Alternatively, collect the two views concurrently
   and merge them with the inventory.
2. Keep a separate completed time frontier for openings and closures.
3. Each poll begins at that listing's newest page without `after_cursor`.
4. Follow every required page through `next_cursor` until the overlap window
   before the previous completed frontier has been covered. There is no
   one-page or 100-record assumption.
5. Reapply the full eligibility predicate to opening candidates. `closed=false`
   alone does not establish that a book accepts orders.
6. Match closed IDs against tracked membership. Duplicate rows are harmless.
   Follow the server's opaque cursor and process the complete timestamp bucket
   at the stopping boundary. Do not assume descending ID order within ties.
   Retain time overlap to revisit boundary rows and delayed publication.
7. Advance a frontier only after its entire required traversal succeeds. A
   partial failure retries from the head with overlap. An old cursor is not
   reused as a durable checkpoint between polls.

The registry must also handle conflicting observations from the two views.
Request completion order alone cannot establish which lifecycle observation is
newer. Merge explicit lifecycle evidence carefully and use a targeted lookup
for contradictions, rather than rechecking the entire inventory.

## What the sample does not establish

The API returns current-state listings rather than documented append-only event
logs. We have not verified that every activation and reopening refreshes
`acceptingOrdersTimestamp`, that every eligible market has the field, or that
`closedTime` is always populated and never corrected or backdated. These fields
are nullable in the documented market shape.

Indexing delay has no documented maximum. A finite overlap cannot prove that
an arbitrarily late row will be discovered. A market that opens and closes
between polls can be absent from the open listing. Temporary order-taking stops
while `closed=false` are also not represented by the closure query.

Therefore, bootstrap plus these two polls is the proposed normal path, with
WebSocket hints optional. A strict guarantee that no subsequent repair scan is
ever needed would require stronger API guarantees or additional verification.
Manual or exceptional recovery scans can remain available without making full
inventory polling part of normal operation. No refresh interval or overlap
duration has been chosen yet, and no collector implementation was added.
