# Seed and incremental lifecycle correctness review

Checked against the source, the saved `data/markets.json`, and bounded public
API responses on 2026-10-25 around 01:45–01:46 UTC. No collector implementation
was added or changed during this review.

**Subsequent scope decision:** the user chose protocol v1 only. The seed now
explicitly requests `version=v1`, and future opening/closure polls use that
scope. Excluding v2 is intentional; the earlier all-protocol coverage finding
below is retained as historical evidence.

## Current implementation

The program performs one initial market scan, filters eligibility, writes the
seed, and exits. Opening and closure polling remain design proposals. The seed
is a moving HTTP view, not an atomic exchange-wide snapshot. Validation of its
retained rows and asset index did not establish complete market coverage.

## Confirmed seed coverage bug

The reviewed request in `src/polymarket/gamma.rs` omitted `version`. The server defaults to
protocol v1; having a parser for v2 does not make v2 markets appear in results.

An identifier lookup for market `5931598` with `closed=false` returned:

- No version parameter: no rows.
- `version=v2`: the market, with all five tradability flags eligible.
- `version=v1&version=v2`: the same eligible v2 market.

The market was absent from the saved seed. Two more eligible v2 examples were
`5931604` and `5931605`. Bootstrap and both future polling queries must select
the intended protocols explicitly and preserve that selection across pages.
To cover both, use repeated `version=v1&version=v2` parameters. Both-protocol
coverage was subsequently excluded from scope; the saved v1 seed remains usable.

Source: [protocol selection and server default](https://docs.polymarket.com/market-data/discover-markets#find-markets-by-oracle-request).

## Query behavior observed

Opening candidates:

```http
GET /markets/keyset?closed=false&order=acceptingOrdersTimestamp,id&ascending=false&limit=100&version=v1&version=v2
```

Explicit closures:

```http
GET /markets/keyset?closed=true&order=closedTime,id&ascending=false&limit=100&version=v1&version=v2
```

The two-page listing tests used the same ordering and page size but omitted
`version`, so their listing observations cover the default v1 view. The
separate identifier test above verified inclusion of v2 with both versions.

The opening sample contained 200 distinct open markets, all eligible, with
non-null activation timestamps in descending timestamp and numeric-ID order.
The newest example, `6168306`, was absent from the saved seed.

The closure sample contained 200 distinct closed markets with non-null closure
times. Market `5630206` was present in the saved seed and appeared closed in
this listing. Applying the closure view would remove this stale membership.

Two follow-up closure samples each contained 200 distinct IDs and descending
closure timestamps. However, each had 39 adjacent pairs that violated a
descending numeric-ID tie order. For example, `5578611` preceded `5578612`
although both reported `closedTime=2026-10-25 01:44:33+00`. An identifier-filtered
query for another equal-time group reproduced ascending IDs. The API's internal
sort implementation and timestamp precision are not established by this test.

Therefore, follow opaque cursors, deduplicate by ID, and consume the entire
timestamp bucket at the stopping boundary. Do not assume that IDs descend
inside ties, fabricate cursors, or stop at the first previously seen ID.

Source: [keyset parameters](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination).

## Conditions for the proposed loops

Start the first catch-up from before bootstrap began, with overlap, to revisit
changes during the scan. Every poll starts at the head. Follow as many cursor
pages as needed to cover the previous completed boundary and overlap; advance
the boundary only after that traversal succeeds. Reapply all tradability flags
before adding a market. Treat repeated additions and removals as idempotent.
Resolve conflicting observations rather than treating HTTP completion order
as exchange lifecycle order.

These rules cover ordinary activations and explicit closures under the
timestamp assumptions. They do not establish a lossless lifecycle feed:

- `closed=true` does not capture `acceptingOrders=false` while `closed=false`.
- An activation or reopening whose timestamp does not refresh can fall behind
  the opening boundary.
- Null timestamps, arbitrarily late indexing, and corrected/backdated fields
  can evade a finite overlap.
- A market that opens and closes entirely between polls can be absent from the
  current open listing; the closure view can remove membership but cannot
  recover historical order books.

The review confirms useful queries, not a completed continuously correct
registry. Stronger coverage needs additional evidence or reconciliation; it
does not follow from two successful pages.

Evidence: [saved bounded observations](../experiments/polymarket-lifecycle-correctness-results.json).
