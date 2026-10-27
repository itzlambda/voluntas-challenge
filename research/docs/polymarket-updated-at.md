# Incremental tradability tracking with updatedAt

Reviewed 2026-10-25, protocol v1 only. This is a researched design; the current
application still performs the initial seed and exits. No production collector
or polling implementation was added in this investigation.

## Decision

Use two shared listings ordered by `updatedAt` as the normal incremental path,
one for currently open records and one for currently closed records. Reapply
the complete eligibility predicate to every returned record. This generalizes
the earlier activation-time/closure-time proposal without adding polling loops.

```http
GET /markets/keyset?version=v1&closed=false&order=updatedAt,id&ascending=false&limit=100
GET /markets/keyset?version=v1&closed=true&order=updatedAt,id&ascending=false&limit=100
```

This is suitable for observed ordinary changes. It is not a documented lossless
change feed, so retain reconciliation as a repair path. Do not claim a permanent
correctness guarantee from this experiment.

## Evidence

Two bounded runs each made 77 successful requests, with no HTTP errors. They
included initial 1,000-row open and closed listings, a recent-closure sample,
and seven rounds observing selected markets and recent listing pages. Each
run's observation phase lasted about two minutes. Raw observations are in the
Git-ignored `data/` directory; result summaries and the analyzer are retained.

In the fresh-query run:

- 3,041 distinct markets were observed across listings and identifier lookups.
- Repeated observations captured 14 distinct open-to-closed transitions. Each
  changed `closed` to true and `acceptingOrders` to false; each advanced
  `updatedAt`.
- Comparing observed records with the saved seed found 861 changed-status
  markets, all with later `updatedAt` values. These observations overlap the
  live-transition evidence and must not be added together as independent cases.
- Sampled listing timestamps were descending, with no null timestamps or time
  inversions. This is sample coverage, not validation of the entire catalog.
- 700 repeated observations advanced `updatedAt` without changing our five
  eligibility flags. General updates therefore include non-lifecycle activity.
- No pause-only, reopening, activation, archival, or order-book-disable
  transition was observed. Their timestamp-refresh semantics remain unproven.

For example, market `6160511` was observed open and accepting orders at revision
`2026-10-25T02:28:38.341729Z`, then closed and not accepting orders at revision
`2026-10-25T02:29:30.026111Z`. Its old activation timestamp was unchanged.

The pinned cohorts alone had no state transitions. The 14 transitions
above came from repeated IDs across the full fresh listing capture. An analysis
limited to the pinned-cohort counter would incorrectly report no transitions.

Evidence: [analysis](../experiments/polymarket-updated-at-analysis.json),
[fresh-run results](../experiments/polymarket-updated-at-fresh-results.json),
[ordinary cached-run results](../experiments/polymarket-updated-at-results.json).

## Cache freshness is a required part of polling

The tested listing responses advertised `Cache-Control: public, max-age=300`.
An ordinary repeated request returned `Age: 130` and `X-Cache: Hit from
cloudfront`; its Date and first record were still from over two minutes earlier.
Adding request headers `Cache-Control: no-cache` and `Pragma: no-cache` did not
refresh that response.

Adding a unique experimental query parameter returned `X-Cache: Miss from
cloudfront`, no Age header, a current Date, and a newer first record. All 77
requests in the fresh-query run were cache misses. Pagination still succeeded.
This cache-busting behavior is observed, not a documented Gamma feature or a
guarantee of zero database/indexing lag.

The ordinary cached run also produced 56 cases where a later response carried
an older `updatedAt` for a previously observed market. No such regressions were
observed in the fresh-query run. This is response-order regression, not proof
that the source database moved its own timestamp backwards.

For implementation, use a fresh per-poll query nonce, preserved across that
poll's pages, and inspect Date/Age/cache headers. Test the mechanism during
integration; do not silently assume that no-cache request headers work. Never
advance progress using local wall time merely because HTTP returned 200.
If cached reads are accepted, their cache horizon must be included in the
overlap and expected detection delay. A five-minute cache can matter greatly
for a five-minute market.

Evidence: [direct cache controls](../experiments/polymarket-updated-at-cache-results.json).

## Registry and checkpoint rules

1. Start each poll at the head. Follow opaque cursors until the previous
   completed updated-time boundary plus overlap is covered. Process the entire
   boundary timestamp bucket; do not assume descending IDs within equal times.
2. Keep separate progress for open and closed listings. Complete all necessary
   pages before advancing either boundary. Retrying a failed traversal replays
   rows safely. Base progress on observed revisions, not request completion time.
3. Upsert by market ID and re-evaluate active, closed, archived, acceptingOrders,
   and enableOrderBook. Eligible records enter the tradable registry; other
   observed records remove existing membership and asset mappings.
4. Keep a last-applied revision for each observed market, including removed
   markets. Ignore older observations. Equal timestamps with contradictory
   status require a fresh targeted lookup, not arrival-order precedence.
5. Catch up changes spanning bootstrap, starting before its start boundary with
   overlap. A complete full scan does not produce an atomic exchange snapshot.
6. Reconciliation remains necessary for unproven timestamp coverage, missing
   timestamps, late/backdated records, or a prolonged polling gap. Use a shared
   catalog repair scan and targeted checks for discrepancies. Its cadence is a
   separate policy choice; no fixed schedule is established by this experiment.

These rules avoid continuous per-market status polling. The normal listing
work follows recent updates, although unrelated metadata churn can increase it.
The observation does not establish a worst-case workload or a latency guarantee.

## What the contract does and does not say

The official OpenAPI schema exposes nullable `updatedAt`,
`acceptingOrdersTimestamp`, and `closedTime` fields. It does not specify that
every relevant transition advances these fields, that publication delay is
bounded, or that timestamps form a durable event log. Finite overlap therefore
cannot prove lossless coverage for arbitrary delay or corrections.

The open listing did expose disabled order-book records and records not yet
accepting orders. Probes sorted by active/archived returned only active,
unarchived rows; they did not prove visibility of every inactive/archived case.
Do not overstate this visibility test as complete lifecycle coverage.

Sources: [Gamma OpenAPI](https://docs.polymarket.com/api-spec/gamma-openapi.yaml),
[keyset listing](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination),
[status definitions](https://docs.polymarket.com/market-data/market-details#market-status).
Evidence: [status-visibility probes](../experiments/polymarket-status-visibility-results.json).

Scheduled `endDate` is not a substitute for trading flags: some selected
records remained open and accepting orders after their scheduled end.

## Reproduction

From the repository root, with the existing saved seed:

```sh
python3 research/experiments/polymarket-updated-at.py --duration 120 --interval 20
python3 research/experiments/polymarket-updated-at.py --duration 120 --interval 20 --fresh
python3 research/experiments/polymarket-updated-at-analyze.py
```

Each observation run writes a distinct raw JSONL capture under `data/` and
updates its named result summary under `research/experiments/`. The analyzer
checks request accounting and includes repeated IDs from all response types.
