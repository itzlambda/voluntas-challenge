# Protocol v1 resolution lag and retained-market backlog

Measured **2026-10-25, 09:21–10:00 UTC**, using public Gamma, Data API, and CLOB
responses. No application code changed.

The **closed-but-unresolved count was zero among all 14,182 markets in a fresh
six-hour closure census**. Separately, none of 1,673 sampled v1 resolution
timestamps was later than Gamma `closedTime`. This does not establish a global
historical zero, or measure an earlier `acceptingOrders=false` transition.

The open inventory also contains **39 confirmed unresolved markets that fail
startup admission**, plus 44 with unknown v1 token state. These are different
from a queue of recently closed markets waiting for resolution.

Evidence: [script](../experiments/polymarket-resolution-lag.py) and
[results, sample IDs, and timestamp observations](../experiments/polymarket-resolution-lag-results.json).
Per-market observation lists were trimmed to a few rows per category; the full
file is in Git history (`git show 76e65b8:research/experiments/polymarket-resolution-lag-results.json`).

## Resolution fields and a cross-protocol timestamp trap

The [documentation index](https://docs.polymarket.com/llms.txt) leads to
[market details](https://docs.polymarket.com/market-data/market-details),
[resolution](https://docs.polymarket.com/concepts/resolution), and the
[API reference index](https://docs.polymarket.com/_llms/en/api-reference/predictions.txt).
The fields used here distinguish metadata from payouts:

- Gamma `closed=true` means resolved **or otherwise closed**. `endDate` is a
  schedule, not an observed order-taking cessation time.
- Data API [`GET /v2/tokens`](https://docs.polymarket.com/api-reference/markets/look-up-tokens)
  exposes `resolved`: an on-chain payout exists. This experiment restricts
  token state to **`module=v1_ctf`**. Fractional `final_price` values allow
  terminal 50/50 payouts; those are not unresolved markets.
- [`GET /v2/resolutions`](https://docs.polymarket.com/api-reference/markets/get-resolution-state)
  documents that condition lookups prefer native V2 lifecycle, then UMA, then
  terminal CTF state. **Gamma `version=v1` does not constrain this other API.**
- The v1 UMA audit therefore uses `question_id`: Gamma `negRiskRequestID` for
  negative-risk markets, otherwise `questionID`. It takes
  `last_update_timestamp` only from a terminal `resolved` row. This is a terminal
  oracle lifecycle timestamp, not an independently recorded trading-stop time.
- For 500 crypto samples without Gamma V2 position IDs, complete token-family
  lookups also confirmed that all tokens were `v1_ctf`. Their condition-level
  terminal `resolved_at` timestamps were therefore usable without V2 ambiguity.

**488 of 1,673 samples had a condition timestamp more than one second later
than their v1 timestamp.** Market `5492370`, for example, had Gamma closure and
v1 UMA resolution at `2026-10-24 21:51:48 UTC`, while its condition lookup returned
`2026-10-25 00:27:35 UTC`: an apparent **9,347-second / 2.60-hour** delay.
That later condition timestamp must not be called this market's v1 lag.

Gamma `umaResolutionStatus` can be stale: all 901 closed `proposed` records and
both closed `disputed` records were already resolved on-chain. `resolvedBy` is
not a resolution flag either: it was populated on nine confirmed unresolved
open exceptions. Gamma `outcomePrices` is not the explicit on-chain payout test
used here.

## Closure-to-resolution timestamp distribution

The sample covers closures in the **24 hours ending 2026-10-25 09:21:44 UTC**.
Within each type, the script sorts IDs by SHA-256 and selects up to 500. All 173
longer-span markets fit under the cap. All 1,673 selected markets had resolved
v1 tokens and usable timestamps.

The table reports **signed seconds: v1 resolution timestamp minus Gamma
`closedTime`**. Negative values mean resolution preceded recorded closure, not
negative physical waiting time. Quantiles use linear interpolation.

| Type | Sample | p10 | p50 | p90 | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Crypto up/down | 500 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Sports | 500 | -8.31 | -5.10 | 0.00 | 0.00 | 0.00 |
| Other | 500 | -10.36 | -3.24 | 0.00 | 0.00 | 0.00 |
| Other, scheduled span at least 30 days | 173 | -31.09 | 0.00 | 0.00 | 0.00 | 0.00 |
| All, unweighted stratified sample | 1,673 | -8.30 | 0.00 | 0.00 | 0.00 | 0.00 |

There were **889 exact matches, 784 negative offsets, and zero positive
offsets**. The minimum was -12,947 seconds, about 3.60 hours, in the longer-span
group. The population-weighted signed mean was -5.17 seconds. The mean and
p10/p50/p90/p99/max of `max(0, signed offset)` were all zero.

This supports resolution occurring at or before recorded Gamma closure in the
measured v1 cohort. It does not measure an unobserved earlier trading pause.
The documented UMA challenge period starts at a proposal; it does not establish
a two-hour queue after Gamma closure.

Types are heuristics, not an authoritative taxonomy: up/down slug patterns take
precedence; sports uses `sportsMarketType` or `gameStartTime`; the longer-span
group uses `endDate-startDate >= 30 days` among the remainder. Rare paths can
be absent from a recent sample.

## Backlog counts

### Fresh closed-market census

A new scan at **09:47:22 UTC** covered the preceding six hours, with payout
checks completed at **09:56:05 UTC**. All **14,182 markets** supplied a v1 token
ID, and every checked `v1_ctf` token was resolved. The result was **zero
unresolved and zero unknown**, or **0% of 254,000 tradable markets**.

The separate 48-hour Gamma cohort contained **139,861 closures**, all marked
`umaResolutionStatus=resolved`. Its payouts were not exhaustively checked;
the 1,673-market sample and fresh six-hour census provide independent checks.

### Open markets failing startup admission

The full moving open view contained **241,900 markets**, including **241,793
passing** `active && !closed && !archived && acceptingOrders && enableOrderBook`.
The **107 failures (0.0442% of this open view)** reproduce the earlier exception
count, although the total catalog changed between measurements.

| Failed field | Count |
| --- | ---: |
| `acceptingOrders` | 107 |
| `enableOrderBook` | 54 |
| `active`, `closed`, or `archived` | 0 |

The failures overlap: 53 fail only `acceptingOrders`; 54 fail both fields.
Missing values fail the strict predicate and are not treated as observed
shutdown transitions.

| v1 payout state of the 107 exceptions | Count |
| --- | ---: |
| Confirmed unresolved | 39 |
| Already resolved | 24 |
| Unknown / no confirmed v1 token row | 44 |

Within this nontradable **open** inventory, the unresolved count is therefore
**39–83** depending on the unknowns: **0.0154–0.0327% of 254,000**, or
0.0161–0.0343% of the 241,793 passing markets in this moving view. This upper
bound does not apply to all historical closed markets.

Thirty of the 39 confirmed unresolved exceptions have `enableOrderBook=false`.
The other **nine have enabled CLOB books but CLOB `accepting_orders=false`**;
all nine were checked directly. Only one has recorded activation metadata:
market `3222414`, activated July 30, with a scheduled end in January 2027. Its
current inactivity is not an overdue post-closure resolution. Its actual
order-taking cessation timestamp is unavailable.

All 107 exceptions fail admission for a collector bootstrapped now. They are
not automatically subscriptions that this new process would retain. A previously
admitted market that later pauses without resolving could remain subscribed;
this snapshot does not establish how many exceptions followed that history.

### Historical coverage limit

The closed `proposed` and `disputed` cohorts were exhausted: **903 records,
all already resolved on-chain**, rather than a 903-market stuck queue.

Older closed records with missing or other resolution statuses were **not fully
enumerated**. Bringing null statuses to the front with
`order=umaResolutionStatus&ascending=false` returned HTTP 500; deep legacy
offsets returned HTTP 422. The exact global historical count of closed v1
markets without payouts remains **unestablished**. Zero in the fresh cohort
is not a bound on that historical count.

## Old, cancelled, archived, or voided markets

No observation here proves that a market will **never** resolve. The oldest
confirmed unresolved open exceptions were created about **310.7 days** earlier,
with scheduled ends about **309.7 days** earlier. Those are disabled-book crypto
records without activation timestamps or `closedTime`; their age does not
establish a 309-day trading-stop-to-resolution lag. One sampled CLOB lookup,
market `964332`, returned 404 despite known unresolved CTF tokens.

No open record failed the archive flag. Historical archived markets were not
exhaustively enumerated, so no global cancelled/archived/stuck count is claimed.
The [resolution process](https://docs.polymarket.com/concepts/resolution) allows
terminal 50/50 payouts in exceptional cases: a voided event need not remain
unresolved.

## Closure rate and rate-times-lag estimate

Windows end at **2026-10-25 09:21:44 UTC**, using Gamma `closedTime`.
These are market counts, not event counts.

| Window | Closures | Average per hour | Daily equivalent |
| --- | ---: | ---: | ---: |
| 1 hour | 2,037 | 2,037.0 | 48,888 |
| 6 hours | 15,944 | 2,657.3 | 63,776 |
| 24 hours | 99,487 | 4,145.3 | 99,487 |
| 48 hours | 139,861 | 2,913.8 | 69,930.5 |

The 24-hour mix was 93,822 sports, 3,287 crypto up/down, 2,205 other, and 173
other longer-span markets. Hour/day/two-day rates differ substantially; a
stationary closure rate should not be assumed.

Using the recorded-closure proxy, Little's-law arithmetic gives:

```text
99,487 / 24 = 4,145.29 closures/hour
weighted mean max(0, v1 resolution time - closedTime) = 0 seconds
estimated closed waiting queue = 4,145.29 * 0 / 3,600 = 0 markets
```

This agrees with the fresh six-hour payout census. It is not an independent
estimate for all paused books: their stop timestamps and duration distribution
are unavailable. It also does not bound subscriptions retained after missing
or delayed WebSocket events.

## Live observation

Market `6113723` (`btc-updown-5m-1792920300`) was polled approximately every
20 seconds with cache-busting Gamma queries, token state, and resolution state.
Its scheduled end was 09:30 UTC. At 09:32:17 it remained **visible** as open,
accepting orders, and unresolved. At 09:32:38 all three views had switched.

Both recorded closure and v1 terminal resolution were **09:31:20 UTC**, earlier
than HTTP visibility. Thus the 21-second bracket describes API visibility,
not the actual order-taking transition. No separate closed-but-unresolved
state was observed. WebSocket resolution delivery was not measured.

## Method and reproduction

The script uses only Python's standard library. Requests share a file lock and
a 0.27-second minimum spacing, below four requests/second, with bounded backoff
on 429/5xx and network failures. It uses gzip, a browser user-agent, and no
credentials. Gamma uses a run-specific dummy parameter; the fresh backlog walk
has a separate current scan. Checkpoints retain selected fields, not raw payloads.

Closed scans follow opaque cursors in `closedTime,id` descending order, consume
the full cutoff page, and then exclude timestamps outside the window. The saved
closed walk had no duplicate IDs or observed timestamp inversions. No numeric-ID
tie order is assumed. The open census unions ascending IDs up to a fixed pivot
with descending IDs above it, consuming each boundary page. It is a moving
09:21–09:56 view, not an atomic inventory; markets can change after their page
is read.

The fresh backlog phase recorded one network failure followed by a successful
retry: 426 successful requests comprised 142 Gamma pages and 284 token batches.
Saved request counters distinguish phases and resumed work; they do not count
all exploratory or interrupted requests. Failed status-sort and offset probes,
and the initial 403 without a browser user-agent, were not treated as empty data.

From the repository root:

```sh
python3 -I research/experiments/polymarket-resolution-lag.py
```

Defaults are a 48-hour closure census, full open view, up to 500 samples/type
from the last 24 hours, and a fresh six-hour payout census. Results and compact
checkpoints are written under `research/experiments/`. After an interruption,
`--resume` reuses observations and does not produce a fresh point-in-time census.
Measurement checkpoints and auxiliary phase files are Git-ignored; the primary
results JSON embeds the audit observations and summaries.
For a live observation, first run the script with
`--observe-slug CURRENT_V1_SLUG --observe-seconds 600`; the main run embeds that
saved observation. Live counts and selected markets will change.

The unresolved measurements are the exact global historical closed backlog,
including null-status records, and the distribution from an earlier
`acceptingOrders=false` transition. These results do not validate lossless
`new_market`/`market_resolved` delivery, reopening coverage, or registry
correctness without reconciliation.

Sources: [Gamma keyset API](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination),
[Gamma schema](https://docs.polymarket.com/api-spec/gamma-openapi.yaml),
[market details](https://docs.polymarket.com/market-data/market-details),
[resolution lifecycle](https://docs.polymarket.com/concepts/resolution),
[token state](https://docs.polymarket.com/api-reference/markets/look-up-tokens),
[resolution state and selector precedence](https://docs.polymarket.com/api-reference/markets/get-resolution-state).
