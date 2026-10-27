# Resolution tail and retained closed books

Measured on 2026-10-25 UTC. Public endpoints only; no collector or application
code changed. This study deliberately samples historical markets and exhausts
selected status cohorts; the companion [recent-lag study](polymarket-resolution-lag.md)
measures the recent distribution. Neither a sample maximum nor a normal dispute
timeline is a guaranteed maximum retention time.

## Documented process and bounds

The [Polymarket documentation index](https://docs.polymarket.com/llms.txt)
links the [resolution concept page](https://docs.polymarket.com/concepts/resolution).
It specifies a two-hour proposal challenge window, a second proposal after the
first challenge, and escalation to UMA DVM if the second proposal is challenged.
Thus there are **two proposal/challenge rounds in this described flow**, not an
unlimited series of ordinary challenges to one proposal. Debate takes 24–48
hours; voting approximately 48 hours. The published disputed timeline is 4–6
days. These durations begin with proposals, not `closedTime`; the page supplies
no maximum delay from trading closure to the first or replacement proposal.
Its phrase “End date — When the market is eligible for resolution” does not
promise that the result is known at that date.

The [Polymarket dispute help page](https://help.polymarket.com/en/articles/13364551-how-are-markets-disputed)
describes the same two-hour challenge and 24–48 hour discussion, followed by
roughly 48 hours of voting, but simplifies the flow by taking a dispute directly
to discussion/voting. Use the concept page for the explicit two-proposal flow;
do not interpret the help page as a separate hard bound. Winning proposals,
rejected proposals, a result that is too early, and an unknown result are
different outcomes: rejection can require a new proposal; too early means the
event has not concluded; unknown/50–50 is a terminal equal payout, not an
unresolved half-priced book. Neither page gives a maximum count of future
restarts following a too-early determination or a maximum wait for the event.

[UMA's oracle overview](https://docs.uma.xyz/protocol-overview/how-does-umas-oracle-work)
describes 24-hour commit and 24-hour reveal phases, scheduled at UTC day
boundaries, and a four-hour minimum age before entry into a voting round.
A single UMA request permits one proposal and one dispute; Polymarket's
application-level two-round flow must not be confused with repeated disputes
of that same request. The generic UMA request description and Polymarket's
adapter workflow describe different layers.

[UMA's DVM 2.0 documentation](https://github.com/UMAprotocol/uma-docs/blob/master/using-uma/voting-walkthrough/dvm-2.0.md)
says votes roll into another round if participation/agreement thresholds fail.
It describes an initial-deployment roll limit of four before deletion. This is
historical configuration, not verification of today's deployed value. The
[UMA VotingV2 implementation](https://github.com/UMAprotocol/protocol/blob/master/packages/core/contracts/data-verification-mechanism/implementation/VotingV2.sol)
has configurable `maxRolls`, automatic request deletion, and per-round capacity
limits. A limit ending in deletion is not a promise that the Polymarket market
resolves by then. Consequently neither “one vote takes 48 hours” nor “four
rolls” establishes a maximum close-to-resolution interval.

The [resolution-state API reference](https://docs.polymarket.com/api-reference/markets/get-resolution-state)
is particularly explicit: `expected_settlement_time` is **“not a guaranteed
resolution deadline”**, and clients must not **“assume that an unresolved vote
settles in its first round.”** `settlement_time_basis` identifies a live proposal
expiry (`managed_proposal_expiration`, `proposal_expiration`, or `liveness`) or
`dvm_round_estimate`, the earliest eligible voting-round end. `extended_review`
can remain true past a managed proposal's normal expiry. These fields are
estimates/status, not eviction deadlines. The documented host is
`https://data-api.polymarket.com/v2/resolutions`; it is not a CLOB listing route.
One selector family is mandatory: `question_id`, up to 20 comma-separated
conditions, or up to 20 event IDs.

Polymarket's current concept page also describes deterministic Chainlink TWAP
resolution for up/down markets, which bypasses UMA's proposal/dispute process.
That is not evidence that every historical v1 market used this path. The
resolution API includes managed proposal expiry/extended review and native V2
lifecycles; native V2 must not be pooled into this v1 study. Legacy market rules
can also mention the Markets Integrity Committee. A resolver address or
`automaticallyResolved` flag alone does not identify a uniform oracle or a
completion SLA. No source checked supplies a universal finite upper bound for
these paths.

[Polymarket's clarification help page](https://help.polymarket.com/en/articles/13364548-how-are-markets-clarified)
says clarification clears the order book and cancels resting orders, including
at an advance announcement. The concept page says clarifications preserve the
question's intent, are published onchain, and inform voters. Neither promises
a turnaround time. An empty/cancelled book therefore does not prove permanent
closure or resolution. Waiting for official data, an unfinished event, or a
market-specific fallback date can extend beyond `endDate`; the rules, not the
title or a calendar cutoff alone, determine eligibility.

## Measurement method

The saved [script](../experiments/polymarket-resolution-tail.py) uses Python's
standard library, serial requests spaced at least 0.4 seconds apart (at most
2.5 requests/s), unique dummy query parameters, and exponential backoff on
429/5xx with `Retry-After`. The [results](../experiments/polymarket-resolution-tail-results.json)
retain summaries, selected rows, and verification evidence rather than raw
catalog payloads. Per-candidate audit lists were later trimmed to a few rows per
class; the full file, which the `--tail-recheck` and `--enrich-only` follow-up
modes read, is in Git history (`git show 76e65b8:research/experiments/polymarket-resolution-tail-results.json`). No credentials or wallet identities are needed.

Gamma keyset listings use explicit `version=v1`, `limit=100`, and returned
`after_cursor` pagination. The deliberate strata are recent `closedTime`
descending, oldest IDs ascending, oldest `endDate` ascending, all closed
`proposed`/`disputed` cohorts, and additional named lifecycle statuses. The
ascending `closedTime` probe returned HTTP 500 even after backoff, so the
historical walks use ID and end date; neither is claimed to be closure order.
The generic `accepting_orders=false` query parameter was ignored in a probe;
non-accepting markets are selected by inspecting returned flags.

Resolution/token point checks use the public Data API. Requests with Python's
default or a research-specific User-Agent initially returned HTTP 403, body
`error code: 1010`; a standard browser User-Agent plus `Accept: application/json`
worked. The CLOB-hosted `/v2/resolutions` and `/v2/tokens` probes initially returned
403 too, then **404** with the corrected headers; the documented Data API host
supplies the required data. Token batches use the documented maximum of ten
conditions; an initial twenty-condition attempt returned 400 and was corrected
before the final token classification. Data API responses
are condition-grain; token rows are restricted to `module=v1_ctf`.

For actual lag, subtract Gamma `closedTime` from `resolved_at`, or, if absent,
`last_update_timestamp` **only on a resolved lifecycle row**. The latter is a
resolution-event proxy, not a measured WebSocket arrival time. Gamma
`umaEndDate`, end date, a populated `resolvedBy`, terminal-looking prices, and
`automaticallyResolved` are never used alone as proof of resolution time.
Token `resolved=false` verifies a current nonterminal condition; Gamma's
`umaResolutionStatus` is used for discovery, not final classification.

## Measured findings

The broad Gamma scan returned **24,873 distinct v1 markets**, including **21,030 closed**. It sampled 10,000 recent closures, 10,000 oldest IDs, and 10,000 oldest end dates (overlapping strata), plus 3,000 old open IDs and status cohorts. These broad walks were capped, not exhausted. The closed `proposed` cohort (901) and `disputed` cohort (2) were exhausted; the other explicitly queried lifecycle statuses returned zero. A further high-volume sample selected 100 resolved markets ending in each of 2024, 2025, and 2026.

The additional recent cross-check selected 2,000 evenly spaced records from an 80,000-row checkpoint produced by the concurrent study. It is a deterministic convenience sample, not a random sample. Its 2,991 resolved timestamp pairs include historical tail candidates; 639 intervals were negative. The 300 high-volume historical pairs added 249 negative intervals. Negative values are retained as evidence that Gamma closure and onchain resolution clocks/semantics differ; they are not zero-clamped into a fictitious latency distribution. These passes overlap and their pair counts must not be added as unique-market totals.

### Largest observed resolved intervals

The largest measured positive interval was **2.6183 hours (2 h 37 min 6 s)**: market `5534366`, with Gamma closure at `2026-10-24 21:42:30+00` and actual `resolved_at=2026-10-25T00:19:36Z`. The following are the **top 20 in the verified samples**, not a claim about the entire catalog. All twenty have `resolution_source=reported`, `was_disputed=false`, and a populated `resolved_at`; the endpoint exposes no reason for their delay. Their sports exact-score/derivative concentration also warns against extrapolating this ranking to all market types.

1. `5534366` — `fl1-rcl-pfc-2026-10-24-exact-score-3-5` — **2h 37m 06s**.
2. `5859521` — `bel2-ksc-kaa-2026-10-24-exact-score-5-5` — **2h 36m 50s**.
3. `5556414` — `scoc-str-tay-2026-10-24-exact-score-3-4` — **2h 36m 33s**.
4. `5556546` — `scoc-clyd-tra-2026-10-24-exact-score-1-5` — **2h 36m 21s**.
5. `5519018` — `spl-riy-taa-2026-10-24-exact-score-0-3` — **2h 36m 05s**.
6. `5556644` — `scoc-loc-spa-2026-10-24-exact-score-5-1` — **2h 36m 05s**.
7. `5534252` — `fl1-rcl-pfc-2026-10-24-halftime-result-away` — **2h 36m 00s**.
8. `5824169` — `isr-iks-bns-2026-10-24-exact-score-0-0` — **2h 35m 58s**.
9. `5922055` — `scoc-su-elg-2026-10-24-exact-score-5-3` — **2h 35m 33s**.
10. `5560747` — `ere-ado-her1-2026-10-24-exact-score-2-1` — **2h 35m 20s**.
11. `5824204` — `isr-iks-bns-2026-10-24-exact-score-5-5` — **2h 35m 20s**.
12. `5511686` — `por-alv-aro-2026-10-24-exact-score-0-0` — **2h 35m 06s**.
13. `5980101` — `hun-zal-kis-2026-10-24-exact-score-0-1` — **2h 34m 48s**.
14. `5540101` — `fr2-ass-nan-2026-10-24-first-to-score-away` — **2h 34m 35s**.
15. `6012618` — `es3-sp-bcf-2026-10-24-exact-score-3-3` — **2h 34m 33s**.
16. `5978253` — `irl1-ker-bra-2026-10-24-halftime-result-away` — **2h 34m 32s**.
17. `6077926` — `cafcl-est-swe-2026-10-24-exact-score-4-0` — **2h 34m 30s**.
18. `5541282` — `itsb-are-ben-2026-10-24-exact-score-5-2` — **2h 34m 20s**.
19. `6077912` — `cafcl-est-swe-2026-10-24-exact-score-0-0` — **2h 34m 15s**.
20. `5556545` — `scoc-clyd-tra-2026-10-24-exact-score-5-0` — **2h 34m 05s**.

Gamma-only timestamp comparison would have fabricated a much longer tail: the largest `umaEndDate − closedTime` was **424.6764 days**, market `246540`. That field is not a validated resolution timestamp. Of 17,086 resolved Gamma rows with both parseable fields, 13,107 differences were zero, 2,021 positive, and 1,958 negative. The top twenty proxy candidates are saved separately and were included in verification.

### Closed but unresolved: verified count and oldest list

**No closed, token-unresolved market was found in the verified tail candidates.** Counts older than **1, 7, 30, and 90 days are each 0 in this checked sample**. Thus there is no honest twenty-row list of verified closed-unresolved markets to print. The JSON contains the empty verified list and the separate raw candidate list, rather than relabelling stale metadata as a backlog. This is not a global zero-backlog assertion.

All **901 closed `proposed` and both closed `disputed` records were token-resolved**. Among other closed status candidates, 98 `settled` and two `requested` records were token-resolved; two more `settled` records were lifecycle-resolved without token coverage. Twenty additional oldest records with no clear Gamma price/status resolution evidence were also token-resolved. Together that is **1,025 checked closed candidates: 1,023 token-resolved, two lifecycle-resolved**. The naive status-only age list reaches **1,686.9 days** and reports 991/991/948/829 aged candidates beyond 1/7/30/90 days; those are stale-status candidate counts, not unresolved counts.

For example, `254671` (`will-trump-mention-biden-the-most-in-south-carolina-speech`) and `254678` (`will-trump-mention-border-the-most-in-south-carolina-speech`) still say `disputed` and `acceptingOrders=true` in Gamma. Both have token payouts and resolved lifecycle rows with `was_disputed=true`. Their years-old Gamma flags do not demonstrate years-long resolution waits.

### Open/non-accepting mismatches and unknown outcomes

The open proposed/disputed follow-up plus point checks found **847 token-unresolved markets marked `closed=false`**: **840 accepting orders** and **seven not accepting orders**. Their oldest end-date examples are saved under `oldest_end_dates_open_token_unresolved_20`; age past end date is not age since trading stopped. Seven other records marked open/accepting in the follow-up were already token-resolved, another warning against treating the flags as an authoritative trading test.

The seven independently fetched non-accepting examples are IDs **3358951–3358957**, Microsoft year-end-2026 market-cap buckets. They have `closed=false`, `acceptingOrders=false`, `enableOrderBook=true`, no activation timestamp, creation on **2026-08-05**, and last-update metadata the same day: approximately **81 days old** at measurement. This supports the existence of old inactive/non-activated books outside `closed=true`; it does **not** prove they traded and then stopped 81 days ago. The saved point checks include every slug and timestamps.

The broad closed sample also had **2,841 `acceptingOrders=true` records**. Such a flag mismatch establishes inconsistent status metadata, not that orders could actually be filled after closure. This study submits no orders and has no historical order-taking transition log. Neither direction of a long gap between flag transitions and actual trading cessation is established. The claim that these flags always flip together therefore does not hold for the public catalog, even if it holds in a recent observed subset.


## Capacity interpretation and limits

There is **no established finite worst-case retention bound** if the only exit
is a future `market_resolved` WebSocket event. The documented 4–6 day dispute
path is useful for a routine reserve, not a hard timeout. Even a resolved
market may need reconciliation after a missed event or reconnect; this
measurement does not establish WebSocket replay or delivery guarantees.

A census of currently closed and token-unresolved v1 markets would estimate
today's retained backlog; these bounded samples do not constitute that census.
For a stable population with a finite mean lag, Little's law gives
`held closed books ≈ closures/day × mean close-to-resolution days`.
Tail-biased samples cannot estimate that mean or a population p99.9. If some
markets never reach the exit condition, the retained inventory can instead
grow with collector lifetime; no finite steady-state capacity follows from
these docs. A memory budget needs a bound on retained books, a durable compact
representation, or a separately chosen retention policy. Those are design
implications, not changes implemented by this research.

This study does not establish that any sampled market will *never* resolve.
It also cannot recover the historical time that `acceptingOrders` first became
false from one snapshot. `acceptingOrdersTimestamp` denotes observed activation
metadata in these records and is not a shutdown timestamp. Creation/update
ages must not be relabelled as proven durations of a continuously disabled
order book.

## Reproduction

From the repository root:

```sh
python3 -I research/experiments/polymarket-resolution-tail.py --pages 100
```

The saved measurement also cross-checks a deterministic stride sample from the
concurrent study's recent Gamma checkpoint. To repeat that additional check
without repeating the broad tail scan:

```sh
python3 -I research/experiments/polymarket-resolution-tail.py --enrich-only \
  --snapshot research/experiments/polymarket-resolution-lag-recent-checkpoint.json
python3 -I research/experiments/polymarket-resolution-tail.py --tail-recheck
python3 -I research/experiments/polymarket-resolution-tail.py --historical-check
```

Live counts and selected rows change between runs. The optional checkpoint is
read only; none of the companion study's files are modified.
