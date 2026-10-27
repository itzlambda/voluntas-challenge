# Missing-publisher market inspection, 2026-10-27

Direct WebSocket subscriptions confirm that a healthy connection can deliver PONGs but no initial books for a market that is not accepting orders or has closed. The three short retries do not manufacture an unavailable book. This explains a class of missing publishers, but the exact 190-market historical set cannot be reconstructed from the retained aggregate metrics.

## Actual WebSocket probe

A separate local Node 24 diagnostic opened two independent sockets per market, subscribed to both outcome assets with `initial_dump=true`, sent PING every ten seconds, and observed each socket for 32 seconds. Missing outcomes received at most three unsubscribe-then-subscribe retries, with five-second response windows and checks. Every sent message and received frame is retained. This was a local probe, not another collector or AWS validation run.

| Market | Current state | Books per socket | Retries per socket | PONGs per socket |
|---|---|---:|---:|---:|
| Texas State vs. Oregon State: Game Goes to Overtime, ID 6283126 | Accepting orders; neither outcome appeared in original run | 2 on each | 0 | 3 |
| Microsoft year-end market-cap bracket, ID 3358951 | Active, order-book-enabled, not accepting orders | 0 on each | 3 | 3 |
| Silver Up or Down, October 26, 10:30–10:45 PM ET, ID 6261977 | Recently closed; published in previous run but absent from fresh output | 0 on each | 3 | 3 |
| Previously published control, ID 6283088 | Accepting orders | 2 on each | 0 | 3 |

All eight sockets opened and closed normally with code 1000, without errors. The bookless sockets continued receiving PONGs throughout the observation. This directly distinguishes transport liveness from book availability. The accepting sports market now initializes successfully, so its earlier failure is not established as a permanent inability to publish. A later successful probe cannot explain why its original baselines were missing.

REST corroborated the distinction. Both outcomes of seven non-accepting Microsoft markets returned 404 with `No orderbook exists for the requested token id`. Both outcomes of the accepting sports market returned 200 with nonempty books. Initial Python HTTP requests returned Cloudflare 403; those requests were excluded from book-availability interpretation. Ordinary curl requests worked. The sports books carried exchange timestamps near the original run's end, which does not establish when their books first became available.

## What can be identified after the run

The complete supplied-reader scans recovered 520,420 emitted symbols from the fresh run and 515,266 from the previous run. Symbols indicate that an outcome appeared at some time; they do not reconstruct final source readiness or missing physical copies.

Read-only Gamma lookups identified one never-published market among the 2,710 retained lifecycle additions. A separate current open-catalog scan retained all 2,590 raw pages, containing 258,901 rows and 258,803 eligible rows. Comparing outcome IDs with the historical output found 14 currently eligible, pre-existing candidate markets with neither outcome emitted. Of these:

- Seven explicitly report `acceptingOrders=false`, zero 24-hour volume, and zero liquidity. They were absent from both runs' output.
- Two omit the accepting-orders flag. They were also absent from both runs. Missing flags and missing volume remain unknown, not zero.
- Five currently report `acceptingOrders=true`. Two were created after the final metric scrape, so they cannot explain its 190-market deficit. The remaining examples include the sports market tested above.

Current metadata is not a frozen copy of the original catalog. These 14 are candidates, not an exact historical final-state list. Eligibility also differs from publisher readiness: the collector deliberately retains active, order-book-enabled markets without requiring `acceptingOrders=true`.

A bounded scan of the 5,000 newest-ID closed v1 listings found another 197 candidate markets. Both outcomes appeared in the previous run, while neither appeared in the fresh run. Their recorded closure times were within five minutes before startup or during the discovery and connection ramp: 85 before startup, 101 during the initial 260.8-second scan, and 11 shortly afterward. All closed within about 281 seconds of startup, before the physical connection ramp had settled. None was a retained lifecycle addition in the fresh run.

This is evidence for closure during discovery or startup, alongside the non-accepting market examples. It does not prove that every candidate belonged to the fresh run's desired set. The original catalog pages and per-market pending outcomes were not captured. In particular, the 197 recently closed candidates cannot be added to the 14 current open candidates and called the exact 190 missing publishers.

## Interpretation and retained evidence

Low trading activity alone is not a sufficient explanation. Earlier quiet-market tests received explicit empty full books, which initialize readiness. This probe instead found unavailable books for representative non-accepting and closed markets, despite healthy WebSockets and retries. Some missing candidates also had successfully published in the previous run, so they were not permanently non-publishing markets.

The exact increase from 36 to 190 remains unproved. The evidence supports separating unavailable or recently closed markets from accepting markets with unexplained initialization failures. No eligibility, discovery, retry, or production instrumentation changes were made.

Ignored evidence is under `data/aws-main-retry-20261027T023925Z/market-inspection/`: raw Gamma pages and metadata, current and previous emitted-symbol sets, market-level CSVs, REST responses, WebSocket cases and script, raw frames, summaries, and verification. `all-candidate-markets.csv` includes the candidate category, current flags, volume, liquidity, closure time, and whether the previous run emitted its outcomes. The CSV is a candidate inventory with the limits above, not a definitive missing-publisher ledger.


## Fresh enabled-but-not-accepting WebSocket test

A second direct probe selected markets by metadata flags alone, without conditioning on whether the earlier run published them. The saved open v1 catalog had seven explicit matches, all belonging to the Microsoft year-end market-cap event, IDs 3358951 through 3358957. Fresh Gamma lookups before and after the test confirmed `active=true`, `closed=false`, `enableOrderBook=true`, and `acceptingOrders=false` for all seven.

Each market used two independent WebSocket connections subscribed to both outcomes for 60 seconds. Every one of the 14 test sockets received an empty JSON array after the initial subscription and after each of three unsubscribe-then-subscribe retries. None received a `book` event for either outcome. Each received five PONGs, reported no errors, and closed normally. An accepting sports control, ID 6283126, received both outcome books on each of its two connections without retries.

An empty response array is not an explicit empty book: it contains no outcome snapshot to initialize. The supported conclusion is that these enabled-but-non-accepting listings did not provide WebSocket books during the observation. This does not prove permanent book absence or establish behavior across unrelated events. It does demonstrate that `enableOrderBook=true` alone did not guarantee an available initial snapshot in this test.

Fresh metadata, case definitions, script, raw WebSocket frames, and verified per-market results are retained in `data/aws-main-retry-20261027T023925Z/market-inspection/ws-enabled-not-accepting-20261027T034912Z/`. No collector code or eligibility rules changed.


## Fresh trade-ready predicate check

A further direct WebSocket check selected six v1 markets from six spread-out catalog pages and freshly required `active=true`, `acceptingOrders=true`, `closed=false`, and `archived=false`. Selection did not use previous publication results. `enableOrderBook` was recorded but was not part of the selection predicate. The markets were IDs 559651, 4664772, 5838774, 5996614, 6209270, and 6288194.

Each market was observed for 60 seconds on two independent local WebSocket connections subscribed to both outcomes. All six markets delivered both outcome books on both feeds without any retry. All twelve connections answered five PINGs, reported no errors, and closed normally. Fresh metadata after the test still satisfied all four requested flags for every market.

This sample supports the documented trading-readiness predicate. It does not establish universal baseline delivery or reconstruct the original 190-market gap. The collector filter remains unchanged. Metadata, selection method, raw frames, script, and verified results are retained in `data/aws-main-retry-20261027T023925Z/market-inspection/ws-trade-ready-20261027T035451Z/`.


## Eligibility and lifecycle admission implementation

After these diagnostic probes, the user approved replacing the Gamma eligibility gate with `acceptingOrders=true` and queueing lifecycle hints until metadata confirms trading eligibility. The implementation now keeps pending markets outside the desired set, performs paced batches of up to 100 with a five-second recheck delay, and uses authoritative Gamma outcome IDs. Duplicate hints are coalesced. Resolutions and metadata-confirmed terminal states stop waiting; incarnation and completed-scan fences reject stale lookup results. Lookup requests use fresh cache-busting nonces and the existing shared request budget. No further AWS validation was run for this implementation.
