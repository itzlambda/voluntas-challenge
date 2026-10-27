# Polymarket subscription capacity experiment

Public-network measurement on 2026-10-24 using Node 24.22.1, with one candidate and one control WebSocket on one event loop. No production settings changed.

## Saved bounds for future scaling

These results cover two separate experiments on 2026-10-24. Each binary market
has two outcome assets, and each asset has its own book with bids and asks.
The actual subscription limit, per-IP connection limit, and maximum reliable
operating capacity remain unknown.

### Subscriptions per WebSocket

- **Largest initialized batch: 20,000 markets, or 40,000 asset subscriptions,
  on one socket.** Every requested asset delivered a valid initial snapshot.
  This was the experiment's guard; higher counts were not tested. The server
  subsequently closed the connection with `1013: slow consumer: send buffer full`,
  so this count establishes acceptance only.
- The delivery binary search initially bracketed 5,120 to 5,200 markets, but
  the 5,120 retry also failed with `1013`. There is no reproducible maximum
  sustainable batch size from this search.
- **Provisional operating batch: 100 markets, or 200 assets.** Five completed
  20-second observations matched all 7,254 control-probe occurrences. Median
  run p99 added lag was 17.1 ms, with run p99s ranging from 6.0 to 493.8 ms.
  Five completed 1,000-market observations also matched all sampled probes;
  the measurements did not establish a latency winner between these sizes.

The [subscription results](../experiments/polymarket-subscription-capacity-results.json)
retain all 38 trials, including failures. See [the delivery search](#acceptance-and-binary-delivery-search)
and [relative delivery measurements](#relative-delivery-and-headroom) for the method.

### Concurrent WebSockets from one observed IP

- **Largest admitted population: 16,000 sockets**, with one distinct binary
  market and two assets per socket. The five-minute observation recorded five
  transport resets, and one socket failed the final heartbeat check. This
  establishes admission, not reliable operation at that count.
- **Largest population to pass the final health check: 14,000 sockets.** The
  300-second observation had no transport resets and received approximately
  5.97 MB/s and 9,676 market-update events/s. All current sockets were initialized
  and had a `PONG` no older than 15 seconds at the final check. During the window,
  lifecycle replacements reduced the initialized population as low as 13,990,
  and 30 participating sockets had stale-heartbeat intervals. This is the largest
  observed final-check pass, not a continuously healthy or proven reliable maximum.
- No explicit per-IP cap rejection occurred. The 32,000 target was not attempted
  after the 16,000 health failure. Connections shared the same observed IPv4
  egress before and after each run; the observation cannot prove every upgrade's
  public source address behind dynamic NAT.

The [connection-scale results](../experiments/polymarket-subscription-capacity-connections-scale-results.json)
retain passes and failures. See [the continuation beyond 4,000 connections](#continuing-beyond-4000-connections)
for population intervals, heartbeat checks, resource use, and reproduction commands.

### Scope of these bounds

The connection-count tests selected markets by reported 24-hour volume and used
one market per socket. The subscription tests put many markets on one socket.
The results cannot be multiplied into a claim about many heavily subscribed
sockets. Both used a Node capture client on a shared six-core host, rather than
the Rust collector maintaining books and writing SBE. Five-minute health checks
do not establish long-term reliability, low update latency, or lossless delivery.
At 200,000 binary markets, the provisional 100-market batch implies about 2,000
sockets. That combined workload remains unverified. No production setting changed.

## Original subscription recommendation

Recommend **100 binary markets, or 200 outcome-asset subscriptions, per connection as a provisional operating point** for the observed workload. Five balanced 20-second observations completed, with all 7,254 control probe occurrences matched. This does not establish a latency guarantee or sustainable capacity for equally active markets.

One socket delivered initial books for **20,000 markets, or 40,000 assets**. This is an acceptance lower bound at the experiment's guard, not an exchange maximum. At 10,240 and 20,000 markets, every book arrived, then the server closed the candidate with `1013`, reason `slow consumer: send buffer full`. A 10,240-market repeat reproduced the failure. Each asset identifies one book with both bids and asks.

## Acceptance and binary delivery search

Gamma discovery fetched 20,000 unique eligible binary markets in 100-market pages ordered by ascending ID. Eligibility requires `active`, `acceptingOrders`, and `enableOrderBook`, with `closed` and `archived` false. Identifier parsing uses v1 `clobTokenIds` and v2 `positionIds`. All sampled markets were v1, so live v2 behavior remains untested.

Five markets from the first page ordered by descending `volume24hr` supply ten fixed probe assets. Their market IDs are `5534599`, `562829`, `5534122`, `601826`, and `5537168`. Both outcomes appear first in every candidate; the control subscribes only to these ten assets.

The exponential acceptance sequence tested 10, 20, 40, 80, 160, 320, 640, 1,280, 2,560, 5,120, 10,240, and 20,000 markets. Every size delivered all requested books. There was no upper rejection bracket for a binary search of acceptance. The 160-market trial subsequently closed with `1006` and no reason; larger successful trials rule out interpreting that as a count cap.

A separate binary search tested connection survival through initialization, five seconds of warmup, five seconds of observation, and three seconds of matching grace. A completed 5,120-market trial and failed 10,240-market trials supplied the starting bracket. Tests at 7,680, 6,400, 5,760, 5,440, 5,280, and 5,200 markets all received every snapshot and then closed with `1013`. The initial observed bracket was 5,120 to 5,200 markets, or 10,240 to 10,400 assets. Endpoint repeats at both 5,120 and 5,200 also closed with `1013`. The successful lower endpoint therefore did not reproduce: **no reliable delivery knee or exact count limit was established**.

## Relative delivery and headroom

The five balanced 100-market runs had a median across run medians of **+0.345 ms**, ranging from -0.967 to +1.118 ms. The median across run p99 values was **17.070 ms**, with run p99 values ranging from **6.048 to 493.848 ms**. Traffic ranged from **24,467 to 56,693 bytes/s**, or **34.2 to 85.15 events/s**. All 7,254 control occurrences matched. There were three adjacent per-asset order reversals across matched payloads. These observations support complete sampled coverage, not an ordering or tail-latency guarantee.

At 1,000 markets, five completed observations matched all **3,133** control occurrences, with zero adjacent per-asset order reversals. The median across run medians was **-0.115 ms**; the median across run p99 values was **18.261 ms**, ranging from **6.709 to 766.020 ms**. Traffic ranged from **15,490 to 253,748 bytes/s**, or **26.6 to 390.6 events/s**. Its fifth scheduled observation lost both feeds with `1006` and no reason; this cannot be attributed to candidate size. The later retry had only 74 probe occurrences and much quieter overall traffic, so the five runs are not equally loaded.

Final probe metadata revalidation found market `5534122` closed and no longer accepting orders; the other four probes remained eligible and all identifiers were unchanged. The catalog is a discovery-time snapshot. The late retry therefore does not establish capacity for five still-active probe markets. Its full book snapshots were nevertheless present, and this lifecycle change is not counted as a subscription rejection. See `probe-revalidation.json`.

Candidate initialization at 100 markets took 129 to 627 ms; at 1,000 it took 467 to 580 ms. Steady event-loop p99 ranged from 3.73 to 9.76 ms at 100 markets and 2.72 to 10.10 ms at 1,000. Maximum observed steady callback work was 66.3 ms versus 391.6 ms; maximum event-loop delay was 192.2 ms versus 673.7 ms. These include shared-host and capture overhead, so they identify local stalls without proving that subscription count alone caused them. Byte rates count received UTF-8 payloads, excluding transport overhead.

Large tail variation and sign changes in candidate-minus-control lag prevent attributing a precise latency knee to batch size. Keeping the provisional batch at 100 limits initialization bursts and is below the observed thousands-market backpressure region. The extra quiet markets do not establish equivalent headroom in update throughput. Five runs per size are evidence for these operating conditions; there is no demonstrated performance winner or reliable tail-latency guarantee.

## Measurement and failure handling

Subscriptions contain `type:"market"`, `assets_ids`, `initial_dump:true`, and `custom_feature_enabled:true`; each socket sends `PING` every ten seconds. Acceptance requires a book for every requested asset on both sockets. Missing assets trigger bounded metadata revalidation; no completed capacity trial had missing assets. The final harness also checks that snapshots contain both bid and ask arrays.

Receipt time is captured as the first operation in each callback using one process monotonic clock, before parsing or disk writes. Offline comparison canonicalizes every payload field. `price_change` entries are compared individually with their full envelope, preserving price, size, side, hash, timestamp, and occurrence multiplicity. A fixture verifies distinct entries sharing an asset, timestamp, and hash; identical repeated occurrences; ordering; and unrelated global notifications.

Lag is candidate callback receipt minus matching control callback receipt. Negative lag means the candidate arrived first. This is relative delivery and callback scheduling, including server and network variation. It never subtracts exchange timestamps from client UTC clocks. Steady windows start five seconds after both feeds finish initialization. Three-second margins support boundary matching. Global `new_market` messages are excluded from probe comparisons but included in traffic totals.

Balanced rounds give both batch sizes the same socket-open order and reverse that order between rounds. Earlier `alternating-*` pilots correlated opening order with size and are retained but excluded from the recommendation. Interrupted or incomplete observation windows are excluded from latency summaries, while their complete failure evidence remains saved.

Order-reversal counts cover all matched per-asset notification types, including summaries and trade notifications. They do not necessarily describe a reversal between two book-mutating updates or a disagreement in reconstructed book state. The trials do not demonstrate a statistically reliable latency winner between 100 and 1,000 markets.

Raw frames and per-trial results retain requested and received counts, close codes and reasons, errors, initialization, bytes, event types, callback work, CPU, sampled RSS, and event-loop delay. Balanced runs record steady CPU and event-loop delay separately. Callback summaries exclude partial one-second buckets. Event-loop delay has a one-millisecond sampling baseline.

Trials are serial, with two sockets at most. Normal backoff is two seconds; failures back off five seconds. Client guards are 30 seconds for initialization, 64 MiB per frame, 32 MiB queued output, 768 MiB received per trial, and 1 GiB RSS. Timeouts and client guards are inconclusive for a server cap. The `1013` failures were server close frames, not these guards. Explicit throttling indications stop the final harness.

## Reproduce and inspect

Run from `research/` with Node 24 and Python 3. No credentials or WebSocket packages are needed. Choose a fresh directory; the harness writes a directory-local `.gitignore` excluding raw contents.

```sh
node experiments/polymarket-subscription-capacity.mjs discover recordings/subscription-capacity-REPEAT 20000
node experiments/polymarket-subscription-capacity.mjs search recordings/subscription-capacity-REPEAT 20000
node experiments/polymarket-subscription-capacity.mjs bisect recordings/subscription-capacity-REPEAT 5120 10240
node experiments/polymarket-subscription-capacity.mjs repeat recordings/subscription-capacity-REPEAT 100,1000 20 5
node experiments/polymarket-subscription-capacity.mjs trial recordings/subscription-capacity-REPEAT 5120 5 endpoint-repeat
node experiments/polymarket-subscription-capacity.mjs trial recordings/subscription-capacity-REPEAT 5200 5 endpoint-repeat
node experiments/polymarket-subscription-capacity.mjs trial recordings/subscription-capacity-REPEAT 6400 5 profile-excluded
python3 experiments/polymarket-subscription-capacity-analyze.py recordings/subscription-capacity-REPEAT
python3 experiments/polymarket-subscription-capacity-analyze-test.py
python3 experiments/polymarket-subscription-capacity-verify.py recordings/subscription-capacity-REPEAT
python3 experiments/polymarket-subscription-capacity-analyze.py recordings/subscription-capacity-REPEAT experiments/polymarket-subscription-capacity-results.json
```

Derive `bisect` endpoints from the current run's evidence. It resolves an observed survival bracket to at most 80 markets. Exclude `profile-excluded` trials from latency comparisons.

Raw evidence is in `recordings/subscription-capacity-20261024/`: `catalog.json`, `protocol.json`, `search.json`, `delivery-search.json`, `trials.jsonl`, and root `analysis.json`. Timestamped directories contain `requested.json`, `candidate.jsonl`, `control.jsonl`, `result.json`, and completed-window `analysis.json`. The profile is sanitized before saving. Two `discovery-*-failed.jsonl` files preserve repeated-page detection with incorrect cursor parameters before using the documented `after_cursor` parameter. The shareable results are in `experiments/polymarket-subscription-capacity-results.json`. Offline verification confirmed valid bid and ask snapshots for every requested asset in all **38 trials**, including the 40,000-asset trial; no malformed snapshots were found.

## Limits and sources

The Node client parses JSON and records raw frames on one event loop. Production Rust has different allocation, parsing, and storage costs. The excluded 6,400-market profile collected 2,180 samples: 62.0% idle, 13.5% directly in the message callback, 6.8% in the raw-output writer, and 2.5% garbage collection. These are self samples including connection setup and shutdown. They show client work but do not establish a CPU saturation ceiling. Server slow-consumer errors identify connection backpressure; they do not establish the first bottleneck in the server, route, decoding, garbage collection, or storage. Snapshot bursts differ from steady updates. Same-process feeds share callback stalls, potentially masking common delay. Historical volume does not guarantee current activity.

The shared host exposes six logical cores. Initial load averages were 0.41, 0.78, and 0.81; background processes were present. This is observed public traffic, not controlled saturation. The result does not establish throughput for 200,000 live markets, 400,000 books, or one host handling that catalog. Production replay with controlled activity, startup bursts, storage backpressure, and independent receipt observation remains necessary.

The [official market-stream documentation](https://docs.polymarket.com/market-data/realtime-data) supports the endpoint, IDs, subscription, and heartbeat. The [official keyset documentation](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination) supports discovery and `after_cursor`. Capacity findings come from the saved experiment, not a documented cap.

## Connections from one IP

A separate public-network experiment on 2026-10-24 held **1,024 simultaneous WebSockets**, each subscribed to a different binary market and its two outcome assets. Every connection delivered both valid bid/ask snapshots and remained healthy throughout a **60.018-second hold at the full count**. There were no rejected upgrades, socket errors, unexpected closes, or client guard failures. This establishes a tested lower bound of 1,024 concurrent connections under the observed load. It does not establish an absolute IP limit; the run stopped at its chosen guard.

The [official rate-limit documentation](https://docs.polymarket.com/api-reference/rate-limits) describes IP-based request-rate limits. Neither that page nor the market-stream page specified a concurrent market-WebSocket cap when checked. Request rate, subscriptions per socket, and concurrent sockets are separate limits.

One Node process paced new connections at roughly two per second. It held all earlier connections while increasing the count through 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, and 1,024. Every intermediate stage completed a 15-second hold with all sockets healthy. No connection was replaced or automatically retried. All 1,024 upgrades returned HTTP `101`. Markets were eligible when discovered; this measures live connections and does not guarantee unchanged market eligibility throughout the run.

Each socket requested `initial_dump:true` and `custom_feature_enabled:false`. This avoids multiplying optional global market-lifecycle broadcasts across every connection while retaining the book feed. Application `PING` runs every ten seconds, with an immediate first ping. Offline verification checks the actual snapshots, matching asset IDs, fresh `PONG` receipts, heartbeat gaps of no more than 15 seconds throughout each hold, and absence of closes during every claimed concurrent stage. It also reconciles raw received bytes with the recorded counters.

The test used one local IPv4 transport address. The exchange hostname's Cloudflare trace endpoint observed the same public IPv4 address before and after the run; actual addresses were compared only in memory and were not saved. This supports the same-egress interpretation. It cannot independently prove the public source address of every upgrade behind a dynamic NAT. The initial harness's separate per-WebSocket address-extraction field was unavailable; the underlying transport-address count and both public-egress checks were recorded. The rerunnable harness corrects that diagnostic field and archives its own source. The exact measured source is preserved as `harness.mjs`, with its hash checked by the analyzer.

At the full count, traffic was **203,360 received payload bytes/s**, steady CPU consumption was approximately **0.40 of one core**, and the process had **1,048 open file descriptors**. Event-loop delay p99 was **32.6 ms**, with a **490.5 ms** maximum. Healthy heartbeats therefore do not establish low update latency. Most additional markets were selected by ascending ID and may be quiet. This run does not validate equally busy feeds, 1,024 sockets each carrying 100 markets, or throughput for 200,000 live markets. It is one ramp, not repeated saturation trials.

All 1,024 sockets were closed after the experiment. Raw data is Git-ignored in `recordings/subscription-capacity-connections-20261024/`. `result.json` retains every connection's counts, heartbeat receipts, errors, and closes; `stages.json` records each hold; `frames.jsonl` and `events.jsonl` retain the evidence; `verification.json` records the independent checks. Shareable results are in `experiments/polymarket-subscription-capacity-connections-results.json`. The committed connection exports keep aggregates and stage summaries; their per-connection records were trimmed and remain in Git history (`git show 76e65b8:research/experiments/<file>`). Production code and settings were not changed.

Use a fresh output directory to reproduce the bounded ramp:

```sh
node experiments/polymarket-subscription-capacity-connections.mjs recordings/subscription-capacity-connections-REPEAT 1024 60
python3 experiments/polymarket-subscription-capacity-connections-analyze.py recordings/subscription-capacity-connections-REPEAT experiments/polymarket-subscription-capacity-connections-results.json
```

## High-volume 2,000- and 4,000-connection observations

The follow-up on 2026-10-24 admitted **4,000 concurrent market WebSockets from the observed public IPv4 egress**. Both requested connection-count targets received five-minute observations, with one binary market and two outcome assets per socket. No explicit connection-cap or rate-limit rejection occurred. This is a tested admission lower bound, not an exchange maximum or a reliability guarantee.

Markets were ranked by Gamma `volume24hr` descending, with the same eligibility and identifier rules described above. Each run fetched 4,500 eligible candidates, including ranked reserves for replacements. This is a much busier selection than the earlier ascending-ID sample. Historical trading volume is a proxy for activity, not a ranking of instantaneous quote rates or a proven worst case. Pagination is not an atomic market-wide snapshot.

At the **2,000 target**, the verified observation lasted **300 seconds**. All 2,000 initialized sockets were present for **297.126 seconds, or 99.04%** of the window. Market lifecycle replacements briefly reduced the initialized population to **1,996**; the longest population dip lasted **0.907 seconds**. Nine complete interior traffic buckets covered 270.022 seconds and averaged **2,592,921 payload bytes/s** and **4,182 market-update events/s**. There were no transport errors in this window. One retiring market stopped answering heartbeats for **65.622 seconds** before its explicit `all subscribed assets resolved` close; after the 15-second freshness allowance, it was stale for 50.622 seconds. This prevents describing the whole population as continuously healthy.

At the **4,000 target**, the completed repeat lasted **300.000 seconds**, receiving **1,090,939,110 payload bytes**, or **3,636,458 bytes/s**. It counted **1,760,246 market-update events**, or **5,867/s**, and **3,502,512 price-change entries**, retaining entry multiplicity. All 4,000 initialized sockets were present for **292.914 seconds, or 97.64%** of the window. Lifecycle replacements briefly reduced the population to **3,992**; the longest dip lasted **1.628 seconds**. There were **42 lifecycle retirements during this hold**, with no transport resets, stale-heartbeat intervals, or non-lifecycle closes in the completed repeat. Of the final 4,000 population members, 3,744 delivered updates during the hold.

The 4,000 repeat consumed approximately **0.84 CPU core** and peaked at **923 MiB RSS**. At the full count it had **4,024 open file descriptors**. Event-loop delay p99 was **44.6 ms**, with a **664.3 ms** maximum. Healthy heartbeats and admitted connections do not prove low update latency or lossless delivery; this trial had no redundant control feeds for payload coverage comparison.

The **first 4,000 attempt failed before five minutes**: an `ECONNRESET` triggered shutdown after **181.356 seconds** at the target count. A second reset was recorded during cleanup. The failure is retained, and it is not classified as an IP-cap rejection. The completed repeat allowed at most eight code-`1006` transport replacements with five-second backoff, but used none. Its effective ramp was approximately 20 new connections/s with at most 16 initializations in flight. `measurement-notes.json` documents an overwritten ramp descriptor in that run; the captured source and observed scheduling are authoritative. Original data is unchanged, and the current rerunnable harness emits the corrected descriptor.

Preliminary runs also exposed ordinary market resolutions, a stream/Gamma lifecycle disagreement, and stale discovery entries whose book requests returned `404`. Those attempts and audits remain saved. The final method replaces only explicit lifecycle/unavailable-book cases and records the metadata evidence. Fixed observation windows preserve every population dip. Restarting the timer after every resolution made an unchanged five-minute cohort impractical for these active markets and was not used as the final reporting criterion.

The 2,000 observation is independently reconstructed from its recorded first five-minute wall-clock window in `recordings/subscription-capacity-connections-active-20261024-final/observation-window.json`. That run's legacy `maximum_passed:0` refers to its stricter unchanged-cohort gate; its separately verified `maintained_observations` contains the completed 300-second observation. The 4,000 repeat is in `recordings/subscription-capacity-connections-active-20261024-4000-repeat/`. The earlier failed 4,000 trial is in `recordings/subscription-capacity-connections-active-20261024-4000/`. The [combined results](../experiments/polymarket-subscription-capacity-connections-active-results.json) link these measurements and their limitations.

All received frame bytes and update events were counted. Initial snapshot frames and every `PONG` were retained; other update payloads were sampled at most once per connection per ten seconds to bound disk overhead. The analyzer reconstructs actual population intervals from raw book, heartbeat, and close receipts and reports heartbeat freshness separately. Every measured source version is archived and hashed. Actual IP addresses are not saved; exchange-edge checks agreed before and after each completed run, and WebSocket diagnostics observed one local IPv4 source address.

All experimental sockets and processes were stopped. No production code or settings changed. A true worst case, a hard per-IP cap, ten-minute stability, and throughput for 200,000 live markets remain unestablished.

To reproduce both fixed observation windows in one bounded run:

```sh
node experiments/polymarket-subscription-capacity-connections.mjs recordings/subscription-capacity-connections-active-REPEAT 4000 300 active-observe 2000
python3 experiments/polymarket-subscription-capacity-connections-analyze.py recordings/subscription-capacity-connections-active-REPEAT experiments/polymarket-subscription-capacity-connections-active-repeat-results.json
python3 experiments/polymarket-subscription-capacity-connections-analyze-test.py
```

## Continuing beyond 4,000 connections

The continuation on 2026-10-24 admitted **16,000 simultaneous WebSockets**, carrying **16,000 distinct binary markets and 32,000 outcome assets**, through the same observed public IPv4 egress. The **16,000 target completed a five-minute observation but failed its final heartbeat health gate**. Five transport resets occurred during that window and were replaced. The final population had 16,000 open, initialized sockets, but one socket's last `PONG` was **19.350 seconds** old, exceeding the 15-second allowance. This is an observed operational failure, not evidence of an exchange IP cap. No explicit concurrent-connection rejection, HTTP `429`, or overload close established a provider limit.

Each completed target below was observed for at least 300 seconds after initialization. Lifecycle replacements continued during the fixed window; the full population was therefore not uninterrupted. “Final gate” means every current socket met the endpoint health check at the end; heartbeat freshness throughout the window is measured separately.

| Target markets / sockets | Outcome assets | Observation | Final gate | Time with full initialized population | Minimum initialized population | Payload bytes/s | Update events/s | Transport resets in window |
| ---: | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| 8,000 | 16,000 | 300.102 s | Passed | 292.005 s (97.30%) | 7,992 | 4,727,594 | 7,669 | 0 |
| 14,000 | 28,000 | 300.050 s | Passed | 284.647 s (94.87%) | 13,990 | 5,971,482 | 9,676 | 0 |
| 16,000 | 32,000 | 300.041 s | Failed | 262.339 s (87.43%) | 15,989 | 5,832,929 | 9,458 | 5 |

At 8,000, 14,000, and 16,000, respectively, all initialized sockets also had fresh heartbeats for **269.042, 277.709, and 256.996 seconds**. There were **53, 30, and 4** participating sockets with at least one stale-heartbeat interval. The longest full-population deficits were **1.572, 4.492, and 11.536 seconds**. These counts distinguish initialization, replacement gaps, and heartbeat freshness. They do not establish dropped market updates. The 16,000 window retained ten error notifications: five `ECONNRESET` transport diagnostics and five corresponding WebSocket callbacks, describing five failures.

The first ramp used approximately 40 new connections/s, 32 initializations in flight, and a 15-second initialization deadline. It passed 8,000, then stopped with an **inconclusive startup timeout near 15,000** before reaching its 16,000 target. Raw receipts show peaks of **14,995 upgraded and 14,991 initialized sockets**. Independent revalidation found all four problematic markets still eligible and both REST books available with HTTP `200`. This was not a lifecycle-only failure. A second ramp used approximately 20 connections/s, 16 initializations in flight, and a 60-second deadline. It reached 16,000, disproving a strict ceiling at the first ramp's failure count. Its 14,000 stage passed the final gate, while its 16,000 stage failed as described above. The planned 32,000 target was **not attempted** after that health failure. A binary search for a hard IP cap is not justified by these non-reproducible startup and health failures.

Both runs fetched **35,200 eligible binary markets**, ranked by observed Gamma `volume24hr` descending, with reserves for replacements. The measured 8,000- and 16,000-market prefixes had positive historical volume. These are active-market selections, but neither historical volume nor the declining activity of the larger prefix proves a worst case. Each asset's initial snapshot had both bid and ask arrays; acceptance was independently verified from raw frames and first `PONG` receipts. Every received payload byte and update event was counted, retaining price-change entry multiplicity. Snapshot frames and all `PONG`s were retained; other update payloads were sampled to bound evidence size.

The 14,000 and 16,000 windows consumed approximately **0.98 and 1.00 CPU core**, respectively, on the shared six-core host. RSS at their ends was approximately **2.89 and 3.30 GiB**. Event-loop delay p99 was **151.3 and 162.3 ms**, with maxima of **1.646 and 1.831 seconds**. The 5 GiB process-memory guard, 2 GiB host-free-memory guard, file-descriptor limit, and one-hour wall-time guard did not trigger. Recorded local TCP diagnostics showed no TCP memory-pressure event at the sampled points. These observations do not identify the cause of the resets or stale heartbeat. There was no independent redundant feed to check update coverage or added delivery latency, so the connection results do not support a low-latency or lossless-delivery claim. They do not measure production Rust or throughput for 200,000 live markets.

This was one observation per new count and one slower validation ramp, rather than repeated saturation trials. **16,000 is a tested admission lower bound; 14,000 is the largest completed final-gate pass in this continuation, with documented heartbeat and replacement gaps. Neither is a production reliability ceiling.** The earlier provisional 100-markets/200-assets-per-socket operating point remains a separate result under its measured traffic. No production setting was changed.

The [compact combined results](../experiments/polymarket-subscription-capacity-connections-scale-results.json), [first ramp aggregates](../experiments/polymarket-subscription-capacity-connections-scale-first-results.json), and [validation aggregates](../experiments/polymarket-subscription-capacity-connections-scale-validation-results.json) retain failures alongside passes. Full raw evidence is Git-ignored in `recordings/subscription-capacity-connections-scale-20261024/` and `recordings/subscription-capacity-connections-scale-validation-20261024/`. Each directory includes archived `harness.mjs`, protocol, catalog, frames, events, stages, result, and independent verification. The first includes `initialization-failure-audit.json`; the second includes `final-health-audit.json` and passive network audits. Actual addresses were not saved. Public-egress checks agreed before and after both runs, and each observed one local IPv4 WebSocket source address. This has the same NAT-observation limitation described above.

All experimental sockets and processes terminated. Thirty validation sockets exceeded the three-second graceful-close allowance and had their transports destroyed; cleanup errors are excluded from runtime failure counts. The final completion event records zero remaining WebSockets.

Reproduce serially with fresh directories. A failed health gate intentionally stops escalation; the final count argument is a guard, not a claim that every listed target will be reached.

```sh
node experiments/polymarket-subscription-capacity-connections.mjs recordings/subscription-capacity-connections-scale-REPEAT 32000 300 active-scale 8000
python3 experiments/polymarket-subscription-capacity-connections-analyze.py recordings/subscription-capacity-connections-scale-REPEAT experiments/polymarket-subscription-capacity-connections-scale-repeat-results.json
node experiments/polymarket-subscription-capacity-connections.mjs recordings/subscription-capacity-connections-scale-validation-REPEAT 32000 300 active-scale-slow 14000 14000,16000,32000
python3 experiments/polymarket-subscription-capacity-connections-analyze.py recordings/subscription-capacity-connections-scale-validation-REPEAT experiments/polymarket-subscription-capacity-connections-scale-validation-repeat-results.json
python3 experiments/polymarket-subscription-capacity-connections-analyze-test.py
```
