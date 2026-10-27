# Admission and combined-main AWS validation, 2026-10-27

Current main completed and drained cleanly. The observed publisher deficit was much smaller than in the immediately previous run, but missing books and incomplete standby recovery remain. CPU and internal latency were lower under 21.7% less input traffic. This single live comparison does not establish an optimization gain or isolate the admission filter's effect.

## Build and execution

- Frozen clean main: `4eff979d8117fef0dc0c12bb9343c7b2c2ca2691`. All 248 archived source files matched their manifest after building. The reusable writer message and symbol buffer remain present.
- Fresh candidate SHA-256: `9f56a058aae4ef820509ee3d629c578eac211ff26660440b57fd7c547d0ef81e`. The deployed binary matched; the historical collector was not reused.
- Rust/Cargo 1.96.0, LLVM 22.1.2; locked offline dependencies; release optimization 3, ThinLTO, one codegen unit, debug information off, incremental off. Isolated target; no inherited Rust flags or compiler wrapper. Prior builds used no cross-crate LTO and 16 codegen units.
- Since previously tested `9bb407e`: release profile, 900-second default scan interval, worker level-step validation/skip-reason handling, accepting-orders admission and pending queue, removal of redundant connection bookkeeping, lazy capture commands, direct ID serialization, and documentation changes. The short missing-book retry policy is unchanged from the previous run.
- Retained London host: four vCPU, approximately 8 GiB RAM. Idle before launch; no other collector interrupted. No provisioning or production changes.
- UTC **04:55:33.346–05:10:34.228**, requested 900 seconds including startup, elapsed **900.873 seconds including drain**, exit 0, `duration_complete`, no injected faults, capture off.
- Four workers, 256 markets/socket, two source copies/market, three lifecycle sockets, 20 connection opens/s, 10 Gamma requests/s, PING 10 s / silence 30 s. Scan interval explicitly 900 s versus 3,600 s previously; only one scan started/completed, so repeated reconciliation was not exercised.
- `RLIMIT_NOFILE` explicitly set and verified in the collector: **65,536 soft and hard**. The host already supplied this high limit; this is not a measured increase over the historical collector. Peak sampled descriptors: **2,036**. No descriptor exhaustion.
- SIGINT drain completed; collector and runner stopped; metrics listener closed. Minimum remaining disk was 53.1 GiB. NTP synchronized before/after, six host steal jiffies accrued, no negative exchange ages.

## Comparison

Primary baseline: immediately previous bounded-retry run, 02:45:48–03:00:49 UTC. Secondary: earlier spread-replica run, 01:13:24–01:28:26. Catalog, eligibility, arrivals, traffic and failures differ; these are descriptive observations.

| Measurement | Earlier spread | Previous retry | Current main |
|---|---:|---:|---:|
| Desired markets, last scrape | 257,669 | 260,398 | 258,526 |
| Publishing markets | 257,633 | 260,208 | 258,509 |
| Ready standby markets | 257,620 | 260,174 | 258,423 |
| Missing publishers | 36 | 190 | **17** |
| Publishing without ready standby | 13 | 34 | **86** |
| Natural session-end incidents, whole run | 3 | 77 | 43 |
| Input decoded events/s, steady window | 40,279.16 | 38,247.19 | 29,946.62 |
| CPU cores of 4, steady window | 1.677 | 1.692 | 1.069 |
| Peak sampled RSS, MiB, whole run | 2,055.11 | 2,093.73 | 2,113.33 |
| Internal mean, microseconds | 107.37 | 112.06 | 70.64 |
| Internal p50 / p95 / p99 bucket bounds | 50 / 250 / 1,000 us | 50 / 250 / 1,000 us | 25 / 100 / 500 us |
| Exchange-age mean, milliseconds | 7.713 | 7.039 | 6.762 |
| Exchange-age p50 / p95 / p99 bounds | 10 / 20 / 50 ms | 10 / 20 / 50 ms | 10 / 10 / 50 ms |

Coverage is **logical last-scrape coverage**, not physical copies or final post-drain totals. Current last scrape: elapsed **896.105 s**, books desired **517,052** / synced **517,018**, book sockets **2,020/2,020**, lifecycle **3/3**. Previously 2,032/2,036 book sockets were connected at the last scrape.

Target steady window is elapsed 595–895 s. Actual endpoints: current **595.616–896.105** (300.488 s), previous retry **595.328–895.528** (300.200 s), earlier spread **595.339–895.546** (300.207 s). Rates and CPU use actual durations; older reports sometimes used nominal 300 s. Internal samples: **8,973,736 / 11,427,496 / 12,110,015**, current/previous/earlier. Exchange-age samples: **8,963,741 / 11,434,271 / 12,069,305**.

Input and exchange-age populations include both feeds. Internal latency covers emitted publisher price-change levels only; all snapshot-derived output is excluded. Percentiles are inclusive histogram bucket bounds. Their populations differ; do not subtract percentiles or interpret sample-count differences as loss. Writer blocked time, decode errors and negative exchange ages were zero through the last scrape in all runs. Current same-source mismatches/older-book skips: **37/12**, previous **38/134**, earlier **70/85**; these do not prove data loss.

## Coverage and admission

The initial 2,585-page scan completed at elapsed **265.809 s**, versus 260.8 s previously and 258.2 s earlier, registering 258,377 markets. Final desired count was 1,872 lower than previously; changed eligibility and catalog both contribute, so this is not a filter-only removal count. There were **506 distinct hint-admission incidents**, no duplicate admitted listing IDs, and no lookup/scan failure incidents. Existing telemetry does not count all pending/deduplicated hints or waiting time; it cannot establish how many entries were held or transitioned to eligibility.

After elapsed 400 s, missing-publisher samples ranged **11–18**, versus **179–192** previously and **35–36** earlier. Publishing without standby briefly reached **701 at elapsed 851.032 s**, then fell to 86 at the last scrape. Previous maximum was 223, earlier 13. Five-second samples can miss short outages.

An initial readiness probe briefly returned 200 before initialization, followed by startup 503s; that artifact also exists in retained baselines. All current post-scan probes were 200, starting at elapsed 270.145 s. Readiness does not certify complete coverage.

## Missing-book retries and direct follow-up

| Maximum attempt in batch | Incident batches | Requested physical outcome copies |
|---|---:|---:|
| 1 | 25 | 335 |
| 2 | 25 | 335 |
| 3 | 24 | 297 |

Twenty-one connections emitted retry incidents; 24 final-budget incidents covered 297 requested source-outcome copies. Counts repeat assets across retries/sessions; they are not unique missing markets or proof of permanent failure. The 38-copy difference at attempt 3 is **shutdown censoring**, not recovery: connection 1696 retried 38 outcomes at 05:10:24 and 05:10:29; shutdown preceded another window. Fixed five-second windows and the three-retry cap were exercised without increasing backoff. Aggregate logs cannot identify every stubborn asset or prove every pending copy recovered.

Post-run metadata plus whole-output symbols identified ten hint-admitted Hong Kong temperature markets, **6290225–6290234**, whose two outcome symbols never appeared. Their later flags remained active, not closed/archived, accepting orders and order-book enabled. This is a diagnostic subset, not the exact last-scrape 17-market deficit.

**Direct WebSocket test:** three of those markets (**6290225, 6290230, 6290234**) and published control **6290135**, each on two independent local feeds for 60 s. All eight sockets received both books without retries, received five PONGs, and closed normally with code 1000; flags remained eligible afterward. Later fresh subscriptions therefore work for these examples. This does not establish availability at original AWS admission or identify the original failure: time, host and grouping differ, and original raw frames were not captured. Eligibility plus three short retries does not guarantee eventual initialization. Late availability versus original session/subscription behavior remains unresolved.

## Closures and failover

Whole-run context: **43 natural ends** = **five 1013 slow-consumer closes, all on even primary-pool connection 1696**, plus **38 1001 server-shutdown closes** across both pools. No retained failed-connect, I/O-reset, silence or decode-failure incidents. Previously: 77 ends (two 1013, 49 1001, 26 transport resets). Earlier: three 1013 ends.

Closures affected 39 distinct connections, with 29 incident ends in the primary pool and 14 in the replica pool. Connection 1696's completed sessions lasted **192.141, 154.177, 67.844, 126.037 and 79.245 seconds**; planned shutdown censored its final session. Early 1013 closes required no publisher switch; only two switch incidents originated from 1696 over the whole run. Pool names are not permanent publisher roles.

Last-scrape metrics recorded **43 remote session ends and 5,563 publisher switches**, all remote-close; current retained switch incidents agree. Previous last-scrape metrics recorded **10,046** switches, while **12,282 whole-run incidents** include the tail after that scrape; do not mix those populations. Current sampled connected-book exposure: **324.9497 socket-hours**. Slow-consumer incidence per 100 sampled socket-hours: **1.5387 current / 0.6053 previous / 0.9155 earlier**. Fewer total closes does not mean fewer slow-consumer closes.

Observed switch mappings identify **20 markets whose two source IDs each closed within one second** in the 1001 bursts. These nonadjacent cross-pool pairs show that closes were not all on unrelated markets. Mappings exist only for observed switches; nearby close timestamps do not establish overlapping downtime or exact duration. Full connection membership and raw input were not captured.

## Independent output evidence and retention

The freshly built supplied-reader scanner decoded **3,892,314,112 bytes**, **5,468,090 snapshots**, **18,240,500 level updates**, **517,022 distinct symbols**, and **64,041,444 snapshot levels**. Framing/domain decoding, baseline-before-update, and sorted unique positive snapshot levels passed. A retained supplemental symbol reader returned matching counts. SBE SHA-256: `98ef6b6847fe9879694f79cba9aef40d1010e95af59a191c8b30f6ff1d9e45d5`.

This is structural evidence, separate from operational latency/throughput, not source completeness, a cross-source oracle, or proof of every periodic snapshot's contents. Lifetime distinct output symbols are not last-scrape book coverage.

Evidence: `data/aws-admission-20261027T045031Z/` retains frozen source/archive/manifest, build metadata, binaries, runner and analysis, all 180 scrapes/samples, incidents, reader results, cleanup proof, and post-run metadata/WebSocket frames. Full SBE remains on the retained host in this run's `full/books.sbe`; the exact remote location is recorded in ignored run metadata. Prior evidence was preserved. This report remains uncommitted; no production instrumentation was added.
