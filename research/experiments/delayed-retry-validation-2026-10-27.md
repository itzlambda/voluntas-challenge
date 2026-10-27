# Delayed book retry validation, 2026-10-27

The fresh candidate completed and drained cleanly. Last-scrape missing publishers fell from **17 to 7**, and publishing markets without ready standby from **86 to 24**, despite more natural failures. Some retry activity continued beyond the former three-attempt limit and later ceased without disconnect/removal. Missing baselines still remain. This is one live observation with changed market arrivals and failures, not proof that the five-second gap alone caused the difference.

## Frozen build and matched run

The source is local main base `4eff979d8117fef0dc0c12bb9343c7b2c2ca2691` **plus the uncommitted working tree**, not that commit alone. All 250 nonignored source files, including the new tests, were copied and checked against working-tree hashes before the build and verified again afterward. Changed-source hashes match the version that passed 136 enabled tests, five ignored, Clippy with warnings denied, formatting and whitespace. The frozen tracked patch SHA-256 is `b30321a4b82d1552ab817f87b8ade091776e92977140d077c4bc63af6686d91e`; new files are separately covered by the source manifest/archive.

Fresh release binary SHA-256: **`5975874ba13924c2b5ec22c14cf05f07d0df78663286dd16e0dba66325f27948`**. The deployed SHA matched and differs from the previous candidate `9f56a058…d0ef81e`. The fresh locked/offline build used a new isolated target: Rust/Cargo 1.96.0, LLVM 22.1.2, optimization 3, ThinLTO, one codegen unit, debug information off, incremental off; no inherited Rust flags or compiler wrappers. Compiler/profile settings match the preceding admission run. Reusable writer message/symbol buffers remain intact.

The new runtime changes are the ten-retry limit, Polymarket's five-second unsubscribe-to-subscribe gap, nonblocking scheduling, routing exclusion during that gap, reconnect/removal handling, and capture/replay support. No metric families or production measurement instrumentation were added.

The same retained London host (four vCPU, about 8 GiB RAM) was idle before launch. No other workload was interrupted. The runner, explicit flags, resource guards and five-second scrape cadence match the preceding run: four workers, 256 markets/socket, two source copies/market, three lifecycle sockets, 20 connection opens/s, 10 Gamma requests/s, 900-second scan interval, PING 10 s / silence 30 s. Capture off, no injected faults. `RLIMIT_NOFILE` was explicitly enforced and verified in the collector at **65,536 soft/hard**; peak sampled descriptors were **2,036**. The host already supplied this high limit.

UTC **05:53:07.657–06:08:08.634**; requested 900 seconds including startup; elapsed **900.969 seconds including drain**, exit 0, `duration_complete`. Collector and runner stopped, metrics port closed, run lock released. Minimum free disk was about 49.5 GiB. NTP was synchronized before/after; eight host steal jiffies accrued. No provisioning, production changes, commits or pushes occurred.

## Primary comparison

The immediately previous admission run at 04:55:33–05:10:34 UTC is the primary baseline. It used three retries and back-to-back unsubscribe/subscribe; both runs include the accepting-orders admission queue and the same release profile. Older retry/spread evidence is also retained in `comparison.json`, but is not the primary comparison.

| Measurement | Previous admission run | Delayed / ten retries |
|---|---:|---:|
| Desired markets, last scrape | 258,526 | 258,535 |
| Publishing markets | 258,509 | 258,528 |
| Standby-ready markets | 258,423 | 258,504 |
| Missing publishers | 17 | **7** |
| Publishing without ready standby | 86 | **24** |
| Maximum sampled standby gap after elapsed 400 s | 701 | **265** |
| Natural session-end incidents, whole run | 43 | **124** |
| Slow-consumer closes, whole run | 5 | 4 |
| Input decoded events/s, steady window | 29,946.62 | 28,981.10 |
| CPU cores of 4, steady window | 1.069 | 1.162 |
| Peak sampled RSS, MiB, whole run | 2,113.33 | 2,098.73 |
| Internal mean, microseconds | 70.64 | 75.27 |
| Internal p50 / p95 / p99 bucket bounds | 25 / 100 / 500 us | 25 / 250 / 500 us |
| Exchange-age mean, milliseconds | 6.762 | 7.214 |
| Exchange-age p50 / p95 / p99 bounds | 10 / 10 / 50 ms | 10 / 10 / 50 ms |

The current last scrape is elapsed **896.197 s**, not the final post-drain state. Books were desired **517,070** / synced **517,057**; book sockets **2,020/2,020**, discovery **3/3**. Coverage counts are logical, not physical replicas. The initial scan completed at elapsed **260.752 s** (incident duration 260.738 s), 2,575 pages, registering 257,341 markets. Previously it took 265.809 s, 2,585 pages, registering 258,377. Current hint-admission incidents were **1,291 distinct listing IDs**, versus 506 previously; the near-equal final catalog count does not mean identical membership or workload. There were no scan, pending-lookup or failed-connect incidents.

Target steady window is elapsed 595–895 s. Actual current endpoints **595.668–896.197** span **300.529 s**; prior **595.616–896.105** span **300.488 s**. Rates/CPU use actual durations. Current/prior internal samples: **8,643,046 / 8,973,736**; exchange-age samples **8,645,328 / 8,963,741**. Input traffic was about 3.2% lower, while failover volume was substantially higher. No performance improvement or regression is causally established.

Input and exchange-age populations include both physical feeds. Internal latency covers emitted publisher price-change levels only and excludes snapshot-derived output. Bounds are inclusive histogram buckets, not exact quantiles. Do not subtract percentiles or interpret sample-count differences as loss. Writer blocked time, decode errors and negative exchange ages were zero through the last scrape. Same-source book mismatches/older-book skips were **32/8**, versus **37/12** previously; these do not prove source completeness or data loss.

Two post-400-second samples were inconsistent across independently published components: at elapsed **650.771 s**, synced exceeded desired by five; at **675.811 s**, by seven. Raw observations are retained. Their negative computed deficits are not complete-coverage evidence. Other nonnegative samples ranged 0–21 missing publishers; a momentary zero also does not identify recovery of every specific market. All post-scan readiness probes were 200 from elapsed 265.149 s. As in the prior run, the very first readiness probe briefly returned 200 before the startup 503s. Readiness is not coverage completeness.

## Retry behavior and remaining recovery limits

| Maximum attempt in batch | Incident batches | Physical outcome-copy request intents |
|---|---:|---:|
| 1 | 28 | 112 |
| 2 | 28 | 112 |
| 3 | 28 | 112 |
| 4 | 28 | 112 |
| 5 | 26 | 108 |
| 6 | 20 | 53 |
| 7 | 19 | 52 |
| 8 | 19 | 52 |
| 9 | 19 | 52 |
| 10 | 18 | 50 |

Twenty-four connections emitted retry incidents. **18 final-budget incidents covered 50 physical outcome-copy requests**, versus 24/297 previously. These repeat outcomes across sessions and are not unique assets or a count still missing at shutdown. `first_book_missing` records retry initiation; delayed subscribe may be cancelled or censored. For a concrete completed tenth retry, connection 579 logged initiation at **06:07:56.592926**, then final-budget use after subscribe at **06:08:01.593916**, 5.001 seconds later. This incident pair is consistent with the intended gap; precise per-message timing and cancellation were independently tested locally.

Connections **2012 and 2013** each had a 16-copy batch reach attempt five and another two-copy batch reach attempt four. Their retry activity then ceased without either socket ending or any market-removal incident in the run, consistent with full books clearing those pending entries beyond the old three-attempt cutoff. This is not an asset-level raw-frame trace, and it cannot distinguish extra retry duration from the delay or spontaneous late upstream availability.

Other cohorts reached all ten retries. Late sessions were censored: connection 1692 initiated attempt five for 16 outcomes at **06:08:05.656**, so the five-second gap had not finished before planned shutdown. A lower count at later attempts therefore cannot be interpreted entirely as recovery.

All **1,291 hint-admitted markets** had both outcome symbols somewhere in the whole-run output when mapped using a bounded post-run Gamma lookup. That proves lifetime output presence for this cohort, not final readiness or physical-copy completeness. Pending hints that were not admitted are not counted by this evidence. It does not identify the exact seven last-scrape publisher gaps: initial catalog membership and pending asset IDs were not retained at source level, and an already published market may later lose readiness. No individual missing-asset cause or guarantee that all pending outcomes recovered can be established. Exhausted entries still accept late full books; healthy sockets do not get an extra reset solely for exhaustion. Unlimited retries remain disabled.

## Natural failures and failover

Whole-run contextual incidents: **124 ends across 120 connection IDs** — **26 closed 1001**, **four closed 1013**, **92 resets without a closing handshake**, and **two OS connection resets**. All four slow-consumer closes were on even primary-pool **1692**, with completed sessions lasting 159.085, 37.095, 55.936 and 313.342 seconds. Different catalog/placement makes this ID non-equivalent to the previous run's 1696.

Incident ends were **64 in the primary pool / 60 in the replica pool**. Observed publisher-switch mappings identify **32 markets whose source IDs each closed within one second**. This does not establish exact overlapping downtime or its duration; mappings are available only where switches were observed. There were no retained silence, decode or sequence-gap incidents.

Last-scrape metrics recorded **30 remote-close and 88 I/O session ends**, plus **16,856 publisher switches** (3,795 remote-close, 13,061 I/O). Whole-run incidents include six more I/O ends after that scrape and **17,768 switches** (3,795 remote-close, 13,973 I/O). Do not mix tail-inclusive incidents with last-scrape metrics. Sampled connected-book exposure was **327.2953 socket-hours**; slow-consumer incidence was approximately **1.2221 per 100 socket-hours**, versus 1.5387 previously. The much larger transport-reset population is a material difference between runs, not evidence of the retry delay's cause or effect.

## Independent output and retained evidence

The newly built supplied-reader scanner decoded the entire **3,825,205,248-byte** output: **5,518,220 snapshots**, **17,861,343 level updates**, **517,060 distinct symbols**, **60,651,624 snapshot levels**. Framing/domain decoding, baseline-before-update, and sorted unique positive snapshot levels passed. The supplemental symbol reader returned matching counts. SBE SHA-256: **`864a7e129fc67afe424ade9eb81bb63e63fb276d031f9c3cfbe769b099fe6094`**.

This is structural validation, separate from operational throughput/latency. Without raw capture it does not prove complete exchange input, exact cross-source depth, or every snapshot's content. Lifetime output symbols are not last-scrape synced-book coverage.

Evidence: `data/aws-delayed-retry-20261027T054849Z/` contains source/archive/patch/manifest, compiler/profile/build metadata, candidate and readers, unchanged runner, analysis/comparison, raw metrics/samples/incidents, reader results, cleanup proof, and bounded post-run hint lookup. Complete SBE remains on the retained host in this run's `full/books.sbe`; ignored run metadata records the exact location. Prior artifacts were preserved. Source changes and this report remain uncommitted.
