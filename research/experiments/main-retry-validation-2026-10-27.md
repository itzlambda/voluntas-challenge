# Bounded first-book retry validation, 2026-10-27

The fresh release run completed and drained cleanly. The three-retry cap and short retry cadence were exercised naturally, but missing baselines remained. Internal latency bucket bounds matched the prior spread-replica run. A late burst of 1001 closes and transport resets makes failure and recovery behavior materially different between these two live observations. This comparison does not establish a causal improvement or regression.

## Frozen build and matched configuration

The candidate is committed local `main` at `9bb407ebe4a3fce2b905bf0cdcbd0dc154cdf990`, tree `540f274c955b9fbaa3ea5c09cc880a11d0258ec0`. HEAD had not advanced when evidence was verified. Concurrent source and documentation edits were excluded using `git archive` and left untouched. The frozen source manifest was checked again after testing. Relative to the historical binary, this revision includes the generic exchange integration and bounded retries; it is not a retry-only change. The reusable writer message and symbol buffer remain intact.

A fresh, isolated target directory was built with Rust 1.96.0, Cargo 1.96.0, LLVM 22.1.2, and the locked dependency graph, offline:

```sh
CARGO_TARGET_DIR=../target CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo build --locked --offline --release --bin engineering-challenge \
  --example standby_record_scan -j 2
```

Cargo's default release profile used optimization level 3, no LTO, 16 codegen units, unwind panics, and disabled debug assertions and overflow checks. No inherited Rust flags or compiler wrappers were present. The shared Cargo target setting was overridden. The reader example was added only to the frozen snapshot and used the unchanged supplied reader. All 119 enabled release-mode tests passed; five opt-in tests were ignored.

Collector SHA256:

- Candidate: `ebdb856a501c78f408a243e305a137b09c79244fbf1a89cb88c3bca2a8e0ec9b`
- Prior spread replicas: `a3c614fbaa41bb865c1c4b33c8dae12ef96a04fb559c4deaa04c7aaa20869278`

The retained London host was idle before launch, with four vCPUs, about 8 GiB RAM, and about 62 GiB free. NTP reported synchronized. The running collector's executable hash and 65,536-descriptor limit were checked directly. No resources were provisioned, faults injected, or production settings changed.

The runner was byte-identical to the prior spread-replica runner, SHA256 `1e5b75e66e8aeaec7764f274f41b5b663ed57c60fd75e02034ac375238c46acd`. Both runs requested 900 seconds including startup, with four workers, 256 markets per socket, two physical feeds per market, three discovery sockets, 20 connection opens/s, 10 Gamma requests/s, PING every 10 seconds, silence reset after 30 seconds, capture disabled, and five-second Prometheus scrapes. The candidate uses five-second missing-book response windows and at most three retries per outcome per session.

## Coverage and failure context

| Measurement | Prior spread replicas | Fresh main |
|---|---:|---:|
| UTC observation | 01:13:24–01:28:26 | 02:45:48–03:00:49 |
| Elapsed including drain | 901.465 s | 901.597 s |
| Exit / stop reason | 0 / duration complete | 0 / duration complete |
| First full catalog scan | 258.238 s | 260.783 s |
| Last scrape elapsed | 895.546 s | 895.528 s |
| Desired markets, last scrape | 257,669 | 260,398 |
| Synced markets | 257,633 | 260,208 |
| Standby-ready markets | 257,620 | 260,174 |
| Markets without publisher | 36 | 190 |
| Publishing without ready standby | 13 | 34 |
| Desired / synced logical outcome books | 515,338 / 515,266 | 520,796 / 520,422 |
| Connected / expected order-book sockets | 2,014 / 2,014 | 2,032 / 2,036 |
| Connected discovery sockets | 3 | 3 |
| Publisher deficit during elapsed 400–895 s | 35–36 | 179–192 |
| Maximum publishing without ready standby in that slice | 13 | 223 |

The candidate had about 1.06% more desired markets and substantial lifecycle additions during the run. Its publisher deficit was already 188 before the first socket failure. Thus the later failures do not explain the initial missing coverage. Readiness stayed 200 after the first post-scan successful probe. Readiness does not certify complete publisher or standby coverage. Five-second samples can miss brief outages, and independently published component gauges can briefly differ.

At the last scrape, Prometheus recorded 50 remote-close and 17 I/O session endings, and 10,046 completed publisher switches: 7,855 remote-close and 2,191 I/O. Silence, decode-error, and sequence-gap switch counters were zero. The final catalog includes `sequence_gap`; no metric families were added for this run.

Whole-run contextual incidents include 77 session endings: two 1013 slow-consumer closes, 49 code-1001 closes, 24 abrupt WebSocket resets, and two TLS EOFs. Five 1001 messages explicitly reported a CloudFlare WebSocket proxy restart. Those messages do not establish a cause for every other reset. No failed connection attempts, silence incidents, or decode failures were retained. Both 1013 closes affected primary-pool connection 1698. Its first failure promoted four markets to four distinct spread replicas.

The historical run had three 1013 closes and no other retained session-ending failures. Sampled connected exposure was 327.691 versus 330.426 socket-hours, giving descriptive 1013 rates of 0.916 versus 0.605 per 100 socket-hours. The lower 1013 count does not offset or explain the candidate's additional failure classes.

Metrics stop at the last scrape, about six seconds before completion. Eight session-end incidents occurred after that scrape; two additional pre-scrape incidents were also absent from its cached metric totals. Further publisher switches appear in the unsampled tail. Whole-run incidents supply failure context, not replacements for missing final numeric metrics. The shutdown events and exit status confirm clean drain, not a final operational-counter snapshot.

## Retry behavior and remaining gaps

| Contextual retry incidents | Attempt 1 | Attempt 2 | Attempt 3 |
|---|---:|---:|---:|
| Prior run: batches / outcome-copy requests | 42 / 154 | 42 / 154 | Not observed |
| Candidate: batches / outcome-copy requests | 208 / 840 | 207 / 838 | 207 / 838 |

Candidate retries touched 203 distinct connection IDs across sessions. There were 207 final-budget incidents covering 838 outcome-copy requests. No logged attempt exceeded three. Adjacent attempt batches within an observed session were approximately 5–10 seconds apart, consistent with fixed five-second response windows and the five-second check cadence. These are batch maximum attempts, not a per-asset attempt trace.

Before the first closure, 198 batches requested 781 outcome copies in each of the three rounds; the counts did not shrink. After the first 1013 close, connection 1698 requested 12 missing copies in all three rounds. The run therefore exercised short retries during recovery as well as startup, without waiting the old 120 seconds. Later reconnects produced additional retry batches.

This validates observed cadence and budget enforcement, not successful repair of all subscriptions. A final-budget incident means the final request was sent. It does not mean its response failed. No pending-outcome success events or asset lists are available with capture disabled. The evidence cannot identify stubborn missing asset IDs, prove all pending copies recovered, or distinguish retry-induced recovery from a late original response. Logical publisher and standby gauges cannot reconstruct physical pending-copy counts. The final 190 publisher gaps and 34 standby gaps remain unresolved.

## Latency, offered load, and resources

Both histogram comparisons use nominal elapsed 595–895-second endpoints. Actual retained windows are 595.339–895.546 seconds for the baseline and 595.328–895.528 seconds for the candidate, spanning 300.207 and 300.200 seconds. Histogram percentiles below are inclusive bucket upper bounds.

| Matched window unless stated | Prior spread replicas | Fresh main |
|---|---:|---:|
| Decoded input events/s | 40,279 | 38,247 |
| Internal latency observations | 12,110,015 | 11,427,496 |
| Internal mean | 107.369 µs | 112.056 µs |
| Internal p50 / p95 / p99 upper bounds | 50 µs / 250 µs / 1 ms | 50 µs / 250 µs / 1 ms |
| Exchange-age observations | 12,069,305 | 11,434,271 |
| Exchange-age mean | 7.713 ms | 7.039 ms |
| Exchange-age p50 / p95 / p99 upper bounds | 10 / 20 / 50 ms | 10 / 20 / 50 ms |
| Mean CPU, cores of four | 1.677 | 1.692 |
| Peak sampled RSS, whole run | 2,055.109 MiB | 2,093.734 MiB |
| Writer-capacity waits, worker-seconds | 0 | 0 |
| Decode errors / negative exchange ages, last scrape | 0 / 0 | 0 / 0 |
| Same-source mismatches, last scrape | 70 | 38 |
| Older full books skipped, last scrape | 85 | 134 |

Rates use actual scrape spacing. Dividing the baseline event delta by the nominal 300 seconds gives the earlier report's 40,306.93/s. This report uses 300.207 seconds instead. No percentile subtraction is used.

Input events and exchange ages include both physical feeds. Exchange age samples accepted subscribed price-change messages and includes clock offset. Internal latency samples emitted publisher price-change levels only, excluding all snapshot-derived output. Their populations differ and do not define a loss ratio. Mismatches compare a source with its own held book; they do not establish cross-replica disagreement or missing exchange events.

The candidate saw about 5% less decoded traffic in this window, different market membership, and many more failures. The unchanged p99 bucket and slightly higher mean do not establish a repeatable performance difference. This measures offered live load, not maximum capacity or a proven bottleneck. Aggregate CPU headroom and zero writer waits do not exclude short per-socket pressure. One historical run per version cannot support a causal performance claim.

## Independent output validation and retained evidence

After drain, the freshly built supplied-reader scanner decoded all 4,563,402,752 bytes: 5,562,788 snapshots, 23,673,377 level records, and 520,420 symbols. It checked framing, domain values, initial baseline presence, and sorted unique positive snapshot levels. This is separate from operational latency and throughput. It does not prove source completeness, every periodic snapshot's exact content, or intermediate history across source switches without raw input.

Output SHA256 is `427536250893fc291df3bf539320db138a76f6a79a9559dacae903106d282759`. The complete SBE remains on the retained host under `/srv/collector-test/main-retry-20261027T023925Z/full/`. No collector, runner, or metrics listener remains active. Older run artifacts were preserved.

Local ignored evidence is under `data/aws-main-retry-20261027T023925Z/`: frozen archive/source, per-file manifest, build and test records, binaries, baseline evidence copy, unchanged runner, individual scrapes, process samples, incident logs, host observations, reader result, artifact verification, comparison JSON, scripts, and HTML. Reproduce the calculation with `python3 data/aws-main-retry-20261027T023925Z/compare-current.py`. Downloaded file hashes and frozen source contents were verified. Documentation is uncommitted; no commit or push was made.

## Did closes affect both feeds of the same market?

The two candidate 1013 slow-consumer closes both hit primary connection 1698. No replica connection had a 1013 close in this run. The earlier spread-replica run differed: primary 1694 and replica 749 closed about 85 ms apart and shared one market, established by its publisher-switch records.

The candidate's other failures were spread across both pools: all 77 retained session-end incidents covered 76 distinct connections, with 41 primary-pool incidents and 36 replica-pool incidents. Some shared-market source pairs failed together. At 03:00:45 UTC, primary 100 reset and its market switched to replica 1311; replica 1311 reset about 1.85 ms after primary 100. The market subsequently switched back after primary recovery. This proves shared exposure during a clustered failure, not that the market caused the resets or an exact both-down duration.

The evidence therefore shows a repeated single-primary slow-consumer problem plus broader clustered failures across both pools. It does not support treating all closes as independent random events, or attributing the broad burst to a particular hot market. The reproducible incident analysis is retained as `close-pool-analysis.py` and `close-pool-analysis.json` alongside the other evidence.
