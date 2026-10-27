# Eligibility recovery validation, 2026-10-27

Targeted eligibility cleanup worked in this run. Thirteen markets were removed after recovery checks found them ineligible. Two eligible markets still supplied no baselines. The final standby deficit was smaller than the previous 15-minute run, but late failures left 16 already-closed markets awaiting their cleanup trigger. This validates cleanup behavior, not a fix for upstream book availability or a performance improvement.

## Build and execution

- Local `main` HEAD: `4eff979d8117fef0dc0c12bb9343c7b2c2ca2691`, plus the retained working-tree patch. Main includes the earlier admission-confirmation and build-profile changes after the original `9bb407e` handoff. The candidate also includes existing uncommitted protocol, delayed-retry, and diagnostic work. Concurrent changes were preserved. Nothing was committed or pushed.
- Binary SHA-256: `7c1eb03b52e3f6f80941157785231883d272e0398bd8bad55edd87c1556efd59`.
- Rust/Cargo 1.96.0; release optimization 3, ThinLTO, one codegen unit, debug 0, incremental off; `book-diagnostics` enabled, matching the latest diagnostic run. The candidate package was rebuilt after cleaning only its private copied package artifacts. Shared dependency artifacts were preserved. Writer source matches the previous diagnostic build.
- A paused-clock test setup was corrected after the initial clean release build. Release source inputs did not change. The final frozen manifest and release build were verified again, with the same binary SHA. All 145 enabled tests passed; five opt-in tests were ignored. Strict Clippy passed with and without diagnostics. Formatting and whitespace checks passed.
- UTC start/end: **09:19:31.345097–09:34:32.374191**. Requested 900 seconds, elapsed **901.020 seconds**, including drain, exit **0**. The last scrape is elapsed **896.324528 seconds**, before shutdown.
- Retained London host: four vCPUs, about 8 GiB RAM. Host idle before launch, validation lock acquired, no competing collector, port free. Collector soft and hard open-file limits were both **65,536**. Four workers, 256 markets per socket, two feeds per market, three discovery sockets, 20 connection opens per second, 10 Gamma requests per second, 900-second scan interval. Five-second scrapes; capture off; no injected faults or manual probe sidecar. No AWS resources were provisioned.
- First scan completed in **259.374 seconds**, 2,587 pages, 258,568 registrations. Clock synchronization was normal before and after the run.

## Coverage comparison

The latest run was the [600-second diagnostic investigation](missing-book-diagnosis-2026-10-27.md). The previous run of matching duration was the [900-second delayed-retry validation](delayed-retry-validation-2026-10-27.md). Both are retained comparisons, rather than repeated or simultaneous controls.

| Last-scrape population | Previous 900 s | Current 900 s |
| --- | ---: | ---: |
| Desired markets | 258,535 | 258,591 |
| Publishing markets | 258,528 | 258,589 |
| Standby-ready markets | 258,504 | 258,573 |
| Missing publishers | 7 | **2** |
| Publishing without standby | 24 | **16** |

The latest 600-second run ended with **5 missing publishers and 8 publishing without standby**. At the matching current checkpoint, elapsed **595.816 seconds**, those deficits were **2 and 0**, with 258,589 desired markets. Comparing its 600-second endpoint directly with the new 900-second endpoint would hide the later failures. The largest sampled standby deficit after elapsed 400 seconds was 75; most affected sources recovered.

The final current book population was 517,182 desired and 517,178 logically synced books. Physical order-book connections were 2,022 expected and connected, plus three discovery connections. Logical book coverage is not physical copy count.

## What the recovery checks did

Structured incidents record 13 ineligible decisions and 13 corresponding `eligibility_changed` removals. Independently fetched post-run metadata shows all 13 closed and not accepting orders. No lookup-error incidents occurred. Eligible decisions retained membership, and local retry rounds continued without requiring HTTP permission.

Five removals at **09:26:57–09:26:59** cleared two publisher gaps and three standby gaps. Eight more removals at **09:32:35** removed closed markets whose primary could no longer initialize. These were removals, not recovered baselines. Other markets remained on the recorded source connections; no whole-socket reset was requested by eligibility cleanup.

For 32 exhausted-outcome incidents, diagnostic commands show that exhaustion followed the tenth subscribe by approximately **10 seconds**, consistent with the five-second response deadline and five-second check cadence. Sixteen observed subsequent rounds started approximately **60 seconds** after exhaustion. These incident checks verify ordering and cooldown behavior; they are separate from numeric throughput and latency evidence.

The two remaining publisher gaps are the same markets as the previous investigation:

| Gamma ID | Market | Current physical sources |
| --- | --- | --- |
| `6282417` | Persib Bandung vs Melbourne Victory, total goals O/U 3.5 | Primary `1918`, replica `1849` |
| `6285465` | Old Dominion vs Coastal Carolina, successful two-point conversions O/U 0.5 | Primary `1940`, replica `1569` |

Both remain active, open, unarchived, and accepting orders. All eight source-outcome states remained uninitialized, with no recorded baseline receipt. None of their four sockets failed. The previous investigation's fresh WebSocket probes and REST book responses established upstream unavailability at that time. This run reproduces the missing baselines and eligible metadata; it did not repeat those manual probes or prove an upstream internal cause.

## Late closures and the remaining standby deficit

All six natural session ends were `1013` slow-consumer closes: **five on primary `1700`, one on replica `717`**. There were no 1001, I/O, silence, decode, or failed-connect incidents. The previous 900-second run had 124 session ends across several causes, including four 1013 closes. That difference in external failure conditions prevents causal attribution to this change.

The six closes over approximately **328.451 connected socket-hours** give **1.827 per 100 socket-hours** for 1013 closes. The previous matching-duration run's corresponding rate was 1.222. The change did not establish an improvement in slow-consumer behavior.

There were **191 publisher switches**, all `remote_close`: 186 away from replica 717 and five away from primary 1700. One shared market switched 717 → 1700 at **09:31:42**, then 1700 → 717 at **09:32:48**. Both switches selected a ready peer. This is failure-driven switching, not automatic failback. Five-second scrapes do not prove that every brief outage was absent.

Every final standby gap belongs to a market already closed and not accepting orders. Their missing source is primary 1700; their 16 distinct replica sources remain ready. The latest continuous baseline loss began at **09:32:48**, about **99.6 seconds before the last scrape** and 104.3 seconds before shutdown. Subsequent sessions lasted about 72 seconds, 1.7 seconds, and then the remainder of the run. Neither ten retries in a session nor the 180-second continuous-missing trigger had completed for these outcomes before shutdown.

The policy therefore still has a configured waiting period before metadata cleanup can begin. Transport loss, the socket's full-book gate, lookup scheduling, and request latency can extend that wait. It does not immediately remove every closed market. Reconnects preserve continuous missing age, but a longer run would be required to observe these particular pending checks complete. Their identities and source states are retained in `recovery-analysis.json`.

## Load and latency

Metrics supply the numeric evidence. Internal latency covers emitted publisher price-change levels and excludes snapshot-derived output. Decoded-event and exchange-age populations include both feeds. Percentiles below are histogram bucket upper bounds, not subtracted percentiles.

The matched steady windows target elapsed 595–895 seconds. Actual current bounds are **595.815791–896.324528**, versus **595.668075–896.197050** previously.

| Measurement | Previous 900 s | Current 900 s |
| --- | ---: | ---: |
| Decoded input events/s | 28,981.10 | 32,856.97 |
| Internal samples | 8,643,046 | 9,868,210 |
| Internal mean | 75.27 µs | 90.68 µs |
| Internal p50 / p95 / p99 bounds | 25 / 250 / 500 µs | 25 / 250 / 1,000 µs |
| Exchange-age samples | 8,645,328 | 9,856,068 |
| Exchange-age mean | 7.214 ms | 7.915 ms |
| Exchange-age p50 / p95 / p99 bounds | 10 / 10 / 50 ms | 10 / 20 / 50 ms |
| CPU cores, steady window | 1.162 | 1.345 |
| Peak sampled RSS, whole run | 2,098.73 MiB | 2,338.20 MiB |

For the latest diagnostic run's matching 295–595-second window, input was 32,057.39 versus 32,723.27 events/s currently; internal mean was 81.09 versus 85.54 µs, with the same p99 bound of 1 ms. Exchange-age means were 7.182 versus 7.252 ms. CPU was 1.219 versus 1.276 cores. Exact bounds and populations are retained in `comparison.json`.

Current writer-blocked time, decode errors, and negative exchange ages were zero at the last scrape. Same-source book mismatches were 61 and older-full-book skips 19. Observed input is workload, not a throughput-capacity measurement. Memory was higher, latency was not uniformly better, and activity and failure patterns differed. These single historical runs do not isolate the change's performance cost.

## Output, evidence, and remaining limits

The fresh supplied-reader scan passed the complete **4,160,749,568-byte** output: **5,500,392 snapshots**, **20,129,994 level updates**, **517,200 distinct symbols**, and **65,623,625 snapshot levels**. Output SHA-256 is `71818dcb816817a88527bddd7316cafa049c41517c678d4bb70bd1095f26dfaa`. This checks framing, domain decoding, baseline presence, and sorted unique positive snapshot levels. It does not establish full upstream completeness or both-copy availability without raw capture. Distinct symbols cover the whole run, including removed markets, rather than the final catalog.

Local evidence is retained under repository-relative `data/aws-eligibility-recovery-20261027T091353Z/`: frozen source and manifest, patch, build metadata and binary, checks, status, samples, all scrapes, structured incidents, final metadata and source states, retry timing analysis, comparison scripts, reader results, and cleanup verification. The complete SBE remains in the corresponding retained-host run directory. The downloaded evidence archive excludes that large SBE; its size and SHA identify the preserved remote output.

Collector and runner stopped, metrics port closed, and validation lock released. Source files still matched the frozen manifest after the run. The metric catalog remains at 17 families. No commits, pushes, resource provisioning, or production changes were made.

Archived-row lookup could not be verified: the candidate query returned no explicitly archived rows. Absence remains unknown and does not authorize removal. This live run did not exercise identity replacement, fixed-mode periodic readmission, or a Gamma outage. Normal-mode readmission after temporary ineligibility can wait for the next full scan. The two eligible bookless markets and cleanup latency after late reconnects remain the practical limitations.

## Post-validation cleanup

The temporary `book-diagnostics` feature and its raw-frame and worker-state dumps
were removed after this run. Permanent recovery behavior, structured recovery
incidents, and regression tests remain. Local checks were repeated after cleanup;
this report's AWS measurements still refer to the frozen diagnostic candidate.
Reusable tools are documented in [the validation guide](aws-live-validation.md).
The frozen source, diagnostic binary, reader results, and run evidence are retained.
