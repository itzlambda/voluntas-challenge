# Measurements

Capacity and latency measurements of the base collector, and what we
concluded from them. Extensions are out of scope.

## Target and status

Internal target: p99 below 100 ms from receiving a message to finishing its
SBE encoding, at three times the observed peak rate (3 × 41,687 =
125,061 messages/s). This is our headroom goal, not a number the challenge
requires. It excludes upstream delay, waiting for a 64 MiB chunk to fill, and
disk write completion.

**Correctness passes. The latency target has not passed.** The live run's p99
was above 255 ms (its histogram only bounds it at about 5 s), and the
pre-generated load test could not keep to the 3× schedule on a shared host.

All figures below predate worker-to-writer batching
([design journal](research/design-journal.md) Phase 18), so they are not
measurements of the final source.

## Live full-catalog run

One initial scan, then five steady minutes, with capture mode on.

| Measurement | Result |
|---|---|
| Initial catalog scan | 239,905 markets, 2,397 pages, 713 s at 4 requests/s |
| Steady message rate | Mean 16,103/s; one-second p99 36,842/s; maximum 41,687/s |
| Busiest 1% of markets | 57% of steady messages |
| Busiest worker / mean | 1.16 with four workers routed by condition ID |
| Book depth per side | Median 2, p99 48 (an independent REST sample agrees: median 2, p99 49) |
| Peak RSS | 1.51 GB, including capture state |
| Books at the end | 479,850 held, 99.72% synced |
| SBE readback | 5.5 M snapshots and 18.7 M updates; no unexplained differences |
| Slow-consumer closes | 10 code-`1013` closes, handled by 3 placement splits |
| Receive-to-encode | p50 ≤ 17 ms; p99 > 255 ms |

The readback found 48 fresh baselines after the placement splits; raw frames
prove each one follows an explicit Remove/Add
([snapshot generations](research/experiments/book-incident-snapshot-generations.md)).

## What the benchmarks told us

- **The writer stalls on disk, not on encoding.** In an isolated profile,
  each 64 MiB chunk rotation delayed the next encode by 93–454 ms (median
  114 ms), almost all of it the synchronous file write inside the supplied
  helper. Encoding itself took about 0.08 µs. More workers cannot remove a
  write that is already blocking the writer
  ([writer profile](research/experiments/m5-writer-profile-findings.md),
  [stage latency](research/experiments/stage-latency-findings.md)).
- **Book storage stays a `BTreeMap`.** A fixed tick array needs about 80 KB
  per side, about 80 GB for 500,000 books. Sorted-vector and hybrid timings
  come from micro benchmarks only and do not justify an end-to-end change.
- **Load is uneven across markets but not across workers.** Routing by
  condition ID kept the busiest worker within 16% of the mean.
- **Output path makes no reliable difference.** Writing to a regular file
  (median 100 MB/s) and to redirected stdout (median 73 MB/s) overlapped
  across runs on a loaded host. Stdout remains the contract.
- **The first synthetic load generator was invalid.** It built JSON inside the
  timed runtime and missed its schedule by seconds. Its replacement
  pre-generates input; at the 125,061/s schedule it reached about 69,600/s
  with full input queues. Host contention and pipeline cost still need to be
  separated before defaults can be chosen.

## Limits

- One bounded live run with capture enabled; machine load varied.
- The one-hour scan interval and 100 markets per connection are provisional.
- No controlled reconnect storm, allocator breakdown, or exchange-wide
  completeness check.
- Receive time is taken after the WebSocket read returns, so data queued in
  the socket is not counted as latency.
- An earlier run that hit a 1,024 file-descriptor limit is excluded.

## Runs and data

These were one-off runs on 2026-10-26. The drivers and benchmark harness that
produced them were removed after the conclusions above were recorded; they are
in Git history at `cba9d47` (`tests/m5.rs`, `research/experiments/m5-*.py`).

- [Live full-catalog run](research/experiments/m5-live-results-2026-10-26.json):
  release collector with capture enabled, one complete scan plus a steady
  window, clean shutdown, and readback with the supplied SBE reader.
- [REST depth sample](research/experiments/m5-rest-depth-2026-10-26.json):
  levels per book side from sampled CLOB `/books` responses.
- [Writer runs](research/experiments/m5-writer-results-2026-10-26.json):
  regular-file and stdout-to-file output, alternated over five rounds with
  every record validated.
- [Book storage](research/experiments/m5-side-results-2026-10-26.json):
  `BTreeMap`, sorted vector, hybrid and tick-array micro benchmarks at depth 25.
- [Pre-generated pilot](research/experiments/m5-pregen-pilot-2026-10-26.json):
  paced pipeline load at 125,061 messages/s with pre-generated input and an
  independent record oracle.

The synthetic capacity test in `tests/load.rs` remains runnable:

```sh
LOAD_WORKERS=4 cargo test --release --test load synthetic_load_full -- --ignored --exact --nocapture
```
