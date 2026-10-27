# Hot-standby validation, 2026-10-26

The implementation is ready for merge under the agreed extension 1 contract.
Both-source failure still stops publication until a complete source returns;
this happened in the live run and is an availability limitation of mirrored
feeds, not evidence of continuous service through correlated failures.

## Build and checks

- Branch: `feat/hot-standby-replicas`, based on main `4645c7b`.
- Release collector SHA256: `7aca4c17c3caa62442d8fe973c713e0ffdec534caa335834601a7a406eb7b89d`.
- The production-source manifest matches the tested working tree. The writer
  and supplied helper sources are unchanged.
- 107 Rust tests passed; seven opt-in benchmark/capture tests were skipped.
  Clippy passed for all targets with warnings denied; rustfmt and whitespace
  checks passed. The independent capture audit has 19 passing self-tests.
- Astra and Opus 5.5 high found no material implementation defects. Astra also
  reviewed the reader correction allowing older timestamps only at an exact,
  input-proven replacement baseline.

The retained AWS host was idle after the other threads finished testing. It
provided four vCPUs and about 8 GiB RAM in London. Tests used a frozen release
build in a new directory; earlier runs were preserved. No other collector ran
alongside these measurements. The instance was retained after testing.

Both runs used four workers, mirrored pairs, capacity 256 markets per physical
socket, text PING every ten seconds, and a thirty-second no-frame deadline.
Other production defaults remained unchanged, including twenty connection
opens per second and ten Gamma requests per second. Fixed-market mode starts
no discovery sockets; full discovery used three.

## Controlled failures and exact output verification

The captured six-market run lasted 300 seconds, from 23:20:44 to 23:25:44 UTC.
It had twelve logical outcome books on two physical sockets. Four controlled
failures occurred in connection order 1, 0, 1, 0: two process-owned socket resets,
then two 38-second inbound blackouts scoped to individual process-owned sockets.
Both replicas were ready before each fault. All temporary firewall rules were
removed and cleanup verified.

The first failure affected the standby and counted no switch. The later failures
produced six I/O-error market switches and twelve silence switches. Silence
incidents recorded thirty seconds without a frame. The socket-destroy utility
printed a warning despite resetting its targets; ordered capture resets and
session telemetry establish the failures, rather than the utility's exit code.

The independent integer-book audit verified:

| Output | Verified count |
|---|---:|
| Price-level records, including source-derived corrections | 3,676 |
| Initial snapshots | 12 |
| Replacement snapshots | 36 |
| Ordinary snapshots matching preceding SBE updates | 60 |
| Completed publisher switches, with matching reason counters | 18 |

All six markets had one unique assignment to captured source prefixes and
replacement boundaries. No arbitrary snapshot difference was waived. A separate
scan using the supplied SBE reader decoded all 3,784 records and checked initial
baseline presence and sorted, unique, positive snapshot levels.

At five-second scrapes, all six markets remained synced; standby coverage
recovered after the faults. The controlled run had zero decode errors,
same-source book mismatches, older-book skips, and writer-capacity waits.
The audit proves consistency with the captured feeds. It does not prove exchange
truth or the uncaptured ordering between independent producers.

## Full-catalog run

The uncaptured full-catalog run requested 900 seconds, from 23:25:55 to
23:40:56 UTC including clean shutdown. Exit status was zero. Discovery completed
its first scan after about 260 seconds. Readiness returned 503 before initial
discovery completed, then remained healthy at subsequent scrapes; there were no
other observation errors.

The last scrape, at elapsed 895 seconds, reported:

| Measurement | Value |
|---|---:|
| Desired / synced / standby-ready markets | 259,214 / 259,024 / 259,024 |
| Desired / synced logical outcome books | 518,428 / 518,054 |
| Expected / connected order-book sockets | 2,026 / 2,026 |
| Expected / connected discovery sockets | 3 / 3 |
| Publisher switches: remote close / I/O error | 1,728 / 256 |
| Natural silence / decode-error switches | 0 / 0 |
| Decoded input events, both physical feeds | 20,118,904 |
| Decode errors / negative exchange ages | 0 / 0 |
| Writer-capacity wait | 0 worker-seconds |
| Same-source full-book mismatches / older books skipped | 54 / 30 |
| Peak sampled RSS | 2,079.3 MiB |

Mismatch observations compare accepted full books with the same source's held
book, across either feed. They are not cross-replica comparisons or proof of
missing exchange data. Skip observations reflect the existing per-source
strictly-older-full-book rule.

The complete SBE file was 4,026,531,840 bytes. The supplied reader scanned all
5,389,840 snapshots and 19,543,470 updates across
518,086 distinct outcome symbols. It verified framing, domain decoding,
initial baseline presence, and snapshot level ordering/validity. This uncaptured
run does **not** have the controlled run's source-by-source completeness proof;
the scan does not infer or excuse natural replacement boundaries.

## Latency populations and window

These are bucket upper bounds from cumulative Prometheus histogram differences
over elapsed 595–895 seconds, a 300-second window after ramp-up. They are not
exact percentiles or a comparison with a single-feed baseline.

| Population | Samples | Mean | p50 upper | p95 upper | p99 upper |
|---|---:|---:|---:|---:|---:|
| Nonnegative exchange age, accepted price-change messages on both feeds | 9,426,545 | 7.459 ms | 10 ms | 20 ms | 50 ms |
| Receipt to encoding, emitted publisher price-change level records | 9,494,030 | 79.34 µs | 25 µs | 250 µs | 1 ms |

Exchange age includes clock offset. Full books are excluded. Internal latency
excludes all snapshot-derived output, including switch snapshots and full-book
corrections. The populations differ: their counts are not a loss ratio, and
subtracting their percentiles is invalid. Lifetime p99 bucket bounds through
the last scrape were also 50 ms and 1 ms respectively.

During that window the collector decoded about 31,491 events/s across both
feeds and used an average 1.26 CPU cores of four, measured from process CPU
counters. This is one public-traffic validation run, not a saturation benchmark
or evidence of a speedup. Aggregate resource headroom does not establish why a
particular upstream socket was closed as a slow consumer.

## Observed availability limits

Twelve `1013` closes affected both connections of one pair. Other natural
session endings also occurred. The 1,984 switch incidents exactly matched the
last scrape's per-reason switch counters; standby-only failures and same-source
recovery did not manufacture additional switches.

The pair sometimes had no ready copy. At elapsed 420 seconds, the market
coverage deficit was 171; at 430 seconds it was 427, an additional 256 unavailable
markets. By 435 seconds it was 187. These are five-second samples, not exact
outage durations. Later, up to 240 synced markets temporarily lacked a ready
standby. The final deficit was 190 markets; this run does not fully diagnose
those incomplete baselines or establish complete exchange-wide coverage.

This demonstrates why desired, synced, and standby-ready must be read together.
Physical connectivity alone does not establish eligibility. Mirrored feeds
hide failures when the peer is ready; they cannot guarantee availability when
both copies fail or rebuild together. No automatic splitting, per-market lag
trigger, checksum checking, or extra metric was added in response.

## Retained evidence and reproduction

Ignored local evidence is under `data/aws-standby-validation/`: build and source
manifests, raw five-second Prometheus scrapes, process samples, run status and
file hashes, controlled raw capture/SBE, fault records, independent audit,
and analysis scripts. The large full-catalog SBE remains on the retained AWS
machine; its hash is recorded in `full/status.json`. Full-file scan source and
binary hashes are retained with build provenance. Last scrapes precede SIGINT;
whole-file record totals include the final unsampled tail and flush.

The repository audit tool can be rerun without AWS access:

```sh
python3 research/experiments/standby-live-audit.py --self-test
python3 research/experiments/standby-live-audit.py \
  data/aws-standby-validation/fixed --failure-order 1,0,1,0 \
  --metrics data/aws-standby-validation/fixed/final-metrics.prom
```

Collector and test processes stopped cleanly, and temporary fault rules were
removed. Test artifacts were not published externally.
