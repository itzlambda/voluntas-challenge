# Replica placement comparison, 2026-10-27

**Observed result:** fewer closes and publisher switches, without the broad
coverage excursion seen previously. The p99 latency bucket bounds were
unchanged. Mean internal latency and CPU use were higher under higher input
traffic. This historical comparison does not establish that placement caused
fewer closes.

## Configuration and scope

The baseline is the [previous mirrored-group run](hot-standby-validation-2026-10-26.md),
not the earlier single-feed run. Both requested 900 seconds on the same retained
London AWS machine: four vCPUs, about 8 GiB RAM, four book workers, 256 markets
per socket, two copies per market, three discovery sockets, twenty opens/s,
ten Gamma requests/s, ten-second PINGs, and thirty-second silence recovery.
Both used release builds, normal SBE output, no raw capture or injected failures,
and five-second metrics scrapes. The host was idle before this run, no other
project test was running, and its clock reported synchronized.

| | Previous mirrored groups | Spread replicas |
|---|---|---|
| UTC window | October 26, 23:25:55–23:40:56 | October 27, 01:13:24–01:28:26 |
| Requested observation | 900 seconds | 900 seconds |
| Exit / stop reason | 0 / duration complete | 0 / duration complete |
| First successful catalog scan | 259.8 seconds | 258.2 seconds |
| Order-book sockets at last scrape | 2,026 | 2,014 |
| Desired markets at last scrape | 259,214 | 257,669 |

Frozen source was built with the same compiler and release settings. The only
production-source difference is src/core/subscriptions.rs. Collector SHA256:

- Previous: 7aca4c17c3caa62442d8fe973c713e0ffdec534caa335834601a7a406eb7b89d
- New: a3c614fbaa41bb865c1c4b33c8dae12ef96a04fb559c4deaa04c7aaa20869278

No production code changed during measurement. All 112 enabled local tests and
Clippy passed before this run; seven opt-in tests were skipped.

## Closures and failover

Incident counts cover complete runs, including the unsampled tail. Connection
exposure integrates sampled connected sockets, excluding unconnected startup
time from the socket-hour denominator.

| Measurement | Previous | New |
|---|---:|---:|
| Slow-consumer 1013 closes | 12 | 3 |
| 1001 remote closes | 6 | 0 |
| Abrupt TLS / I/O session ends | 1 | 0 |
| Failed connection attempts | 0 | 0 |
| Total session-ending incidents | 19 | 3 |
| Sampled active socket-hours | 328.65 | 327.69 |
| 1013 closes per 100 socket-hours | 3.65 | 0.92 |
| Completed publisher switches | 1,984 | 228 |
| Switch reasons | 1,728 remote close; 256 I/O | 228 remote close |

The new closes affected one sequential primary-pool socket twice and one spread
replica-pool socket once. Neither pool is permanently required to publish.
There were no silence or decode-error session ends.

The failed primary-pool socket's 47 market switches went to 47 distinct replica
connections; the failed replica-pool socket's 181 switches went to 181 distinct
primary connections. All 228 handoffs used non-adjacent connection IDs. Two
handoffs were between the two failed sockets for one shared market: independent
groups still allow both copies of an individual market to fail.

## Coverage and standby eligibility

| Last scrape | Previous | New |
|---|---:|---:|
| Synced / desired markets | 259,024 / 259,214 | 257,633 / 257,669 |
| Markets without a complete publisher | 190 | 36 |
| Standby-ready markets | 259,024 | 257,620 |
| Synced markets without ready standby | 0 | 13 |
| Synced / desired outcome books | 518,054 / 518,428 | 515,266 / 515,338 |

For the equal elapsed 400–895-second slice, after both connection ramps, the
previous sampled market deficit ranged from 171 to 427, including an additional
256 unavailable markets. The new deficit stayed between 35 and 36. Maximum
sampled synced-but-unprotected markets in that slice were 240 previously and
13 now.

No broad coverage loss was recorded in the new run. Five-second samples can
miss brief both-copy outages, so this does not establish zero gaps. Deficits
also include incomplete initial/recovery baselines and changing catalog
membership. Thirteen publishing markets still lacked a ready second copy at
the final scrape.

Follow-up inspection separated the final deficit into 36 markets already
missing before the closes, five existing standby gaps, and eight additional
standby gaps after a close. Missing-book requests retried 154 source outcome
books across 42 connections twice. The socket with repeated closes had only
117 seconds and then about 85 seconds in its recovery sessions, both shorter
than the existing 120-second first-book retry wait. The
[design note and bounded retry policy](../design-journal.md#missing-initial-books-and-bounded-resubscription-2026-10-27)
record the evidence and planned changes. The upstream cause of the missing
baselines remains unproved.

Readiness stayed healthy after the first post-scan healthy probe in both runs.
Both raw traces also contain an early startup 200 before the initial scan,
followed by startup 503 responses. That pre-existing startup behavior is not
treated as steady-state health or a placement regression.

## Latency, workload, and resources

Both comparisons use histogram differences over elapsed 595–895 seconds,
a 300-second window. Upper bounds are bucket edges, not exact percentiles.
Exchange age samples accepted price-change messages on both feeds and includes
clock offset. Internal latency samples emitted publisher price-change levels.
Full books and snapshot-derived output are excluded. The populations differ:
their counts are not a loss ratio, and their percentiles cannot be subtracted.

| Measurement | Previous | New |
|---|---:|---:|
| Decoded input events/s | 31,491 | 40,307 |
| Internal latency observations | 9,494,030 | 12,110,015 |
| Internal mean | 79.34 µs | 107.37 µs |
| Internal p50 upper bound | 25 µs | 50 µs |
| Internal p95 / p99 upper bounds | 250 µs / 1 ms | 250 µs / 1 ms |
| Exchange-age observations | 9,426,545 | 12,069,305 |
| Exchange-age mean | 7.459 ms | 7.713 ms |
| Exchange-age p50 / p95 / p99 upper bounds | 10 / 20 / 50 ms | 10 / 20 / 50 ms |
| Mean CPU, cores of four | 1.26 | 1.68 |
| Peak sampled RSS | 2,079.3 MiB | 2,055.1 MiB |
| Decode errors / negative exchange ages | 0 / 0 | 0 / 0 |
| Writer-capacity waits, worker-seconds | 0 | 0 |
| Same-source full-book mismatches | 54 | 70 |
| Strictly older full books skipped | 30 | 85 |

The new window had about 28% more decoded traffic. Higher CPU use and mean
internal latency cannot therefore be assigned to placement alone. Aggregate
CPU headroom and zero writer waits do not exclude a brief per-socket bottleneck.
Mismatches compare each source with its own held book, not another replica;
they do not prove lost exchange events. Skip counts use the unchanged older
full-book filter.

## Output and retained evidence

After clean shutdown, the supplied SBE reader decoded the entire
4,697,620,480-byte output: 5,492,556 snapshots and 24,827,894 price-level records
across 515,266 outcome symbols. It verified framing, domain values, initial
baseline presence, and sorted unique positive snapshot levels. Without raw
capture, this does not independently reconstruct every source or establish
update completeness across publication periods.

Ignored evidence is under data/aws-replica-placement-20261027T011046Z/: frozen-source
and build manifests, feature patch, binaries, unchanged baseline runner, raw
metrics, process samples, incidents, comparison scripts/JSON, HTML, and reader
results. The large SBE remains on AWS; its hash is recorded in full/status.json.
Previous artifacts were preserved. No collector remains running, and no
firewall rules or fault injections were used for this comparison.

One historical run per version with different live traffic is not a controlled
causal result. The observed closure rate was lower and independent handoffs
worked. Repeated matched observations would be needed to claim a repeatable
reduction in slow-consumer closes or a latency improvement.
