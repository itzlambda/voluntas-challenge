# Where receive-to-encode latency accumulates

Measured on 2026-10-26. Ordinary decoding and book updates take microseconds;
most elapsed latency is waiting for a worker or the writer. The benchmark
also directly observes synchronous 64 MiB file writes lasting over 100 ms.
A separate CPU profile records substantial runnable-thread scheduling delay.
These observations explain mechanisms that create a slow tail. They do not
prove what caused the historical full-catalog run's five-second maximum.

## Scope

Release builds, four workers, six runtime threads, 64 producers, 5,000 markets,
10,000 books, and 25 levels per side. Inputs include absolute updates, removals,
reinsertion, and a full-book refresh every 32 messages. Capture is disabled.
The test uses the actual decoder, workers, and SBE writer, with a separate
oracle validating every output update, every periodic snapshot, and final books.
There is no WebSocket, TLS, discovery, or live exchange in this experiment.

One in 67 input messages carries monotonic stage timestamps in an isolated
copy of the source. This interval avoids always sampling the same full-book
position. Every emitted level record from a sampled message is measured.
A message producing two records shares its decode and processing stages.
All stages use the same samples, so their means add to the total. Their
percentiles must not be added.

The receive proxy is actual entry to the decoder. It excludes producer
lateness. The historical M5 JSON field `receive_to_encode` starts at scheduled
arrival, so that field is called **scheduled-to-encode** in this report.
Current benchmark output names it `scheduled_to_encode` explicitly.
The diagnostic changes message-envelope size and adds timing overhead.
These are rough diagnostic timings, not a new production latency guarantee.

## Main breakdown

The 70-second normal-rate case offers 16,000 messages/s, achieves about
15,994 messages/s, validates 10,000 periodic snapshots, and samples 32,460
emitted updates. Average stage durations are:

| Stage | Mean | Median | p99 |
|---|---:|---:|---:|
| Decode JSON and construct events | 18.89 µs | 4.54 µs | 34.32 µs |
| Decoded event to worker start | 15.811 ms | 3.77 µs | 620.883 ms |
| Worker book handling and record construction | 5.53 µs | 2.02 µs | 13.99 µs |
| Handling complete to writer submission | 4.27 µs | 0.21 µs | 12.44 µs |
| Writer submission to writer start | 5.646 ms | 0.576 ms | 101.907 ms |
| Sampled record conversion, encoding, possible chunk write | 0.84 µs | 0.20 µs | 3.96 µs |
| **Actual decode entry to encoded update** | **21.487 ms** | **0.774 ms** | **664.466 ms** |

Waiting stages include queue residence, blocked channel sends, and task or
thread scheduling. The timestamps do not separate those causes per record.
Service-stage wall times can also include preemption.

Sparse sampling misses some records that trigger chunk rotation. Timing
**all 2,194,364 record writes** in the same run captures a maximum of
237.925 ms; their median is 0.19 µs and p99 is 4.04 µs. The five direct file
flush measurements range up to 237.874 ms, with a 191.317 ms median. This
includes the final flush, which does not itself delay subsequent encoding.
Four earlier chunk rotations happen before the output finishes. Six complete
record writes exceed 100 ms, so disk flushes are not the only possible source
of long wall-clock write durations.

## Load cases

Each row is one run on a shared six-CPU host. No configuration winner or
speedup is claimed.

| Offered rate | Feed duration | Achieved rate | Actual receive-to-encode median | p99 | Mean |
|---|---:|---:|---:|---:|---:|
| 16,000/s | 10 s | 15,995/s | 2.105 ms | 337.478 ms | 15.968 ms |
| 42,000/s | 10 s | 41,956/s | 2.327 ms | 204.868 ms | 21.861 ms |
| 125,061/s | 10 s | 101,516/s | 777.993 ms | 4,656.416 ms | 1,173.196 ms |
| 16,000/s, including periodic snapshots | 70 s | 15,994/s | 0.774 ms | 664.466 ms | 21.487 ms |

All four runs pass output correctness. The high-load case fills worker input
queues and the writer queue and fails to sustain its offered schedule.
Its mean wait before worker processing is 1,071 ms. This reproduces seconds
of backlog without seconds of JSON parsing or book mutation.

Short cases are diagnostics, not full-duration M5 acceptance runs. Even normal
and peak cases have producer-lateness spikes, so they cannot pass the strict
scheduled-load acceptance gate. They remain useful for measuring what happens
after actual decoder entry and for proving that output work completed.

## CPU and scheduling evidence

A separate 10-second, 42,000-message/s profile records 793 user-space CPU
samples with zero lost samples, and completes output validation. Runtime
threads account for 88.27% of these samples; the writer accounts for 11.60%.
Individual hot symbols include JSON string parsing, book handling, side
updates, routing, and subscription-ID hashing. There is no evidence that SBE
encoding alone dominates CPU.

Across the monitor's observations, the writer accumulates 1.964 seconds of
runnable CPU wait. Each runtime thread accumulates 1.470–1.784 seconds.
These scheduler counters include preparation and finishing observations;
they are cumulative per-thread values, not additional per-message latency.
Inspected CPU-quota controls report no throttling; the shared host still has
CPU pressure. Scheduling contention is observed, but its exact source is not
isolated by this experiment.

The first profile attempt selected a short-lived perf helper instead of the
benchmark process, failed, and is excluded. The corrected retry selected the
benchmark process, recorded only the feed/drain phase, and passed correctness.
Profiled latency is excluded from the load-case table.

## Uninstrumented control

The control is reconstructed from the exact original source hashes, with
all stage probes removed. One 10-second run at each of 16,000/s and 42,000/s
passes correctness and sustains its offered rate. Its scheduled-to-encode
p99 bounds are 130 ms and 178 ms respectively. These include producer lag
and cannot be compared directly with the actual-receipt table above.
They show that the slow tail also exists without the diagnostic probes.
Because controls ran later on a variable shared host, these runs do not
quantify probe overhead or establish an optimization speedup.

## Optimization priorities

1. **Investigate blocking chunk writes first for writer-tail latency.**
   A synchronous `write_all` happens inside the encoding path. Test a design
   that lets completed chunks drain separately if changes to the supplied
   helper are permitted. Keep chunk ordering, backpressure, final flush,
   shutdown, and exact SBE readback as validation requirements. This would
   address one measured stall source, not all scheduling delay.
2. **Control host scheduling before changing defaults.** Repeat on a quieter
   host or isolated service. Compare a small set of runtime/worker counts
   with five alternating runs before choosing a configuration. Increasing
   queue capacity alone does not increase service capacity.
3. **Measure snapshot bursts in time before changing their scheduling.** The
   periodic-snapshot case has its largest waits before worker processing.
   Newly seeded books have nearby timer deadlines. A follow-up should correlate
   queue spikes with timer firings, then test staggered first deadlines while
   preserving the 60-second cadence. These runs do not isolate snapshot cost
   from host variation.
4. **Treat parser improvements as throughput work.** Parsing is visible in the
   CPU profile, but its ordinary per-message duration is tens of microseconds.
   Parser-only optimization cannot by itself eliminate 100–250 ms disk stalls.

No production code or default was changed for this investigation. The recorded
runs predate worker-to-writer batching.

## Method

A probe script copied the sources, injected the diagnostic timestamps only in
that copy, built the paced pipeline benchmark in release mode, and ran the cases
serially. The script and benchmark were removed after this report; they are in
Git history at `cba9d47` as `research/experiments/stage-latency-probe.py` and
`tests/m5.rs`.

Evidence: [results, controls, source fingerprints, and CPU profile summary](stage-latency-results-2026-10-26.json).
The JSON also records the failed profile attempt and measurement limitations.
