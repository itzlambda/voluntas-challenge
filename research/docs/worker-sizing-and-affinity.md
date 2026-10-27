# Worker sizing and CPU affinity

The collector now defaults to one book-processing task per unit of CPU
parallelism available to the process. This replaces the fixed default of
four and removes the power-of-two and 16-worker restrictions. Explicit
`--workers N` accepts any positive count.

## Tasks, threads, and CPUs

A book worker is a Tokio task that owns a fixed set of markets. Tokio runs
these tasks alongside socket, TLS, decoding, discovery, and control tasks on
its multi-thread runtime. The runtime's worker threads are a separate setting:
[Tokio sizes them automatically unless overridden](https://docs.rs/tokio/1.54.1/tokio/runtime/struct.Builder.html#method.worker_threads).
The writer and telemetry each have their own blocking OS thread.

The default book-worker count comes from
[`std::thread::available_parallelism`](https://doc.rust-lang.org/std/thread/fn.available_parallelism.html),
falling back to one if detection fails. This estimates available logical CPU
capacity rather than physical cores or current idle CPUs. On Linux it can
account for CPU affinity and cgroup quotas, subject to the documented access
and detection limitations. Query it at startup, before any optional future
per-thread affinity restriction. The worker count stays fixed for a process
lifetime, so changing CPU limits does not move live books between workers.

Examples, assuming the process has the stated available CPU capacity:

- 6 CPUs: 6 book workers by default.
- 16 CPUs: 16 book workers by default.
- 32 CPUs: 32 book workers by default.
- `--workers 14`: 14 book workers regardless of the default.

Using CPU count minus one or two can be tested as an explicit override. In
the current shared runtime, fewer book tasks do not reserve particular cores
for I/O or the writer. `--workers` does not change Tokio's thread-pool size,
and an explicit `TOKIO_WORKER_THREADS` setting is independent of this default.
CPU-based sizing is a portable starting policy, not a measured optimum for
every workload. It cannot remove a single writer's I/O bottleneck.

## Routing beyond 16 workers

The old routing rule used only the final hex digit of the condition ID.
It could distinguish at most 16 buckets, so raising the worker-count limit
alone would leave some workers unused on larger machines.

Routing now uses the low 64 bits of the condition hash modulo the worker
count. For powers of two, it reads only the necessary low hex digits and
uses a bit mask. This preserves assignments for the old 1/2/4/8/16 counts
and reaches all buckets for larger counts. Other positive counts use modulo.
The suffix is read directly from bytes without allocation. Both outcomes
and every message for one market still use the same worker for the process
lifetime. No writer, timestamp, snapshot-cadence, or output-format change is
part of worker sizing.

Market ownership permits parallel book workers without shared mutable books;
automatic sizing lets that scale beyond four.

## Affinity remains a separate experiment

Tokio's [`pin!`](https://docs.rs/tokio/1.54.1/tokio/macro.pin.html) pins a value
in memory. It does not pin a task to a CPU. A normal spawned task can move
between runtime worker threads. Pinning runtime threads through an
[`on_thread_start`](https://docs.rs/tokio/1.54.1/tokio/runtime/struct.Builder.html#method.on_thread_start)
hook alone does not give a particular book worker a permanent CPU.

A dedicated-thread design would give each book worker its own OS thread,
drive a current-thread runtime or local task set there, and apply OS CPU
affinity to that thread. I/O and the writer need a separate CPU budget.
Affinity may reduce migration and improve cache locality, but it does not
eliminate context switches or reserve CPU time from other processes. Logical
CPU IDs also need not represent distinct physical cores.

No CPU pinning is enabled by this change. Measure cache/migration costs and
end-to-end latency before replacing the shared runtime. Keep the explicit
worker override for controlled comparisons.

## Validation

The new CLI and routing regressions fail on the earlier power-of-two/16-worker
restriction. They pass with the new policy. The normal suite passes 81 tests;
six existing opt-in tests remain ignored. Formatting and strict Clippy pass.
Independent reference replays cover 3, 6, and 32 workers across book updates,
periodic snapshots, reconnection, and lifecycle removal/re-addition. Legacy
replays and their bounded-drain deadlines remain unchanged.

A 12-second local process check restricted only the collector to three
allowed logical CPUs and omitted `--workers`. Startup reported three book
workers. Its 3,095 SBE records covered all 124 generated assets, including two
retirement records, with zero checker errors, zero gaps, zero decode errors,
and zero book mismatches. Artifacts remain under the ignored
`fake-exchange/runs/cpu-default-1793036321666267698/` directory.
This verifies automatic sizing and output correctness; it is not a throughput
comparison or a measurement on 16- or 32-core hardware.
