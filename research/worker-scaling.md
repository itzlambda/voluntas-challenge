# Worker scaling (open question)

Status: open, 2026-10-25. To analyze in the future; nothing in M0–M2 depends on it.

## Question

Each worker is an independent Tokio task with its own markets, so throughput
was expected to grow linearly with the number of workers. It doesn't at full
scale. Why?

## How it was measured

`worker_scaling_full` in `tests/load.rs` (release build):

- the synthetic workload is generated and all JSON decoded before timing;
- each worker's input queue is filled completely before the worker starts,
  so workers run flat out with nothing feeding or competing with them;
- the same total input runs with 1, 2 and 4 workers, with output discarded
  (workers alone) and with the real writer.

Run one case with `LOAD_SCALING_SINK=discard|writer` and
`LOAD_SCALING_WORKERS=N`. Machine: KVM VM, 6 vCPUs (1 thread per core), AMD
EPYC, 8 MiB shared L3, 11 GB RAM.

## Results

50,000 markets, 2.1 million events, one run per case:

| Workers | Discard: events/s | Speedup | Each worker | Writer: events/s | Blocked on writer |
|---|---|---|---|---|---|
| 1 | 104,026 | 1.00× | 106,694 | 91,439 | 0 s |
| 2 | 155,365 | 1.49× | ~80,000 | 183,391 | 0.07 s per worker |
| 4 | 250,737 | 2.41× | ~66,000 | 201,778 | 0.68 s per worker |

15,000 markets, 0.6 million events, discard, one run each: 1 worker 69,202
events/s, 4 workers 278,931 events/s (4.0×, linear).

## Evidence so far

1. **The VM doesn't throttle busy cores:** `openssl speed -multi N sha256`
   (pure compute, tiny working set) scaled linearly to 6 copies.
2. **Same work, more cycles:** at 30,000 markets / 1.2 million events,
   `perf stat` over the whole process showed the same instruction count and
   the same cache-miss count for 1 and 4 workers, but more CPU time and a lower
   effective clock rate (1.31 → 1.16 GHz). Whole-process counters are diluted
   by generation and decode, which are identical in both runs.
3. **Where the extra cycles go:** `perf record` on the worker threads
   (`tokio-rt-worker`) showed 32% more samples at 4 workers for the same
   events. Most of the increase was in `OrderBookSide::set` (+43%) and
   `OrderBookSide::differences` (+59%), which walk `BTreeMap` nodes, and in
   `tokio::sync::mpsc::list::Tx::push` (×2.9), the send into the one output
   channel shared by all workers.
4. **Smaller working set scaled linearly** (the 15,000-market runs above).

## Hypotheses

- **Shared memory and cache:** 100,000 books far exceed the 8 MiB L3, so
  workers compete for cache and memory bandwidth. Fits points 3 and 4.
- **Shared output channel:** all workers push into one MPSC channel drained by
  one thread, which also frees every record (cross-thread frees; the profile of
  JSON decoding showed allocator contention of this kind).
- **Noise:** identical runs varied by 10–15%, and 1-worker results ranged from
  69,000 to 120,000 events/s across sessions. Single runs can't separate this
  from the effect.

## Next steps

1. Repeat each case several times and report the median and range.
2. Separate in-process from hardware contention: run the 1-worker case as
   several separate processes at once (shares hardware, nothing in our
   process). Generate the data once or use fewer copies; an attempt with 4
   concurrent processes, each generating and decoding its own data, overran
   10 minutes and was stopped.
3. Vary the market count (10,000 → 50,000) at fixed events to see where
   scaling stops being linear.
4. Test the channel hypothesis by giving each worker its own output channel and
   sink in the test.
5. If cache-bound, revisit the `OrderBookSide` TODO (SPEC section 9): a sorted
   `Vec` is more compact than a `BTreeMap`.

Related: SPEC section 12, measurements 3, 4, 5, 6 and 9.
