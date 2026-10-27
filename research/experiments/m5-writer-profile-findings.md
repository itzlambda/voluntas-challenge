# M5 writer latency profile — 2026-10-26

The measured writer latency limiter is synchronous chunk rotation, with additional host scheduling noise. Generated SBE encoding is inexpensive in ordinary calls. Increasing worker count or reducing the existing writer queue cannot remove the write that precedes encoding the next message. These profiles do not establish production p99, sustained writer capacity, or a speedup.

The probe uses the existing public `AlignedSbeWriter` and an instrumented `SbeEncode` wrapper; the wrapper delegates to the existing generated encoder. No other benchmark ran during the profile, but the host was not dedicated: other services and possible compiler activity remained.

## Measurements

Raw per-run evidence was not retained in the repository. Every successful run decoded every output record and checked symbol, side, price, sequence quantity, both timestamps, total count, and padded file length. Validated files were deleted after readback.

- Five direct runs each encoded and validated 2,000,000 updates: 10,000,000 total. Each message was 122 bytes with a 77-byte synthetic symbol; each run produced four 64 MiB chunks. This is an update-only probe, not a snapshot-depth benchmark.
- Across 15 internal rotations, encoder-entry delay was **93.085 ms minimum, 113.819 ms median, 453.946 ms maximum**. Twelve of fifteen exceeded 100 ms. The delay includes symbol allocation, helper checks, padding, the synchronous write, and scheduling; syscall tracing below identifies the main component.
- Ordinary complete call medians were 0.19–0.21 microseconds across the five runs. Generated encoder median was 0.08 microseconds in every run; per-run p99 was 0.10–0.13 microseconds. Instrumentation adds clock reads, so these are diagnostic timings, not uninstrumented capacity estimates. Generated-encode maxima reached 46.595 ms in the direct runs: elapsed-time outliers include descheduling.
- Final flushes took 99.510–130.708 ms. They are outside the clarified encoding target and should not be charged to the last message's encoding completion. Internal rotations are different: they delay subsequent encoding.
- A separate, traced direct run validated 1,200,000 updates. Three `write(3, ..., 67108864)` calls took **200.877, 271.058, and 297.846 ms**. The first two coincide with measured encoder-entry delays of 204.665 and 272.249 ms; the final call is final flush. Thus write syscalls account for approximately 98% and 99.6% of the internal rotation delays. There were no fsync/fdatasync calls. This is synchronous `File::write_all` acceptance latency, not a disk-durability measurement.
- One paced diagnostic validated 1,600,000 updates at a planned 250,122 records/s (two outputs per 125,061 price-change messages/s). Its rotations delayed encoder entry by 265.162 and 338.196 ms; the bounded queue filled. Scheduled-age p99 was 340.840 ms, maximum 383.295 ms. Producer p99 lateness was 159.457 ms, so **this is not a valid offered-load gate**. Backpressure and scheduling affected the producer. A 144.150 ms generated-encode elapsed-time outlier also demonstrates host descheduling. Do not report this planned-age p99 as actual receipt-to-encode p99.
- Six logical CPUs; load averages before were 5.76/6.26/8.26 and after 7.37/6.55/8.22. Process snapshots omit identifying command arguments. `perf stat -e cycles:u true` succeeded with `perf_event_paranoid=2`; no sysctl was changed. A CPU sampling profile was unnecessary to attribute the traced rotation pause and was not collected.

## Source interpretation

`src/core/writer.rs` takes one record using `blocking_recv`, constructs its domain value, computes encoded length, and calls `write_msg`. There is no periodic polling delay in `blocking_recv`. Its elapsed waiting time can mean an empty input queue, not expensive channel overhead. The paced probe accumulated 3.509 seconds inside `blocking_recv`, including legitimate waits for paced arrivals and scheduling; that is not a per-message channel service-cost estimate.

`AlignedSbeWriter::write_msg` checks fit, calls `flush` when needed, resizes its chunk buffer, then calls the generated encoder. `flush` pads and calls `File::write_all` synchronously. With 122-byte messages and an eight-byte reserved sentinel, 550,072 messages fit in a chunk. The following message waits for the preceding chunk's write before its own encoding begins. The traced writes account for the measured pause even with no worker, telemetry, queue, or JSON generator in the direct path.

The generated level-update encoder writes fixed fields, checks numeric/conversion ranges, and copies symbol bytes. Its encoded-length check is constant work. Snapshot length calculation and encoding traverse both groups; the writer currently performs length calculation before the helper repeats it. Snapshot preparation also creates domain entry vectors. Those costs were inspected, not benchmarked here. At depth 25 per side and a 77-byte symbol, a snapshot is 913 bytes: 100,000 periodic snapshots add 91.3 MB of payload, more than one chunk, even though their average rate is small. A full periodic stress run is still necessary.

`queue_peak` samples `records.len() + 1` after receiving. A racing producer can refill the freed slot before the sample, so a reported 65,537 does not imply a channel configured above 65,536. Bounds are message counts, not byte or time guarantees.

## Existing matrix evidence

The original 20-case matrix remains invalid for offered-rate conclusions: every case has `generator_valid=false`. For N=1/2/4/8, median feed times for a scheduled ten-second feed were 28.646/24.706/15.786/23.708 seconds. Median accumulated decode times were 18.064/22.855/18.559/27.132 seconds; these are sums across tasks, not wall time. Median writer encode-and-chunk-write totals were 3.410/3.759/4.051/5.225 seconds. Writer queue peaks reached the bound for some N=4 cases and all N=8 cases. Correctness passed, but scheduled ages include generator lag. These counters do not support choosing N or claiming a worker or writer capacity ceiling.

The standalone stdout matrix timeout was traced to telemetry holding stderr locked while idle and racing diagnostic `eprintln!`. Its output file was complete. **It was not evidence of stdout file-write failure.** The bounded nonblocking telemetry logger replaced that path.

## What the 100 ms target can mean

The accepted target is receipt to completed SBE encoding in memory, excluding physical file visibility. Under the current topology that memory is the aligned writer's chunk buffer: internal writes still block the next encoding. Our direct measurements demonstrate individual calls beyond 100 ms without any generator or worker queue. They do not alone prove that production p99 exceeds 100 ms: that requires actual arrival timing and the full pipeline distribution.

For illustration, 250,122 update records/s fill a chunk in 2.199 seconds. During a 338 ms rotation, at least arrivals in the first 238 ms already wait over 100 ms before the writer resumes; queue drainage adds further delay. This arithmetic explains why mean throughput margin can coexist with a p99 failure. It is an illustrative queueing bound, not a measured production percentile.

- **Generator fix:** necessary to make the gate interpretable. It cannot remove measured write pauses. Selecting N or declaring a gate outcome needs a pre-generated full pipeline test ([stage latency](stage-latency-findings.md)).
- **N and existing queue bounds:** can address worker contention or backlog, but cannot eliminate a rotation-triggering record's own pre-encode write. Shrinking queues moves waiting upstream; enlarging them permits more unencoded waiting. Neither is evidence of a p99 pass.
- **Hardware/I/O and host isolation:** could make the unchanged design pass; effective 64 MiB write acceptance must be below the remaining latency budget. A 100 ms whole-write budget alone requires over 671 MB/s before encoding, scheduling, and queueing allowances. Current traced acceptance took 201–298 ms. Faster physical storage is not proven to be the sole remedy: these wall timings include filesystem, dirty-page throttling, page faults, and scheduling. Re-test the actual deployment path without capture contention.
- **Encoding endpoint change:** timing only the generated encoder after dequeue would omit queue residence and rotation delay and would not satisfy receipt-to-encoding completion.

## Possible change if the latency gate still fails

This was not implemented: SPEC section 5 marks output improvements as not planned. It depends on whether a complete independently owned SBE message buffer, encoded by its owning worker, qualifies as the approved completion point. If approved, workers would use the unchanged generated encoder to produce complete bytes and enqueue an owned `SbeEncode` wrapper; the single writer would pass every wrapper to the unchanged `AlignedSbeWriter`. The wrapper's `encode` copies the already encoded message into the aligned chunk, retaining helper alignment, padding, and schema behavior. Per-worker record order must remain intact, and all records must still be delivered and validated after final flush. Completion of the full output remains a separate required check.

This would move encoding ahead of file rotation but would not make output throughput or backpressure disappear. Encoded backlog bounds must absorb measured stalls plus periodic snapshots; 65,536 update messages cover only about 262 ms at the illustrative 250,122 records/s. Long or repeated stalls would still reach upstream receipt-to-encoding latency. No architecture prototype was needed to establish the present limiter, and none was made.

## Method

The probe was a standalone Rust program linked against the release library and
driven by a Python script. It ran five direct profiles, a separate syscall
trace, and one paced diagnostic, validated all output, and saved counters. Both
were removed after this report; they are in Git history at `cba9d47` as
`research/experiments/m5-writer-profile.rs` and `m5-writer-profile-run.py`.
