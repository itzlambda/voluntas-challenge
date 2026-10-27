# Research

Evidence and reasoning behind the collector. [SPEC.md](../SPEC.md) defines the
current behavior; this directory explains why it looks that way, which
alternatives were rejected, and what the measurements do and don't establish.

## Operational metrics transition

Operational statistics moved to [Prometheus metrics](../LIVE_METRICS_SPEC.md).
The [final metric contract](../METRICS.md) is the allowlist.
Shutdown summaries and their custom production histograms were removed.
Measurement readers use `/metrics` or the Prometheus HTTP API and record query
windows and sample counts. Independent raw-feed analysis and SBE correctness
checks remain separate.

## Start here

- [Key findings](FINDINGS.md): the facts about Polymarket the design relies on.
- [Design journal](design-journal.md): how the design evolved, the approaches
  we dropped and why, and how agents were used.
- [Telemetry](telemetry.md): what the collector reports and how the latency
  metrics are defined.

## Design decisions and alternatives

- [Architecture options](collector-architecture-options.md): the four
  ownership models considered and their trade-offs.
- [Market discovery and lifecycle](market-discovery.md) and
  [updated-time polling](docs/polymarket-updated-at.md): the polling design
  replaced by lifecycle sockets plus reconciliation scans (SPEC section 7).
- [Extension 4: Latency improvements](../LATENCY_IMPROVEMENTS.md):
  TCP_NODELAY rationale, existing evidence, and the comparison not yet run.
- [Release profile](release-profile.md): the adopted ThinLTO configuration
  and its rationale. No comparative performance gain is claimed.
- [Worker sizing and affinity](docs/worker-sizing-and-affinity.md): CPU-based
  worker count and why affinity remains a separate experiment.
- [Worker scaling](worker-scaling.md), open: why throughput did not grow
  linearly with workers, with profiles and hypotheses.
- [Synthetic exchange](fake-exchange.md): why the end-to-end test server is a
  separate implementation.
- [On-chain discovery](on-chain-discovery.md), deferred: when it would be worth
  revisiting.

## Exchange behavior experiments

Market discovery and lifecycle:

- [Polymarket API reference](docs/polymarket-api.md) and
  [comparison of market-listing methods](docs/polymarket-market-discovery.md)
- [Seed coverage and lifecycle correctness](docs/polymarket-lifecycle-correctness.md)
- [Lifecycle WebSocket coverage and reliability](docs/polymarket-lifecycle-ws.md)
- [Recent closures](docs/polymarket-recent-closures.md) and
  [incremental openings and closures](docs/polymarket-recent-openings.md)
- [Protocol v1/v2 adoption](docs/polymarket-protocol-adoption.md)
- [Resolution lag and retained-market backlog](docs/polymarket-resolution-lag.md)
  and [resolution tail](docs/polymarket-resolution-tail.md)

Market WebSocket:

- [Observed messages](docs/polymarket-observed-events.md)
- [Subscription, heartbeat, and book-format behavior](docs/polymarket-subscription-behavior.md)
- [Zero-asset subscriptions](docs/polymarket-empty-assets.md)
- [Subscription and connection capacity](docs/polymarket-subscription-capacity.md)
- [Ten connections to one book](docs/polymarket-redundancy.md) (for the
  redundancy extension)
- [Exchange-to-receive age](docs/polymarket-exchange-to-receive.md)

## Correctness and performance checks

- [Live full-book ordering incident](experiments/live-book-ordering-incident-2026-10-26.md):
  four disagreements traced to earlier-state books overwriting newer updates,
  with an offline timestamp-filter experiment.
- [Five-minute trade and depth capture](experiments/trade-depth-coverage-2026-10-26.md):
  whether received price changes reconstruct full books while trade notifications
  are ignored, and the limits of matching individual trades to depth changes.
- [Saved book incident replay](experiments/book-incident-findings.md),
  [snapshot generations](experiments/book-incident-snapshot-generations.md), and
  [incidents from the resource-limited run](experiments/book-incident-fresh-findings.md)
- [Established and joining feed experiment](experiments/book-handoff-results-2026-10-26.md)
- [Stage latency](experiments/stage-latency-findings.md) and the
  [writer profile](experiments/m5-writer-profile-findings.md); capacity
  results are summarized in [MEASUREMENTS.md](../MEASUREMENTS.md).

## Before submission

- [ ] Review and commit the intended working-tree changes. Include or hide the
  experimental order-book explorer using the [README instructions](../README.md).
- [ ] Check README, SPEC, SUBMISSION, measurement reports, and demo claims against
  the final source and challenge requirements. Keep historical measurements
  dated; the internal 3× load target remains unpassed.
- [ ] Validate the final commit from a clean checkout: formatting, strict Clippy,
  Rust tests, demo and operations tests, and a release build. Collect a bounded
  live sample, stop with SIGINT, and verify its SBE output.
- [ ] Check for secrets, identifying paths, large captures, broken links, and
  unsupported claims. Keep required evidence durable and scratch data out of Git.

## Future improvements

- **Reassess the worker count.** The pool was intended to take over SBE encoding,
  but encoding remains in the writer to preserve the supplied encoding path.
  Current CPU usage suggests one book worker may suffice. Benchmark one worker
  against the CPU-sized pool before simplifying. See the
  [worker-count proposal](../LATENCY_IMPROVEMENTS.md#future-improvement-reassess-the-worker-count).

- **Further latency experiments.** The
  [extension 4 notes](../LATENCY_IMPROVEMENTS.md#future-improvement-separate-encoding-from-file-writes)
  retain the proposed encoding/I/O split, snapshot staggering, scheduling
  isolation, activity-aware worker placement, WebSocket tuning, and lower-priority
  receive-queue experiments. Multiple collector hosts are a later option if
  these optimizations are insufficient. These are proposals, not measured gains.

- **Benchmark socket-to-worker handoff.** WebSocket tasks already read and
  decode messages before forwarding events to book workers. I'd benchmark
  decoding, dispatch, queue wait, and downstream backpressure under bursts
  before changing scheduling or queue sizes. The
  [extension 4 notes](../LATENCY_IMPROVEMENTS.md#future-improvement-benchmark-socket-to-worker-handoff)
  describe this as further tuning of the existing pipeline.

- **Start collecting while the initial scan is still running.** I had this
  behavior earlier, but removed it during cleanup to simplify the edge cases
  around partial discovery and lifecycle reconciliation. I'd like to revisit
  subscribing markets as keyset pages arrive, so collection can begin before
  the full catalog is available. Early pages would admit additions; only a
  successful complete scan could establish the catalog used for removals.
  I'd need to handle failed scans and retries, deduplicate markets found through
  both REST and WebSocket hints, and preserve resolutions received during the
  scan so an older page cannot re-add them. The benefit would be less time to
  first data, with the same complete-scan reconciliation guarantees.

- **TCP/TLS connection reuse and path selection.** I'd like to measure how much
  connection setup and teardown cost us before adding another pool. The Gamma
  client already reuses a `reqwest::Client` with its own HTTP connection pool.
  For WebSocket startup and reconnects, I'd investigate a bounded pool of
  preconnected TCP/TLS streams, with latency measurements, health checks, and
  expiry for idle candidates. An active WebSocket owns its underlying transport
  for the session; that transport can't return to an HTTP pool after the
  upgrade, and a closed TCP connection needs replacing. This could also support
  Extension 4 experiments with source-port choices and destination addresses
  to see whether different connections take better network paths. I'd compare
  setup time, RTT, and arrival times for matching market events, since a faster
  handshake doesn't necessarily mean faster market data. I'd keep the pool
  small and only retain the approach if measurements justify its resource use
  and complexity; changing a source port doesn't guarantee a different route.

- **Investigate remaining ordering and trade-to-depth limits.** The worker now
  skips strictly older full books while synced, using the maximum exchange
  timestamp of applied depth per outcome book. Equal or newer books remain
  accepted; older absolute price changes remain arrival-ordered. Reconnect
  baselines reset the comparison period. The retained incident replay skips two
  stale books and prevents its four rollback and repair disagreements. This does
  not establish universal timestamp ordering: an equal-time stale book can still
  overwrite newer state, an older delta can still overwrite a level, and an older
  full book might contain a useful correction. Further captures should quantify
  these cases. Separately, check trade-to-depth coverage without subtracting
  trades. Existing reconstruction checks do not establish upstream completeness.
  See [the filter validation](experiments/older-full-book-filter-2026-10-26.md).

- **Thread-per-core runtimes.** I'd like to compare
  [Monoio](https://github.com/monoio-rs/monoio),
  [Glommio](https://github.com/DataDog/glommio), and other async runtimes with
  the current Tokio runtime and dedicated, optionally pinned Tokio workers.
  I'd use the existing collector benchmarks and fake exchange with the same
  workloads, CPU budgets, and correctness checks, comparing throughput and
  receipt-to-SBE latency, including the tails. Any benefit would need to justify
  the WebSocket/TLS integration work and maintenance cost.

- **CPU processing and SIMD optimizations.** My current assessment is that
  CPU processing isn't the main bottleneck, so I haven't prioritized
  CPU-specific optimizations. I'd confirm that with profiling and benchmarks
  before optimizing parsing, allocations, copies, or book-update data
  structures, or trying SIMD. I'd compare targeted changes with the current
  implementation using the same workloads and correctness checks, looking
  for lower CPU use and end-to-end latency, including the tails. I haven't
  measured a benefit from these proposed optimizations yet.

- **Agent-driven optimization experiments.** Once I'm confident that the
  end-to-end checks can reliably catch incorrect output and missed work, I'd
  like to try an automated research loop on the implementation. An agent could
  profile it, propose a change, run the correctness checks and benchmarks, and
  use the results to choose the next experiment. I'd keep the workload and
  verification rules fixed, and only keep changes with repeatable speedups or
  lower latency that preserve the collector's behavior.

- **Known Polymarket and SDK issues.** I'd like to review the Polymarket SDK
  repositories and their GitHub issues for problems other users have already
  encountered. I'd check which reports still apply to the APIs and behavior we
  rely on, then reproduce the relevant cases locally and add targeted patches
  or workarounds where needed. That should help me catch known problems before
  running into them in production. I'd also record why each workaround exists
  and when it can be removed after an upstream fix.

- **Balance markets across sockets by activity.** I'd like to distribute markets
  across WebSocket connections based on their expected update traffic rather
  than just their market IDs. I'd start by checking whether recent trading
  volume is a useful proxy for event rate, then compare it with the message
  rates and bytes we actually receive. The goal would be to spread busy markets
  across sockets so a few active markets don't overload one connection. I'd
  also look at rebalancing as activity changes, while limiting subscription
  churn and preserving book correctness during any handoff.

- **Reusable message slots.** If profiling shows that message allocations are
  a meaningful cost, I'd like to benchmark a fixed pool of reusable slots
  against our current bounded queues. For example, each WebSocket task could
  start with 1,000 slots, with the actual pool size chosen from measurements.
  A producer would fill a free slot and pass ownership through the processing
  pipeline to the writer. Once encoding finishes, the slot and its buffer
  capacity would return to the pool for reuse. When no slots are free, the
  producer would wait, providing backpressure without allocating more slots.
  Only the current owner would be allowed to modify a slot. I'd also bound
  payload capacity or total bytes, since a fixed slot count alone doesn't bound
  memory when messages and snapshots vary in size. I'd measure allocation
  savings, retained memory, pool coordination costs, and latency before deciding
  whether this is worth the added complexity.

## Archived test-host incident

The [original test-host connectivity incident](archive/test-host-connectivity/README.md)
records why we initially chose a long silence timeout and why the attribution
changed after the two-host and AWS observations. The current collector uses
hot-standby failover and a 30-second no-frame limit. Obsolete probes and
generated timelines were deleted; anonymized measurement summaries remain.

## Scripts, results, and raw data

Scripts and result summaries are in `experiments/`. Their relative paths and
reproduction commands assume `research/` as the working directory.

Raw captures (`recordings/`) and saved Gamma catalogs (`../data/`) are
Git-ignored and are no longer kept in the checkout. They were archived outside
the repository; reports cite the summaries saved here. The ignored replay
check in `tests/book_incidents.rs` needs two of those recordings restored.

Some large result files were trimmed to aggregates and sample rows. Each has a
`compaction` field naming the full original in Git history, for example
`git show 76e65b8:research/experiments/polymarket-resolution-tail-results.json`.

The first implementation, its recording tools, and its book-processing
benchmarks are in local Git commit `19b2bea` under `v1/`, for example
`git show 19b2bea:v1/src/polymarket/discovery.rs`. Reports about that code
were removed from this directory after commit `76e65b8`; FINDINGS.md keeps
their measurements.
