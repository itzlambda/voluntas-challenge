# Telemetry design decisions

The collector exposes operational measurements through a private HTTP endpoint.
[METRICS.md](../METRICS.md) is the final metric catalog.
[LIVE_METRICS_SPEC.md](../LIVE_METRICS_SPEC.md) defines the runtime contract.

## Keep the operational set small

The 18 metric families answer specific questions about latency, synchronized
coverage, input activity, connectivity, decoding failures, observed book
divergence, older-book rejection, failover, standby coverage, bottlenecks, and
publication freshness. Histogram counts supply
sample populations. Prometheus supplies rates, percentiles, reachability, and
history. Those derived values do not need separate application metrics.

Desired and synchronized markets provide the headline coverage measure.
Book counts expose partial initialization within a market, because its outcome
books receive separate initial snapshots even when they share a socket.
No market, asset, book, or connection IDs become metric labels. Worker IDs appear
only on elapsed input-processing time, where aggregation could hide an overloaded
worker.

## Separate exchange age from internal delay

Exchange age compares local UTC receipt with the exchange's event timestamp.
It includes clock offset, upstream processing, buffering, and read scheduling.
Only subscribed price-change messages contribute. Full books are excluded because
their timestamps can describe old state. Negative ages have a separate counter
rather than being clamped to zero. Signed means and lifetime maxima add no signal
needed by the live view.

Internal receipt-to-encode delay uses a monotonic instant. Clock adjustments
cannot make it negative. It is weighted per emitted price-change level record.
All snapshot-derived output is excluded. The exchange-age and internal-latency
populations differ, so comparing their counts does not establish loss and
subtracting their percentiles does not isolate a processing stage.

UTC receipt timestamps remain unchanged in SBE and capture files. Runtime-only
monotonic instants do not change the output schema. Encoding completion does not
promise subsequent final flush, file visibility, or fsync durability.

## Keep publication outside the data path

Components own cumulative counters and fixed coarse histogram bins. They replace
their last published snapshot about once per second, including while idle. If
publication cannot acquire its slot immediately, it retries later without losing
cumulative counts. A background task merges published snapshots into an immutable
response. An HTTP request never reads the order-book index or resets counters.

Connection tasks persist across reconnects and idle subscriptions. Their slots
are never reused during a process lifetime, which avoids generation coordination.
Retired counters remain in totals exactly once. Retired gauges and freshness
requirements disappear. Memory depends on configured component capacity, not
reconnect count.

The oldest component publication time detects a frozen worker or writer even
when the HTTP endpoint still answers. An aggregate publisher must not invent a
fresh timestamp for unchanged component state. Readiness also checks publisher
freshness and initial catalog setup.

## Keep diagnostics and independent evidence

Incident logs retain contextual failure information. The bounded logger prevents
slow stderr readers from blocking collection. Operational counters and histograms
are published independently of whether an incident log is dropped.

The shutdown summary events, periodic numeric health event, custom production
histograms, duplicate delivery-latency aliases, and `m5-analyze.py` are removed.
Future measurement scripts must fetch metrics or Prometheus query results and
save their query windows and sample populations. Do not restore log-parsed
operational statistics.

Raw-frame
analysis and independent SBE verification check evidence that aggregate metrics
cannot reconstruct. Queue-depth metrics, snapshot deadline monitoring, per-book
stall detection, and additional stage timings require a concrete unanswered
operational question before they are added.

## Implemented hot-standby metrics

The current implementation exports 18 families, including three histograms.
The [hot-standby contract](../SPEC.md#live-metrics-and-diagnostics) adds one family
and one gauge state to the 16-family baseline with older-full-book filtering,
and `collector_writer_busy_seconds_total` adds writer occupancy:

- `collector_publisher_switches_total{reason}` counts completed market publisher
  changes by remote close, I/O error, silence, or decode error. Count a market
  once when a different connection becomes publisher, not once per socket or
  outcome. Initial selection, same-source recovery and retries without a ready
  target do not count. Carry the specific remote-close, I/O-error, stall or
  decode cause in the ordered reset message to the worker; do not collapse
  remote closes and I/O errors into one disconnected reason. Retain the
  triggering cause across a wait for readiness and reconnect retries.
  Physical session endings cannot tell us actual failover volume or flapping;
  bounded incident logs remain contextual, not an operational counter source.
- `collector_markets{state="standby_ready"}` counts logical markets with both a
  ready publisher and a ready peer. It is a subset of synced markets, not extra
  coverage to add to desired/synced counts. Connected sockets and healthy
  published coverage can hide missing standby baselines. Brief reset dips are
  expected; persistent incomplete recovery is the actionable case. No separate
  readiness gate, standby-book family, ratio or identifier label is added.

Both additions are implemented through the owning worker's
existing cumulative publication path. Replica roles can differ by market on the
same socket, so a socket-wide role label would be misleading.

Keep logical desired market/book counts undoubled. Synced coverage follows one
source per market, with partial books reported from a single candidate while
awaiting publication; never combine different sources' outcomes into an
apparently complete market. Input-event and exchange-age populations include
both physical feeds. Internal receipt-to-encode latency includes actual emitted
price-change records only; switch snapshots, other snapshot-derived records and
standby applications are excluded. Do not subtract their percentiles or use
sample-count differences as a loss ratio.

Authoritative-book mismatches retain their same-source meaning on either feed:
count a differing accepted full book only against an already-synced local source
image, before output. The older-full-book filter runs independently on each
source. Rejected books increment `collector_older_full_books_skipped_total` and
`collector_older_full_book_lag_seconds`, not mismatch counts. Initial and recovery
baselines establish a fresh timestamp maximum. Skip lag uses exchange-age buckets
and is not delivery latency. Do not reinterpret a different switch snapshot as a
mismatch or add cross-replica divergence checking. Both physical task slots remain
required for publication freshness even during reconnection or standby operation;
fresh reconnecting tasks do not themselves fail readiness. A stale required
standby task does. Preserve retirement accounting, fatal task-loss behavior and
final publication on shutdown.

We considered and dropped the draft's per-connection frame-gap maxima and PONG
RTT histogram requirement. Existing session-end reasons, incident context and
controlled lab tests cover the current failure policy. No switch-duration,
queue, stage or snapshot/churn families are added. Operational readers use
metrics/Prometheus with explicit windows and populations, not legacy numeric
health or shutdown logs. The existing demo API stays fixed until UI work is
separately requested; missing/stale data rules do not change.
