# Collector metrics

The collector exposes live operational statistics in Prometheus format at
`http://127.0.0.1:9090/metrics`. This document explains what each metric
means, how to read the metrics together, and how they are produced and served
to the demo.

There are 18 application metric families, including three histograms.
Histogram buckets, counts, and sums each produce several Prometheus time
series. Counters are cumulative for the process lifetime and reset on restart.
Durations are in seconds. Timestamps are Unix seconds (UTC).

## Quick reference

| Metric | Type | Labels | In one line |
|---|---|---|---|
| `collector_markets` | Gauge | `state` | How many markets we want, how many are publishing, and how many have a usable backup. |
| `collector_books` | Gauge | `state` | The same coverage counted per outcome book. |
| `collector_connections` | Gauge | `kind`, `state` | WebSockets that should be open versus those that are open. |
| `collector_session_ends_total` | Counter | `kind`, `reason` | WebSocket sessions that ended, and why. |
| `collector_publisher_switches_total` | Counter | `reason` | Markets that failed over to their standby connection. |
| `collector_events_total` | Counter | none | Market-data events decoded from the exchange. |
| `collector_decode_errors_total` | Counter | none | Recognized messages that could not be decoded. |
| `collector_book_mismatches_total` | Counter | none | Exchange full books that disagreed with our local book. |
| `collector_older_full_books_skipped_total` | Counter | none | Out-of-date full books we refused to apply. |
| `collector_older_full_book_lag_seconds` | Histogram | none | How far out of date those refused books were. |
| `collector_exchange_to_receive_seconds` | Histogram | none | Age of exchange events when they reach us. |
| `collector_exchange_age_negative_total` | Counter | none | Events that appear to arrive before the exchange sent them. |
| `collector_receive_to_encode_seconds` | Histogram | none | Our own processing latency, from receipt to SBE encoding. |
| `collector_worker_busy_seconds_total` | Counter | `worker` | Time each book worker spends handling input. |
| `collector_writer_busy_seconds_total` | Counter | none | Time the single output writer spends encoding and writing. |
| `collector_writer_blocked_seconds_total` | Counter | none | Time workers spend waiting because the writer is full. |
| `collector_last_successful_scan_timestamp_seconds` | Gauge | none | When the last full market-discovery scan completed. |
| `collector_oldest_required_component_publication_timestamp_seconds` | Gauge | none | Whether every component is still reporting fresh numbers. |

## What each metric means

### Coverage

**`collector_markets`** describes how much of the catalog the collector is
actually serving. It has three overlapping `state` values:

- `desired`: logical markets the collector intends to collect, from discovery
  or the `--markets` list.
- `synced`: desired markets whose selected publisher connection has received
  both outcome baselines in its current session. These markets are producing
  output.
- `standby_ready`: synced markets whose standby copy is also ready, so a
  connection failure can switch immediately. This is a subset of `synced`.

Each market counts once, however many physical copies exist. Do not sum the
states. The coverage ratio is `synced / desired`. A gap between `synced` and
`standby_ready` means part of the served set has no immediate backup.

**`collector_books`** repeats the `desired` and `synced` counts per outcome
book (two per market). It shows partial progress that market counts hide, such
as a market with one initialized outcome during startup or recovery. While no
publisher is selected, book coverage uses the single candidate with the most
initialized outcomes, preferring the primary on a tie. It never combines
partial books from two connections into one apparently complete market.

### Connectivity and failover

**`collector_connections`** counts WebSockets by `kind` and `state`:

- `kind="orderbook"`: sockets carrying book snapshots and price updates.
- `kind="market_discovery"`: sockets receiving market creation announcements.
- `state="expected"`: sockets that should exist, including ones reconnecting.
- `state="connected"`: sockets currently open.

`expected` minus `connected` is the number of sockets currently down.

**`collector_session_ends_total`** counts established sessions that ended, by
`kind` and a fixed `reason`:

| Reason | Meaning |
|---|---|
| `remote_close` | The exchange closed the socket, for example a `1013` slow-consumer close. |
| `io_error` | A network or TLS error. |
| `decode_error` | Malformed data forced a reconnect. |
| `protocol_error` | The protocol detected an inconsistency, such as a sequence gap. |
| `silence` | No frame of any kind arrived for 30 seconds. |
| `emptied` | Every subscribed market was removed, so the socket closed. Not a failure. |
| `shutdown` | Planned shutdown. Not a failure. |

Failed connection attempts are not counted. A rising failure rate indicates
flapping; stderr incidents identify the affected connections.

**`collector_publisher_switches_total`** counts logical markets that moved to
their standby connection after the publisher failed, by `reason`:
`remote_close`, `io_error`, `silence`, `decode_error`, or `sequence_gap`.
One failed socket carrying 256 markets produces 256 switches, so this measures
failover volume separately from physical session ends. Initial selection,
recovery on the same connection, standby-only failures, and retries without an
eligible target do not count. A delayed switch keeps its original cause.

### Traffic and data quality

**`collector_events_total`** counts decoded market-data events from both
physical feeds, once at the decoding boundary. Heartbeat and control frames
are excluded. Its rate separates "the market is quiet" from "the collector is
slow". With hot standby, input is roughly twice the published output.

**`collector_decode_errors_total`** counts recognized frames or messages that
failed to decode. Increases point to protocol drift or malformed data. Sequence
gaps end the session with `reason="protocol_error"` and are not counted here.

**`collector_book_mismatches_total`** counts accepted exchange full books that
differ from the same connection's already-synchronized local book. The
collector adopts the exchange book and, if that connection is publishing,
emits corrections. Initial and recovery baselines are not compared, and
replicas are never compared with each other. A mismatch is observed
divergence, not proof that the exchange or collector lost an update.

**`collector_older_full_books_skipped_total`** counts full books rejected
while synchronized because their exchange timestamp is older than depth
already applied for that outcome on the same connection. Applying them would
roll the book back. Skipped books change no state or output.

**`collector_older_full_book_lag_seconds`** records, once per skip, how much
older the rejected book was than the newest applied depth. It describes stale
refreshes, not delivery latency.

### Latency

The collector measures two different latencies. They have different sample
populations, so never subtract one percentile from the other or compare their
counts.

**`collector_exchange_to_receive_seconds`** is the age of exchange data when
it reaches the collector: local UTC receipt time minus the exchange event
timestamp. It is recorded once per accepted price-change message containing
subscribed updates, on each physical feed. Full books are excluded. The value
includes clock offset, upstream delay, network buffering, and read scheduling.
A rise while internal latency is flat points outside the collector.

**`collector_exchange_age_negative_total`** counts messages whose computed
age is negative, which are excluded from the histogram. A significant share
means clock offset makes exchange-age percentiles unreliable. The share's
denominator is this counter plus the histogram count.

**`collector_receive_to_encode_seconds`** is our main internal latency
measure: monotonic time from frame receipt to successful SBE encoding in the
output buffer. It is recorded once per emitted price-change level record, so a
message with 50 levels produces 50 samples. It excludes standby-only
applications, all snapshot-derived output (initial, periodic, removal,
correction, and switch snapshots), and the later chunk flush to disk. The
project target is p99 below 100 ms.

### Load

**`collector_worker_busy_seconds_total`** is elapsed time each book worker
spends handling input messages, labelled by bounded `worker` index. It excludes
idle waits, waits for the writer, and snapshot timer work. Uneven rates show
concentrated load. It is not CPU utilization.

**`collector_writer_busy_seconds_total`** is elapsed time the single writer
thread spends converting, encoding, writing, and flushing, including snapshots
and chunk rotation. A one-minute rate near 1 means the writer has little
headroom. Disk stalls count as busy time because writes are synchronous.

**`collector_writer_blocked_seconds_total`** sums the time workers wait for
space in the writer's channel. It shows output backpressure. Because several
workers can wait at once, it can increase by more than one second per second.
It also counts the brief time of every send that finds space immediately, so
it is never exactly zero; read its rate, not whether it moved.

### Freshness

**`collector_last_successful_scan_timestamp_seconds`** is when the last full
Gamma discovery scan completed. A stale value means reconciliation is failing
even if existing streams look healthy. It is absent in fixed-market mode.

**`collector_oldest_required_component_publication_timestamp_seconds`** is
the oldest last-publication time among active components: book workers, the
writer, discovery, and every connection slot, including standby-only ones. If
any component stops publishing, this falls behind and readiness fails, so
frozen numbers are never presented as live.

## Reading the metrics together

| Question | Query |
|---|---|
| What fraction of markets is publishing? | `collector_markets{state="synced"} / ignoring(state) collector_markets{state="desired"}` |
| How many sockets are down? | `collector_connections{state="expected"} - ignoring(state) collector_connections{state="connected"}` |
| Internal p99 latency | `histogram_quantile(0.99, sum by (le) (rate(collector_receive_to_encode_seconds_bucket[5m])))` |
| Exchange-age p99 | `histogram_quantile(0.99, sum by (le) (rate(collector_exchange_to_receive_seconds_bucket[5m])))` |
| Input event rate | `rate(collector_events_total[1m])` |
| Writer busy share | `rate(collector_writer_busy_seconds_total[1m])` |
| Seconds since the oldest component published | `time() - collector_oldest_required_component_publication_timestamp_seconds` |

Apply `rate` before aggregating, and use windows covering at least four
scrape intervals. The demo uses five-minute windows for latency and one minute
for traffic. A percentile above the largest finite bucket is shown as greater
than 60 seconds.

When internal latency rises, compare writer busy share, writer blocked time,
and worker busy time to locate the bottleneck. Busy time is elapsed time, not
CPU utilization; use host metrics to establish CPU, memory, or disk causes.
Short bursts can hide inside one-minute averages.

Synced counts do not prove per-book activity, exact agreement with the
exchange, or complete exchange-wide discovery. Component snapshots can briefly
disagree during placement changes. A zero `desired` count makes the coverage
ratio unavailable.

## Socket traffic

Order-book sockets additionally expose cumulative event counts, connection state,
and subscribed-market counts at `/metrics/sockets`. This endpoint is separate
from `/metrics`; scrape it under `collector_sockets` every 15 seconds with a
three-second timeout. `ops/prometheus.yml` contains both jobs.

- `collector_socket_events_total{socket_id,pool}` counts decoded data events,
  including full books and both physical feeds. Heartbeats are excluded.
- `collector_socket_connected{socket_id,pool}` is 1 for an open connection.
- `collector_socket_markets{socket_id,pool}` counts subscribed logical markets.
  Zero indicates an empty task waiting for future subscriptions.
- `collector_socket_process_start_timestamp_seconds` identifies the collector
  process for restart markers and rate-window isolation. It has no labels.

Socket IDs persist across reconnects and subscription changes within a process.
Counters reset on process restart. Retired slots retain counters but stop
exporting live connection and market gauges. Aggregate metrics stay unchanged.
The separate endpoint adds about 6,001 series for 2,000 socket slots, without
per-market labels or per-socket histogram buckets.

The Live page uses one-minute rates with at least four samples. Clicking a
histogram bucket filters a 25-row socket list. Selecting a socket loads only
its history, with a maximum of 1,000 points. Available windows are 15 minutes,
one hour, six hours, and 24 hours. The first minute after process start is a
history gap. Rates include reconnect snapshot bursts.

The panel combines existing aggregate freshness checks with socket-job scrape
health and a 45-second maximum source sample age. Stale values remain labeled
with their age. An independent socket-job failure does not change aggregate
chart status.

## Label rules

The aggregate `/metrics` endpoint carries no market IDs, book IDs, connection
IDs, file paths, publisher or standby roles, or error text. The socket endpoint
adds bounded connection IDs and a fixed primary or replica pool label. The collector keeps per-market state for
correctness and exports only bounded aggregates. Per-market detail is in the
structured incidents on stderr. The only per-worker label is `worker` on busy
time.

Prometheus supplies `up`, and the HTTP probes supply liveness and readiness;
these are not collector metric families.

## How metrics are produced

Each component owns its counters and histogram bins: book workers, each
connection slot, discovery, and the writer. About once per second, including
while idle and after the final drain, each publishes a cumulative snapshot to
its own slot without blocking. A new snapshot replaces the previous one, so a
missed publication loses no counts. If a slot is busy, the owner retries.

A background publisher merges the slots once per second into an immutable
Prometheus response. HTTP requests read that cached response. Scrapes never
traverse books, ask workers for data, reset counters, or hold component locks.
A slow reader cannot block collection.

Connection slots persist across reconnects and are never reused, so counter
totals need no per-session bookkeeping. Retiring a slot removes its live gauges
and freshness requirement and keeps its counters until exit. Storage is bounded
by worker count, discovery listeners, and connection capacity. Both members of
an active pair remain required components while idle, reconnecting, or acting
only as standby.

The writer runs on its own OS thread with a private timer runtime, so it can
publish while waiting for batches. A blocked output write can delay its
publication. The publisher never refreshes a timestamp on a blocked component's
behalf.

At receipt, each connection captures UTC time and a monotonic instant before
decoding. UTC provides SBE timestamps and exchange age. The monotonic instant
travels with the event for internal latency and is never written to SBE or
capture files.

Histogram bucket upper bounds, in seconds (the `+Inf` bucket equals the count):

- Exchange age and older-full-book lag: `0, .005, .01, .02, .05, .1, .2, .5, 1,
  2, 5, 10, 30, 60`.
- Internal latency: `0, .00001, .000025, .00005, .0001, .00025, .0005, .001,
  .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60`.

`src/core/metrics.rs` owns slots, aggregation, buckets, and exposition.
`src/core/metrics_http.rs` owns HTTP. Components receive publication handles;
HTTP types stay outside the data path.

## HTTP endpoints

`--metrics-bind IP:PORT` sets the listener, default `127.0.0.1:9090`. Port zero
selects an available port. The listener binds before output creation or
exchange connections, and a bind failure exits nonzero. The
`metrics_available` startup event on stderr announces the address.

- `GET /metrics`: Prometheus text with HELP and TYPE lines.
- `GET /metrics/sockets`: cached per-socket Prometheus text, for the separate
  15-second scrape job.
- `GET /healthz`: 200 while the HTTP service runs, including startup.
- `GET /readyz`: 200 once initial catalog setup is done and components have
  published, while every required component has published in the last
  15 seconds and shutdown has not begun; otherwise 503. In discovery mode,
  setup requires a successful first scan. In fixed-market mode, it requires
  market lookup and subscription dispatch. Coverage and standby readiness do
  not gate readiness; a stale standby component does.
- Unknown routes return 404 and unsupported methods 405. Request bodies and
  oversized URIs are rejected. Headers are limited by an 8 KiB read buffer. At
  most 64 connections run at once, each with a five-second deadline and no
  keep-alive.

Unexpected loss of the publisher or HTTP task triggers fatal shutdown;
individual failed requests do not. Keep this endpoint and Prometheus private.

## Demo reader

Prometheus scrapes the collector every second with a 900 ms timeout and keeps
metric history. `demo/server.py` reads Prometheus, never the collector
directly, and supports one collector target under the `collector` job. It
serves `/` and `/live` from an asset allowlist, plus:

- `GET /api/demo/stats`: one fixed instant query set, cached for one second,
  returned as JSON with `schema_version: 1`. It includes coverage, connection
  counts, event rate, latency percentiles and sample counts, standby coverage,
  switch totals, negative exchange-age share, and freshness timestamps.
  Unavailable values are `null`, never NaN, infinity, or a substituted zero.
- `GET /api/demo/history?window=15m|1h|12h|24h|all`: aligned range-query series at
  one-, five-, 60-, and 120-second steps for the fixed windows, with per-bucket
  counter increases under `bars` (30-second, two-minute, 20-minute, and
  40-minute buckets). All covers retained collector telemetry within the configured
  seven-day retention period, using about 900 line points and 72 bar buckets.
  Its start is discovered from retained samples, including across collector restarts.
  When the collector has stopped, All ends at its last data. Each window has a
  shared cache. Responses are limited to 4 MiB. If the collector stopped
  before the window, history ends at the last data and sets
  `anchored_to_last_data`.
- `GET /api/demo/sockets`: current fleet rates and state, cached for five seconds.
- `GET /api/demo/socket-history?socket=ID&window=15m|1h|6h|24h`: selected-socket
  history with source freshness gaps and restart markers.
- `GET /api/prometheus/query`: read-only pass-through of frontend-defined
  PromQL to the Prometheus `query` or `query_range` API (optional `time`, or
  `start`, `end`, and `step`). Expressions are limited to 8,192 characters.
- `GET /healthz`: checks the demo HTTP server.

Prometheus queries have a two-second timeout and the network call three
seconds. At most 16 requests run at once. Unknown paths and query-string
variants of the fixed APIs are rejected.

The `status` field is one of:

- `starting`: no component has published yet, or, in discovery mode, no scan
  has succeeded.
- `live`.
- `degraded`: an expected order-book or discovery socket is not open. Coverage
  gaps alone do not degrade the status; they are shown as numbers.
- `stale`: the latest scrape failed, or the oldest required publication is
  more than 15 seconds old.
- `stopped`: Prometheus answers, but no scrape has succeeded for ten seconds.
- `metrics_unavailable`: Prometheus did not answer.

`scan_overdue` flags a successful scan older than twice the configured scan
interval without changing the status. Run the reader with
`--scan-interval-secs 0` for fixed-market mode. When the collector has
stopped, the reader returns the last published values, marked `retained`, with
`values_at` and `age_seconds`. The browser adds `api_unavailable` and `offline`
states, keeps its last good values dimmed with their age, backs off up to about
30 seconds, and pauses while the tab is hidden. Freshness always comes from
scrape and publication timestamps, so cached data never shows a live indicator.

## Shutdown

SIGINT and SIGTERM disable readiness immediately, stop discovery and
connections, drain workers, and flush the writer. The endpoint stays available
during the drain, and final component publication precedes HTTP shutdown
within the ten-second deadline. Exit does not wait for a final scrape, so
activity after the last scrape may be missing from history. A second signal
forces exit. Restarts reset counters and rebuild books from discovery.

SBE is the only stdout output. Incidents and lifecycle events are structured
JSON on stderr through a bounded, nonblocking logger. Operational statistics
come only from `/metrics`.

## Deliberately not measured

Queue depths, snapshot kinds, churn, per-book freshness, switch durations,
per-connection maxima, and PONG round-trip times are outside this contract.
Add a family only for an operational question the existing set and stderr
incidents cannot answer. SBE correctness checks and lab benchmarks keep their
own evidence and are not operational metrics.
