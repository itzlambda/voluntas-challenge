# Fake Polymarket exchange

A standalone Rust 2024 crate for synthetic Polymarket traffic. It runs on
loopback over plain HTTP and WebSocket. The collector runs unchanged.

- `fake-exchange` owns the true books, serves Gamma discovery, generates
  market traffic, and optionally records history.
- `fx-check` compares collector SBE output with history and prints one JSON
  summary. Errors and interior gaps fail the check. `--allow-gaps` downgrades
  gaps. Empty or truncated input and incomplete history fail the check.
- `fx-load` subscribes clients discovered through Gamma and discards frames.
  It reports throughput, latency, errors, and early closes. It extracts
  timestamps from generated updates without validating their bodies.

Only the checker imports the collector crate, for `util` and `Side`.
The server and generator implement their own book and protocol logic.

## Quick start

Run from the repository root on Linux with Rust 2024 support, Cargo, Bash,
and Python 3 installed. The smoke script builds both crates with at most two
build jobs, starts the server and collector, collects for 60 seconds, stops
both with SIGINT, and checks the SBE output against server history:

```sh
fake-exchange/scripts/smoke.sh
```

The default workload has 200 initial markets, two outcomes per market,
1,000 level changes/s, two threads per process, new markets, and resolutions.
No credentials, live exchange connection, or TLS certificates are needed.
Build time is additional to the collection interval.

The script prints its artifact directory under the ignored
`fake-exchange/runs/`. Inspect:

- `check.json`: `errors.count` and `gaps.count` should be zero.
- `coverage.eligible_without_snapshot`: should be zero after discovery has
  had time to finish. Coverage is reported without failing the command.
- `tail`: trailing changes near shutdown, reported separately from gaps.
- `collector.jsonl`, `server.jsonl`, and `decode-errors.jsonl`: diagnostics.
- `books.sbe` and `history/`: inputs for rerunning `fx-check`.

Errors and interior gaps make the script exit nonzero. Coverage and tails
need separate inspection. Empty output and incomplete history also fail.
The collector buffers SBE chunks, so inspect the file after clean shutdown.

For a shorter check or different ports:

```sh
FX_SMOKE_SECONDS=15 FX_WS_PORT=19011 FX_GAMMA_PORT=19012 \
  fake-exchange/scripts/smoke.sh
```

`FX_RUN_DIR` selects a fresh artifact directory. Each script refuses to
reuse an existing directory. Smoke defaults to ports 19001 and 19002.
The scripts resolve Cargo target directories through metadata, including
`CARGO_TARGET_DIR`. Both wait up to three minutes if the one-minute host
load average is above 12, then exit without starting a workload.

## Run a custom workload

Build from the repository root:

```sh
cargo build --release -j 2 --manifest-path fake-exchange/Cargo.toml
cargo build --release -j 2 --bin engineering-challenge
```

The examples below use the default Cargo target directories. If
`CARGO_TARGET_DIR` is set, use its release directory instead. In terminal 1,
choose a new run directory and start the server:

```sh
mkdir -p fake-exchange/runs/manual
./fake-exchange/target/release/fake-exchange --cores 2 --markets 200 \
  --rate 1000 --new-markets-per-second 0.2 --resolutions-per-second 0.1 \
  --history fake-exchange/runs/manual/history \
  2> fake-exchange/runs/manual/server.jsonl
```

In terminal 2, point both collector endpoints at the server:

```sh
./target/release/engineering-challenge --workers 2 \
  --ws-url ws://127.0.0.1:9001/ws/market \
  --gamma-url http://127.0.0.1:9002 \
  --decode-error-log fake-exchange/runs/manual/decode-errors.jsonl \
  > fake-exchange/runs/manual/books.sbe 2> fake-exchange/runs/manual/collector.jsonl
```

After collecting, stop the collector with Ctrl+C and wait for it to exit.
Then stop the server with Ctrl+C and wait for the history completion marker.
Run the checker:

```sh
./fake-exchange/target/release/fx-check \
  fake-exchange/runs/manual/books.sbe fake-exchange/runs/manual/history
```

Use a fresh directory for the next run. Increase `--markets` and `--rate`
gradually. `--rate` counts level changes, which can be batched into one
frame. `--cores` controls server threads and shards. Collector `--workers`
accepts any positive book-task count and defaults to detected available CPU
parallelism. Start with one or two cores per process on a shared machine.
Retain history for correctness checks; omit `--history` when only measuring
delivery capacity. Generation continues for unsubscribed assets.

## Optional server capacity check

`load.sh` runs the server against `fx-load`, without the collector or history.
It measures the generator's delivery capacity and does not check SBE output.
Build the fake exchange release binaries first, as above:

```sh
FX_RATE=10000 FX_LOAD_SECONDS=10 fake-exchange/scripts/load.sh
```

It uses 5,000 markets, 50 connections with 200 assets each, one pinned core
per process, and a default offered rate of 100,000 level changes/s.
`FX_LOAD_SECONDS` accepts 1 through 30; the default is 20.
`FX_SERVER_CORE` and `FX_CLIENT_CORE` default to 0 and 1. Choose allowed CPU
IDs on this machine. `FX_OUTBOUND_CAPACITY` defaults to 16,384 queued frames.
`FX_RUN_DIR`, `FX_SERVER_BIN`, `FX_CLIENT_BIN`, and `FX_BIN_DIR` override
artifacts and executables. Load uses ports 19101 and 19102.
It also needs `taskset` and GNU `/usr/bin/time`.

Results include `load.json`, process CPU reports, and logs. A busy machine
can increase latency and generator lag. These measurements are diagnostics,
not a fixed throughput or latency requirement for using the server.
Use the same client executable when comparing server versions.

## Server flags

`fake-exchange --help` lists all flags. Defaults:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--ws-port` | 9001 | Loopback market WebSocket, any path |
| `--gamma-port` | 9002 | Loopback Gamma HTTP `/markets/keyset` |
| `--cores` | 2 | Tokio workers and book shards |
| `--seed` | 1 | Per-shard RNG seed is seed plus shard index |
| `--markets` | 200 | Initial market count, two outcomes each |
| `--levels-min`, `--levels-max` | 5, 15 | Inclusive initial levels per side, at most 49 |
| `--rate` | 1000 | Total true level changes per second, including mirrors |
| `--zipf` | 1.2 | Market weight proportional to ID raised to minus exponent |
| `--entries-min`, `--entries-max` | 1, 4 | Base entries per frame, before optional mirrors |
| `--mirror-probability` | 0.25 | Probability of an extra complementary outcome entry |
| `--repeat-book-rate` | 2 | Total unsolicited books per second, weighted by activity |
| `--noise-rate` | 1 | Total ignored events per second, weighted by activity |
| `--new-markets-per-second` | 0 | Catalog additions and immediate lifecycle announcements |
| `--resolutions-per-second` | 0 | Close oldest unresolved markets in Gamma and remove their books |
| `--outbound-capacity` | 4096 | Queued frames per connection before a 1013 close |
| `--history DIR` | off | Binary change log, initial asset table, and lifecycle log |

All rates can be zero. Every twentieth market starts with empty books.
Each bid stays in ticks 1 through 49, and each ask in ticks 51 through 99,
so neither initial books nor generated changes cross. Deletions use zero
absolute size; snapshots omit deleted levels. Sizes use at most six decimal
places. Book hashes use the binding SHA1 recipe, including the frame's
millisecond timestamp and `last_trade_price` formatted as `0.500`.

Markets are assigned by FNV-1a of their condition ID. Each shard owns its
books, RNG, and asset subscriber maps. Subscription commands use channels.
A frame is serialized once, shared as `Arc<[u8]>`, and sent to each
connection at most once even when it holds both outcomes. Each market keeps
a deduplicated subscriber list, maintained when subscriptions change. Both outcome
entries remain in market frames; clients filter entries for assets they do
not subscribe to. Initial dumps aggregate books from the owning shards
before queued updates are written.

The RNG is repeatable per shard for the same command and timer ordering.
Wall-clock timestamps, subscriber timing, and task interleaving are not
bit-for-bit deterministic. No-PING sockets are dropped after 125 seconds.
Resolution closes occur when all subscribed assets have resolved. Merely
unsubscribing all assets does not close a socket.

Gamma returns descending ID pages with numeric opaque cursors in
`next_cursor`, consumed through `after_cursor`. Limits are capped at 100;
`closed=false` and `closed=true` filter rows. Repeated `id=` parameters
select IDs; `closed=any` requires at least one ID. Other query filters are
ignored. No CDN staleness, 429 responses, fault injection, scenario files,
control API, TLS, or lifecycle delays are implemented.

Stats are JSON lines on stderr every ten seconds. Rates count successfully
written frames, price-change entries, and text payload bytes. Generated
changes include assets without subscribers. Queue depth is the interval
maximum. Shard lag is the remaining scheduled change budget divided by
that shard's target rate. Overload preserves that budget instead of silently
dropping scheduled work. Generation uses elapsed wall-time credits. A shard works until caught up
or until about 500 microseconds have elapsed, then yields. A backlogged
shard resumes without waiting for a rounded timer tick; late wakes and time
spent generating both contribute to its remaining debt. One frame can
overshoot the time budget. Stats also
include current connections, subscriptions, and interval 1013 close counts.

## History format, version 1

A history directory contains:

- `manifest.json`: version, shard count, seed, and start time in Unix ms.
- `assets-N.jsonl`: initial books for shard N, including assets added later.
  Each row has numeric `asset`, `market`, `created_ms`, and `levels` with
  `bids` and `asks` maps. Prices and sizes are signed integers scaled by
  1,000,000. JSON object keys encode integer prices as strings.
- `shard-N.bin`: eight ASCII bytes `FXHIST01`, followed by 40-byte records.
  Records preserve true mutation order within that shard, including
  successive entries in the same frame. Each record is little-endian:
  time in Unix ms as i64, asset ID as u64, side as u8 with BUY=0 and SELL=1,
  seven zero reserved bytes, price as i64 fixed six, new size as i64 fixed
  six. Size zero deletes a level. Initial levels are in the asset table,
  not represented as changes. No-op changes are never generated.
- `lifecycle.jsonl`: `market`, `event` as `created` or `resolved`, and `time`
  in Unix ms. Initial markets also have creation rows.
- `complete.json`: successful shutdown time. Written only after all shards
  and the lifecycle writer flush. The checker requires this marker.

History uses buffered synchronous file writes in each owning shard. Disable
it when measuring pure server throughput. Disk stalls can increase shard
lag.

## Checker behavior

The checker reads history once and indexes one 128-bit additive fingerprint
per true state. A fingerprint sums two mixed 64-bit hashes per nonzero
level and updates in constant time. It also holds one mutable observed book
per collected asset. Memory is proportional to true changes plus the
current books, rather than full books times changes. Fingerprints have a
negligible but nonzero collision probability.

The first SBE record for an asset must be a snapshot. Its latest matching
true state at or before the exchange timestamp is the baseline. Each level
update must produce the next true state and carry that change's exact
timestamp. On mismatch, remaining updates for that asset with identical
exchange and received timestamps are treated as one catch-up diff. A later
matching state is an interior gap; no matching state is an error. Later
snapshots must match the current or a later true state. The collector also
emits an empty retirement snapshot when removing a published market. The
checker accepts that record only for an asset marked resolved in server
history, after a baseline, with exchange time no later than resolution and
non-regressing data timestamps. An unchanged repeated book can advance those
timestamps without emitting SBE, so exact equality with the last SBE record
is not required.
It counts these as `retirements`, preserves the last true state for tail
reporting, and rejects further records for the retired asset. Empty snapshots
for unresolved markets must still match true book contents. The JSON summary
keeps counts and at most twenty samples per finding category.

Trailing changes are reported separately from interior gaps. `tail` gives
per-observed-asset counts through the last received timestamp anywhere in
the SBE file, plus changes after that cutoff. In-flight updates at collector
shutdown can produce a nonzero tail in a clean run. Tails do not fail the
check. Coverage reports missing snapshots for assets open for at least
`--coverage-grace-secs` before the last received record, and creation-to-first
snapshot latency. Coverage is reported, not a failure criterion. Latency is
received minus exchange time, with 0.1 ms histogram buckets and exact
minimum and maximum values.

`fx-load --help` lists endpoint, connection, asset-count, duration, and core
flags. It completes Gamma discovery and WebSocket handshakes before timing,
then includes subscriptions and initial books in the measured interval.
It reports per-event timestamp latency, including initial books, and fails
on socket errors, early closes, or a run with no book frames. For recognized
generated `price_change` frames, it extracts the timestamp and discards the
body without validating its JSON. Other frame shapes use JSON parsing.
The receive timestamp is taken after the complete WebSocket frame arrives,
before metadata extraction. The load tool measures delivery; wire tests
and the independent checker validate correctness. Its latency
clock has 1 ms resolution. Use server stats to distinguish generator lag,
queue saturation, and client limits; one run on a busy host cannot establish
a reproducible hardware ceiling.

## Validate

```sh
cargo fmt --check --manifest-path fake-exchange/Cargo.toml
cargo clippy --manifest-path fake-exchange/Cargo.toml -j 2 --all-targets -- -D warnings
cargo test --manifest-path fake-exchange/Cargo.toml -j 2
```

The hash vector in `tests/book.rs` was independently computed with Python
`hashlib.sha1(json.dumps(book, separators=(',', ':'), ensure_ascii=True).encode('ascii'))`.
The ordered preimage has market 1, asset 2, timestamp `1700000000123`, bids
`0.1` at `2.5` and `0.49` at `3`, asks `0.9` at `4.125` and `0.51` at `7`,
and the metadata above. Its digest is
`0df56fbea236471f0f4ce762731044b425d22ada`.

## Validation evidence

The test suite covers the independent SHA1 vector, serialized frame parity,
cached-book mutations, protocol behavior, socket fairness, and checker errors
and gaps. The final implementation smoke run collected 59,817 records with
zero errors, zero gaps, full snapshot coverage, and zero tail through the
last receive time. This is one bounded correctness run, not proof of every
exchange behavior.

[performance.json](performance.json) contains the profiled before/after
measurements and final smoke summary. [validation.json](validation.json)
retains the earlier baseline, including failed overload attempts. Those
historical results used the collector revision recorded with each run.
Raw logs, SBE files, and profiles stay in the ignored `runs/` directory.

`tests/support/mock_exchange.rs` remains the deterministic test helper for
paused-clock recovery scenarios. `examples/sbe_summary.rs` checks SBE internal
consistency. This server and `fx-check` add real-time generated traffic and
independent exchange history. Each tests a different property.
