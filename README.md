# Polymarket order book collector

A Rust service that records live order books for every regular Polymarket
market. It was built for the
[Voluntas engineering challenge](https://voluntas.io/engineering-challenge).

The collector:

- discovers markets through the Gamma API and lifecycle WebSockets, adding new
  markets and removing resolved ones as they happen;
- spreads about 250,000 markets across many WebSockets and keeps both outcome
  books for each market;
- writes every price-level change, plus a full snapshot of each book every
  60 seconds, as SBE to stdout using the supplied encoding helpers;
- subscribes every market on two independent connections and switches to the
  backup, with replacement snapshots, when a connection fails;
- exposes live Prometheus metrics, with a small dashboard to view them.

## Challenge extensions

| # | Extension | Status |
|---|---|---|
| 1 | Redundant WebSockets with source switching | **Implemented** |
| 2 | Subscription monitoring | **Not implemented, deliberately** |
| 3 | Exchange-generic encode/decode | **Implemented** |
| 4 | Latency experiments | **Not done**, apart from `TCP_NODELAY` |

**1. Redundant WebSockets.** Every market is subscribed on two independent
connections, one in each socket pool. The worker keeps a separate book for
each copy and publishes one. When the publishing connection fails (close,
I/O error, decode error, or 30 seconds of silence), the market switches to
the ready copy and writes full replacement snapshots, as the challenge
requires. Second copies are spread across sockets, so one overloaded socket
fails over to many others. See [failover](docs/ARCHITECTURE.md#failover-extension-1).

**2. Subscription monitoring: why we did not build it.** We replayed every
per-market capture we had against a "resubscribe when the update rate drops"
rule, and it never triggered. The deepest dips were whole-socket blackouts,
which the 30-second silence check already handles. The real subscription
failure we saw was different: some first books never arrive. A rate rule
cannot catch that, because a market with no data has no rate, so we built
bounded missing-book retries instead. A rate rule would also resubscribe
healthy markets during genuinely quiet trading, adding churn and making the
failures we were still investigating harder to debug. We would revisit it if
a capture shows a market going quiet while its socket stays healthy. Details
in [the architecture](docs/ARCHITECTURE.md#why-subscription-monitoring-was-not-built).

**3. Exchange-generic.** Reconnects, heartbeats, subscriptions, first-book
retries, and ordered delivery live in one shared connection loop. Each
exchange only implements the `BookProtocol` trait: connection requests,
subscription messages, heartbeats, and decoding. Workers accept both absolute
and relative quantity updates. Polymarket is the production adapter; a second,
sequence-numbered test protocol exercises the same boundary. See
[the exchange boundary](docs/ARCHITECTURE.md#exchange-generic-boundary).

**4. Latency.** We did not have time to run latency experiments. The one
change in place is `TCP_NODELAY` on every socket, so small writes such as
subscriptions and `PING`s are not held back. Its benefit is not measured.

The collector is built so the main experiment is easy to add. The obvious
test is racing several connections for the same market and promoting the
fastest. The core already supports this pattern: a market is subscribed on
several sockets at once, each socket's data goes into its own book, one
source is selected as publisher, and switching sources is a tested
snapshot handoff. The number of sources per market is currently fixed at two
(`[SourceBook; 2]` in the worker and `MarketSources`); widening that to more
candidates is a contained change, not a redesign. The remaining work is
matching the same update across feeds, since Polymarket timestamps and hashes
are not unique, plus a promotion policy. This and the other latency ideas are
in [future improvements](docs/FUTURE_IMPROVEMENTS.md#latency-extension-4).

## Build

Requires a Rust toolchain with edition 2024 support (Rust 1.85 or later).

```sh
cargo build --release
```

The binary is `target/release/engineering-challenge`.

## Run

Each WebSocket uses a file descriptor, so raise the open-file limit to at
least 65,536 before a full-catalog run. A limit of 1,024 caused connection
failures in an earlier run. Set it in the same shell that starts the
collector:

```sh
ulimit -Sn 65536
ulimit -Sn   # must print 65536 or higher
./target/release/engineering-challenge > books.sbe 2> collector.jsonl
```

If the soft limit cannot be raised, raise the hard limit first. Under systemd,
set `LimitNOFILE=65536`.

For a quick test on a few markets, pass current Gamma market IDs:

```sh
./target/release/engineering-challenge --markets GAMMA_ID,GAMMA_ID --output books.sbe 2> collector.jsonl
```

Stop with Ctrl+C, SIGINT, or SIGTERM. The collector closes its connections,
drains queued records, flushes the final chunk, and exits with status 0. A
second SIGINT forces exit.

Output notes:

- SBE is the only output on stdout. JSON diagnostics go to stderr.
- Redirect stdout to a regular file. The supplied reader uses positioned
  reads and cannot read from a pipe.
- Output is written in 64 MiB chunks, so a quiet run can leave the file
  empty until a chunk fills or shutdown flushes it. There is no fsync
  guarantee.
- Plan disk space for continuous output, and monitor it when running
  unattended.

### Useful flags

| Flag | Meaning |
|---|---|
| `--markets ID,ID` | Collect a fixed list of Gamma market IDs instead of the full catalog. |
| `--output PATH` | Write SBE to a file instead of stdout. |
| `--output-mode file\|stream` | `file` (default) writes SBE to `--output`. `stream` writes nothing there and serves records over WebSocket instead; see [stream mode](docs/ARCHITECTURE.md#stream-mode). |
| `--stream-bind IP:PORT` | Stream mode listener, default `127.0.0.1:9100`. |
| `--stream-client-queue N`, `--stream-max-books N` | Messages a stream client may lag by before it is disconnected (default 4096), and books it may subscribe to (default 1024). |
| `--workers N` | Number of book workers. Default: available CPU parallelism. |
| `--markets-per-connection N` | Markets per WebSocket, default 256. Each market has one copy in each of two connection pools. |
| `--scan-interval-secs S` | Full catalog reconciliation interval, default 900. |
| `--worker-queue N`, `--writer-queue N` | Queue limits for workers and the writer. |
| `--metrics-bind IP:PORT` | Metrics listener, default `127.0.0.1:9090`. |
| `--capture PATH` | Record raw frames for replay. Development only; its queue is unbounded. |

All flags and defaults are in
[Polymarket configuration](src/exchange/polymarket/config.rs) and
[collector configuration](src/core/config.rs).

## Metrics and dashboard

The collector serves Prometheus metrics at `http://127.0.0.1:9090/metrics`,
plus `/healthz` and `/readyz`. To view them, run Prometheus with this config:

```yaml
# prometheus.yml
global:
  scrape_interval: 1s
  scrape_timeout: 900ms
scrape_configs:
  - job_name: collector
    static_configs:
      - targets: ['127.0.0.1:9090']
```

Then start Prometheus and the dashboard:

```sh
prometheus --config.file=prometheus.yml --web.listen-address=127.0.0.1:9091
python3 demo/server.py --prometheus-url http://127.0.0.1:9091
```

Open `http://127.0.0.1:8000`. For a fixed `--markets` run, start the dashboard
with `--scan-interval-secs 0`. Keep the collector endpoint and Prometheus
private; only the dashboard is meant to sit behind a public reverse proxy.

## Documentation

| Document | What it covers |
|---|---|
| [Architecture](docs/ARCHITECTURE.md) | How the collector works: components, data flow, output format, connections, failover, discovery, and the challenge extensions. |
| [Design decisions](docs/DESIGN_DECISIONS.md) | Key trade-offs and why we made them. |
| [Metrics](docs/METRICS.md) | What each metric means, how to read them, and the dashboard API. |
| [Future improvements](docs/FUTURE_IMPROVEMENTS.md) | What we would do next, grouped by latency, performance, and reliability. Includes the Extension 4 latency notes. |
| [Polymarket notes](docs/POLYMARKET_NOTES.md) | Exchange and API behavior we measured, much of it undocumented. |

The supplied [SBE helpers](src/util/), [side type](src/side.rs), and
[schema](sbe_schemas/) are unchanged, and all output goes through them.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --lib --bins
```
