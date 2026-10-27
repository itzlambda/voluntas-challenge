# Agent guide

Context for coding agents working on this repository. Start with the
[README](README.md), then [architecture](docs/ARCHITECTURE.md).

## Documents

| Document | Use it for |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How the collector works. |
| [docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md) | Why it works that way, and alternatives already rejected. |
| [docs/POLYMARKET_NOTES.md](docs/POLYMARKET_NOTES.md) | Measured exchange and API behavior. Check here before assuming how Polymarket behaves. |
| [docs/METRICS.md](docs/METRICS.md) | Metric definitions, HTTP endpoints, and the dashboard API. |
| [docs/FUTURE_IMPROVEMENTS.md](docs/FUTURE_IMPROVEMENTS.md) | Known gaps and planned work. |

Code comments cite "SPEC section N". The spec was condensed into
`docs/ARCHITECTURE.md`; the sections map as follows:

| SPEC section | Now in ARCHITECTURE.md |
|---|---|
| 3 Architecture, 4 Routing | Overview, Components |
| 5 Output contract | Output |
| 6 Polymarket adapter | Data flow (and POLYMARKET_NOTES.md) |
| 7 Discovery and lifecycle | Discovery |
| 8 Connections | Connections |
| 9 Worker | Book maintenance |
| 10 Backpressure and shutdown | Backpressure, failure, and shutdown |
| 11 Extensions | Failover, Challenge extensions |
| 14 Telemetry | METRICS.md |

## Source map

| Path | Role |
|---|---|
| `src/main.rs`, `src/core/app.rs` | Startup, component supervision, bounded shutdown. |
| `src/core/discovery.rs`, `registry.rs`, `recovery.rs` | Desired market set, scan reconciliation, missing-book eligibility checks. |
| `src/core/subscriptions.rs` | Places each market on one primary and one replica socket. |
| `src/core/book_connection.rs`, `connection.rs` | Shared socket loop: heartbeats, reconnects, first-book retries, ordered delivery. |
| `src/core/routing.rs` | `condition_id mod N` worker routing. |
| `src/core/worker.rs`, `order_book.rs` | Per-source books, publisher selection, snapshot timers. |
| `src/core/writer.rs` | Single OS thread; encodes with the supplied SBE helpers. |
| `src/core/metrics.rs`, `metrics_http.rs`, `telemetry.rs` | Prometheus metrics, HTTP endpoints, stderr JSON incidents. |
| `src/core/fixture.rs`, `capture.rs`, `pipeline.rs` | Capture format and offline replay used by tests. |
| `src/exchange/protocol.rs` | `BookProtocol` trait between the shared loop and an exchange. |
| `src/exchange/polymarket/` | Polymarket adapter: CLI config, Gamma client, decoding, lifecycle sockets, exact number parsing. |
| `src/model.rs` | Core types shared by every component. |
| `src/util/`, `src/side.rs`, `sbe_schemas/` | Supplied by the challenge. **Do not modify.** |
| `demo/` | Dashboard: `server.py` reads Prometheus; static pages render charts. |

## Invariants

Breaking any of these corrupts output or ordering:

- **The supplied helpers and schema stay unchanged.** All output goes through
  `AlignedSbeWriter` with 64 MiB chunks, because the supplied reader assumes
  that size.
- **stdout carries SBE only.** Diagnostics go to stderr.
- **No floats for prices or quantities.** Parse decimal strings exactly into
  fixed point (1,000,000 = 1).
- **One market, one worker, for the process lifetime.** Only that worker
  touches its books. Routing is a pure function of the condition ID.
- **Each connection task is the only sender of its data and its
  `MarkUnsynced` reset**, and it reads, decodes, and sends sequentially. This
  is what guarantees a worker sees pre-reset data before the reset. Do not
  decode frames in parallel or let other components send resets.
- **Controls before subscriptions.** `AddMarket` reaches the worker before
  the socket subscribes; `RemoveMarket` before it unsubscribes.
- **The two source feeds are never compared, merged, or deduplicated.**
  Timestamps and hashes are not unique.
- **Only the publisher writes output.** Source switches write full
  replacement snapshots, not diffs.
- **A failed or partial scan never removes markets.**

## Working on the code

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --lib --bins
```

- Worker and connection tests replay fixtures on Tokio's paused clock, so
  they are deterministic. Prefer adding a fixture scenario over a live test.
- Live runs need a raised open-file limit; see the README.
- Measure before optimizing. Several plausible designs were disproved by
  live data (see the table at the end of the design decisions). Record new
  measurements with their date, method, and limits.
- Operational numbers come from `/metrics` or Prometheus queries, not from
  parsing logs.
