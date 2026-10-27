# Synthetic Polymarket testing server

Status: implemented in `fake-exchange/`. The [usage guide](../fake-exchange/README.md)
is the source for commands, flags, protocol coverage, and checker limits.

## Scope and decisions

The server generates markets and absolute level updates, serves initial and
repeated full books, and announces market creation and resolution. Its small
Gamma keyset endpoint lets the production collector discover synthetic
markets through its normal path. All traffic stays on loopback.

- A separate crate owns the books and wire encoding. The server does not
  reuse collector logic. Only the checker imports the supplied SBE reader
  and `Side` type from the collector crate.
- Traffic is generated, rather than replayed from captured live frames.
- Plain `ws://` and HTTP avoid certificate setup and TLS measurement cost.
- Book shards own their state and generate traffic in parallel. The server,
  collector, and load client run on this machine with modest core counts.
- Optional history records the server's true mutations. `fx-check` compares
  SBE snapshots and updates against that independent history.
- A null client measures delivery capacity separately from SBE correctness.
  Shared-machine throughput and latency are diagnostic measurements, not
  acceptance requirements. Maximum throughput tuning is outside this scope.

The server implements a subset of the exchange. Fault injection, scenario
files, a control API, lifecycle notification delays, proxy mode, replay,
TLS, trade execution, and Kalshi are outside the implemented scope.
The usage guide lists the exact behavior and limitations.

## Hash interpretation

The synthetic server uses lowercase SHA1 over compact ASCII JSON with keys
in this order: `market`, `asset_id`, `timestamp` as a string, `hash` as an
empty string, `bids` ascending, `asks` descending, `min_order_size`,
`tick_size`, `neg_risk` as a boolean, and `last_trade_price` with three decimals.
Level prices and sizes are decimal strings without trailing zeros.

A `book` carries the hash of itself. A synthetic `price_change` entry carries
the hash after that entry, using the frame timestamp. This is the synthetic
server's chosen delta rule; it is not established as a universal rule for live
Polymarket traffic. The independent Python vector and frame-parity
tests guard the synthetic server's chosen encoding.

## Existing tools retained

`tests/support/mock_exchange.rs` drives deterministic recovery tests with
Tokio's paused clock. The standalone server drives real-time generated load.
Neither replaces the other. `examples/sbe_summary.rs` checks SBE internal
consistency, while `fx-check` compares output with exchange history.

The two run scripts have distinct purposes: `smoke.sh` runs the collector
and checker; `load.sh` runs the null client without collector output.
Historical measurement scripts and live capture experiments remain evidence
for their own investigations. They are not duplicate server launchers.
