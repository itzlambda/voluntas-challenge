# Base challenge submission

Scope: the base Polymarket collector through M5. Extension 1 is implemented
through hot-standby replicas. Extension 2 remains deferred.
Extension 3 is implemented through a shared book-connection loop and
exchange protocol contract. Polymarket is the production adapter; a second
binary test protocol verifies the boundary. See [Extension 3](SPEC.md#3-exchange-generic).
[Extension 4 notes](LATENCY_IMPROVEMENTS.md) document the enabled
TCP_NODELAY setting and its unmeasured latency rationale.
The final source includes complete-scan lifecycle reconciliation and batched
worker-to-writer output. The historical catalog and capacity measurements
predate those changes and retain their original source fingerprints.

## Requirements and evidence

| Base requirement | Implementation | Verification |
|---|---|---|
| Discover live regular markets continuously | Gamma keyset scans, three lifecycle hint connections, complete-scan reconciliation with recent lifecycle events | Registry/admission tests and real-binary mock Gamma/WebSocket lifecycle test |
| Subscribe across multiple WebSockets | Subscription manager, fixed per-connection capacity | Placement tests, two-socket fake exchange scenario, full-catalog measurement |
| Maintain both outcome books | Exact fixed-point numbers, absolute quantities, zero removal, authoritative refreshes | Independent per-record SBE model, saved raw-window Decimal comparison at N=1,2,4 |
| Write level changes and full snapshots every minute | Worker-owned books and snapshot timers, single SBE writer thread | Paused-clock periodic snapshot and unsynced-skip tests, exact fake-server record oracle |
| Use supplied SBE helpers | `AlignedSbeWriter`, generated domain types, `AlignedSbeReader` | Complete chunk readback in pipeline and fake-exchange tests |
| Close sockets and output on SIGINT | Ordered producer cancellation, worker drain, writer flush, exit status | Real binary gets actual SIGINT, closes three active sockets, exits 0, produces readable output |
| Recover from connection failures | Per-connection reset ordering, fresh-book requirement, missing-first-book retry, silence rule | Forced disconnect, pre-refresh update rejection, malformed frame recovery, paused silence checks |

The fake-exchange tests compare every emitted record with an independent
server-owned oracle. The binary test uses the existing endpoint flags, actual stdout,
and a real signal. It needs no production-only test hook.

The raw incident test observes the worker's complete pre-refresh state by
inserting virtual-time snapshot checkpoints between original frames. It keeps
raw JSON frame strings and array order intact. Clock checkpoints do not apply
the incoming book. All six original changed refreshes agree with the
independent Decimal model before the incoming snapshot is applied at each
worker count. These cases therefore provide no evidence of a Rust
reconstruction error. They do not establish why the upstream books changed.

## Reproduce the checks

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo test --test e2e_mock_exchange -- --nocapture
cargo test --test book_incidents saved_raw_windows_match_decimal_before_every_refresh -- --ignored --exact --nocapture
```

The last command requires the saved local raw recordings. The normal suite
includes a small portable regression fixture covering the corresponding
quantity increases, full-book corrections, ignored trades, equal timestamps,
zero removals, and unchanged refreshes. Capacity runs and their exact workload
are in [MEASUREMENTS.md](MEASUREMENTS.md).

## Operational limits

- Before a full-catalog run, set the soft open-file limit to at least 65,536
  in the collector's launch environment. Use `ulimit -Sn 65536` in the same
  shell, or `LimitNOFILE=65536` in a systemd service. See the
  [run instructions](README.md#run-the-collector).
- Protocol-v1 regular markets are the configured scope. Protocol-v2 and
  Combos/RFQ products are outside this implementation.
- A changed full-book refresh demonstrates divergence, not its cause.
  No exchange-provided sequence or verified hash completeness oracle has
  been established. Trade notifications are not subtracted again.
- After a disconnection, events before a fresh full book cannot reconstruct
  an unknown gap on that connection. Each market's mirrored pair switches
  publishers only on connection failure.
- An already-synced asset can become silent while PONGs or another asset
  keep its connection active. Per-subscription stall detection, latency-based
  source selection, and automatic failback are not implemented.
- The internal 100 ms latency target applies to SBE encoding completion in the output
  buffer. It is a project stress goal, not an explicit numerical challenge requirement. The required 64 MiB chunks determine file visibility. There is no
  fsync durability promise.
- The local fake server covers one valid Gamma page. Its end-to-end test
  does not establish real TLS behavior, pagination failure recovery, or the
  second-SIGINT forced-exit path. The live measurement complements it.
- A short catalog run cannot establish day-long discovery coverage or prove
  that every exchange change was delivered. Capture and output replay check
  observable input and internal output consistency separately.

## Release status

The base collector and offline validation are implemented. The normal test
suite, all-target Clippy, formatting, and handoff-analysis controls pass and are
reproducible with the commands above.

Historical full-catalog correctness readback passes. The internal three-times-
peak latency target has not passed, and no fresh catalog capacity run validates
the later discovery and batching changes. Do not treat the old figures as
measurements of the final source. [MEASUREMENTS.md](MEASUREMENTS.md) records the failed target and
measurement limits. [Stage findings](research/experiments/stage-latency-findings.md)
separate queueing, processing, and writer stalls, and the
[exchange-to-receive report](research/docs/polymarket-exchange-to-receive.md)
records a bounded live run with clean shutdown and SBE verification.

The handoff investigation is complete as a bounded observation, with no claim
of exchange-wide completeness. Its [report](research/experiments/book-handoff-results-2026-10-26.md)
records unsuccessful joins and observation limits. The [reviewer demo](demo/)
implements Overview, Architecture, and Live pages.
