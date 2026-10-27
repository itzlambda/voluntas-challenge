# Polymarket exchange-to-receive age

One bounded live observation, 2026-10-26. This measures local application receipt
minus the exchange-provided event timestamp. It includes upstream processing,
network buffering, read scheduling, and clock differences. It does not establish
network transit time or an exchange send timestamp.

The existing [`FrameTotals`](../../src/core/connection.rs) already recorded
message-weighted delivery-age mean/max. This change preserves that sampling
point and adds a fixed histogram, sample counts, and negative-age accounting.
[`run_book_connection`](../../src/core/book_connection.rs) captures receipt
immediately after the WebSocket read, before decoding. The
[`decoder`](../../src/exchange/polymarket/decode.rs) produces one grouped update
for each accepted `price_change` object with subscribed levels. Full `book`
snapshots are excluded because their timestamps may describe old state.
See [metric semantics](../telemetry.md#implemented-exchange-to-receive-age).
Receive-to-encode and scheduled-to-encode measure different parts of the path.

## Reproduce a bounded observation

The public Gamma query below returns three active markets ordered by reported
24-hour volume. It is one small request, not a catalog scan. Confirm the returned
markets still have `active=true`, `closed=false`, and `enableOrderBook=true`,
then use their IDs with `--markets`. The observed run used 2589812, 608540, and
608545, corresponding to the October Fed no-change market and the Mbappe and
Yamal Ballon d'Or markets. These were the three returned IDs at run time.

```sh
curl --fail --max-time 20 'https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=3&order=volume24hr&ascending=false'
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo build --release --bin engineering-challenge --example sbe_summary
uptime
nproc

target_dir=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
run_dir=$(mktemp -d /tmp/exchange-age.XXXXXX)
cp "$target_dir/release/engineering-challenge" "$run_dir/collector"
cp "$target_dir/release/examples/sbe_summary" "$run_dir/reader"
(
  cd "$run_dir"
  prlimit --fsize=134217728:134217728 timeout --preserve-status --signal=INT --kill-after=15s 90s \
    ./collector --markets 2589812,608540,608545 --workers 1 \
    --markets-per-connection 3 --worker-queue 1024 --writer-queue 1024 \
    --max-frame-bytes 1048576 --output books.sbe \
    --decode-error-log decode-errors.jsonl > /dev/null 2> telemetry.jsonl
  # For measurement runs, fetch /metrics before the collector exits.
  ./reader books.sbe > verification.txt
  python3 - <<'PY'
import collections, json
rows = [json.loads(line) for line in open("telemetry.jsonl")]
print(json.dumps(collections.Counter(row["event"] for row in rows), indent=2))
print("Fetch live exchange-age metrics from /metrics or the Prometheus HTTP API during the run.")
PY
  cat verification.txt
)
```

The run has one connection, three fixed markets and six outcome books. Capture
is disabled by omitting `--capture`; no lifecycle sockets or scans start in
fixed-market mode. Each child output file is capped at 128 MiB. The collector
runs for 90 seconds including setup, then receives SIGINT, with a 15-second
shutdown deadline enforced externally. The actual run used Python `Popen`,
`RLIMIT_FSIZE`, and timed waits with the same limits and signal sequence.
Release binaries were copied before starting to isolate the sample from other
builds. The SBE writer pads its final output to a 64 MiB chunk.

## Observation

Results are saved in [the sanitized aggregate](../experiments/polymarket-exchange-to-receive-results.json).

The observation ran from 15:56:51.853 to 15:58:22.640 UTC, including shutdown and
reader verification. SIGINT was sent after 90 seconds. Collector and reader
both exited 0. The reader found an 89.2-second receipt span.

- Update-message samples: 1,228. Negative ages: 0. Histogram overflow: 0.
- Signed mean: 29.946804 ms. Observed maximum: 508.127 ms.
- p50 lies in [10, 15) ms, p95 in [115, 120) ms, and p99 in [240, 245) ms.
  These are nearest-rank histogram bounds at 5 ms resolution, not exact quantiles.
- Input: 1,494 text frames and 1,414 decoded book/update events.
- Output: six books, all with updates, 12 snapshots, and 2,468 level records.
  Encoded payload was 340,685 bytes; the padded file was 67,108,864 bytes.
- Reader checks: zero snapshot mismatches, zero snapshot receipt regressions,
  and zero books with updates before their first baseline.
- Decode errors: 0. Decode-error log bytes: 0. Telemetry drops: 0.
  No catalog scans or lifecycle hints ran. Final book coverage was 100%.

One live error occurred: `Connection reset by peer (os error 104)` ended the
first session after 71.688 seconds. All six books became unsynced, then the
collector reconnected and recovered. The summary records one unsync mark,
12 catch-up levels, and zero mismatched books. The age distribution combines
both sessions; no per-session percentile claim is made. A successful final
reader check does not establish lossless delivery during that disconnect.

The host reported six available CPUs and a pre-run load average of
10.39, 7.64, and 5.05 over one, five, and fifteen minutes. This was one small
observational sample on a busy shared host, with unknown clock offset. It is
not a feed-wide estimate, a network transit measurement, or a comparison of
collector performance. Neither clock synchronization nor time spent in each
upstream stage was verified. No profiler or per-message logging ran.

## Local verification

`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and
`cargo test --all-targets` passed. There were 63 passing tests and three existing
ignored load tests. The release collector and `sbe_summary` example built.

Focused tests cover one sample for a multi-level update, separate samples for
multiple objects in a frame, exclusion of snapshots and rejected/empty/unknown
messages, preserved receipt timestamps, signed means and negative maxima,
merging, empty distributions, nearest-rank bucket boundaries, and overflow
without replacing p99 with max. See
[`tests/exchange_age.rs`](../../tests/exchange_age.rs). Per-message accounting is
in [`connection.rs`](../../src/core/connection.rs) and the histogram in
[`metrics.rs`](../../src/core/metrics.rs).
