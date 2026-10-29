# Future improvements

Things we would do next with more time. None of these are built or measured
yet. [Architecture](ARCHITECTURE.md) describes what the collector does today.

## Top priorities

1. **Support Polymarket protocol v2.** New markets may move to v2 from
   2026-11-02, so v1-only coverage will shrink.
2. **Re-run the load test on the final code.** The latency target (p99 under
   100 ms at three times peak traffic) has not passed, and the last full run
   predates later changes.
3. **Stop disk writes from blocking encoding.** This is the one latency
   bottleneck we measured directly.
4. **Run closer to the exchange.** Probably the biggest latency win, with no
   code changes.

## Latency (Extension 4)

Latency here means the time from an exchange event to its SBE record.

`TCP_NODELAY` is already enabled on every WebSocket; see
[the design decision](DESIGN_DECISIONS.md#tcp_nodelay-on-every-websocket-extension-4).
Everything below is not built yet.

### Network path

- **Deploy near the exchange.** Polymarket
  [hosts in AWS eu-west-2](https://docs.polymarket.com/api-reference/geoblock#server-infrastructure-and-co-location)
  and offers co-location to eligible participants. Compare regions by when
  matching updates arrive, not by ping.
- **Promote the fastest connection.** Hot standby already keeps two copies of
  every market. Add a temporary third connection, and promote it if it is
  consistently faster. Redundant sockets differed by up to about 65 ms in our
  tests. The hard part is matching the same update across feeds, because
  Polymarket timestamps and hashes are not unique.
- **Keep TLS connections ready.** Preconnected streams would shorten startup
  and reconnects, and could test whether different routes are faster.
- **Tune the transport.** Measure what `TCP_NODELAY` actually gains, then try
  WebSocket read buffers, TCP receive buffers, and `TCP_QUICKACK`, changed
  one at a time.
- **Use several collector hosts.** This spreads network risk, but adds
  forwarding delay and cross-host coordination.

### Inside the collector

- **Separate encoding from disk writes.** The writer encodes records and also
  writes each 64 MiB chunk synchronously. A write took 200–300 ms in our
  profiling, longer than the whole latency budget. Records queued behind it
  wait. The preferred fix is a separate I/O thread with reusable chunk
  buffers. This changes the supplied writer helper, which is why we left it
  alone.
- **Spread out snapshots.** Books added together reach their 60-second
  snapshot at the same moment, causing bursts. Stagger the first snapshot
  while keeping each book's cadence.
- **Give book work its own threads.** Sockets and book workers share Tokio
  threads. Dedicated, optionally pinned threads would keep socket reads
  prompt. Only worth it if profiling shows contention.
- **Measure socket-to-worker queueing** during bursts before changing queue
  sizes.

## Performance

These items reduce CPU, memory, and allocation cost, which raises how much
traffic the collector can handle.

- **Right-size the worker pool.** Workers were meant to take over encoding,
  which stayed in the writer. One worker might be enough. In earlier tests,
  four workers gave only 2.4 times the throughput of one. Most of the extra
  time went to `BTreeMap` updates and the shared output channel.
- **Try a more compact book structure.** If cache pressure is the limit,
  compare a sorted `Vec` with the `BTreeMap`.
- **Place busy markets on separate workers.** Hashing by market ID can put
  several hot markets on one worker.
- **Cut per-record allocations.** Encode straight from the book, borrow the
  symbol instead of copying it, and reuse message buffers through a fixed
  pool. The first two need changes to the supplied code generator.
- **Profile before optimizing parsing or trying SIMD.** Parsing costs tens of
  microseconds per message, which is small next to the write stalls.
- **Compare thread-per-core runtimes** such as
  [Monoio](https://github.com/monoio-rs/monoio) and
  [Glommio](https://github.com/DataDog/glommio) with Tokio.

## Reliability and failover

- **Detect a single stalled market (Extension 2).** We decided not to build
  this, because none of our captures showed a market going quiet on a healthy
  socket ([evidence](ARCHITECTURE.md#why-subscription-monitoring-was-not-built)). Revisit if one
  does.
- **Fail back and rotate connections.** Today we switch only when a
  connection fails, and never return to the original publisher.
- **Balance sockets by traffic.** Markets are placed by count, so a few busy
  ones can trigger slow-consumer disconnects.
- **Tune connection settings.** The 256 markets per socket, 10-second
  heartbeat, and 30-second silence limit work, but have not been optimized.

## Coverage and correctness

- **Protocol v2 markets.** See
  [the design decision](DESIGN_DECISIONS.md#polymarket-protocol-v1-only).
  This needs a second discovery query, eligibility checks for both versions,
  and confirmation that v2 books use the same messages.
- **Start collecting during the first scan.** The first full scan takes
  about five minutes. Subscribing as pages arrive would get data sooner,
  while still letting only complete scans remove markets.
- **Close the remaining ordering gaps.** We reject full books older than
  applied updates, but a stale book with an equal timestamp, or an older
  update, can still overwrite newer data. Longer captures would show how
  often this happens.
- **Check trade-to-depth coverage** over longer captures.
- **Review known Polymarket SDK issues** and add workarounds where they
  apply.
- **On-chain discovery hints.** Worth exploring only if API discovery
  becomes too slow.

## Output

- **Flush smaller chunks, or flush on a timer.** Output would appear sooner,
  and a crash would lose less data. This requires changing both the supplied
  writer and reader, which assume 64 MiB chunks.

### Stream mode

- **Shared broadcast ring instead of per-client queues.** Under discussion.
  Workers' records would be encoded once into one ring that overwrites its
  oldest slot; each client reads at its own cursor, filters for its books,
  and is disconnected when it lags past the ring. The sender cost no longer
  depends on the number of clients, but every client scans the full stream,
  and the lag a client survives is set by the whole stream's rate, so the ring
  must be sized as peak rate times tolerated stall (about 1M slots and
  150–250 MB for 10 seconds at peak).
- **Server pings and an idle timeout.** A client on quiet books whose
  connection died silently is never detected until its queue fills.
- **Authentication, `Origin` checks, per-IP and per-client limits, TLS** before
  exposing the endpoint beyond loopback.
- **Prometheus metrics** for connected clients, kicks by reason, and routed
  messages and bytes.
- **Snapshot on subscribe**, so a new client does not wait up to 60 seconds for
  a book.
- **Browser SBE decoder.** `examples/stream_client.rs` shows the logic.

## Testing and validation

- **Load and failure tests.** A full-catalog run at three times peak traffic,
  a reconnect storm, and simultaneous failure of both copies of a market.
- **A richer fake exchange.** Add pagination failures, real TLS, and partial
  scans.
- **Longer live runs.** Day-long discovery coverage, and markets that are
  not yet accepting orders.
- **Agent-driven tuning.** Once the checks reliably catch bad output, let an
  agent profile the collector, try changes, and keep only repeatable gains.

## Housekeeping

These are small refactors with no change in behavior: split the worker's
book-update handlers, add typed decode errors, and shorten `run_collector`.
