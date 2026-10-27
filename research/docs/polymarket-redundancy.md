# Ten connections to one outcome book

Status: saved research for the optional redundancy extension. Implementation is
deferred until the base challenge is complete.

On October 24, 2026, two exploratory runs subscribed ten independent WebSocket
connections to the Yes token for “Will Bitcoin reach $82,000 on October 24?”
The collector was not changed.

- Market ID: `6104736`
- Condition: `0xecc23cf4dd24785b04a3f10d0ad6c6577858109296851d6a7cde76dd88682577`
- Yes asset: `42820435881084171095305173309280682294357335411274100735935831763831134276262`
- [Market page](https://polymarket.com/event/what-price-will-bitcoin-hit-on-october-24-2026/will-bitcoin-reach-82k-on-october-24-2026)

## What happened

Run 1 started at 10:08:25 UTC. It used ten seconds of warm-up, a 120-second
comparison window, and ten seconds of grace. All ten books initialized to the
same levels. Connections 1 and 8 subsequently closed with code 1006 at about
65 seconds. The other eight received the same 72 price-change messages in the
comparison window, in the same order, and reconstructed the same state after
each matched message. The failed connections received only one of those 72
messages and retained stale books.

All ten also had about 60.4 seconds without any received application message,
including PONG, before a burst resumed. The capture does not identify the
cause. Redundant connections on one host and endpoint did not prevent this
shared delivery pause. The first run exited nonzero to report the failures;
it is retained as evidence rather than counted as a healthy latency sample.

Run 2 started at 10:13:15 UTC. It used ten seconds of warm-up, a 60-second
comparison window, and ten seconds of grace. All ten stayed connected. Each
received the same 27 price-change messages in the window, in the same order,
with no observed omissions relative to the union of the ten captures. Initial
and final books agreed, as did reconstructed state at every matched event.
Across the complete 80-second session, every connection received one initial
snapshot, 34 price-change messages, seven PONG replies, and 18 unrelated
new-market announcements. Announcements were excluded from book comparisons.

For the 27 matched updates in run 2, the spread between the first and last
socket callback was median 6.917 ms, p95 60.140 ms, and maximum 64.666 ms.
Connection 2 arrived first for 13 updates; connection 1 for ten; connections
6 and 7 for one each; and connection 10 for two. No one socket was always
first. A first-arrival merge happened to preserve the reference order in
both samples; this is an observation, not an ordering guarantee.

## Deduplication keys that failed

Distinct updates shared both a timestamp and a hash. In run 1, these three
separate Yes updates all had timestamp `1792836510065` and hash
`9a589e0deb3130fea2da8687b279577d13c5d661`:

```text
BUY  $0.04 -> 764 shares
BUY  $0.03 -> 4197.69 shares
SELL $0.11 -> 50 shares
```

Using timestamp, hash, or `(asset_id, timestamp, hash)` alone as the
deduplication key would discard valid level updates. This example occurred
during warm-up; collision checks cover the full captured session.
Different payloads shared timestamps in both runs. Neither run happened to
contain conflicting quantities for the same asset, side, price, and
millisecond; their absence does not establish that this can never happen.

For comparison, the analysis fingerprints the complete JSON event with sorted
object keys and pairs repeated identical payloads by occurrence count. This
keeps different level updates separate and retains genuine repeated payloads.
It is an offline matching convention, not a guaranteed exchange event ID.
An online, bounded deduplication cache could use full content, but cannot
prove exactly-once delivery or order on that basis.

## Invariants for a future redundant collector

1. Each connection maintains its own book, initialized from its own snapshot.
   Apply messages in that connection's order and filter every price change
   by the target asset. Never assume every event in a subscription belongs
   to the subscribed outcome; the raw files preserve unrelated messages too.
2. One selected connection owns the published book at a time. A reconnect
   starts a new generation; discard late work from the old generation.
   Absolute quantities make immediately repeated assignments harmless, but
   a delayed old assignment can undo a newer one.
3. A standby is eligible for a controlled handoff only when its initialization,
   health, overlapping message history, and state agreement have been checked.
   Keep pending messages attached to their connection and generation. Matching
   state is useful evidence of agreement, not proof of complete market history.
4. If continuity is uncertain, reconnect/resubscribe, initialize from the fresh
   snapshot on that connection, and record a new SBE snapshot and a gap status.
   A snapshot restores known current state; it cannot recover missed history.
5. Timestamps and hashes are not a documented contiguous sequence. A per-level
   timestamp rule would need a monotonic-version contract, a tie rule, and
   tombstones for deletions; this experiment does not establish that contract.
   Sorting by timestamp or accepting the first copy alone cannot guarantee a
   correct merged history. Conflicting same-version values require resync or
   an authoritative ordering mechanism, not a guessed tie-breaker.
6. Track PONG timeouts, connection generation, last application-message time,
   observed cross-feed lag, and state disagreements. A quiet market alone is
   not a broken connection. Ten sockets can share failures in the host, network,
   or upstream infrastructure; socket count is not independent source count.

Sharding different markets across sockets spreads load and requires no cross-feed
deduplication. Subscribing several sockets to the same market provides redundancy
and creates this ordering problem. These are different operations.

Later [subscription-capacity trials](polymarket-subscription-capacity.md) compared
a loaded connection with a small control feed. They recorded complete sampled
probe coverage but variable relative delivery times and some per-asset
notification-order differences. That comparison includes multiple event types;
it does not by itself establish conflicting book-level updates. The source
selection and ordering constraints above remain necessary.

## Measurement scope

All sockets used Node v24.22.1 in one process on one six-core host, connecting to
the same endpoint. Initial load averages were 1.77, 0.99, and 0.88. Subscriptions
were sent only after all ten opening handshakes completed. Receipt times were
captured at callback entry with one monotonic clock, before offline JSON parsing
and comparison. First snapshots and heartbeats were excluded from the comparison
window. Events were admitted by their earliest observed receipt during the window;
the grace period let trailing copies arrive. Source timestamps did not define the
window, so delayed batches are included as received.

These numbers measure application delivery, including client scheduling, TLS,
buffering, and network effects. They are not exchange or wire latency. Run 2 had
event-loop delay p99 4.444 ms and maximum 90.440 ms, so sub-millisecond differences
should not be interpreted as network advantages. CPU use was 3.903 seconds user
and 2.784 seconds system over the 80-second session; maximum queued output was
11,156 bytes. Run 1's maximum event-loop delay was 312.214 ms, much shorter than
the shared delivery pause, but the pause's cause was not diagnosed. Both runs
preserve errors and actual output. Two short samples on one market cannot prove
that all healthy connections always receive identical data or that all sources
did not miss the same update.

The offline analyzer was checked against synthetic omissions, reordered events,
timestamp and hash collisions, divergent books, and repeated identical messages.
No production failover or merge policy was implemented or benchmarked.

## Repeat and inspect

The selected market is dated and the capture script deliberately refuses to run
if it has closed or the Yes asset identity has changed.

```sh
node experiments/polymarket-redundancy.mjs /tmp/polymarket-ten-sockets 120
python3 experiments/analyze-polymarket-redundancy.py /tmp/polymarket-ten-sockets
```

Use a fresh output directory. The first command logs failures and exits nonzero
if a socket fails, while retaining data for analysis. It does not reconnect,
which makes each captured source one unambiguous connection session.

Shareable summaries are in
[`experiments/polymarket-redundancy-results.json`](../experiments/polymarket-redundancy-results.json).
Complete raw frames, subscriptions, heartbeats, and connection boundaries remain
locally in `recordings/polymarket-redundancy-run-1/` and
`recordings/polymarket-redundancy-run-2/`; raw capture files are Git-ignored.

The [official WebSocket schema](https://docs.polymarket.com/asyncapi.json) defines
the messages used here. It documents absolute level quantities, millisecond
timestamps, and hashes, but no contiguous order-book sequence number.
