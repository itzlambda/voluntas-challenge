# Design decisions

The main choices we made, what we chose instead of, and what each one costs.
[Architecture](ARCHITECTURE.md) describes how it works; [future improvements](FUTURE_IMPROVEMENTS.md)
lists what we would revisit.

## Architecture

### A fixed pool of workers owns all books

Each market stays with one book worker for the life of the process.

- **Instead of:** one central book owner, books owned by each WebSocket task,
  or one actor per market.
- **Why:** most CPU goes to TLS, WebSocket framing, and JSON parsing, which
  run in the socket tasks. An actor per market adds about 200,000 mailboxes
  and timers for a few microseconds of work per event. Books owned by sockets
  tie book work to socket reads, risking slow-consumer disconnects, and need
  an outside arbiter for redundancy.
- **Cost:** throughput does not scale linearly with workers. A synthetic test
  reached 1.5 times at two workers and 2.4 times at four. The cause is not
  settled.

### Route by condition ID

A message goes to worker `condition_id mod N`, using the low 64 bits of the
condition ID.

- **Instead of:** hashing the market ID, or assigning a worker at discovery
  and storing it in a lookup table.
- **Why:** condition IDs are already uniform hashes, so no extra hashing is
  needed. Both outcome books share one, so they land on the same worker.
  Nothing is stored, and every component uses the same function.
- **Cost:** balance is by market count, not activity. Changing the worker
  count moves markets, so it is only allowed at restart.

## Books and failover

### Two copies of every book, one published (Extension 1)

Each worker keeps one book per source connection and publishes one of them.

- **Instead of:** resubscribing after a failure, or keeping a third published
  copy and writing a catch-up diff at each switch.
- **Why:** failover needs a standby that already holds a current book;
  otherwise changes during resubscription are lost. A switch writes the new
  source's full books as replacement snapshots, which the challenge allows
  because two sockets can legitimately hold different books.
- **Cost:** both feeds are decoded and applied, so input work and book memory
  double. A replacement snapshot restores the new source's state; it does not
  rebuild missing intermediate updates.

### Switch only on connection failure, never fail back

- **Instead of:** picking the faster source, a cross-source lag detector, or
  returning to the original source after it recovers.
- **Why:** there is no safe key for comparing two feeds. Distinct updates were
  seen sharing both timestamp and hash. Failure-only switching needs no
  comparison.
- **Cost:** a slower source keeps publishing while connected. A recovered
  source becomes standby and waits for the next failure.

### No per-market stall detection (Extension 2)

- **Instead of:** resubscribing markets whose update rate drops.
- **Why:** replaying every capture we had found no case where it would have
  helped. Real quiet periods look the same as stalls, so it would mostly
  churn healthy markets and hide other failures while debugging. Details in
  [the evidence](ARCHITECTURE.md#why-subscription-monitoring-was-not-built).
- **Cost:** a book that goes quiet while its socket stays active is not
  detected.

### Exchange full books are input, not output

A repeated full book from the exchange is compared with the held book, and
only the differences are written.

- **Instead of:** writing every exchange full book as an SBE snapshot.
- **Why:** the challenge asks for one snapshot per minute. Comparing also
  counts any divergence as a mismatch.
- **Cost:** a mismatch shows divergence, not its cause. The cases we traced
  were the exchange sending a full book ahead of the matching update, not a
  collector fault.

### Reject full books older than applied depth

- **Instead of:** accepting every full book, or buffering and sorting
  messages by exchange time.
- **Why:** a captured session showed full books replaying depth 8 ms older
  than changes already applied. Accepting them rolled the book back, then
  needed a repair.
- **Cost:** books with equal timestamps are still applied in arrival order.
  Exchange timestamps are not proven to give a complete order.

## Connections

### Second copies placed independently

Every market is subscribed on two sockets, one in each pool, and the second
copies are spread across sockets.

- **Instead of:** mirrored socket pairs carrying identical market groups.
- **Why:** in an earlier AWS run, slow-consumer closes hit both members of a
  mirrored pair, leaving markets with no source. Spreading second copies
  means one busy socket fails over to many different sockets.
- **Cost:** it cannot protect a market whose two copies both fail. One
  15-minute comparison saw 3 slow-consumer closes and 228 switches against 12
  and 1,984 before, which is suggestive, not proof.

### Hash-ordered, least-loaded placement with spare sockets

- **Instead of:** filling the lowest primary socket with room, in Gamma
  listing-ID order.
- **Why:** on 2026-10-29, all 97 `1013 slow consumer` closes in a 90-minute
  journal window came from eight primary sockets in the highest-listing-ID
  band, which held recently listed, busy markets together. The busiest
  primary averaged 936 events per second against 132 for the busiest
  replica, which was already spread. Spare sockets let new markets spread
  instead of filling one new socket.
- **Cost:** about 20% more sockets. Markets never move, so a market that
  becomes busy after placement can still make its socket hot, and a restart
  with the same market set rebuilds the same layout.

### 256 markets per connection

- **Instead of:** the earlier 100.
- **Why:** hot standby doubled subscriptions. At 256 with 20% spare, a
  250,000-market catalog needs 1,172 sockets per pool, 2,344 in total.
- **Cost:** 256 works in our 15-minute full-catalog AWS run but is not shown
  to be optimal. Slow-consumer closes also happened at 100, so market count
  alone does not set a safe limit.

### 10-second PING, 30-second silence limit

- **Instead of:** the earlier 60-second PING and 180-second silence limit.
- **Why:** the old values were chosen around long receive pauses on our first
  test host, later traced to that host or its network rather than the
  exchange. With a standby ready, replacing a silent socket sooner is cheap.
  Without PINGs, Polymarket closed sockets after about 125 seconds.
- **Cost:** the 30-second limit has not been calibrated against live data.

### Bounded retries for missing first books

A missing initial book is requested again with unsubscribe, a five-second
pause, then subscribe, up to ten times per round. Rounds are 60 seconds
apart. Markets that stay incomplete are checked against Gamma, and only those
reported ineligible are removed.

- **Instead of:** resubscribing alone (which returns no new book), or long,
  growing waits between retries.
- **Why:** some subscribed outcomes never received their first book.
- **Cost:** an experimental policy, not a documented exchange requirement. In
  our last validation run, two eligible markets still never got a book.

### TCP_NODELAY on every WebSocket (Extension 4)

Nagle's algorithm is disabled on both order-book and lifecycle sockets.

- **Instead of:** leaving Nagle's algorithm on, the default.
- **Why:** everything we send is small: subscriptions, resubscriptions, and
  `PING`s. Nagle can hold a small write until earlier data is acknowledged,
  which would slow initial synchronization, missing-book retries, and
  heartbeats.
- **Cost:** possibly more small packets. It only affects what we send, not
  how fast the exchange delivers updates. A side-by-side probe showed no
  measurable difference, so the benefit is unproven.

## Discovery

### Polymarket protocol v1 only

- **Instead of:** also collecting protocol-v2 markets and Combos/RFQ products.
- **Why:** when we measured on 2026-10-25, v2 was new with almost no usage:
  123 v2 markets against 253,740 tradable v1 markets. Supporting it meant a
  second discovery query, a second eligibility check, and verifying v2's
  messages and book shape, all for almost no data. We spent that time on
  correctness and redundancy instead. Combos/RFQ are not in the regular
  order-book catalog.
- **Cost:** v2 markets are filtered out. Polymarket has tentatively announced
  that new markets move to v2 from 2026-11-02, which would shrink coverage,
  so v2 support is the top [future improvement](FUTURE_IMPROVEMENTS.md#top-priorities).

### Lifecycle sockets for speed, full scans for truth

Three lifecycle sockets announce new markets, and a full Gamma scan every
15 minutes reconciles the catalog. Only a complete scan can remove markets.

- **Instead of:** WebSocket announcements only, or repeated scans only.
- **Why:** single sockets missed announcements and never replayed them; three
  together caught all 519 in a 65-minute run. `market_resolved` never arrived
  on these sockets (0 of 401 resolutions), so removals need the scan. A failed
  or partial scan must never drop markets, so pages are buffered and applied
  together.
- **Cost:** a market missed by every socket waits for the next scan, and
  cleanup of closed markets can wait a full interval. At startup, the catalog
  is not subscribed until the first scan finishes, about five minutes.

## Output

### Timestamps use receive time

`PriceLevelUpdate.backtest_timestamp` is the source frame's receive time, and
`Snapshot.received_at` is the receive time of the latest message applied to
that book. A quiet book's periodic snapshot keeps its previous timestamps.

- **Instead of:** stamping snapshots with the time they were taken.
- **Why:** backtests keep when data first reached the collector, before
  parsing and queueing. Mixing receive-time updates with capture-time
  snapshots could make a queued update sort before a snapshot that does not
  include it.
- **Cost:** receive times can tie or go backwards across sockets, so readers
  replay each book in file order, not by timestamp. Details in
  [the architecture](ARCHITECTURE.md#timestamps) and on `OutputRecord` in
  [src/model.rs](../src/model.rs).

### One writer thread encodes everything

- **Instead of:** encoding in the workers and passing bytes to the writer.
- **Why:** it is the intended use of the supplied helpers, which stay
  unchanged. Encoding moves only if measurement shows the writer is the
  bottleneck.
- **Cost:** the supplied writer flushes each 64 MiB chunk synchronously, and
  writes over 100 ms were observed, delaying later encoding.

### Workers send one batch per input

- **Instead of:** sending every record separately on the shared writer
  channel.
- **Why:** with four workers, the shared channel's send showed 2.9 times the
  profile samples. A batch goes out as soon as its input is processed, so it
  adds no delay.
- **Cost:** the queue limit counts batches, not records or bytes.

## Assumptions disproved by data

| Assumption | What we found |
|---|---|
| One WebSocket can carry every market. | One socket accepted 20,000 markets, then was closed as a slow consumer. |
| Redundant feeds can be deduplicated by hash. | Distinct updates shared both timestamp and hash. |
| `market_resolved` announces closures. | It arrived for 0 of 401 resolutions. |
| One lifecycle socket sees every new market. | Two of three sockets missed some of 519 announcements and never replayed them. |
| Market IDs arrive in creation order. | They do not, so "fetch IDs above the last seen" would miss markets. |
| Long receive stalls were the exchange pausing. | Controls and a second host pointed to our first test host or its network. |
| Every exchange full book is current. | Captured full books replayed depth 8 ms older than changes already applied. |
| Resubscribing returns a fresh book. | It returned nothing 10 of 10 times; unsubscribe then subscribe worked 35 of 35. |
