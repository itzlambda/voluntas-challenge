# Design journal

Raw material for a write-up of how the collector design evolved: the
approaches we tried, why each was kept or dropped, and how agents were used.
Entries are in rough chronological order. Facts and numbers are in
[FINDINGS.md](FINDINGS.md); the current design is in [SPEC.md](../SPEC.md).

## Phase 1: First implementation and exploratory research (2026-10-23 to 10-24)

**Approach.** Build a working Polymarket collector quickly, then measure the
exchange before deciding on scale.

- First implementation (now archived in commit `19b2bea` under `v1/`):
  - one Tokio current-thread runtime
  - markets batched onto sockets
  - one book owner with a bounded channel from the sockets
  - a synchronous minute checkpoint
  - recording and replay tools
- Optimized the per-message path from 11.1 µs to 3.4 µs per frame by replacing
  `serde_json::Value` with typed, borrowing parsing.
- Live experiments against Polymarket:
  - observed message types
  - subscription capacity per socket (accepted 20,000 markets, then `1013 slow consumer`)
  - socket count per IP (16,000 admitted)
  - 10 redundant sockets on one book (timestamp and hash aren't unique; shared 60 s stall)
  - empty-asset lifecycle sockets
- Lesson: several assumptions, such as "one socket can carry everything" and
  "dedupe redundant feeds by hash", were disproved by measurement, not by
  reading docs.

## Phase 2: Market discovery research (2026-10-25)

**Approach 1: seed scan plus two incremental listings.** A full keyset scan
at startup, then poll "recently activated" (`acceptingOrdersTimestamp`) and
"recently closed" (`closedTime`) listings.

- Problems found:
  - equal timestamps with IDs out of order
  - no documented guarantee that the timestamps cover every transition
  - the `version` parameter silently defaults to v1 (a coverage bug in the seed)
- Decision: v1 only (253,740 tradable v1 markets against 123 v2).

**Approach 2: two `updatedAt` listings.** Generalized approach 1: poll open and
closed listings ordered by `updatedAt`.

- Found that responses are CDN-cached for 300 s. `no-cache` headers don't help;
  a unique query parameter does.
- Needed per-market revisions, overlap windows and tie handling.
- It worked on samples, but felt fragile: correctness depended on undocumented
  timestamp behavior.

## Phase 3: Architecture options and independent review (2026-10-25)

**Setup.** Wrote `collector-architecture-options.md` with four options and an
explicit independent review prompt, so a fresh agent could challenge the
recommendation instead of rubber-stamping it. The prompt asked for a
design-level comparison of the four options at about 200,000 markets and the
simplest architecture that meets the goals, without reading the code.

The four options:
1. One central book owner.
2. Each WebSocket owns its books.
3. One actor per market.
4. A fixed number of workers, each owning many markets.

An earlier multi-agent review of the architecture proposal converged on
stable book ownership. One candidate didn't finish within its timebox.

**Independent review (Claude Opus 5.5, this session).** It read the options doc
and the measurement notes, but deliberately not the code. It mostly agreed with
option 4, for different reasons:

- The CPU is in TLS, WebSocket framing and JSON parsing, not book updates. So
  parsing should move to the connection tasks in any option. That makes the
  "single CPU path" weakness of option 1 much smaller than claimed.
- Moving markets between workers is never needed: the exchange resends a full
  book on subscribe, so state is cheap to rebuild.
- Rebalancing connections is just redundancy that lasts briefly: subscribe on
  the new socket, switch once it's ready.
- Option 3 (an actor per market) adds 200,000 mailboxes and timers for no
  benefit when work per event is a few microseconds.
- Option 2 couples book work to socket reads, which risks `1013` disconnects,
  and needs an outside arbiter for redundancy.

## Phase 4: Refining the chosen architecture (conversation with the reviewer)

How markets are routed to workers went through three versions:

1. **Hash the market ID.** Concern: hashing adds cost on every event.
2. **Assign a worker at discovery time** and carry the worker index through a
   per-connection lookup table. Cheap, but more moving parts.
3. **Final: the last hex character of the condition ID, `& (N-1)`.** Suggested
   by the user as "just take the ID modulo N". The reviewer refined it:
   - Use the condition ID, not the asset ID, so both outcome books land on the
     same worker.
   - Make N a power of two, so the last hex character alone decides the worker.
   - No lookup table is needed.

Snapshots:

- Initially there was concern about a burst every minute.
- The user pointed out the challenge doesn't fix *when* in the minute each
  snapshot happens. So snapshots are spread across the minute, each book about
  every 60 s, using a sorted FIFO queue with no heap.

Redundancy:

- The reviewer first conflated failover with planned moves ("the new socket
  sends a snapshot anyway").
- The user corrected this: failover needs a **hot standby** already holding a
  current book, otherwise changes during resubscription are lost.
- That became the design: one book per source in the same worker, one
  publisher, and a local SBE snapshot on a switch.

`source_id` and `generation`:

- These took a few rounds of explanation with concrete examples.
- Later refined: `source_id` became the connection's number, so connections need
  no per-asset table.

**Output: `SPEC.md`**, the full specification. Repository cleanup: everything
except the challenge scaffold was removed, and the earlier seed code was saved
in a Git stash.

## Phase 5: Rethinking market lifecycle

**Approach 3 (reviewer): desired-state reconciliation.**

- Subscribe when a market is *created*, not when it starts trading. In the
  seed, 94% of markets traded within 5 minutes of creation, and an extra
  subscription costs almost nothing.
- WebSocket `new_market` and `market_resolved` serve as fast hints.
- A slow, continuous Gamma scan is the source of truth. No timestamp
  bookmarks are needed.

**Approach 4 (user): WebSocket-only after startup.**

- Rely on `new_market` and `market_resolved` with no further REST, accepting
  that closed-but-unresolved books stay in memory.
- The reviewer flagged the main risk: a missed `new_market` loses data
  permanently, and redundant sockets don't protect against shared stalls.

**Approach 5 (combined, final):**

- `new_market` from 3 redundant lifecycle sockets for additions.
- One reconciliation scan, newest IDs first. It runs at startup, every T, and
  after a lifecycle outage (the user's idea: reuse the scan instead of a
  separate catch-up query).
- The same scan doubles as cleanup.
- The user's position: `closed` is final, so cleanup is safe.

### Using agents to test the assumptions

Three **GPT-6.1 Sol** research agents ran in parallel, delegated as T3 child
tasks. Each got a self-contained brief and wrote its script, results and report
under `research/`:

1. **Backlog**: how many closed but unresolved markets exist, and closure rates.
2. **Worst-case resolution time**: documented limits and the longest observed cases.
3. **Live lifecycle sockets**: 65 minutes, 3 sockets, checked against Gamma.

Web-fetch subagents read the challenge page and the Polymarket docs (AsyncAPI,
market details, changelog).

What the agents found, and how the design changed:

- **`market_resolved` never arrived on empty sockets** (0 of 401). This killed
  the WebSocket-only removal idea; the scan became the only removal path.
- **Single sockets missed events and didn't replay them**, but 3 together
  caught all of them. Redundancy went from "nice to have" to required.
- **Receive stalls of up to 119 s on the original test host** initially led
  us to wait instead of reconnecting thousands of sockets. The assumed
  exchange-side cause was later undermined by host and network controls.
  The [archived incident](archive/test-host-connectivity/README.md) records the revised interpretation.
- **IDs aren't in creation order on the wire.** This confirmed dropping the
  "IDs above the last seen" catch-up query.
- **Gamma flips `closed=true` at resolution, not when trading stops.** So the
  scan can't remove a market earlier than resolution anyway. The remaining gap
  (stopped trading, still `closed=false`) is accepted and documented.
- The user challenged the "closed but unresolved is zero" result. The sample
  was mostly short-lived sports and crypto markets. The design doesn't depend
  on it.
- The worst-case agent found no documented maximum resolution time, which
  mattered less once the scan was in place.

## Phase 6: Output path

- **Ordering across books:** per-book order is guaranteed; order across books
  isn't, and isn't needed. The exchange has no cross-market sequence either, so
  consumers merge by receive timestamp if they want one timeline.
- **Periodic snapshots** are scheduled by each worker per book: a sorted FIFO of
  due times, at most 60 s between snapshots. Any full `book` from the exchange
  resets a book's clock.
- **Where to encode.** The reviewer proposed encoding in workers and passing
  bytes to `AlignedSbeWriter` through an `SbeEncode` wrapper. The user chose to
  start with single-threaded encoding in the writer, the intended use of the
  helpers, and to move encoding only if measurement shows a bottleneck. The
  wrapper and other helper changes are listed as possible improvements in the
  spec.

## Phase 7: Closing the remaining questions

- **Timestamps:** the reviewer suggested anchoring the wall clock to a
  monotonic clock and timestamping as early as possible. The user rejected it
  as overcomplicated. Instead: three timestamps from the language's default
  clock (exchange, received, processed), so delivery latency and internal delay
  can each be measured. The processed time goes to stderr metrics, because the
  schema has no third field.
- **Full `book` messages:** the reviewer's spec wrote every exchange `book` as
  an SBE `Snapshot`. The user corrected it: a received full book is input. The
  worker compares it with its book and writes only the differences, and
  snapshots are written only once a minute, as the challenge asks. This also
  made reconnects simpler: compare the new `book` with the held book, with no
  snapshot needed. Any difference also signals a missed update.
- **Scope:** v1 only. v2 markets are ignored for the challenge.
- **Clock:** `chrono::Utc::now()` everywhere. Tokio has only a monotonic
  `Instant`, which can't produce UTC.
- **`backtest_timestamp`** is undefined by the challenge. At this stage it was
  set to received time with a confirmation TODO. The
  [settled timestamp decision](../DESIGN_DECISIONS.md#timestamp-semantics)
  retains receive time and closes that TODO.
- **Snapshot timestamps:** the reviewer first proposed copying the last event's
  times, then reversed after weighing the backtesting goal. Decided:
  `received_at` is when the snapshot was taken, and `exchange_timestamp` is the
  latest applied exchange message. This keeps the file close to time order,
  shows that quiet books were confirmed, and never claims newer exchange data.
  This early snapshot decision was superseded by the receive-time policy in
  [the timestamp decision](../DESIGN_DECISIONS.md#timestamp-semantics).
- **Gaps** are reported on stderr only.

## Phase 8: Telemetry before implementation

- The user asked for telemetry research *before* implementation, to avoid later
  redesign. The reviewer started from the challenge's priorities (completeness,
  time accuracy) and extensions, derived operator questions, and mapped each to
  a signal and the component that produces it ([telemetry.md](telemetry.md)).
- Result: no architecture change, plus six small hooks fixed in the spec. The
  most important: telemetry must never block the data path.
- Ideas that could set the submission apart: a data-quality score (coverage %,
  unsynced book-seconds, missed-update mismatches from book comparison),
  latency per connection, and an independent verifier of the SBE output.

## Phase 9: Code structure and implementation plan

- Extension 3 (exchange-generic) was designed in before writing code: one core
  that knows no exchange, and adapters for wire formats, identifiers and
  discovery calls.
- The reviewer found the spec's adapter boundary too narrow for other
  exchanges. Some use additive deltas with sequence numbers, per-connection
  subscription state, and authenticated connections. Normalized events now
  support set and add changes and optional sequence numbers.
- A test-only fake exchange is a second adapter from milestone 0, to keep the
  boundary honest without building a second real exchange.
- The implementation plan set the layout and milestones M0–M6 (the full plan,
  since removed from SPEC, is in Git history: `git show 76e65b8:PLAN.md`).

## Phase 10: Types and naming

- The user steered the types toward the domain: `OrderBook` instead of `Book`.
  Only two kinds of order book data exist, full snapshots and level updates.
  Prices and quantities are `i64`, matching the schema.
- The reviewer's combined `FeedEvent` enum and its Kalshi-style additive
  deltas and sequence numbers were dropped: Polymarket only, with separate
  decode paths for order book and lifecycle connections.
- **`generation` was removed.** Going through the worker's types showed that
  the design had carried two mechanisms for one job since the first spec: a
  per-message session number and an in-order reset message. The reset alone
  is enough, because each connection is the only sender of its data and a
  channel keeps one sender's messages in order. The spec now states those
  ordering rules. The check that drops messages from a connection other than
  the market's own stays, because it covers markets re-added on another
  connection, which `generation` never did.
- The reset became `MarkUnsynced { connection, reason }`. It is named for its
  effect, because a disconnect is only one reason to distrust a connection's
  data. `ConnectionId` became a `u16`.

## Phase 11: Independent spec review

- Three **GPT-6.1 Sol (high reasoning)** reviewers read the spec and plan, each
  with a different focus: data path correctness; requirements, lifecycle and
  connections; performance, plan and simplicity. They were read-only.
- The reviewer (Claude) merged and triaged 33 findings by whether they matter
  for this challenge, and the user decided each. The user often chose the
  simpler path and asked for context before accepting a fix.
- A further GPT-6.1 Sol agent tested exchange behavior the fixes depended on:
  - a duplicate subscribe sends no book, while unsubscribe then subscribe does
  - markets with no orders get an empty book
  - connections close about 125 s without `PING`
  - full books are always sorted and clean
- Changes that came out of it:
  - the first full book after `AddMarket` is written as an SBE `Snapshot`, so a
    re-added market can't leave stale levels
  - snapshot `received_at` became the last applied event's received time, so
    timestamps per book never go backwards (both data reviewers found this)
  - periodic snapshots use `tokio_util::time::DelayQueue` with exactly 60 s
    and no random spread, which removed a hand-written queue and its bugs
  - workers became Tokio tasks on the shared runtime
  - subscription batching was dropped, since churn is one change per
    connection every 15–40 minutes
  - `PING` every 60 s, given the observed ~125 s timeout
  - v2 hints are filtered out
  - the scan is never restarted; a trigger queues one more pass
  - parse failures reject the whole message, go to a debug log, and reconnect
  - `BTreeMap` stays, with a benchmark TODO for alternatives
- Rejected as over-engineering for the challenge: a separate head-refresh
  scan, byte-accounted channels, and keeping `PING` going while blocked on a
  full worker queue.

## Phase 12: Pre-implementation cleanup

- Before starting implementation, the reviewer reread the spec and plan as a
  new implementer would. Two contradictions were found and fixed:
  - book connections subscribed with custom features on, while the lifecycle
    section said they were off
  - section 5 and section 9 disagreed on what workers send the writer; it is
    `OutputRecord`
- Several gaps were closed:
  - the missing `DecodeError` reason
  - an unimplementable "keep provisional markets that deliver data" rule,
    dropped
  - which component owns the missing-first-book check: the connection
  - one shared ID lookup
- Extension-only terms (source, publisher) moved out of the base sections.
- An implementers' note now marks the spec and plan as the source of truth and
  lists the superseded research notes.

## Patterns worth discussing in the write-up

- **Ask for an independent review with a prompt that requires challenging the
  recommendation**, and hide the existing code so the review isn't anchored.
- **Measure before deciding.** Several plausible designs were disproved by
  live data: hash deduplication, socket capacity, `market_resolved` coverage,
  and ID ordering.
- **Use parallel agents for research with self-contained briefs**:
  known facts, exact questions, rate limits, where to write results, and a
  rule not to touch the spec or code.
- **The human kept steering the simplifications:**
  - modulo routing
  - spreading snapshots across the minute
  - the hot-standby correction
  - WebSocket-first lifecycle
  - reusing the scan for catch-up

  The agent's job was to stress-test each one and find where it breaks.
- **Treat agent results critically.** The "zero backlog" result was challenged
  for sampling bias. Agents' own reports also flagged their limits, such as
  stale Gamma fields and v1/v2 timestamp mixing.
- **Keep the evidence.** Every claim in the spec links to a saved report with
  its method, so the reasoning can be audited later.

## Phase 13: Implementation starts (M0–M1)

- The plan said the worker and registry would be plain synchronous state
  machines with time passed in. That conflicted with the spec's
  `tokio_util::time::DelayQueue`, which reads Tokio's clock and needs a
  runtime. The user chose a Tokio service throughout instead: `#[tokio::main]`,
  every component a Tokio task. Determinism in tests now comes from Tokio's
  paused clock, which fixtures move forward.
- The writer and telemetry writer stay plain OS threads. A lifelong
  `spawn_blocking` writer would stop the paused clock from auto-advancing in
  tests, and the spec already chose a thread for blocking writes.
- The coordinator delegated the readback tests and the synthetic load test to
  GPT-6.1 Sol subagents, which own only the test files and report src bugs
  back instead of fixing them.
- The M1 load test (50,000 markets, 2 million stream messages, release, 6
  cores) replayed about 28,000–30,000 events/s with 1, 2 or 4 workers, with
  zero time blocked on the writer. The limit was the single replay task that
  decodes for every connection; live, decoding is spread over connection
  tasks. Per-stage cost is left for measurement 3 in M5.
- A worker-scaling test (queues filled before workers start) showed
  throughput is not linear in workers at full scale: 1.5× at 2 and 2.4× at 4
  workers, but linear at a smaller market count. Profiles point to shared
  memory/cache and the shared output channel. Left open for later analysis in
  [worker-scaling.md](worker-scaling.md).

## Phase 14: Live collector (M2–M4)

- The user asked to finish the remaining milestones and run the collector
  end to end on the whole catalog. The five open M1 choices were kept as
  implemented and recorded as implementation choices in SPEC section 13.
- Built: book connections (PING, reconnect with `MarkUnsynced`, shared-pause
  health rule, missing-first-book requests, `1013` reports), capture mode,
  the subscription manager (placement, per-page subscribe, slot reuse,
  splitting), the registry and Gamma client, three lifecycle sockets, the
  discovery task (startup/timer/outage scans that are never restarted,
  provisional checks, after-pass lookups), and the SIGINT sequence with a run
  summary.
- The core reaches the exchange through a small `Listings` trait (scan page,
  ID lookup), so tests use fakes and the registry never sees Gamma's format.
- A fixed-list mode (`--markets GAMMA_ID,...`) is M2's "fixed list of markets
  from config". A first live run on 6 busy markets wrote a readable file with
  every snapshot matching the book rebuilt from updates.
- **First full-catalog runs** (2026-10-25, defaults, this 6-vCPU VM): the
  startup scan registered 249,118 markets from 2,482 pages in 11.4 minutes
  and opened about 2,500 connections. In steady state: 20,000–25,000 frames/s,
  about 1.3 GB RSS, no telemetry dropped. The first 2.7 GB of output read back
  with the supplied reader: 498,218 books, 3.9 million snapshots, 14.9 million
  level updates, no snapshot differing from the book rebuilt from earlier
  records, and no timestamps going backwards. SIGINT with about 850
  connections open exited cleanly in about 1 s.
- Bugs the live runs found: ID lookups rejected a full page's `next_cursor`
  (fixed: follow it); delivery latency counted full books, whose timestamp is
  the last change (fixed: level updates only); and the shared-pause health
  rule reconnected about 300 connections in the 16 s after a 55 s pause
  ended, because connections resume a few seconds apart (fixed: "most
  connections fresh" must hold for 30 s, SPEC section 8).
- Still to look at in M5: about 20 mismatches per minute (repeat books that
  differ from a synced book by about one level) and 26–52 server connection
  resets per minute across 2,500 connections.

## Phase 15: Shared pauses diagnosed

- An overnight full-catalog run showed thousands of "stalled" reconnects. The
  user pointed out that with one connection per market every reconnect loses
  data, and asked for the root cause before the next run.
- We initially interpreted a side-by-side probe as a Polymarket-side pause:
  other controls continued while market WebSockets stopped, and some sessions
  resumed with a backlog. Later address-family controls and a second host
  undermined that attribution. The [archived host incident](archive/test-host-connectivity/README.md)
  supersedes the original explanation.
- The health rule was simplified rather than tuned: no PONG timeout and no
  "most connections fresh" comparison. A connection reconnects on close,
  error, or 180 s with nothing received. The earlier 30 s settle refinement
  was removed with it.
- The overnight run ended when a background disk-guard script hit the
  session's 2-hour task limit and the output filled the disk. Long-running
  helpers must run detached from the agent session.


## Phase 16: Base submission validation (2026-10-26)

- The user asked to finish the saved-window replay, two-feed startup experiment,
  full fake-exchange test, and M5 before starting extensions.
- The internal stress target was recorded as p99 below 100 ms from receipt to completed SBE encoding in
  the output buffer at three times the observed peak load. Physical chunk
  visibility is outside this target. Buffering can still delay later encoding
  because the supplied helper flushes synchronously before rotating a chunk.
- All six saved first-refresh divergences match the independent Decimal
  reconstruction before application at N=1,2,4. Their cause remains unknown.
  A divergence counter alone does not establish a missing exchange update.
- The correctly provisioned catalog run completed one scan and five steady
  minutes, with clean SIGINT. A checker initially flagged 48 fresh baselines
  produced after three connection-placement splits. Explicit Remove/Add
  controls and complete raw books prove each reset. Exact-content readback
  then reports zero unexplained differences. The independent checker accepts
  these resets only with a matching proof manifest.
- The fake-exchange tests now include a positive ordering fence after the
  final late removed-market update. Every expected record is checked against
  an independent server oracle. No extension behavior is claimed.
- The first detached measurement hit its inherited 1,024-descriptor limit and
  is excluded from capacity conclusions. The corrected run used 65,536,
  directly observed while running. Its capture-enabled latency exceeded the
  target, with slow-consumer closes and output backpressure. Controlled
  pre-generated-input measurements are required before choosing defaults.
- The writer diagnostic driver exposed a lifetime stderr lock in telemetry.
  A focused test failed before the one-line per-write-locking fix and passed
  afterwards. The actual stdout writer then completed and its output passed
  independent readback. The failed driver run is retained as a calibration
  failure, not a stdout throughput result.
- Side experiments from this phase were later removed as superseded; they
  are in Git history at `0c4800a` under `research/experiments/m5-*`. A decoder
  before/after comparison matched outputs but had overlapping timing ranges
  under host contention, so no speedup was claimed. A detached-service gate
  run passed correctness but reached 87,712 of 125,061 offered messages per
  second with an invalid generator, the same failure recorded in
  [MEASUREMENTS.md](../MEASUREMENTS.md). A five-second live wire sample showed
  full books at 3.7% of book and price-change messages. The REST depth, writer
  matrix, load matrix and pipeline profile drivers produced results that are
  either kept as JSON or superseded by the writer profile and stage probe.
  The remaining M5 drivers, the paced benchmark harness `tests/m5.rs`, and the
  stage-latency probe were removed later; they are in Git history at `cba9d47`.
  The writer profile, stage latency report and MEASUREMENTS.md keep their
  results and conclusions.

## Phase 17: Code-quality pass and deferred refactors

A ranked refactor plan kept the challenge contract and architecture fixed and
allowed only internal APIs and diagnostic wording to change. It is preserved
in Git history (`git show 76e65b8:REFACTOR_PLAN.md`); the decisions are below.

- **Worker sends in one place** (`5b928c6`): a small `Workers` value with
  `send_to_market` and `broadcast` replaced routing and channel-error handling
  repeated in four modules. It adds no task, queue, or routing cache, and
  `routing::worker_index` stays the single routing rule.
- **One fixed reconnect schedule** (`5b4c0b9`): book and lifecycle connections
  had duplicated backoff formulas with a random multiplier. Both now share
  `ReconnectBackoff`: the first reconnect is immediate, then 1, 5, 10 and 30
  seconds, then 30 seconds for every further retry. A session lasting more
  than 60 seconds resets it. Backoff decides when to open a replacement after
  a connection ended; the separate 180-second silence limit decides when an
  open connection has failed. Its conservative value came from the original
  test-host incident, initially attributed to Polymarket. It is retained here
  pending the separate standby/replica work.
  The connection-opening rate limiter still applies to immediate retries.
- **Named data shapes** (`f743b3b`, `6d23197`): `MarketBooks` with exactly two
  order-book IDs replaced `(MarketId, Vec<OrderBookId>)`, and `BookPos`
  replaced `(Slot, usize)`. The two-book array is safe only after admission:
  all 253,740 rows of the later saved catalog had two distinct tokens, and all
  but 44 token-less rows of the earlier one (256,131 rows), while closed historical records
  with three or five tokens exist. Raw metadata therefore stays variable-length
  and the eligibility check rejects it.
- **Discovery reconciliation** (`36472d6`, `30d9bd0`): lifecycle additions are
  admitted immediately with no provisional HTTP lookup; an ascending-ID scan is
  buffered complete before any change, the latest lifecycle event per market
  since the last successful scan is overlaid on it, and a failed scan changes
  nothing. The first scan starts at once instead of waiting for a lifecycle
  socket.
- **Left as optional cleanup:** splitting the worker's full-book and
  level-update handlers, typed decode errors, `Result` instead of booleans in
  the connection session, and a smaller `run_collector`. None changes
  behavior.
- **Deliberately not done:** a new exchange abstraction or ownership redesign,
  passing a telemetry handle through every component, changes to the supplied
  `src/util/` helpers or SBE schema, generic encoding helpers that would merge
  the writer's two record types, further reconnect or TCP tuning, and
  exact-text tests for errors or telemetry.
- Verification relied on the existing output tests, which read the SBE file
  back and compare every record per book against an independent reference.
  A byte-identical whole-file baseline was not required, because unrelated
  books may interleave differently across workers.

## Phase 18: Worker-to-writer batching

- One decoded frame can change several levels, and a snapshot timer emits a
  whole book. The worker already gathered those records in `pending`, but then
  sent each one separately on the single channel shared by all workers. The
  [worker-scaling profile](worker-scaling.md) showed that shared channel's
  `Tx::push` at 2.9 times the samples with four workers. Sending the gathered
  records as one batch reduces operations on that shared channel.
- The worker now sends one `Vec` per input or snapshot timer (`23d4787`). The
  batch is sent as soon as that input is processed; there is no timer that
  waits to fill it, so batching adds no delay. Per-book order, receipt
  timestamps, final draining, and fatal writer errors are unchanged.
- `--writer-queue` and queue peaks now count batches, not records. Snapshot
  batches can be much larger than update batches; a byte bound is added only
  if memory measurements show a need (SPEC section 10).
- Batching reduces handoff work only. It does not remove the supplied
  writer's synchronous 64 MiB chunk writes, which the
  [stage-latency findings](experiments/stage-latency-findings.md) observed
  lasting over 100 ms. No speedup has been measured, and the M5 runs predate
  this change.

## Phase 19: Archive the original test-host connectivity incident

- Early long receive stalls led us to replace the short PONG timeout with a
  180-second total-silence guard. We initially assumed the exchange held
  delivery. Later independent controls and the two-host comparison pointed to
  the original host or its network path instead. The London run did not
  reproduce that broad pattern, though its telemetry cannot exclude shorter
  individual connection pauses. The failed component remains unproven.
- The [incident archive](archive/test-host-connectivity/README.md) retains the
  observations and revised attribution. Diagnostic scripts and generated
  timelines were deleted. No IPv4-only workaround was added to the collector.
- The current timeout remains unchanged by this cleanup. Its slower isolated
  failure recovery will be addressed in the separate standby/replica work.

## Phase 20: Reuse the writer's level-update message

I noticed that the writer copied the asset ID into a newly allocated `String`
for every level update, then freed it after encoding. The ID was already held
in shared immutable storage elsewhere, but the supplied SBE domain type owns
its symbol string. I chose to reuse that string's capacity inside the writer
rather than change the generated types or supplied helpers.

The writer now creates one `reusable_update` before receiving batches. For each
level update, it overwrites every field, clears the symbol, and appends the new
asset ID. Clearing a string retains its capacity; a longer ID can grow the
buffer, and later updates reuse that space. The symbol bytes still get copied.
The supplied writer encodes synchronously into its own output buffer, so it
has finished reading the message before I reuse it for the next record.

I kept ownership in the existing single writer thread, so this needs no locks,
shared mutable message slots, or pool coordination. Snapshot allocations are
unchanged. A wider message pool is a separate idea I'd only pursue if profiling
justifies it. This change targets allocation churn; it doesn't remove the
synchronous chunk writes or establish an end-to-end latency improvement.

The focused readback test alternates long and short asset IDs, both sides,
different prices and timestamps, and zero quantity across batches with an
intervening snapshot. It checks every decoded field and the byte totals, so a
stale symbol suffix or another field left over from the previous update fails.
Formatting, strict all-target Clippy, and the normal test suite pass.

I checked the allocation mechanism with a temporary release-mode probe of the
exact old and new writer loops. Inputs were prepared before measurement, and
allocation calls were counted only on the writer thread. Five alternating
pairs each encoded and read back all 200,000 updates. Every pair reduced
allocation calls from 200,087 to 88: 199,999 fewer calls, replacing one symbol
allocation per update with one reusable allocation. Encoded content and byte
counts matched the independent input oracle. This was an instrumented
writer-only check on a busy host, with variable timings dominated by final
chunk flushing; I haven't established a production throughput or latency gain.

## Live operational metrics, 2026-10-26

At this stage, the collector published 14 families through a private HTTP
service. The current [metric catalog](../METRICS.md) includes the later additions
below. The operational set focuses on latency,
synchronized coverage, traffic, connectivity, decoding, observed divergence,
bottlenecks, and freshness. Prometheus calculates rates and percentiles and
retains history. Detailed stage, queue, churn, and per-book metrics stay outside
the initial contract.

Components retain exclusive ownership of their counters and coarse histogram
bins. They publish cumulative snapshots, and the background publisher prepares
cached exposition. A slow scrape cannot block book processing. Connection task
slots persist across reconnects, so counter accounting needs neither per-session
retention nor slot-generation coordination.

Exchange age uses UTC and includes clock offset. Internal encoding latency uses
a monotonic receipt instant, weighted per emitted price-change level record.
All snapshot-derived output is excluded. The two populations differ and must
not be treated as a shared denominator. SBE timestamps and schema are unchanged.

Markets count as synchronized only after every required outcome book initializes
on the current owning connection session. Book counts retain visibility into
partial market recovery. Connection purposes are named orderbook and
market_discovery. Expected and connected are overlapping aggregate counts.

Publication freshness gates readiness and the demo's live indicator. The demo
reader uses fixed Prometheus queries and retains labelled stale values after
source failure. Shutdown first disables readiness, drains output, publishes final
state, and closes HTTP within the existing deadline.

Removed the three shutdown summary emitters, numeric health log, duplicate
latency aliases, custom production histogram modules, and m5-analyze.py. Future
agents must fetch metrics or Prometheus queries and preserve windows, populations,
and evidence. Do not restore log-parsed operational statistics. Independent
correctness and scheduled-arrival experiments stay in the lab.

## Phase 21: Snapshot-only source switching for extension 1 (2026-10-26)

- **Initial interpretation:** the standby draft required every switch snapshot
  to equal the previously published book plus the updates already written.
  It therefore proposed emitting a catch-up diff before the new source's
  snapshot, and retaining a third book image for the last published state.
  This was a stronger output-continuity rule than the challenge requires.
- **Why we changed it:** while preparing the extension 1 implementation, the
  user questioned both the synthetic handoff updates and the extra book copy.
  We reread [the challenge's extension 1](https://voluntas.io/engineering-challenge).
  It requires a snapshot when changing source specifically because sockets
  can hold different books after dropped or delayed messages. It does not
  require a diff that makes their histories agree before that snapshot.
- **Decision:** relax the stronger interpretation at source switches and
  recovery after neither source was available. Emit the selected source's
  full snapshot for each outcome, then continue with that source's updates.
  The snapshot is an authoritative replacement baseline. Do not synthesize a
  cross-source catch-up diff or keep a third published-book image for it.
  Maintain two independent source books per outcome and publication metadata.
- **Verification:** a switch snapshot need not match the old baseline plus
  preceding updates. Recognize intended replacement boundaries using fixture
  controls or switch telemetry, then continue replay from the new snapshot.
  Ordinary periodic snapshots remain checked against intervening updates.
  This does not excuse unexplained differences or reconstruct intermediate
  updates missed during a connection failure.
- **Status:** recorded in [SPEC sections 5 and 11.1](../SPEC.md#11-extensions)
  before implementation; the implementation status is recorded below.

## Phase 22: Standby metrics aligned with the live service (2026-10-26)

- The standby worktree was fast-forwarded to `9400fff`, preserving its spec
  changes, the finalized live metrics service and the writer's reusable
  symbol-buffer optimization. The baseline at that point was 14 families;
  no extension implementation is included in this documentation change.
- We initially proposed only ready-standby coverage and considered socket
  session-end counts plus incidents sufficient for switch activity. The user
  asked to discuss the value first and identified switch counts by reason as
  useful. After comparing both signals, the user approved both for the spec.
- Add `collector_publisher_switches_total{reason}` for actual per-market source
  changes, with fixed remote-close, I/O-error, silence and decode-error causes.
  A socket failure and a successful market failover are different populations.
  Count adoption once, not failures, retries, outcomes, initial selection or
  recovery onto the same connection. Incidents retain identifiers and detail.
- Extend `collector_markets` with `state="standby_ready"`. Normal reset dips
  are expected; a persistent gap from synced coverage exposes incomplete
  backup initialization while published data still looks healthy. No market
  IDs, extra book-readiness family, new readiness gate or derived ratio metric.
- Preserve logical coverage once per market/outcome, both-feed input/receipt-age
  populations, and published-price-change-only encoding latency. Same-source
  authoritative refresh mismatches are not cross-replica comparisons. Both
  active connection tasks, including standby, remain publication-freshness
  requirements; normal reconnects keep their slots and cumulative counters.
- This is one new family plus one additional gauge state, for 15 planned
  families after extension 1. Drop the earlier draft's frame-gap maxima and
  PONG RTT histogram requirement. No switch-duration, stage, queue or churn
  metrics, log-parsed operational statistics, or shutdown summaries were planned.
  The accepted definitions are in [SPEC section 11.1](../SPEC.md#live-metrics-and-diagnostics).

### Reset causes and snapshot timestamp semantics

- The connection sends the specific remote-close, I/O-error, stall or decode
  cause in its ordered worker reset message. This keeps per-reason switch
  counters accurate without a side channel or inference from incident logs.
  Reconnect attempts do not overwrite the original publisher-failure cause.
- Removal snapshots use the same timestamp semantics as other snapshots:
  the latest message applied to that outcome from the publisher, including
  unchanged full-book refreshes that emitted no output. The earlier standby
  draft's "last output timestamps" wording was inconsistent with that rule.
  Keep the timestamps as metadata while sources rebuild; standby data does
  not advance them. No additional book image or timestamp policy is needed.

## Older full-book rejection, 2026-10-26

A retained six-market capture showed two full books replaying historical depth
with timestamps 8 ms below newer applied changes. Accepting them produced two
rollback and two later repair disagreements. The worker now rejects a full book
only when already synced and its timestamp is strictly below the maximum applied
depth timestamp for that outcome in the current synchronized period.

The maximum is separate from the last applied source timestamp. Older absolute
deltas still apply and can move the latter backward. An unsync keeps levels for
catch-up, but the next full book establishes a fresh baseline even if its time
predates the previous session. Rejected books change neither output metadata nor
timers. Equal timestamps remain arrival-ordered. No trades are subtracted and no
messages are sorted, buffered for ordering, or split onto new connections.

Aggregate skip counts and lag histograms distinguish intentional rejection from
accepted-book disagreements. The captured incident is a deterministic regression,
not a proof that exchange timestamps provide a complete order. See
[the validation record](experiments/older-full-book-filter-2026-10-26.md).

## Full books ahead of incremental updates, 2026-10-26

The fixed collector's 15-minute AWS run reported two accepted-book disagreements.
Tracing the retained frames shows one episode across the two outcome books:
full books already reflected a quantity reduction, while the corresponding
absolute `price_change` arrived afterward. That update carried an exchange
timestamp 5 ms later than the full books and assigned the quantities they had
already supplied. The preceding equal-timestamp update affected other levels.

This is a Polymarket feed sequencing inconsistency, not a collector correctness
problem in this episode. The observed order is consistent with delayed emission
of the incremental update, but the capture cannot establish Polymarket's internal
cause. The full books supplied useful current depth; they were not the stale
rollback seen in the earlier incident.

Decision: no further collector fix is needed for this episode. Accept the full
books and apply subsequent absolute updates normally. The disagreement counter
records a temporary difference between received full depth and locally held
levels; it does not by itself prove missing data or a collector bug. Do not
reject equal-timestamp books merely to suppress these flags. See
[the validation record](experiments/older-full-book-filter-2026-10-26.md).

## Phase 23: Hot standby implemented (2026-10-26)

Live subscriptions now use mirrored pairs with 256 markets per physical
connection by default. Each worker keeps two independent source images per
outcome and selects one publisher for the market. Connection failures invalidate
only their source. A ready peer takes over with two replacement snapshots in one
writer batch. No cross-source diff, third book image, lag detector, automatic
failback, coordinator task, or runtime compatibility flag was added.

Both connection paths send PING every 10 seconds. Book sockets reset after
30 seconds without a received frame, including while their peer is healthy.
These settings supersede the historical 60-second PING and 180-second silence
policy above. Old test-host and AWS results retain their original populations
and cannot establish the performance or failure timing of the new policy.

The older-full-book filter applies independently to each source. It compares
against that outcome's maximum applied depth timestamp in the current sync
period. Rejected books do not change levels, publication timestamps, or timers.
Accepted same-source refreshes retain ordinary mismatch accounting. Initial and
recovery baselines establish a fresh maximum; source switches never compare books.

The catalog now has 17 families and three histograms: main's 16-family baseline
with older-book skip count and lag, plus the publisher-switch counter. Workers
publish standby-ready market coverage and cumulative switches using their
existing slots. Retirement keeps counters exactly once and removes live gauges.
Both physical feeds contribute input metrics; only published price-change records
contribute internal latency. Active standby slots remain freshness requirements,
while standby coverage does not gate readiness. Saved single-source fixtures and
isolated benchmarks retain their existing behavior.

Implementation is in `src/core/worker.rs`, `src/core/subscriptions.rs`,
`src/core/book_connection.rs`, and `src/core/metrics.rs`. The current contract is
in [SPEC section 11.1](../SPEC.md#11-extensions) and [METRICS.md](../METRICS.md).
Deployment testing and comparative performance claims require separate evidence.

## Hot-standby implementation validation (2026-10-26)

The [AWS validation record](experiments/hot-standby-validation-2026-10-26.md)
contains the frozen build, 107 passing Rust tests, independent reviews, exact
captured-output checks, and full-catalog telemetry. Controlled failures verified
18 market switches and snapshot-only replacement. The full run handled 1,984
natural switches; both sockets of one busy pair also received repeated `1013`
closes, producing temporary unavailable markets. This is the accepted both-down
behavior, not a guarantee that a replica removes every slow-consumer outage.
The observed limitation did not introduce splitting, hashes, lag detection,
or additional metrics.

## Independent replica socket placement (2026-10-27)

Replace identical whole-socket groups with two disjoint subscription pools.
Keep primary placement sequential and spread replicas over the planned pool.
The least-loaded replica rule produces round-robin placement on an empty pool
and also handles later additions and freed capacity. Tie-break by connection
ID, retain assignments through reconnects, and never reshuffle existing markets.
Workers still own two source images and select publishers per market; pool
membership does not force publishing or failback. No worker API, timer,
connection-health rule, output format, or metric changes are needed.

This is a placement experiment prompted by repeated `1013` closes on both
members of a mirrored group in the earlier AWS run. It may spread correlated
hot groups, but a single hot market can still overload both copies and affect
different neighbours. Small or incremental batches can re-cluster. No reduction
in closes or latency is claimed without a new live comparison. The earlier
[validation record](experiments/hot-standby-validation-2026-10-26.md) describes
the fixed-pair implementation; it is not live validation of this placement.

### Placement follow-up on AWS (2026-10-27)

The [fifteen-minute comparison](experiments/replica-placement-comparison-2026-10-27.md)
completed cleanly with three slow-consumer closes and 228 publisher switches,
versus twelve and 1,984 previously. Failover used many independent target
connections, as intended. Five-second samples showed no broad unavailability
excursion, but thirteen synced markets lacked a ready standby at the final
scrape. Input traffic was higher; p99 latency bucket bounds were unchanged
while mean internal latency and CPU use were higher. This is one historical
comparison, not proof that placement caused fewer closes.

## Missing initial books and bounded resubscription (2026-10-27)

Some subscribed outcomes did not receive the initial full `book` needed to
initialize their local state. The collector already tracks these outcomes per
connection and requests them again with separate `unsubscribe` and `subscribe`
messages. A repeated `subscribe` alone does not request another baseline.
This recovery mechanism predates the replica-placement experiment.

The [placement comparison](experiments/replica-placement-comparison-2026-10-27.md)
ended with 36 markets without a complete publisher and 13 publishing markets
without a ready standby. The 36-market gap existed before the closes. Five
markets already lacked a ready standby, and eight more lost standby readiness
after a close. Logs contain two rounds of missing-book requests for 154 source
outcome books across 42 connections. These counts describe physical copies,
not 154 logical markets. The run did not capture enough information to establish
why the exchange did not supply the missing baselines.

The version measured in that run waited 120 seconds before its first retry,
and subsequent waits increased without a retry-count limit. Reconnect starts the initial-book tracking again. One
failed socket closed again after 117 seconds, before its first retry, and had
about 85 seconds left after its next reconnect. Its second close invalidated
496 of 512 source books, consistent with 16 still-uninitialized outcomes and
the eight additional markets without a ready standby.

The user chose a maximum of three resubscription attempts after the original
subscription. The implementation replaces the 120-second increasing waits with
five-second response windows and a three-retry limit. The policy is:

1. Keep the existing per-connection, per-outcome pending map. A full `book`,
   including an empty book, clears the pending entry. Deltas and PONGs do not.
   A market copy becomes ready only when both outcome books are initialized.
2. Give the original subscription and each retry five seconds to deliver a full
   book. The existing five-second check timer acts on expired windows, so a
   request can occur up to one check interval after its deadline. There is no
   increasing backoff. The user accepted a response window for each request
   instead of issuing all three requests before a response can arrive.
3. Request only pending outcome subscriptions. Count a completed unsubscribe
   followed by subscribe as one retry. A send failure follows ordinary socket
   recovery. Never refresh an initialized peer outcome or the other connection.
4. After the third request, accept a late full book but issue no more requests
   for that outcome in the same session. Keep the book unsynced until it arrives.
   Record once that the retry budget has been used for the still-pending
   outcomes. This does not prove that the final request failed, since its book
   can arrive later. Do not close an otherwise healthy socket.
5. Unsubscription removes pending state. A new subscription or reconnect starts
   a new baseline wait and three-retry budget. The cap is per subscription in a
   connection session, not a lifetime limit across reconnects.
6. Reuse missing-book incidents for retry attempts and emit
   `first_book_retry_budget_used` once per batch that uses its final retry. Existing logical coverage and standby-ready
   gauges show the remaining impact. No new metric family is proposed.
7. Test exact retry limits with a controlled clock, full-book arrival before and
   after exhaustion, one missing outcome, healthy-peer isolation, removal, and
   reconnect. Verify that send failures use existing ordered reset handling.
   Run the normal checks and a controlled exchange test before another AWS
   comparison using the same metric populations and observation window. The
   completed placement run used the old 120-second policy and does not validate
   this shorter, bounded recovery policy.

This change bounds resubscription attempts within each session. It cannot
guarantee that an unavailable exchange book arrives.
There is no per-market lag detector, periodic refresh of initialized books, or
socket recycling in this plan.

The updated controlled exchange test verifies the five-second threshold, three
requests per missing outcome, no requests after the cap, and initialization from
a late full book. Heartbeat and binary lifecycle fixtures now initialize their
standby books outside deliberately incomplete scenarios. All 112 enabled tests
pass, with seven opt-in tests ignored. Clippy with warnings denied, formatting,
and whitespace checks pass. This recovery change has not yet run on AWS.

### Integration with current main (2026-10-27)

The merge retains main's exchange-neutral protocol and command capture. Missing
book retries use `BookProtocol::request_books`; the Polymarket adapter still
sends unsubscribe before subscribe. Both the distributed-placement test and
main's independent relative-update and sequence-gap test remain enabled. The
generic control-frame test now checks that missing-book refreshes stop at three
requests while the connection stays healthy. The integrated suite passes all
125 enabled tests, with seven opt-in tests ignored. Clippy with warnings denied
also passes. The shorter retry policy remains untested on AWS.

## Delayed missing-book resubscription, 2026-10-27

The combined-main AWS validation still left missing publishers and standby
copies. Later fresh WebSocket subscriptions received books for three eligible
markets absent from that run's output. This does not establish why their earlier
subscriptions failed. The user requested a five-second unsubscribe-to-subscribe
gap and more retries; use ten retries per outcome per session for now.

Polymarket preparation sends unsubscribe, then the connection task schedules
subscribe at least five seconds after that send completes. Reads, heartbeat,
publication and cancellation continue during the gap. Missing outcomes are
excluded from routing while paused, preventing an in-flight book from ending
the retry or initializing a source before subscription is restored. Removal
cancels delayed work; reconnect restores desired membership and fresh budgets.
The response window starts after subscribe completes. Generic protocols without
preparation retain their direct refresh operation. Capture/replay records both
stages. This delay is an experimental recovery policy, not a documented upstream
requirement or a proven fix. Unlimited retries are not enabled. This revision
has not yet been validated on AWS.

Validation: 136 enabled tests pass; five opt-in tests remain ignored. Wire-level
tests cover the minimum delay, continued healthy-outcome reads and heartbeat,
in-flight books during the gap, removal, reconnect and cancellation. The existing
retry test exercises all ten retries and acceptance of a late empty full book.
Independent fixture replay covers both delayed subscribe and reconnect during
the gap. Clippy with warnings denied, formatting and whitespace checks pass.


## Missing-book eligibility recovery, 2026-10-27

The diagnostic run distinguished closed markets from eligible markets with no
upstream books. Connections now run ten-retry rounds separated by a 60-second
cooldown and report unresolved membership without blocking socket reads.
Discovery shares its paced lookup scheduler between admission and recovery.
Explicit ineligibility removes both recorded source copies. Unknown results
retain membership, and HTTP availability never gates local retry rounds.
Admission generations and ordered source-attachment markers cover readmission,
including subscriptions first sent after disconnected backoff. See the
[reviewed recovery design](docs/missing-book-eligibility-recovery.md).

The [AWS validation](experiments/eligibility-recovery-validation-2026-10-27.md)
confirmed targeted cleanup. Temporary raw-frame diagnostics were removed after
validation. Portable run, Prometheus analysis, and structural output-validation
tools are retained under `research/experiments/` and `examples/`. Historical
measurements refer to the frozen candidate, not subsequent cleanup.
