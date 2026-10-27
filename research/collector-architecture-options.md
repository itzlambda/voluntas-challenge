# Collector architecture options

Design alternatives considered on 2026-10-25. The collector uses fixed book
workers; [SPEC.md](../SPEC.md) defines the implemented behavior.

## Goals and constraints

The collector discovers tradable Polymarket markets, maintains both outcome
books, and writes state changes as binary SBE records to stdout. It also writes
a full snapshot of each book every minute. Discovery adds newly tradable
markets and retires markets that stop trading.

Plan for roughly 200,000 markets, with many markets on each WebSocket. This is
a sizing target, not demonstrated processing capacity. Activity, depth,
initialization bursts, and output bandwidth determine actual load.

The comparison considered redundant feeds, source switching, subscription
health monitoring, connection load balancing, and adapters for other exchanges.
Redundant feeds, source switching, and the shared protocol boundary are now
implemented.

## 1. One central processing owner

All WebSocket tasks send events to one worker. That worker owns every book,
handles market lifecycle and source selection, and produces output records.

Ownership and ordering are straightforward. Snapshots and source switches
happen alongside book updates in one execution path.

All book processing shares one CPU execution path. A busy market or expensive
checkpoint can delay unrelated markets. This is a good choice when measured
load fits comfortably on one worker. It can also be the initial configuration
of option 4.

## 2. Each WebSocket owns its books

Each connection task receives messages, maintains its assigned books, and
produces output independently. Parallel CPU execution requires multiple
execution threads. Multiple asynchronous tasks on one thread do not provide it.

The processing path is direct and can avoid a queue between transport and book
processing. Connections with disjoint subscriptions can work independently.

Book ownership follows the connection assignment. Moving a market to another
socket requires a handoff. Redundant connections produce competing copies of
the same book, so a publication authority must select a source and reject stale
output. Expensive book work can also delay socket reads when both share a task.

This fits stable, disjoint connection assignments. The extensions remain
possible, but require coordination between connection owners.

## 3. One independent actor per market

Each market has its own mailbox and state machine. Connection tasks route
events to it. The actor owns both outcome books and can later maintain separate
copies for redundant sources.

Ownership follows the domain. Market lifecycle, source selection, and switching
stay within one actor. Connection replacement does not move market state.

Hundreds of thousands of actors introduce mailboxes and scheduling overhead.
Per-market timers and buffers can add memory costs. Actors share execution
threads, so actual parallelism and fairness depend on the scheduler.

This fits systems where independent market scheduling is worth the runtime
overhead. Its performance relative to grouped workers needs measurement.

## 4. A fixed number of workers, each owning many markets

Assign each market to one worker. Connection tasks route its events to that
worker regardless of which socket delivered them. Each worker processes its
markets sequentially. Different workers run on separate execution threads.

Each market retains an independent state machine, including both outcome books.
The worker can later own separate source copies and select one for publication.
Replacing or rebalancing sockets does not move authoritative market state.

This provides parallel processing with fewer scheduling units and mailboxes
than one actor per market. A busy market can delay other markets on its worker,
and uneven activity can create imbalance. Moving markets between workers later
requires an explicit handoff. Dynamic redistribution can wait until necessary.

This is the current recommendation. Keep the worker count configurable and
choose it through measurement. One worker is a valid starting configuration,
not a capacity claim for the target workload.

## Decisions shared by all four options

One final writer owns stdout and preserves complete record boundaries and the
supplied SBE framing. Encoding can happen in that writer or in private worker
buffers. Workers must not write concurrently into a shared encoder buffer.
Preserve each book's output order. Unrelated books need no global exchange order.

For redundant feeds, reconstruct each source independently and select one
publisher per market. On a switch, publish a full snapshot of the new source's
current local book before publishing its subsequent deltas. Reject late output
from the old source. A standby must have a valid baseline before promotion.
The snapshot restores known state but cannot recover missed historical updates.
See the [redundancy findings](docs/polymarket-redundancy.md) for why timestamps
and hashes do not provide a safe cross-source merge key.

Bound queues and pending snapshots by memory as well as item count. Sustained
overload must cause an explicit failure or recovery policy. Additional workers
cannot fix insufficient stdout bandwidth. Capture periodic snapshots at the
state owner and keep them before subsequent deltas for the same book.

Event sourcing is a separate choice. Any option can persist incoming events
and lifecycle controls to a durable replayable log. That adds storage and
recovery requirements. The challenge's derived SBE output does not require
such an input log.
