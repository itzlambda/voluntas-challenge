# Lifecycle WebSocket coverage and reliability

Measured on **2026-10-25, 09:18:02.662 to 10:23:02.740 UTC**, for
**3,900.078 seconds**, with three independent empty-asset WebSocket connections.
**A startup scan followed only by these lifecycle sockets is not supported by
this measurement.** New-market redundancy recovered individual socket losses,
but no socket received any resolution event, including for 401 markets with
independently recorded resolution timestamps inside the observation window.

## New-market coverage

Gamma contained **519 protocol-v1 markets created inside the window and observed
tradable during capture**. All 519 also fell inside the first 60 minutes, leaving
at least five minutes for delivery. The independent post-window creation-time
scan found no additional eligible v1 creation inside this window.

- Union of the sockets: **519/519, 100%**. None of this cohort was missed by all three.
- Socket 0: **519/519, 100%**.
- Socket 1: **510/519, 98.27%**. Nine creations were missed.
- Socket 2: **518/519, 99.81%**. One creation was missed.

Socket 1 missed IDs `6182945`, `6182946`, `6182948`, `6182950`, `6182951`,
`6182952`, `6182953`, `6182956`, and `6182958`. Socket 2 missed `6183216`.
The union covered all ten. An individual connection did not replay these
missing events after reconnecting.

Latency from Gamma `createdAt` to the first receipt on any socket, for the 519
eligible creations, was **29.853 seconds median, 100.168 seconds p95,
114.399 seconds p99, and 121.751 seconds maximum**. The minimum was
7.240 seconds. This includes creation-to-notification delay, not just
network transport. Server event timestamp to local receipt across all 1,592
new-market deliveries had a 0.051-second median and 51.875-second maximum.
Cross-system clock offsets were not independently calibrated.

## New-market precision and repeats

The sockets received **534, 525, and 533** new-market frames respectively.
Their union contained **534 distinct IDs**. No ID repeated on an individual
socket. The additional eight confirmed v1 IDs outside the 519-creation cohort
were created before capture began and notified during capture.

Of the 534 distinct event IDs:

- **527, 98.69%, were confirmed v1**. All 527 were eligible at their first Gamma
  observation and were observed eligible during capture.
- **Zero were confirmed v2**. This does not classify the seven unknown IDs as v1.
- **Seven, 1.31%, remained unknown** after explicit both-version ID batches and
  direct Gamma ID lookups. The direct lookups returned HTTP 404. They were
  `6183227`, `6183228`, `6183232`, `6183233`, `6183238`, `6183239`, and `6183242`.

No confirmed event ID was initially rejected for `enableOrderBook=false`,
`acceptingOrders=false`, or another eligibility flag. At the latest checks,
525 confirmed IDs remained eligible and two, `6182961` and `6183055`, were
closed and not accepting orders. Those later closures are not evidence that
the original additions were false positives. Unknown IDs have no established
protocol or eligibility classification.

The event's own `active` flag is insufficient for eligibility. It was false on
143 distinct event IDs, including 136 whose latest Gamma record was eligible.
Eligibility here requires `active=true`, `closed=false`, `archived=false`,
`enableOrderBook=true`, `acceptingOrders=true`, and at least two token IDs.
The analyzer uses `clobTokenIds` for v1 and `positionIds` for v2.

The whole-window rate was **0.137 distinct IDs/second**. The busiest minute
contained **142 IDs**. This run did not reproduce a sustained 3–4 events/second
or establish that v2 or ineligible markets explain the earlier short-run rate.
The saved per-minute counts expose the bursts and quiet periods.

## Resolution coverage

The paginated v1 closure listing found **660 markets with `closedTime` inside
the window**. All 660 reported `umaResolutionStatus=resolved`. Their v1 tokens
were independently found and reported `resolved=true`; the resolution endpoint
also reported `status=resolved` for all 660 conditions. There were 652 markets
with binary outcome prices at 0 and 1. The other eight did not meet that exact
price test, so outcome prices alone were not used as the resolution criterion.

**None of the three empty sockets received `market_resolved`: 0/660 for this
closure-window, confirmed-resolved cohort.** More narrowly, **401 records had
`resolved_at` timestamps inside the capture window, and coverage was 0/401,
0%, on every socket and on their union**. The other 259 resolution records
lacked that timestamp. For resolutions in the first 60 minutes, allowing the
remaining five minutes for delivery, coverage was **0/356**.

For example, the summary retains market IDs with both Gamma `closedTime` and
independent `resolved_at` values in `market_resolved.resolution_timestamp_samples`.
No closed-time-to-event lag can be measured because no resolution event arrived.
This does not prove that a notification could never arrive later. It does show
that redundant empty sockets supplied no removal signal for this cohort during
a full 65-minute observation.

The exact-resolution-time subset is drawn from the closure-window cohort.
Markets closed before the window but resolved during it were not exhaustively
scanned. Therefore 401 is a confirmed in-window resolution denominator, not an
exchange-wide enumeration of all resolutions during this hour.

## Agreement, disconnects, and shared pauses

The complete event sets and payload multiplicities were not identical, because
of the socket-specific omissions above. Unexpected closes all used code `1006`:
one on socket 0, two on socket 1, and two on socket 2. Reconnection took
1.128–1.240 seconds from close callback to the next open callback. All sockets
finally closed with code `1000` during client cleanup.

There were five gaps of at least 20 seconds with **no received frame on any
socket**, including no PONGs:

- 09:23:24.752 to 09:24:03.340 UTC: 38.588 seconds.
- 09:36:25.982 to 09:38:25.402 UTC: 119.420 seconds.
- 09:50:16.722 to 09:52:13.012 UTC: 116.290 seconds.
- 10:04:11.271 to 10:05:10.906 UTC: 59.635 seconds.
- 10:17:27.766 to 10:19:25.743 UTC: 117.977 seconds.

The largest sent-PING interval was **10.350 seconds**. Local heartbeat
scheduling continued during the shared receive pauses. This distinguishes the
pauses from a comparable local event-loop suspension, but does not identify
whether the server, a proxy, or a shared network path caused them. All three
connections shared one machine and network path. Later host and address-family
controls undermined an exchange-side attribution. These gaps are retained as
part of the [archived original-host incident](../archive/test-host-connectivity/README.md),
not as evidence of a normal Polymarket delivery cycle.

The result JSON also lists per-socket PONG gaps and lifecycle-only gaps. A long
lifecycle-only gap can reflect quiet market creation and is not, by itself,
evidence of a delivery stall. The shared receive gaps above use every inbound
frame and provide a stronger observation.

## ID ordering and catch-up

Numeric ID and creation time were not strictly monotonic. Among **1951
observed v1 records**, there were **11 adjacent creation-time inversions**
after sorting by numeric ID. The largest inversion was 12.039 ms.
For example, `6182775` was created at `09:00:20.961076Z`, while the greater
ID `6182776` was created earlier, at `09:00:20.949037Z`.

Delivery order differed much more often. On socket 0, **392 of 534 first-time
event IDs** were lower than an ID already received. For example, `6182939`
arrived before `6182938`, `6182936`, and `6182937`. Socket 1 permanently missed
nine lower IDs while subsequently receiving higher IDs.

A catch-up query restricted to IDs greater than the last WebSocket ID seen
would not recover such lower-ID omissions. This measurement also does not
establish a bound on Gamma indexing delay, ID allocation order, or visibility
of older IDs. The millisecond timestamp inversions alone do not prove large
allocation reversals, but strict creation ordering and a WebSocket-derived
high-water mark are not safe assumptions.

## Method and limits

The Node 24 capture opened three unauthenticated connections to
`wss://ws-subscriptions-clob.polymarket.com/ws/market` and sent this subscription:

```json
{"type":"market","assets_ids":[],"initial_dump":false,"custom_feature_enabled":true}
```

Each connection sent text `PING` every ten seconds. Every inbound frame was
saved with local UTC receive time and monotonic time. Sent frames, errors,
opens, and closes were also recorded. Failed sockets reconnected after one
second and sent the same subscription again. There was no anchor asset or
order-book subscription, and no collector implementation was changed.

One serialized REST loop used a unique `_research_poll` parameter on every
request, with a 600 ms request scheduler. Live polling made
**1,977 successful requests, zero HTTP errors**, averaging 0.507 requests/second.
The recorded Gamma requests never exceeded two starts in a rolling second.
The loop repeatedly fetched these heads and followed opaque `next_cursor`
values through complete pages until a page was entirely older than the start
minus two minutes:

```text
https://gamma-api.polymarket.com/markets/keyset?version=v1&closed=false&order=id&ascending=false&limit=100
https://gamma-api.polymarket.com/markets/keyset?version=v2&closed=false&order=id&ascending=false&limit=100
https://gamma-api.polymarket.com/markets/keyset?version=v1&closed=true&order=closedTime,id&ascending=false&limit=100
```

Every new-market ID entered a targeted lookup queue, batched up to 100 IDs.
The executed live script used Gamma's default v1 filter for that queue. A final
recheck explicitly used `version=v1&version=v2&closed=any`, and direct lookups
checked the seven remaining unknown IDs. The reusable capture script now
explicitly selects both versions for targeted lookups. An exact copy of the
executed capture is retained with the ignored raw files.

Post-window checks independently paginated `order=createdAt,id` for both
versions and for open and closed markets separately. Gamma rejects
`closed=any` without an identifier filter, so the initial unfiltered attempts
returned 422; the corrected 14-page traversal succeeded. The initial Python
HTTP client received 403 responses on Data API checks. A curl retry obtained
all 660 token and resolution confirmations. Failed attempts remain in the raw
records and summary, separate from the successful live poll.

The documented resolution routes used were:

```text
https://data-api.polymarket.com/v2/tokens?condition=CONDITION_ID
https://data-api.polymarket.com/v2/resolutions?condition=CONDITION_ID
```

Conditions were batched ten per request and paced with a 650 ms scheduler. The suggested `https://clob.polymarket.com/v2/tokens` host returned 404
in a preliminary probe. The working token response has separate `closed` and
`resolved` booleans and identifies v1 tokens with `module=v1_ctf`.

Coverage is relative to the observed Gamma cohort and eligibility observations.
This was not a full catalog audit, an atomic snapshot, or a proof against
arbitrarily delayed or backdated records. Temporary eligibility changes in
older markets and reopenings were not exhaustively measured. The report makes
no guarantee about future completeness or independent network failure domains.

The experiment supports using new-market events as additions that require
confirmation and deduplication. It does not support retiring REST discovery
and reconciliation after startup: resolution delivery failed this observation,
individual sockets lost events, and shared receive pauses lasted almost two
minutes despite redundancy.

## Evidence and reproduction

- [Capture script](../experiments/polymarket-lifecycle-ws.mjs)
- [Post-window ground-truth script](../experiments/polymarket-lifecycle-ws-ground-truth.py)
- [Offline analyzer](../experiments/polymarket-lifecycle-ws-analyze.py)
- [Saved results](../experiments/polymarket-lifecycle-ws-results.json)

Raw JSONL captures, the executed capture source, metadata, and diagnostic
records remain in `research/recordings/lifecycle-20261025-0920/`. Its `.gitignore`
excludes raw files and preserves only `.gitignore` for version control. Raw
files occupy about 1.2 GB. The directory name is a label; the measured start
was 09:18:02 UTC, as recorded in metadata.

From the repository root, with Node 24, Python 3, and curl installed:

```sh
cd research
node experiments/polymarket-lifecycle-ws.mjs recordings/lifecycle-repeat 3900
python3 experiments/polymarket-lifecycle-ws-ground-truth.py recordings/lifecycle-repeat
python3 experiments/polymarket-lifecycle-ws-analyze.py recordings/lifecycle-repeat experiments/polymarket-lifecycle-ws-results.json
```

Use a new output directory for each capture. Run ground-truth checks only
after capture stops, to preserve the combined REST rate bound. The analyzer
reads raw files without network access. Rerunning the experiment measures a
new live window rather than reproducing these exact counts.

## Sources

- [Market WebSocket AsyncAPI schema](https://docs.polymarket.com/asyncapi.json)
- [Market channel reference](https://docs.polymarket.com/api-reference/wss/market)
- [Gamma keyset pagination reference](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination)
- [Token lookup and resolved flags](https://docs.polymarket.com/api-reference/markets/look-up-tokens)
- [Resolution state and timestamps](https://docs.polymarket.com/api-reference/markets/get-resolution-state)
- [Earlier empty-asset observation](polymarket-empty-assets.md)
