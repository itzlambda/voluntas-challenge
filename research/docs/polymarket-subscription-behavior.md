# Market WebSocket subscription behavior

Tested on 2026-10-25. The main observation ran from 14:55:17.655 to
15:00:19.396 UTC. Supplemental captures expanded book-format coverage and
repeated transitions on an asset selected by observed update traffic. The format
supplement ran from 14:58:29.313 to 14:59:00.970 UTC. The flow supplement ran
from 14:58:58.769 to 14:59:19.177 UTC.

## Method and evidence

The experiment used Node 24's WebSocket client against
`wss://ws-subscriptions-clob.polymarket.com/ws/market`, without credentials.
Initial messages requested `type: "market"`, `initial_dump: true`, and explicit
asset IDs. Later messages used `operation: "subscribe"` or `"unsubscribe"`.
Normal sockets sent text `PING` every ten seconds. Three sockets sent no PING.

Each discovery pass fetched six 100-market keyset pages ordered by descending
ID and six ordered by descending `volume24hr`, using `version=v1&closed=false`.
Requests were sequential with at least 650 ms between responses and the next
request, keeping REST below two requests per second. Eligibility required
`active`, not closed, not archived, `acceptingOrders`, and `enableOrderBook`.
Asset IDs came from the JSON-encoded `clobTokenIds` field.
The main pass found 1,200 eligible markets. The busy group used the six highest
reported 24-hour volumes in this sample. The quiet group used the six lowest.
Example market IDs are `601826` and `6199989`, respectively.

Every received text frame was saved before decoding, with UTC `received_at`,
monotonic milliseconds, and a connection identifier. Sent messages, trial
boundaries, errors, and closes were also saved. A trial waited 1.5 seconds after
its initial subscription, performed its transition, and observed for 2.5 seconds.
Latency measures local send-record time to local receive-record time, including
network transit and scheduling. A post-subscribe book is an observed snapshot,
not a protocol acknowledgement or proof of snapshot generation time.

The main run performed five trials per case in each group. The flow case waited
up to 15 seconds for a price change before the transition. Quiet sockets never
received one. Four of five busy sockets did. A supplemental run screened live
activity for five seconds, selected the most active observed asset, then performed
five more flow trials. All five observed price changes before their transitions.
These are repeated trials, not five independent market samples.

[Capture script](../experiments/polymarket-subscription-behavior.mjs),
[analyzer](../experiments/polymarket-subscription-behavior-analyze.py), and
[results with per-trial IDs and counts](../experiments/polymarket-subscription-behavior-results.json)
are saved. Raw directories are listed in the results and ignored except for their
`.gitignore` files. They include discovery responses and exact received frames.
Raw recordings must remain available locally to rerun the analysis. An independent
raw-frame count reconciled all 8,339 books and 55 subscription-operation trials. All 67 sockets
have recorded close events. Node syntax checks, Python compilation, and
`git diff --check` passed.

## 1. A new dynamic subscription delivered a book

All ten trials received a book for the new asset. Busy trials had a median
latency of 28.60 ms and a range of 22.48 to 72.26 ms. Quiet trials had a median
of 29.54 ms and a range of 22.04 to 55.48 ms.
The asset was absent from that connection's initial subscription.

## 2. A duplicate subscription did not refresh the book

Zero of ten trials received a book during the 2.5-second window after subscribing
again to an already subscribed asset. This includes five busy and five quiet
trials. This establishes no refresh within the measured window, not a guarantee
that a later ordinary book event cannot arrive.

## 3. Unsubscribe followed by subscribe delivered a book

All 35 transition trials received a post-subscribe book.

- Back to back: five of five busy trials, median 25.52 ms, range 23.36 to
  45.95 ms. Five of five quiet trials, median 34.21 ms, range 26.53 to 37.07 ms.
- One-second gap: five of five busy trials, median 23.50 ms, range 21.82 to
  24.84 ms. Five of five quiet trials, median 25.47 ms, range 21.06 to 31.92 ms.
- Flow attempts: ten of ten busy trials including the supplement, median
  25.54 ms, range 23.31 to 80.27 ms. Five of five quiet trials, median
  29.55 ms, range 24.28 to 45.60 ms. Nine busy trials observed prior updates.
  The five quiet trials establish the transition during inactivity only.

Messages for the target asset arrived after unsubscribe and before the first
new book in six busy trials. One back-to-back trial received a price change.
One original flow trial received a last-trade event. Four supplemental flow
trials received 1, 5, 2, and 3 price-change events, respectively.
All these events arrived after the subsequent subscribe send. None arrived in
the measured interval between the two sends. No gap trial had intervening events.

These payloads contain no subscription generation or request ID. Their timestamps
and hashes cannot identify whether they belong to the old or new subscription.
The experiment does not establish an unsubscribe processing barrier or an atomic
snapshot boundary. The flow condition means an update was observed immediately
before the transition logic ran; it does not prove uninterrupted server activity.

## 4. Empty books were delivered

The discovery sample contained no eligible `acceptingOrders=false` candidates
with order books enabled. The fallback subscribed to both outcomes of ten recent,
accepting markets on five fresh connections. Each connection received 20 books,
and all 100 books had `bids: []` and `asks: []`.
Thus an empty book can produce an explicit snapshot. The behavior of a market
that is not yet accepting orders remains unestablished. The result file retains
the 20 sampled asset IDs in every candidate trial.

## 5. No-PING connections ended after about 125 seconds

All three no-PING sockets received their initial frame and continued reading.
They closed 125.043, 125.053, and 125.053 seconds after opening. All reported
local close code `1006` and an empty reason, with an error callback.
The main run retained a five-minute observation limit even after these closes.

Code `1006` indicates abnormal closure without a received WebSocket close frame.
It is not a server-sent close code. This is consistent with an idle transport
limit but does not identify the server or intermediary responsible.
The heartbeat-enabled bulk socket also failed abnormally after 80.884 seconds,
and two duplicate-trial sockets reported abnormal closes after client cleanup
had been requested. Six error callbacks occurred across the captures.
These failures limit attribution of the no-PING result to heartbeat policy.
No trial reconnects automatically or silently substitutes another connection.

## 6. Full-book format and ordering

The analyzer checked 8,339 books covering 1,800 assets in 900 markets, alongside
101,691 price-change events. It compared decimal prices exactly and counted
fractional digits in their original strings.

- Bids were ascending in all 8,339 books. Asks were descending in all 8,339.
  There were 3,156 books with at least two levels per side. Empty or single-level
  sides also satisfy both sort directions and do not independently establish order.
- Across 137,904 bid levels and 138,878 ask levels, neither side had duplicate
  prices or zero sizes. No price or size string had more than six decimal places.
- Both outcome books were not always in the same frame. On feeds subscribed to
  both outcomes, 3,598 market occurrences had two books in one frame and 934
  had only one. Separate frames do not establish a simultaneous paired update.
- Zero books had a timestamp older than the greatest timestamp of any earlier
  received price change for that asset on that connection. Equal timestamps were
  allowed. This checks the proposed rollback condition; it does not establish
  global ordering, sequence completeness, or a guarantee against future rollback.

These observations cover the sampled markets and time windows. They do not
establish protocol-wide precision limits, sorting guarantees, or snapshot pairing.

## Reproduction

Run from the repository root with Node 24 and Python 3. Use fresh output directory
names because captures refuse to overwrite a directory.

```sh
node research/experiments/polymarket-subscription-behavior.mjs research/recordings/subscription-behavior-20261025
node research/experiments/polymarket-subscription-behavior.mjs research/recordings/subscription-behavior-format-20261025 --format-only
node research/experiments/polymarket-subscription-behavior.mjs research/recordings/subscription-behavior-flow-20261025 --flow-only
python3 research/experiments/polymarket-subscription-behavior-analyze.py research/recordings/subscription-behavior-20261025 research/experiments/polymarket-subscription-behavior-results.json research/recordings/subscription-behavior-format-20261025 research/recordings/subscription-behavior-flow-20261025
```

The format supplement opens two bulk feeds for 30 seconds. The flow supplement
selects one asset from observed activity and repeats five transitions, waiting up
to 30 seconds for an update each time. Discovery and market activity can change
the resulting counts. The scripts close their sockets at the end of each run.
