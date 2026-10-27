# Empty-asset market WebSocket subscriptions

Tested on 2026-10-24. **An empty asset list with custom features enabled received
`new_market` events without any order-book data.** This provides an observed
way to listen for new-market hints before discovering or subscribing to books.
It is not a documented guarantee of a complete global lifecycle feed.

## Subscription

Connect to:

```text
wss://ws-subscriptions-clob.polymarket.com/ws/market
```

Send:

```json
{
  "type": "market",
  "assets_ids": [],
  "initial_dump": false,
  "custom_feature_enabled": true
}
```

Send the text frame `PING` every ten seconds. The server replies with `PONG`.
No credentials were used. Omitting `initial_dump` also worked with an empty
asset list. Subscribing to an asset and then unsubscribing the final asset also
left the connection open and receiving new-market events.

## Paired observations

Each run used five concurrent sockets: empty with snapshots disabled, empty
with the snapshot default, empty with custom features disabled, one subscribed
control asset, and one asset unsubscribed immediately after its initial book.
The observation began after all sockets opened and a three-second warmup.

The first observation lasted **120.002 seconds**. Each enabled empty feed
received **332 distinct new-market events**. The socket emptied by unsubscribing
received the same 332 events. Their complete payloads and occurrence counts
matched when canonicalized offline. Empty subscriptions received no book,
price-change, best-price, or trade messages. The feature-disabled empty socket
received only heartbeat replies.

The first control used a high-volume market. It received 58 new-market events
before an unexpected code-`1006` disconnect, with an error callback. All 58
were present on the enabled empty feeds. This failed control is retained and
does not support a complete-window comparison or an explanation of the failure.
The empty feeds remained open and were closed by the client at the end.

A second observation used a quieter subscribed market and lasted **60.002
seconds**. The subscribed control, both enabled empty feeds, and the socket
emptied by unsubscribing each received **263 distinct new-market events** with
matching complete payloads and multiplicities. The feature-disabled empty feed
again received no JSON events. All five sockets stayed connected, answered
heartbeats, and closed cleanly with code `1000` during cleanup.

No `market_resolved` event appeared in either observation. These runs establish
delivery of `new_market` with zero assets, not global resolution coverage or
delivery of every possible metadata event. They do not establish exact ordering,
long-term reliability, low latency, or complete coverage of all exchange creations.
An event can also precede a usable REST market record, so eligibility checks
still need retries and reconciliation.

## Design implication

A single empty-asset connection can supply prompt new-market hints without
subscribing to an arbitrary anchor book. The public documentation enables
lifecycle events through `custom_feature_enabled`, but does not explicitly
promise this empty-asset mode as a supported metadata-only service. Keep REST
reconciliation as the source of confirmed trading eligibility and closure.
There is no documented `market_closed` or `acceptingOrders` change notification
on the checked market channel. A new-market hint is not proof that trading is
already available.

Sources: [market-channel specification](https://docs.polymarket.com/api-reference/wss/market),
[real-time event documentation](https://docs.polymarket.com/market-data/realtime-data).

## Evidence

- [First observation](../experiments/polymarket-empty-assets-results.json)
- [Repeat observation](../experiments/polymarket-empty-assets-repeat-results.json)
- [Independent raw-frame verification](../experiments/polymarket-empty-assets-verification.json)

Raw frames, sent frames, connection events, and the exact executed scripts are
Git-ignored in `research/recordings/empty-assets-20261024/` and
`research/recordings/empty-assets-20261024-repeat/`. The shareable results retain
subscriptions, event identities and hashes, counts, errors, closes, and cleanup
state. Verification reread every raw frame, reconciled counters, and compared
canonical full lifecycle payloads while preserving occurrence multiplicity.
All ten experimental sockets were closed. The challenge scaffold was unchanged.
