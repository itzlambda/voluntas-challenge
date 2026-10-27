# Five-minute trade and depth capture

On 2026-10-26, all 380 repeated full books agreed with depth reconstructed
from the initial books and subsequent absolute price changes, without applying
trade notifications or corrections from repeated books. This sample does not
show that ignoring trade notifications caused the earlier book disagreements.
The [results](trade-depth-coverage-results-2026-10-26.json) contain counts,
capture hashes, and source fingerprints. Raw messages and scripts are retained
privately.

## Collection

A Python client selected three eligible binary markets with the highest reported
24-hour volume among Gamma's top 100 results. Each market used its own WebSocket
and subscribed to both outcome assets. The client recorded every received
application message, array order, a per-socket sequence number, and wall-clock
and monotonic dequeue times. It did not reconnect, filter timestamps, or
subtract trade quantities.

Each subscription lasted 300 seconds, approximately 21:18:37–21:23:37 UTC.
Initial books arrived 12–17 ms after subscription. Observed initialized spans
ended at each socket's last received frame, about 299.79–299.94 seconds after
initialization. Each socket sent 29 application PINGs and received 29 PONGs.
All three completed normally. Local file hashes, byte counts, frame sequence
numbers, event counts, subscription payloads, and completion records were checked.
These checks do not establish complete upstream delivery.

The anonymized market labels cover two football award markets and one US
interest-rate decision market:

- Market A: 92 trade notifications and 184 repeated books.
- Market B: 83 trade notifications and 166 repeated books.
- Market C: 15 trade notifications and 30 repeated books.

The capture contains 7,564 received application messages, 190 trade
notifications, 6 initial books, 380 repeated books, and 6,904 price-change events.

## Reconstruction

Prices and quantities were parsed exactly into integer millionths. Absolute
updates assign quantities, and zero removes a level. Every repeat book was
compared before replacing state. A second model used only the initial baseline
and price changes, ignoring all subsequent books. Both models ignored trades.
Events remained in received order, including entries within arrays.

Both models matched every repeated book across both full sides. There were
zero disagreeing levels and zero repeated books older than the per-asset
exchange-timestamp maximum. This run did not reproduce the earlier rollback.

## Individual trade candidates

The analysis also searched for same-asset, same-price reductions matching the
trade size on the side consumed by the taker. The official
[trade schema](https://docs.polymarket.com/asyncapi.json) defines trade side from
the taker's perspective. The search used a one-second window on either side of
application dequeue time, not wire-arrival time.

Of 187 trade notifications outside the boundary exclusion window, 69 had one
compatible reduction, 12 had multiple candidates, and 106 had none. Candidates
can be shared by multiple notifications. They do not establish causal matches.
An unmatched notification does not establish a missing delta: aggregation,
intervening changes, quantity precision, publication timing, and the search
window can prevent an individual numerical match. For example, Market A
reported a trade size of 5.537635 at price 0.186, while a preceding update at
that same asset, side, and price reduced depth by 5.54 about 18 ms earlier.
These quantities are close but do not pass an exact-size match. Proximity
does not establish that they describe the same execution.
The stronger observation is that both complete
depth models still agreed with all 380 full-book checkpoints.

This is a short observation of three markets. It does not establish universal
trade coverage, behavior between checkpoints, or the cause of the earlier 18 or
199 disagreement occurrences. Timestamp filtering remains a deferred
investigation.
