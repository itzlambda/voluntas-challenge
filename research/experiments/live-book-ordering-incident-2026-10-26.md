# Live full-book ordering incident

An online Python verifier captured four book disagreements in one ordering
episode on 2026-10-26. Two full books restored earlier state after newer price
changes had arrived. Another pair of books corrected the remaining reverted
levels about 0.4 seconds later. Ignored trade notifications are not needed to
explain these four disagreements.

The [results](live-book-ordering-results-2026-10-26.json) include source and raw
file hashes. Raw messages, complete incident books, and analysis scripts remain
private. No production behavior changed.

## Capture and checks

Six high-volume binary markets used separate Python WebSocket subscriptions,
with both outcomes subscribed. The verifier applied absolute price changes and
compared each full book before replacing state. The first mismatch arrived at
21:43:09 UTC, about three minutes after subscription. Capture continued for
another minute and then stopped normally.

The run compared 994 repeated books and recorded four disagreements, all on
one market's two outcomes. Each socket sent 26 application PINGs and received
26 PONGs. File hashes, frame sequences, event counts, terminal records, and
complete held and incoming books were checked. A separate flat-map Decimal
reconstruction matched all four live incidents and their previous-input
provenance. These checks establish local capture and reconstruction consistency,
not upstream completeness.

## What arrived

For one outcome, in receive order:

1. Frame 2194 carried a price change stamped 21:43:08.999 UTC.
2. Frames 2195–2198 updated four other levels, stamped
   21:43:09.005–21:43:09.007.
3. Frame 2199 carried a full book stamped 21:43:08.999. Its complete depth
   matched the earlier reconstructed state at frame 2194, and its advertised
   hash also matched that earlier update. Applying the book undid all four
   newer level changes.
4. Frame 2216 carried a book stamped 21:43:09.407. It corrected two levels
   still carrying the values restored by frame 2199. The other two levels had
   received further absolute updates in the meantime.

The complementary outcome followed the same pattern at frames 2200 and 2217.
That produces four disagreements: two early rollbacks and two later repairs.
It does not establish four independent missing-message incidents.

At the 0.062 bid in the first outcome, the published quantity would go from
2636.37 to 2627.25 on a delta, back to 2636.37 on the earlier-state full book,
and finally to 2627.25 on the next full book. No update to that level arrived
between the two books. The final books exactly matched the independent model
that kept the initial baseline and applied only price changes.

## Offline policy experiment

An offline replay rejected strictly older repeated full books per initialized
outcome, using the maximum accepted exchange timestamp. It skipped two books
and accepted the other 992 repeated books with zero disagreements. Deltas were
not filtered. This result supports the proposed investigation for this capture;
it does not prove that timestamp rejection is universally safe, particularly
across equal timestamps or reconnect baselines.

The original verifier completed before a guarded switch to a safety revision
could run. That revision adds a missing-PONG deadline and includes incident
files in its byte budget, but it was never launched. This capture's heartbeat
history and actual evidence size were checked directly.

All subscriptions and the temporary status monitor stopped after the incident.
The earlier 18 and 199 disagreement occurrences have not been individually
attributed to this mechanism.
