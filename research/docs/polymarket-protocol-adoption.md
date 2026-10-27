# Polymarket protocol adoption snapshot

Checked 2026-10-25. Protocol v1 dominates the currently tradable market count
and the available reported trading-volume fields. Protocol v2 is in a limited
production rollout.

## Comparable tradable-market figures

The v1 data comes from the completed seed collected at 01:32–01:37 UTC. The v2
data comes from a full explicit `version=v2` scan at 01:54 UTC. Both sides use
the same active/open/unarchived/accepting-orders/order-book-enabled predicate.

- v1: 253,740 tradable markets; available `volume24hr` fields sum to about
  $70.09 million; available lifetime-volume fields sum to about $5.02 billion.
- v2: 123 tradable markets; available `volume24hr` fields sum to $232;
  available lifetime-volume fields sum to about $19,886.
- Positive reported 24-hour volume: 20,107 v1 markets and 6 v2 markets.

These are sums for markets that were tradable when read, excluding closed
markets. They are not the entire platform's last-24-hour volume or its lifetime
volume. No volume is added from event objects, avoiding event/market duplication.
Lifetime volume uses `volumeNum`, falling back to `volume` if absent.

Missing values are not established zeros. In v1, 233,627 records lacked
`volume24hr` and 206,213 lacked both lifetime-volume fields. In the v2 tradable
set, the corresponding missing counts were 117 and 113. Further, 709 v1 rows
reported 24-hour volume greater than lifetime volume. These inconsistencies
prevent treating the sums as audited exact turnover or calculating an exact
protocol-wide market share from them.

## Historical v2 inventory

The v2 scan separately exhausted open and closed cursors: 123 open and 4,427
closed markets, or 4,550 records total. All open v2 records in this scan passed
the eligibility predicate. The scan made 47 successful requests with no errors
and checked distinct IDs and requested protocol/status on each page.

Reported lifetime volume across these open and closed v2 records sums to about
$40,904; their reported 24-hour fields sum to $385. This broader v2 sum should
not be compared directly with v1's open-market-only sum. A complete historical
v1 inventory was not scanned for this question.

The earliest `createdAt` observed among the v2 closed records was October 13,
and among the open records October 19. Those are catalog observations, not the
protocol's announcement or first-ever trade date.

## Rollout history and coexistence

Reporting dated October 6 describes the protocol lead's October 5 announcement
and start of a limited live canary rollout. The stated canary window ran through
October 30, with a tentative November 2 switch for new markets. As of this
snapshot, November 2 is a future target, not a completed migration.

Sources: [announcement reporting](https://www.theblock.co/news/business/2026-10-06-polymarket-rolls-out-v2-upgrade-with-single-erc-1155-positions-contract-pusd-collateral-417804),
[original announcement link](https://x.com/0xrajath/status/2107122541219205344).
The X post could not be fetched directly; the dated timeline here relies on
the reporting, rather than claiming direct access to that post.

The official migration guide states that existing CTF holdings are not
converted. Existing balances remain in their original ledger, so integrations
retain CTF support alongside PositionManager. Coexistence preserves existing
positions while newer markets adopt the new contracts.

Source: [official protocol migration overview](https://docs.polymarket.com/migrate/polymarket-v2/overview.md).

For our collector, v1 supplies nearly all observed workload. After this review,
the user chose a v1-only scope, and the seed explicitly selects `version=v1`.
Both-protocol coverage is deferred; its query semantics remain recorded in the
[correctness review](polymarket-lifecycle-correctness.md).

## Reproduction

From the repository root, using the existing saved seed:

```sh
python3 research/experiments/polymarket-protocol-adoption.py data/markets.json
```

This reads the seed and queries both v2 status listings. It writes the
[machine-readable aggregation](../experiments/polymarket-protocol-adoption-results.json).
Rerunning updates that result file; live values can change.
