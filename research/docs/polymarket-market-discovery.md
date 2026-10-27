# Polymarket market discovery examples

Checked: 2026-10-23. Goal: determine which outcome order books the collector maintains, adds, and retires.

Recommendation: Gamma market keyset scans plus known-market status lookups and WebSocket lifecycle notifications.

We chose `/markets/keyset` because discovery must cover every eligible market,
including markets whose parent event is inactive, archived, closed, or absent.
An exhausted `/events/keyset` walk omitted 306 markets that remained eligible
on direct recheck: 304 had open parents missing from event listings, one had
a closed parent, and one had no parent link. Fewer event pages cannot justify
those coverage gaps. The [full comparison and counterexamples](#full-coverage-check-on-2026-10-26)
record why events are not our primary discovery source.

The [offset decision](#decision-confirmed-on-2026-10-26) separately records
row-shifting omissions and the deep-offset rejection. Our coverage approach
combines complete market cursor walks with redundant new-market hints,
deduplication by market ID, REST-confirmed removals, and repeated reconciliation.

All requests below are public GET requests with no JSON body or authentication. Response examples show the wire structure, trimmed to relevant fields and one record where practical. IDs, questions, and cursors are illustrative placeholders; the CLOB count/limit describe its full observed page. Do not send the shortened asset IDs.

## 1. Gamma markets with a cursor

Discover open markets directly, then inspect each market's book and trading status.

Request:

```http
GET https://gamma-api.polymarket.com/markets/keyset?closed=false&limit=2
```

Response, reduced example:

```json
{
  "markets": [
    {
      "id": "1001",
      "question": "Will Example Co. IPO this year?",
      "version": "v1",
      "conditionId": "<CONDITION_ID>",
      "outcomes": "[\"Yes\",\"No\"]",
      "clobTokenIds": "[\"101\",\"102\"]",
      "active": true,
      "closed": false,
      "archived": false,
      "acceptingOrders": true,
      "enableOrderBook": true
    }
  ],
  "next_cursor": "<CURSOR>"
}
```

Next-page request, relative to the same service:

```http
GET /markets/keyset?closed=false&limit=2&after_cursor=<CURSOR>
```

Response is an object. Send next_cursor back as after_cursor. The cursor is omitted on the last page. Maximum limit: 100.

Advantages:

- Market-level status and outcome IDs in one listing.
- Opaque cursors avoid numeric page offsets shifting as earlier rows disappear.

Limitations:

- Every page still needs fetching; the walk is not a guaranteed atomic snapshot.
- The open-only result omits closed markets, so disappearance needs a status lookup.

Closure handling: Use closed=false for discovery. For a previously tracked market that disappears, fetch its current status by ID before retiring its books.

Live response: HTTP 200, with markets and next_cursor. Additional fields were present.

Source: [Official endpoint reference](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination).

## 2. Gamma markets with an offset

Direct market discovery through the alternative Gamma pagination interface.

Request:

```http
GET https://gamma-api.polymarket.com/markets?closed=false&limit=2&offset=0
```

Response, reduced example:

```json
[
  {
    "id": "1001",
    "question": "Will Example Co. IPO this year?",
    "version": "v1",
    "conditionId": "<CONDITION_ID>",
    "outcomes": "[\"Yes\",\"No\"]",
    "clobTokenIds": "[\"101\",\"102\"]",
    "active": true,
    "closed": false,
    "archived": false,
    "acceptingOrders": true,
    "enableOrderBook": true
  }
]
```

Next-page request, relative to the same service:

```http
GET /markets?closed=false&limit=2&offset=2
```

Response is a bare array. Advance offset by the number of returned records. Continue to exhaustion; deduplicate by market ID.

Advantages:

- Same useful market metadata as the cursor listing.
- Simple request parameters; useful if the keyset interface is unavailable.

Limitations:

- Insertions or removals before an offset can shift rows, creating omissions or duplicates.
- An empty or failed individual page is not evidence that tracked markets closed.

Closure handling: The same market status fields are available. As with keyset discovery, confirm missing markets by ID.

Live response: HTTP 200 and a bare array, without an envelope cursor.

Source: [Official endpoint reference](https://docs.polymarket.com/api-reference/markets/list-markets).

### Decision confirmed on 2026-10-26

Use `/markets/keyset` for the collector's full market scan. Live requests for
`limit=1000` returned only 100 rows on both market endpoints, so the offset
endpoint offered no page-size advantage in this check. More decisively,
`GET /markets?closed=false&limit=1&offset=100000` returned HTTP 422 with
`offset too large, use /markets/keyset for deeper pagination`.
See the [reproducible discovery probe](../experiments/polymarket-events-discovery.py).

Offset pagination also has a structural problem while the listing changes.
For example, page one of `[A, B, C, D]` returns `[A, B]`. If A closes and
leaves the open listing, the next page at offset 2 returns `[D]`, skipping C.
A cursor after B instead reaches C and D. This example is reproduced in
the [saved probe results](../experiments/polymarket-events-discovery-results.json).
Insertions before an offset can produce duplicates, and newly inserted rows
behind an already visited boundary can be missed until a later pass.

This is why we reject offset pagination for full-catalog discovery, rather
than a claim that keyset provides an atomic snapshot. A keyset walk can still
miss a new market added behind its cursor or a market whose eligibility
changes after its page was visited. Use a fixed ID order, repeat scans, retain
WebSocket new-market hints, and confirm missing markets by ID before removal.

## 3. Gamma events, then child markets

Discover markets while retaining the parent event grouping.

Request:

```http
GET https://gamma-api.polymarket.com/events/keyset?closed=false&limit=2
```

Response, reduced example:

```json
{
  "events": [
    {
      "id": "2001",
      "title": "Example Co. IPO timing",
      "closed": false,
      "markets": [
        {
          "id": "1001",
          "version": "v1",
          "conditionId": "<CONDITION_A>",
          "clobTokenIds": "[\"101\",\"102\"]",
          "active": true,
          "closed": false,
          "acceptingOrders": true,
          "enableOrderBook": true
        },
        {
          "id": "1002",
          "version": "v1",
          "conditionId": "<CONDITION_B>",
          "clobTokenIds": "[\"201\",\"202\"]",
          "active": true,
          "closed": true,
          "acceptingOrders": false,
          "enableOrderBook": true
        }
      ]
    }
  ],
  "next_cursor": "<CURSOR>"
}
```

Next-page request, relative to the same service:

```http
GET /events/keyset?closed=false&limit=2&after_cursor=<CURSOR>
```

The envelope contains events and next_cursor. Flatten events[].markets[]. Use after_cursor for the next event page.

Advantages:

- Event context and its constituent markets arrive together.
- Useful when the collector needs event-level metadata for another purpose.

Limitations:

- closed=false filters parent events; child markets still need individual status checks.
- More nesting and irrelevant event data for a book-only collector.

Closure handling: Never unsubscribe all child markets because of a parent flag alone. Evaluate every child and retain market IDs for later status lookups.

Live check: both sampled open events included closed child markets.

Source: [Official endpoint reference](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination).

### How to build a market list from events

An event is a parent grouping, such as a tournament winner question. Its
`markets` array contains the individual binary markets, such as whether each
team wins. These REST event objects are different from WebSocket messages
whose `event_type` is `new_market` or `market_resolved`.

`GET https://gamma-api.polymarket.com/events` returns an offset-paginated
array. For a full walk, use the cursor counterpart:

```http
GET https://gamma-api.polymarket.com/events/keyset?version=v1&closed=false&limit=100&order=id&ascending=false
```

For every response:

1. Iterate over every event and every item in its `markets` array.
2. Apply the collector's eligibility rule to each market, including protocol
   version, market status, condition ID, and valid outcome asset IDs. Parent
   status alone is insufficient. Use the current rule in SPEC section 7;
   do not add liquidity or volume filters.
3. Deduplicate by Gamma market ID. Parse `clobTokenIds`, a JSON-encoded array
   of decimal strings, into the outcome asset IDs used for subscriptions.
4. Pass `next_cursor` as `after_cursor`, preserving all other query filters,
   until no cursor remains. Subscribe page by page rather than waiting for
   the entire listing.

The API documents a maximum of 100 events per page and includes nested
markets in the response. One event may contain many markets. Omitted
`closed` includes open and closed events, so set the intended parent filter
explicitly. Market eligibility still needs checking within each event.
Sources: [Event keyset reference](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination),
[protocol filters](https://docs.polymarket.com/market-data/discover-markets),
and [events and markets](https://docs.polymarket.com/concepts/markets-events).

### Bounded coverage check on 2026-10-26

The saved probe fetched three pages at each end of the ID order from both
keyset listings, then independently looked up the sampled markets' parent
events with `closed=any`.

- All 600 eligible market IDs had parents, appeared in open-parent payloads,
  passed the same eligibility rule there, and had matching condition IDs
  and outcome asset IDs.
- The 300 oldest open events contained 2,872 child-market occurrences,
  including 365 closed children. Only 1,161 children matched our eligibility
  rule. The parent filter therefore cannot replace child filtering.
- This is an oldest/newest sample, not a random sample or complete-catalog
  coverage proof. The listings change between requests.

The [probe script](../experiments/polymarket-events-discovery.py) and
[results](../experiments/polymarket-events-discovery-results.json) preserve
the requests and comparison counts. Run from the repository root:

```sh
python3 research/experiments/polymarket-events-discovery.py --pages 3
```

Event discovery must revisit old parent events: a new market can be added
to an existing event without giving the parent a new ID. Scanning only newer
event IDs would miss that child. Before replacing market listings with event
listings, compare both complete catalogs and check apparent misses by market
ID. Keep direct market lookups and WebSocket hints even if events become the
bulk discovery source.

The same probe can exhaust both catalogs, retain only eligible market
identities, and recheck the status of IDs present in only one result:

```sh
python3 research/experiments/polymarket-events-discovery.py --full-comparison --requests-per-second 8 --checkpoint-dir /tmp/polymarket-event-coverage-20261026 --output research/experiments/polymarket-events-coverage-results.json
```

The two scans run sequentially. New markets and closures between them can
produce set differences even when both methods work correctly. This is a
coverage comparison, not a controlled performance benchmark.
The tool retries transport failures and writes compressed checkpoints every
100 pages and at the end of each scan. Reuse the checkpoint directory to
resume that comparison, or choose a fresh directory to start a new one.

### Full coverage check on 2026-10-26

The completed comparison ran from 10:43 to 10:55 UTC, scanning open v1
markets first and open v1 events second. Both cursor walks reached the end:

- `/markets/keyset`: 2,393 pages, 239,212 market rows, 239,114 eligible IDs.
- `/events/keyset`: 220 pages, 21,909 events, 404,579 child-market occurrences,
  239,140 eligible IDs. The children included 134,792 closed markets.
- 238,756 eligible IDs appeared in both results, with no condition-ID or
  outcome-token differences among those common IDs.
- 358 market-scan IDs were absent from the event result. Direct rechecks
  found 52 no longer eligible and 306 still eligible.
- 384 IDs appeared only in the later event result and remained eligible on
  recheck. These walks are not simultaneous, so this difference alone does
  not establish that the market-listing method loses coverage.

The event result's slightly larger total therefore concealed eligible
markets that it missed. The [complete comparison](../experiments/polymarket-events-coverage-results.json)
stores ID sets as digests, all differing IDs, and request metadata.

The [follow-up investigation](../experiments/polymarket-events-missing-market-results.json)
looked up every still-eligible missing market and its referenced parents:

- 304 markets belonged to open events absent from the event listing even
  when explicitly requested by ID with `closed=any`. Direct
  `GET /events/<EVENT_ID>` returned those events and their eligible children.
  All 137 omitted parents were inactive or archived: 15 inactive and not
  archived, 120 inactive and archived, and two active and archived. Their
  child-market flags still satisfied our eligibility rule.
- One eligible market, `1340780`, belonged to event `199537`, which was
  `closed=true`. An open-event scan necessarily omitted it, although both
  direct market and nested child flags said the market was open and eligible.
- One eligible market, `2278824`, had no parent-event link in its market
  response. Event discovery alone cannot account for that record.

Targeted checks of a listed parent and two omitted parents also tried
`include_children=true`, `active=false`, `archived=true`, both protocol
versions, and the offset `/events` endpoint. Those variants did not recover
the omitted parents. These are observed response inconsistencies, not proof
that every inactive or archived event behaves identically.

Decision: do not replace the direct market-keyset scan with event discovery
as the sole source of complete coverage. Events could supply candidate markets
before a background market-keyset audit,
but that design still needs the direct scan to fill gaps and market-ID
lookups to confirm removal. Fewer event pages do not by themselves prove a
faster full scan; the nested payload also includes irrelevant markets.

Reproduce the gap investigation after the full comparison:

```sh
python3 research/experiments/polymarket-events-discovery.py --inspect-differences research/experiments/polymarket-events-coverage-results.json --output research/experiments/polymarket-events-missing-market-results.json
```

## 4. CLOB simplified markets

Read outcome identifiers and basic market status directly from the CLOB service.

Request:

```http
GET https://clob.polymarket.com/simplified-markets
```

Response, reduced example:

```json
{
  "data": [
    {
      "condition_id": "<CONDITION_ID>",
      "version": "v1",
      "tokens": [
        {
          "token_id": "101",
          "outcome": "Yes"
        },
        {
          "token_id": "102",
          "outcome": "No"
        }
      ],
      "active": true,
      "closed": true,
      "archived": false,
      "accepting_orders": false
    }
  ],
  "next_cursor": "<CURSOR>",
  "limit": 1000,
  "count": 1000
}
```

Next-page request, relative to the same service:

```http
GET /simplified-markets?next_cursor=<CURSOR>
```

The envelope contains data, next_cursor, limit, and count. The next request uses next_cursor, not Gamma's after_cursor.

Advantages:

- tokens[] already pairs each token_id with its outcome label.
- Basic status fields accompany the IDs, without Gamma's larger metadata object.

Limitations:

- The documented query exposes a cursor, not a closed-only filter; old markets can dominate the scan.
- No Gamma market ID or question in this simplified shape; metadata joins need condition_id.
- Current v2 identifier coverage and the cursor's terminal convention need validation before adopting it.

Closure handling: Inspect each row's closed and accepting_orders fields. Never treat active=true alone as permission to maintain a live book.

Observed first page: 1,000 records, including 941 closed markets and no rows with both closed=false and accepting_orders=true. This is one page, not the entire catalog.

Source: [Official endpoint reference](https://docs.polymarket.com/api-reference/markets/get-simplified-markets).

## Asset identifiers

For Gamma v1 markets, JSON-decode clobTokenIds and outcomes. For v2, use positionIds and the corresponding outcomes. Select by version even when both ID fields are present. The subscription key is an outcome asset ID, not Gamma id or conditionId.

Source: [Protocol V2 identifiers](https://docs.polymarket.com/migrate/polymarket-v2/api-integrations).

## Confirming closure

An open-only scan stops returning markets once they close. The absence of a market alone is not closure evidence. A follow-up lookup includes both open and closed states:

```http
GET https://gamma-api.polymarket.com/markets?closed=any&id=1001&id=1002
```

Example response, independent of ordering:

```json
[
  {
    "id": "1001",
    "active": true,
    "closed": false,
    "acceptingOrders": true,
    "enableOrderBook": true
  },
  {
    "id": "1002",
    "active": true,
    "closed": true,
    "acceptingOrders": false,
    "enableOrderBook": true
  }
]
```

The IDs in this example are placeholders. A live batch lookup using real IDs returned both an open and a closed market. A direct lookup of the closed market also returned HTTP 200.

- **Eligible new market:** Subscribe to its outcome assets and wait for initial book snapshots.
- **closed=true or archived=true:** Retire that market's books, unsubscribe its assets on all assigned sockets, flush pending output, and release book memory.
- **acceptingOrders=false, closed=false:** Keep its registry entry and recheck. This flag alone does not establish permanent closure; an existing subscription need not be torn down.
- **Missing from one scan, timeout, or 404:** Treat status as unknown. Confirm by market ID; preserve the previous catalog on scan failure.
- **No recent trades or endDate passed:** Neither condition alone proves that trading has closed.

The end of trading and resolution are distinct. Waiting only for market_resolved can retain books after trading has stopped.

Sources: [Status fields](https://docs.polymarket.com/market-data/market-details), [Known-market lookup](https://docs.polymarket.com/api-reference/markets/get-market-by-id), [Listing filters](https://docs.polymarket.com/api-reference/markets/list-markets), [Lifecycle events](https://docs.polymarket.com/api-reference/wss/market).

## Proposed reconciliation policy

1. Build a registry keyed by Gamma market ID, retaining condition ID, protocol version, and every outcome asset ID.

2. Discover open markets through Gamma /markets/keyset and evaluate each market's active, closed, archived, acceptingOrders, and enableOrderBook fields. Only validated asset IDs become new subscriptions.

3. Repeat the full cursor walk periodically. Deduplicate market IDs. A completed scan identifies markets that need follow-up status checks; it is not an atomic snapshot.

4. Refresh known IDs with closed=any so closed markets remain visible. Send at most 100 market IDs per lookup batch. Confirm missing or conflicting records individually with GET /markets/<ID>.

5. Retire books on explicit closed or archived status. Keep temporarily unavailable markets in the registry. New scans can rediscover a market that becomes eligible again.

6. Use new_market and market_resolved WebSocket events with custom_feature_enabled=true to accelerate checks. Continue REST reconciliation because disconnects can lose notifications and a market can close before resolution.

This is a proposed collector policy. A periodic scan's interval sets the fallback discovery and retirement delay. WebSocket lifecycle coverage and message ordering still need live validation. Listing markets does not itself synchronize or repair an order book.
