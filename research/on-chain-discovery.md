# Future on-chain market discovery

Deferred improvement. The current discovery design continues to use Polymarket
HTTP APIs and WebSockets. No blockchain client, RPC service, contract ABI,
wallet, or on-chain subscription is required by the current implementation plan.

Revisit this improvement only if new-market detection through the Polymarket
APIs and WebSockets is too slow for a future requirement. On-chain activity may
provide earlier hints about upcoming markets. Any latency advantage must be
measured against the existing API-only approach before adoption.

On-chain hints would still require API confirmation of market metadata and
order-taking status. Condition preparation does not prove that trading has
started, and native Protocol V2 binary and negative-risk identifiers have no
preparation event. See the [official on-chain indexing documentation](https://docs.polymarket.com/resources/onchain-position-data.md).

Research must establish:

- Which chains, protocol versions, deployed contracts, and verified event ABIs
  cover the regular markets in scope.
- The distinction between question creation, condition preparation, token or
  position registration, exchange listing, and order-taking activation.
- How contract identifiers map to Gamma market IDs, condition IDs, outcome
  asset IDs, and event metadata.
- Which trading-state changes are visible on-chain, especially whether trading
  closure can be detected independently of outcome resolution.
- Log backfill, reconnect recovery, duplicate handling, reorgs, confirmation
  policy, durable checkpoints, and races with delayed API indexing.
- How an optional chain observer would supply discovery hints while the API
  remains responsible for confirming exchange trading eligibility.

The work is research and design only. Findings must distinguish verified
contract behavior from assumptions and include primary-source references.
