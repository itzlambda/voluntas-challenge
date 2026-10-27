# Eligibility checks after missing-book recovery

Status: implemented after two design-review rounds, 2026-10-27. See the [AWS validation report](../experiments/eligibility-recovery-validation-2026-10-27.md). Timing defaults are policy choices, not guarantees established by one live experiment.

## Purpose

A market that remains without a baseline after recovery receives a fresh eligibility check. Discovery removes a confirmed ineligible market from both physical feeds. An eligible market with unavailable books remains desired and receives paced recovery.

The [missing-book investigation](../experiments/missing-book-diagnosis-2026-10-27.md) found both cases. Some markets had closed after their scan observations. Two others still met the metadata eligibility rule but supplied no books on fresh independent WebSocket subscriptions. More retries alone cannot distinguish these cases.

This proposal covers first baselines and baselines lost after a source reset. It does not detect stale books on a source that remains initialized. Socket health, placement, publisher selection, snapshot handoffs, and output encoding retain their existing contracts.

## Ownership

| Owner | Responsibility |
| --- | --- |
| `BookConnection` | Own local retry rounds, cooldowns, continuous missing duration, and current source session. Report unresolved recovery and recovery completion. |
| `Registry` | Own logical membership, admission generations, deduplicated eligibility work, and reconciliation protection. Decide whether a lookup can remove a market. |
| `Discovery` | Schedule asynchronous batched lookups, process results, and dispatch registry actions. |
| `SubscriptionManager` | Record both actual source assignments. Detach one market from both sources and carry membership generations on commands. |
| `Worker` | Own source books, publisher choice, and logical coverage. Apply market removal through the existing ordered message path. |

Connections never make Gamma requests or decide market eligibility. Discovery never grants retry rounds. Recovery continues locally when Gamma is unavailable. A missing standby can trigger a check even when the publisher remains healthy. Confirmed ineligibility removes that market even if its publisher still has usable books, consistent with admission and scan policy.

## Recovery timing and state

The ordinary response window remains five seconds. A retry retains the five-second unsubscribe-to-subscribe gap. The initial subscription is followed by at most ten retries per outcome. Subsequent rounds each allow ten more unsubscribe-and-subscribe retries, with no extra uncounted subscription. The existing five-second check cadence may delay detection by one interval.

| Setting | Proposed value | Meaning |
| --- | --- | --- |
| Retry limit | 10 | Resubscriptions per outcome in one recovery round. |
| Continuous missing limit | 180 seconds | Eligibility-check trigger that survives reconnects. |
| Retry cooldown | 60 seconds | Local wait after a round's final response window expires before another round starts. |
| Lookup follow-up interval | 60 seconds | Minimum wait after an eligible or unknown result before another recovery check. |
| Lookup scheduling | At most one batch start per second | Shared by pending admission and recovery checks. |
| Lookup batch size | At most 100 distinct listing IDs | One logical batch in flight, using the existing shared Gamma request budget. |

After the tenth resubscription, wait for its response window to expire before reporting exhaustion. `first_book_retry_budget_used` currently records the final send, so it is not the exhaustion trigger.

Each connection retains a missing-since timestamp per outcome outside its per-session retry state. Receiving a valid full book, including an empty book, clears that outcome's missing-since, cooldown, and pending retries. PONGs, deltas, and empty response arrays do not clear it. A later source reset starts a new missing period for previously initialized outcomes.

After a final response window expires, the connection starts its 60-second cooldown. It continues accepting late books and serving every other market. When the cooldown expires, it starts another round for still-missing outcomes. No HTTP response is needed. An already-issued unsubscribe always finishes its scheduled subscribe unless removal or a session change cancels it.

Reconnect sends the current membership's initial subscription and resets per-session attempt counts. It preserves missing-since, the reporting latch, and any unexpired cooldown. Thus a new session can recover through its initial subscription but cannot bypass an existing cooldown. After a cooldown expires, a new session still gets its initial five-second response window before any retry. Reported exhaustion does not pause local recovery.

The first of these conditions requests a check:

1. An outcome uses ten retries and its final response window expires.
2. An outcome has continuously lacked a baseline for 180 seconds, including time disconnected, and the connected session successfully sends a missing-book retry while the socket has already received at least one full book in that session.

Evaluate condition 2 at successful retry sends, after the existing response window and socket-liveness checks. Consider every successful retry send until the one-shot condition fires, so a full book arriving after the first retry does not permanently suppress the check. PONGs do not establish that a socket can deliver books. Do not request these checks merely because a socket is waiting for the open limiter or reconnect backoff. A session delivering no full books cannot trigger condition 2. Normal round exhaustion can still trigger condition 1. This gate reduces outage-driven checks but does not prove the remaining failure is specific to a market. While a source remains disconnected, ordinary full scans still reconcile eligibility in normal mode.

Condition 2 fires once per continuous missing period. Reconnect and cooldown completion do not re-arm it. A valid baseline clears its latch. Later round exhaustion reports a new transition, but never interrupts a running round. Discovery owns follow-up checks after the first report, so periodic connection reports are unnecessary. Exhaustion and the continuous-missing trigger for the same state coalesce.

## Eligibility decisions

Use the same eligibility rule as admission: supported v1, active, not closed, not archived, accepting orders, valid condition ID, and two distinct numeric outcome IDs. `enableOrderBook` remains outside the gate. See the [Polymarket market-status documentation](https://docs.polymarket.com/market-data/market-details#market-status).

Recovery removal requires a trustworthy row for the expected listing and condition ID. Those two IDs define market identity. Outcome IDs define its subscription configuration. Missing or changed outcomes do not prevent removal of a matching, explicitly closed market. Preserve enough normalized detail at the exchange boundary to distinguish explicit ineligibility from missing fields or invalid identity. Today's `ListingStatus::NotDesired` alone does not preserve that distinction.

| Observation | Decision |
| --- | --- |
| Matching identity with explicit closed or archived status | Remove both copies. Stop pending checks. A future scan can still discover a legitimate later change. |
| Matching identity with explicit inactive or not-accepting status | Remove both copies. In normal mode, a later successful scan can re-admit it. In fixed mode, retain its configured ID for confirmation every 60 seconds. |
| Matching identity with explicitly supported-as-out-of-scope `v2` | Remove both copies. Do not automatically queue admission in normal mode. |
| Eligible with the same outcome IDs | Retain membership. Local retry rounds continue independently. |
| Eligible with different, valid outcome IDs, while a reporting source remains incomplete | Remove the old membership with `IdentityChanged`. Queue fresh admission confirmation for the same listing and condition ID, first due 60 seconds after removal. Re-admit only after confirmation, with a new generation and Gamma's current outcome IDs. |
| Missing row, missing eligibility fields, malformed or changed market identity, unknown version, ambiguous response, or HTTP error | Unknown. Retain membership and retry eligibility after 60 seconds. Local retry rounds continue independently. |

After an eligible or unknown result, discovery schedules another check after 60 seconds while a reported source remains incomplete. Later reports merge into that work without shortening its deadline. Stop follow-ups once all reporting sources recover. An HTTP failure does not reset local attempts, start a retry round, or extend a connection cooldown.

Only an explicit `v2` response establishes protocol exclusion for recovery removal. Preserve an unfamiliar or missing lookup version as unknown for positive eligibility confirmation. A scan query constrained to v1 does not establish the version of a lookup that requests both versions. Every readmission path introduced here, including identity replacement and fixed-mode confirmation, requires explicit v1 metadata. Existing lifecycle-admission parsing remains unchanged; its missing-version fallback is an acknowledged separate gap. Explicit negative eligibility facts can authorize removal without every other flag being present, but identity must match. An eligible recovery result requires all admission requirements.

Automatic readmission after a temporary pause uses the existing full scan in normal mode, avoiding another permanent five-second polling queue. The accepted coverage delay can be the default 900-second scan interval plus a full scan's duration, even if the market resumes trading sooner. A fresh lifecycle hint can also enter the existing admission path. Identity replacement uses a fresh pending confirmation first due after 60 seconds, with the same interval for further checks. That confirmation must survive the old membership's removal overlay without allowing an older scan to resurrect the old outcome IDs.

REST book `404`, low activity, zero liquidity, and a WebSocket `[]` response do not authorize removal. Never synthesize a full empty book from those observations.

## Lookup queue and coordination

Extend the registry's pending-work model to distinguish admission confirmation from recovery validation. Use one scheduler and the existing `Listings::lookup` operation, with a fresh nonce and shared request budget. Do not create a second independent HTTP loop.

Deduplicate recovery work by logical market and admission generation. Merge reports from both feeds and both outcomes. Retain the latest recovery state for each reporting source. Schedule oldest due work with fair service between admission and recovery queues, so a burst of recovery reports cannot indefinitely delay new markets.

Send coalesced recovery-state updates when a reported source recovers or reconnects. These updates allow the registry to stop future checks when all reporting sources are complete. They do not suppress a valid ineligibility result from a request already in flight.

Bound recovery work by current membership: at most one entry per admitted market and two current source states per entry. A bounded ingress channel must not block socket reads. Each connection retains the latest dirty state when `try_send` is full and retries delivery from connected ticks, connect transitions, and the outer reconnect loop. A later recovered state replaces an undelivered missing report. Removal drops dirty state for that generation. Session changes replace source state instead of creating unbounded history. State updates include a monotonically increasing source revision; discovery ignores older updates.

Extend source membership with a generation assigned on admission and carried through subscribe, unsubscribe, and recovery state reports. A session epoch distinguishes reconnects within that generation. Suggested messages are semantic, exchange-neutral types:

```text
RecoveryState(market, admission_generation, source, session_epoch,
              revision, missing_outcomes, missing_since, trigger)
SourceAttached(market, admission_generation, source)
```

An empty missing-outcomes set reports completion. Send state updates only for sources that have reported or have an undelivered report. A generation or session change invalidates old state updates. These are one-way reports; there is no retry permission or acknowledgement protocol.

## Removal and race handling

On a confirmed ineligible result, the registry records a removal with a dedicated `EligibilityChanged` reason. Discovery dispatches the existing market-removal action. The manager detaches that market from its worker and unsubscribes both outcomes on both recorded source assignments. Spread replicas require the recorded assignments, not arithmetic on connection IDs.

Cancel pending delayed subscribes, recovery tickets, and missing-since state for the removed generation. Other markets on those sockets remain subscribed. A socket may close through the existing empty-membership path if no markets remain. Desired, synced, book, and standby counts remain with their existing owners. Do not decrement them again in the reporting connection.

The implementation must handle these races:

- **Remove and re-admit:** a lookup for an earlier admission generation cannot remove the new membership. An unsubscribe for an old generation cannot detach the replacement.
- **Reconnect during lookup:** a valid ineligible observation still applies to the same logical admission. Retain validation work and refresh source state. HTTP results do not affect local retry state.
- **Book arrival during lookup:** clear local retries for recovered outcomes. A late book does not override confirmed ineligibility. Once all reporting sources recover, no further recovery checks are needed, although an already-running valid ineligibility result still applies.
- **Outcome change after recovery:** if all reporting sources have recovered on the old IDs when a changed-outcomes result arrives, treat the claimed change as unknown and log the contradiction. Do not remove a recovered membership based on that result. With no unresolved sources, no recovery follow-up is scheduled. Ordinary future scans and lifecycle events retain their existing behavior.
- **Older scan during lookup:** a successful lookup decision must survive an older in-flight scan's reconciliation. Extend the registry's recent-event overlay to cover removal and retention decisions. Preserve existing scan-epoch rejection of older lookup observations. A result discarded by this fence schedules confirmation again after five seconds if still relevant, without consuming the 60-second result interval. Define ordering by request start and completed-scan epoch, not assumed upstream snapshot timestamps.
- **Resolution during lookup:** remove immediately and invalidate its tickets. The late lookup cannot resurrect the market.
- **Temporary pause:** a newer eligible admission can re-add the market with a new generation. Pending admission never contributes to desired coverage.
- **Late source data:** use the ordered source-attachment mechanism below. Market ID alone is insufficient for this race.

On `AddMarket`, the worker creates the new generation with every source detached and unsynced. The connection records `attach_pending` for each newly admitted membership generation, even when it applies a subscribe command without a socket. After the first successful wire subscription containing that membership, it sends `SourceAttached` on its own ordered worker channel before it reads and routes further data. This includes both incremental subscriptions and initial subscription batches during reconnect. A failed wire send leaves `attach_pending` set. Clear the flag after successful marker delivery to the worker. Duplicate markers for an already attached current generation are idempotent and do not invalidate its books. The worker accepts a marker only for its current generation and expected source. Data from that source is ignored until the marker arrives, and deltas remain unusable until full baselines arrive.

The connection serializes unsubscribe, subscribe, marker, and data routing. It cancels old delayed retries when removing membership and rejects stale-generation commands. Thus old worker-channel data precedes the new marker and is discarded. Reconnect retains the existing ordered source reset and requires fresh baselines. The marker does not prove an upstream frame belongs to a particular subscription request: Polymarket frames have no subscription-generation tag. Existing full-book and continuity rules still apply to frames read after the new subscribe.

Fixed-market mode must process recovery reports and lookup results for its configured IDs, without starting a full scan or admitting unrelated markets. This adds a loop to the current startup-only lookup behavior. Retain every configured ID, including IDs skipped or absent at startup and IDs removed later. Confirm non-admitted configured IDs every 60 seconds for possible admission, including closed or excluded rows. Require explicit v1 and the normal eligibility fields before admission through this new loop. Readiness keeps its existing initialization contract and does not wait for every configured ID to become eligible.

## Evidence and shutdown

Use structured incidents for requested checks, decisions, identity changes, and local retry-round transitions. Include listing ID, market ID, generation, source, session, trigger, missing outcomes, elapsed wait, and normalized result where relevant. Emit transitions, not per-tick repeats. Do not label numeric metrics with market IDs or introduce new metric families as part of this change.

Existing Prometheus metrics remain numeric evidence for desired, publishing, standby coverage, load, and latency. A decrease in desired coverage after removal must be reported separately from successful baseline recovery.

Cancellation drops pending HTTP futures and queued recovery work. Connection and writer drain retain the existing shutdown deadline. No new HTTP wait may hold shutdown open.

## Implementation sequence and acceptance

1. Extend exchange-boundary listing assessment to distinguish explicit ineligibility from unknown data. Preserve current admission behavior for missing fields. Test both purposes separately.
2. Add local cooldown rounds, admission generations, source recovery reports, and source-attachment markers through the app, manager, connection, and worker boundaries. Keep the generic protocol independent of Gamma.
3. Extend registry pending work and discovery's existing asynchronous lookup scheduler. Add removal and retention reconciliation protection.
4. Reuse manager detach and worker removal. Prove delayed commands and data cannot affect a replacement generation. Test admission during disconnected backoff, failed incremental subscription followed by reconnect, initial reconnect subscription markers, and duplicate markers.
5. Add deterministic clock tests for exhausted final windows, one-shot continuous-missing triggers across reconnects, preserved cooldowns, and recovery during an extended Gamma outage. Test broad reconnects with timely books, PONG-only sessions, a first full book arriving after the first retry, and missing outcomes whose 180-second deadline passes before a later successful retry. Exercise fixed-mode startup-skipped IDs and normal-mode delayed scan readmission.
6. Add mock-exchange tests for one-market removal from both spread sources, healthy-neighbor continuity, late books, HTTP failure, resolution, concurrent scans, and remove-and-re-admit. Verify bounded-channel saturation does not block reads or lose checks.
7. Verify that the existing lookup returns a known archived row before relying on archived-status removal. If it omits that row, keep the response unknown and document that full scan reconciliation supplies normal-mode cleanup. Do not infer archived status from absence.
8. After local validation, run a separately authorized fresh AWS build. Retain exact lookup decisions and retry-round incidents alongside five-second metric scrapes. Report removed ineligible markets separately from eligible unresolved markets. Keep independent output validation separate from coverage and latency.

The [SPEC](../../SPEC.md) and design journal record the adopted recovery policy. The numeric metric catalog is unchanged. Temporary raw diagnostics used during AWS validation were removed afterward.
