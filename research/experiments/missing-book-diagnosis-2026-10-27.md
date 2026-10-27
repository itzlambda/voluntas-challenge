# Exact missing-book diagnosis, 2026-10-27

The ten-minute diagnostic run reproduced **five exact markets without publishers**. Fresh manual WebSocket subscriptions from the same AWS host also returned no books for all five, while a positive control worked. Original collector sockets received PONGs and empty `[]` responses to their retries. No baseline reached the worker for these assets, and none of their ten source sockets failed.

The evidence identifies two different conditions: **three markets closed during the scan/opening window and remained in the desired set**, while **two markets remain eligible in metadata but have no retrievable CLOB books**. This does not demonstrate a collector decoder or socket-read bug. It does demonstrate why an eligibility flag and more retries cannot guarantee a publisher.

## Which markets

Titles below are shortened; IDs and full technical identities are retained in evidence. These are the five reproduced in this new run, not a reconstruction of the seven identities missing from the earlier aggregate-only run.

| Gamma ID | Market | Status during investigation | Source connections |
|---|---|---|---|
| 5668191 | CA Nacional Potosi vs Club Guabira, exact score 3–0 | Closed; not accepting orders | 532 / 1049 |
| 5957206 | Tweet count 160–179, October 23–30 | Closed; not accepting orders | 1066 / 277 |
| 6264100 | Table-tennis game handicap −1.5 / +1.5 | Closed; not accepting orders | 1796 / 1337 |
| 6282417 | Persib Bandung vs Melbourne Victory, goals O/U 3.5 | Open; accepting orders; no retrievable books | 1926 / 325 |
| 6285465 | Old Dominion vs Coastal Carolina, successful two-point conversions O/U 0.5 | Open; accepting orders; no retrievable books | 1948 / 51 |

Every final missing-market snapshot had both source readiness flags false and both outcomes' last-received timestamps at the epoch. Thus these five never initialized in this run; they were not publishers lost after a socket reset. Their ten source IDs have no retained session-end incident. Final identity count matches the Prometheus deficit: desired **258,336**, publishing **258,331**.

The numeric outcome IDs for the two open bookless markets also appear in neither
of the preceding two retained outputs. The three newly closed markets' outcome
IDs appear in both. Historical desired-set membership was not retained, so this
does not reconstruct all seven earlier gaps, but it distinguishes recurrent
bookless identifiers from markets newly unavailable after closure.

## Actual manual WebSocket tests

The independent Rust probe uses raw JSON and WebSocket frames, not the production book decoder. It ran on the retained AWS host while the collector continued. Each target used two fresh sockets for 60 seconds, subscribed to the exact two original outcome IDs with `initial_dump=true`, then unsubscribed at about ten seconds and subscribed again after a five-second gap.

- All ten target sockets completed without a probe error, received six PONGs, and received **zero book events**. Both initial subscription and delayed resubscription yielded empty array responses.
- Positive control 6290225, a Hong Kong temperature market, received both outcome books initially and again after delayed resubscription on both fresh sockets: four book events per socket. Returned condition IDs matched the expected market.
- Original collector trace likewise contains only PONGs and empty array responses for the five targets' retrying outcomes, not full books rejected by the decoder. Across each market's two sources, 20 empty retry responses were retained. Diagnostic frame/snapshot caps were not reached.

An empty response `[]` contains **no book event**. It is different from a valid `book` event with empty bids and asks; the collector accepts the latter as a baseline.

We also reproduced both original subscription groups for target 5668191 on four independent sockets. Each request contained **512 assets**. Each socket returned books for **all other 510 assets**, initially and after resubscription, omitting exactly the target's two assets. Those assets were at positions 332/333 and 272/273 in the respective request arrays, not at the end. This does not support a generic two-asset truncation or a collector parsing failure for this target.

## Independent CLOB corroboration

Follow-up `GET /book?token_id=...` calls returned **404, `No orderbook exists for the requested token id`**, for all ten target outcome IDs. Both control outcome IDs returned 200 and populated books. These are follow-up REST observations, not a substitute for the contemporaneous WebSocket tests.

The two open targets report active, not closed/archived, accepting orders and order-book enabled in both Gamma and CLOB market metadata. Gamma reports zero volume and liquidity. Gamma supplies two numeric outcome IDs for each, but CLOB's market records contain two placeholder entries with **empty `token_id` and `outcome` strings**. The control's CLOB token entries are populated and match Gamma.

This is not evidence of different valid token IDs that the collector should substitute. It is evidence of incomplete CLOB token records alongside unavailable books despite eligibility flags. Incomplete upstream setup or lazy book creation is a possible explanation, but its internal mechanism is not proven by these read-only tests. Zero liquidity alone must not become an exclusion rule: a valid empty book can still exist.

## Why closed markets were still desired

The scan started near **07:00:29 UTC** and completed at **07:04:49 UTC**. Gamma's recorded close times are:

- 5668191: **07:02:38.122**, during the scan.
- 5957206: **07:03:38.281**, during the scan.
- 6264100: **07:06:10.232**, during connection ramp-up.

The full pass buffers rows and applies them after completion. Eligibility observed earlier can therefore be stale by admission or socket opening. This does not prove the HTTP filter ignored `closed=true`; the retained scan rows were not an atomic snapshot, and page-level raw responses were not captured. The design does not reconfirm every scan row immediately before subscribing. No market-removal incident occurred, and only one scan completed in this ten-minute run. The normal 900-second scan interval plus pass time leaves a cleanup delay when lifecycle resolution events do not remove a market.

## Why eight publishers lacked ready standby

All eight final standby gaps are **also closed, non-accepting markets**: 6243464, 6243465, 6243466, 6243467, 6243468, 6243470, 6243475 and 6243479. They are crypto up/down markets for the 03:00–03:05 AM ET interval, with recorded close time **07:06:20 UTC**.

They share primary-pool source **1692**, whose first slow-consumer close was at 07:06:55, followed by three more closes. The odd-pool sources retain initialized state, while 1692's final copies are unready. Some state was received after the recorded closure, so the close flag does not establish an exact instant when CLOB stopped serving books. The key eligibility fact is that all eight were already closed before the final coverage observation. They are not eight currently eligible markets whose redundancy is established to be broken.

As a counterfactual using these identified states, removing the eleven confirmed-closed markets would leave the two open bookless markets as the publisher deficit, and no remaining observed standby deficit. That arithmetic is not a rerun result.

## Implications for a fix

The useful collector change is **targeted eligibility revalidation for stubborn missing/recovery baselines**, rather than more aggressive whole-socket resets. Confirmed closed or non-accepting markets should be removed through discovery's normal registry path. Markets still eligible but lacking a book need an explicit waiting/availability policy and later rechecks; `acceptingOrders=true` is not an acknowledgement that either outcome baseline exists now.

Do not synthesize empty baselines from `[]` or a 404, and do not silently discard eligible markets based on zero liquidity. Additional retries can help late availability, but cannot create an absent upstream book. This investigation leaves that policy choice explicit; it does not introduce an unproven production admission/recovery change.

## Build, execution and retained evidence

Fresh diagnostic candidate SHA-256: `023a93bec608dd5f235f6ae78d107b3a486017a2791cf033f5a5b25510816dbb`, built from main base `4eff979` plus frozen uncommitted changes (251 source files), with opt-in `book-diagnostics`. Release optimization 3, ThinLTO, one codegen unit, Rust/Cargo 1.96.0, debug/incremental off. Final feature-enabled checks: 136 tests passed, five ignored; Clippy with warnings denied passed. Ordinary builds leave these diagnostics disabled; the 17 numeric metric families are unchanged.

Unrelated documentation/demo/operations edits appeared in the shared checkout
after this freeze. They were excluded from this candidate and preserved without
modification. Validation applies to the retained frozen source, not all later
workspace edits.

The same idle London host ran UTC **07:00:29.116–07:10:30.094**: requested 600 seconds, elapsed **600.970 including drain**, exit 0. Four workers, 256 markets/socket, two sources/market, three lifecycle sockets, 20 connection opens/s, 10 Gamma requests/s, runtime open-file limits **65,536**. No injected collector faults. Manual probes were separately logged and finished inside the collector run. This diagnostic workload is not a controlled performance comparison.

The final scrape reported five missing publishers and eight publishing without standby. The whole run retained five natural 1013 closes: four on 1692 and one on 963. None involved the missing publishers' source sockets. Decode errors, negative exchange ages and writer-capacity blocked time were zero at the last scrape.

The complete **2,147,483,648-byte** SBE output passed the freshly built supplied-reader structural checks: **2,906,338 snapshots**, **9,913,197 level updates**, **516,662 symbols**, **34,129,147 snapshot levels**. SHA-256: `dbda2e36c7858b59c756671e3a75ebb5824d868a1095c145af5a1ad96fa56b4b`. This validates framing/domain/baseline/order invariants, not source completeness.

Evidence is retained under `data/aws-book-diagnostic-20261027T065420Z/`: frozen sources/patch/build metadata/binaries, all scrapes, exact worker and retry identities, selective original responses, manual fresh/group replay frames, Gamma/CLOB metadata, HTTP statuses/bodies/headers, analysis, reader result and cleanup proof. Full SBE remains in the retained host's run directory; exact location is in ignored run metadata. Collector, runner and manual sidecar stopped; the metrics port closed. No provisioning, production changes, commits or pushes occurred. Diagnostics and this report remain uncommitted.
