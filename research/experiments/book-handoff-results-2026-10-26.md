# Established-feed / joining-feed experiment, 2026-10-26

Inconclusive: no repeat-book reconstruction incident observed. No production change is justified by this negative observation. This sample does not establish subscription-handoff correctness or explain earlier captures.

The public-feed experiment ran from 2026-10-26T11:29:51.734Z to 2026-10-26T11:53:16.439Z. The 3 bounded continuous-segment captures totalled 1162.969 s including their pilots; elapsed wall time including the interruption was 1404.705 s. An established socket failed, so these are separate baseline epochs, not an uninterrupted 20-join baseline.

- Epoch 1: 2026-10-26T11:29:51.734Z to 2026-10-26T11:37:02.419Z; completed schedule: False; 6/7 successful joins; 50 repeat comparisons, 0 differences. Baseline 45 s, each joining feed 40 s, control gaps 10 s, planned tail 35 s.
- Epoch 2: 2026-10-26T11:38:32.619Z to 2026-10-26T11:41:27.021Z; completed schedule: False; 1/3 successful joins; 32 repeat comparisons, 0 differences. Baseline 30 s, each joining feed 35 s, control gaps 7 s, planned tail 25 s.
- Epoch 3: 2026-10-26T11:43:58.557Z to 2026-10-26T11:53:16.439Z; completed schedule: True; 8/10 successful joins; 34 repeat comparisons, 0 differences. Baseline 30 s, each joining feed 40 s, control gaps 7 s, planned tail 20 s.

Market `682705` was active, not closed, accepting orders, and had a future end date in all fresh Gamma responses. The selection pilot observed 1,026 absolute level entries in 25 s; the final 10 s segment pilot observed 384. Both asset IDs were subscribed explicitly:

- `20006732765674855733524007935991362439352594042415161039767275461950712817548`
- `54533569528927082430133047556849600511985191158927477533124630301934489498530`

Each connection sent `{"type":"market","assets_ids":["<asset-1>","<asset-2>"],"initial_dump":true}`; custom features were omitted. Feed A retained its socket until the end of each epoch or an observed failure. Each B was a fresh socket. Later epochs reused the same active pair with a newly established A. No reconnect is represented as continuous observation.

Results count received observations across sockets and outcomes, not unique exchange actions:

- Successful joins: 15/20; censored or otherwise unsuccessful joins: 5. Success requires both initial books, the full requested observation duration, a requested clean close, no B error, and A open through B's observed close. A delayed B close after A fails is conservatively censored even if the intended observation duration had elapsed.
- Repeat-book comparisons: 116; differing comparisons: 0; differing levels: 0.
- FIRST refreshes: 32 comparisons, 0 differences. Later refreshes: 84 comparisons, 0 differences. Joining feeds contributed 26 FIRST and 26 later comparisons; the remaining comparisons came from A.
- Initial-to-FIRST-refresh per-outcome windows compared: 26; outcome/join windows with no FIRST refresh: 14. Same-receive-window content alignment left 40 A and 1412 B occurrences unmatched. These include interval boundaries, repeated content ambiguity, delivery skew and censored intervals; these counts alone do not establish a handoff loss. All A-unmatched occurrences were checked against B's complete captured session. 38 have equal-content B occurrences before its initial book or after its FIRST refresh within 2 s of A receipt. A further 2 occurrences already had their target quantity in B's initial book, leaving 0 without either observation. Boundary and quantity matches describe observed content, not unique event identities or proof of completeness.
- Established-feed-only controls: 20 intervals, 283.201 s, 11,600 absolute entries, 20 repeat comparisons, 0 differences.
- Main socket closes: 23 ({"1000": 18, "1006": 5}); errors: 5. Abnormal closes: `B06` at 2026-10-26T11:35:51.678Z (code 1006), `A` at 2026-10-26T11:36:58.347Z (code 1006), `A` at 2026-10-26T11:41:13.456Z (code 1006), `B06` at 2026-10-26T11:50:15.761Z (code 1006), `B08` at 2026-10-26T11:51:06.206Z (code 1006). Pilot sockets are excluded from main counts. Close-handshake timeouts after 3 s: 2. Maximum simultaneously open B sockets: 2; extra B opens while an older B was still closing: 2. Closing sockets remain captured and modeled separately, and their overlap is excluded from established-feed-only controls. Empty error messages and code 1006 do not establish a cause.
- Main application messages: 36,133; absolute level entries: 71,536. Application receive gaps over 5 s: 2; maximum application gap: 67712.105 ms; maximum market-message gap: 67712.105 ms. Per-feed initial and terminal receive gaps are retained separately in JSON.
- Capture-index discontinuities: 0; per-feed message-index discontinuities: 0; JSON failures: 0; duplicate prices inside received books: 0; unexpected subscriptions: 0.
- Collector timer delays over 100 ms: 54; maximum timer delay: 1390.498 ms. These indicate shared-host/event-loop scheduling limits and prevent treating receive time as packet-arrival time.
- Cross-feed books differing from A's locally received state at B receipt: 48/92. In 30 differing comparisons, every differing level's target quantity was subsequently observed in an A update within 2 s. Initial-dump, FIRST-refresh and later-refresh as-of differences were 8, 18 and 22, respectively. These receive-time comparisons include delivery skew and are not missing-update incidents; repeated content does not establish event identity.

Each socket is reconstructed independently with exact decimal price keys and absolute quantities. Zero removes a level. Every array entry and message is applied in original received order. Before replacing a book, replay compares the entire incoming book with that socket's current model. It ignores trades for depth, does not synthesize complementary updates, and never sorts or deduplicates messages by time or hash. Local index, epoch, feed, message number, array index, change index, exchange timestamp, UTC and monotonic receipt times identify evidence positions. Hashes and exchange times are diagnostic fields only.

For each outcome on each join, private analysis retains its initial-to-FIRST-refresh window and occurrence-aware A/B update-content alignment in the same receive interval. A second A window expanded by 2 s is diagnostic for delivery skew. Repeated content occurrences remain represented. Full B books are also compared against A's reconstruction as locally received. If a changed refresh occurs, the analyzer retains the affected level's history and looks for the same numerical book-only change on A within 2 s; supporting observation is distinct from event identity and cause.

The sample contains no observed changed refresh to classify as a joining-feed-only omitted update or a matching established-feed book-only correction. All sockets shared a collector, runtime, host, network route and potentially upstream infrastructure. Correlated omissions and buffered delivery remain possible. Node's built-in WebSocket exposes complete application messages, not TCP packets, wire fragmentation, or automatic protocol ping/pong. Application `PING`/`PONG`, subscription sends, opens, errors, requested closes, observed close codes and timer delays were recorded. Quiet intervals do not prove missing frames, and contiguous local indices prove only that this collector's own records are contiguous.

Run from the repository root (the capture refuses an existing output directory):

```sh
node research/experiments/book-handoff-capture.mjs /tmp/submission-handoff/live
node research/experiments/book-handoff-capture.mjs /tmp/submission-handoff/continuation '{"joins":14,"baseline_seconds":30,"join_seconds":35,"control_gap_seconds":7,"tail_seconds":25,"pilot_seconds":10,"market_id":"682705"}'
python3 -B -I research/experiments/book-handoff-supervise.py /tmp/submission-handoff/followup
python3 -B -I research/experiments/book-handoff-verify.py
python3 -B -I research/experiments/book-handoff-analyze.py /tmp/submission-handoff/live /tmp/submission-handoff/analysis-initial.json
python3 -B -I research/experiments/book-handoff-analyze.py /tmp/submission-handoff/continuation /tmp/submission-handoff/analysis-continuation.json
python3 -B -I research/experiments/book-handoff-analyze.py /tmp/submission-handoff/followup/segment-01 /tmp/submission-handoff/analysis-followup-01.json
python3 -B -I research/experiments/book-handoff-summarize.py /tmp/submission-handoff/analysis-initial.json /tmp/submission-handoff/analysis-continuation.json /tmp/submission-handoff/analysis-followup-*.json /tmp/submission-handoff/sources.json research/experiments/book-handoff-results-2026-10-26
```

The initial command used the earlier source version fingerprinted in epoch 1; its exact source is retained privately as `/tmp/submission-handoff/capture-initial.mjs`. The current script preserves its default schedule and adds bounded configuration for the continuation. Each collector has a 20 minute hard stop and a 512 MiB raw-message cap. The final supervisor attempts ten additional joins, with at most four separate A epochs and a 15 minute deadline; it never represents reconnects as continuous observation. Raw messages, initial source copy, detailed analyses and official source copies remain private under `/tmp/submission-handoff/`. The sibling JSON contains per-feed/per-join counts, first-refresh content-alignment counts, control intervals, quality results and SHA-256 fingerprints of inputs, scripts and retrieved official documentation. Nine behavioral controls cover omitted updates, shared book-only changes, array order, repeated keys and content, stale timestamps, trade/mirror exclusion, within-frame window boundaries, delivery skew, established-socket continuity, and delayed-close overlap.

The official [WebSocket guide](https://github.com/Polymarket/agent-skills/blob/91ee44ae113e958affd20cd505c6e9d9d6100e0b/websocket.md) documents market subscriptions, zero-size deletion and heartbeats. The current [real-time data page](https://docs.polymarket.com/market-data/realtime-data) describes public market events. Neither is used as a completeness guarantee; startup handoff behavior is the empirical question. No orders, production edits, commits, load benchmarks or external publication occurred.
