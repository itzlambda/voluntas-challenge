# Additional incidents from the resource-confounded run

The stopped M5 run reached its soft file limit of 1,024 descriptors. Its network
failures, incomplete coverage, and timing cannot establish venue behavior or
capacity. These additional cases are diagnostic only. The original six saved
incidents remain the primary regression result.

A single streaming pass over the completed 1,976,955,805-byte raw capture selected
one condition each from connections 264, 400, and 661. The independent Decimal
model retained their original controls and frame order. It found the six books
reported by the selected live telemetry, with the same differing-level counts.

- Connection 264, raw lines 586358/586359: both outcomes increase from 671.72
  to 771.72 at ask 0.93 and bid 0.07. These are the first refresh after the
  session-2 baseline, but the twelfth full book since initial placement.
- Connection 400, lines 1108211/1108212: each outcome decreases at eight levels,
  across both sides, for 16 decreases total. These are the eighteenth full books
  of session 4 and the sixtieth since placement. This case occurs after many
  matching refreshes, rather than only at a new subscription's initial handoff.
- Connection 661, lines 1125072/1125077: bid 0.03 and ask 0.97 appear with size
  3333 after being absent. These are the first refresh after the session-2
  baseline and the third full book since placement.

Sessions above count the captured `disconnected` controls. All six books were
synced immediately before the differing refresh. No trade subtraction, inferred
complement changes, event sorting, deduplication, or startup exclusion was used.

Three short windows start from each affected asset's previous authoritative full
book. They retain the original `AddMarket` control, full raw frame strings,
array order, and intervening events. Both complete before books in each window
match the full-prefix Decimal reconstruction. The saved fixtures total about
658 KiB, contain 63 frames and 102 applied absolute changes, and replay through
current Rust with N=1,2,4. All 18 complete before comparisons pass.

A separate streaming audit of the actual stopped run's SBE file retains only
these six books. Immediately before each refresh's first corrective record,
both complete SBE sides equal the independently reconstructed raw before book.
All six comparisons pass. Thus a worker omission of a captured update does not
explain these selected incoming differences. The comparison does not determine
why the incoming full books changed or rule out an upstream or capture problem.

The primary evidence is
[book-incident-fresh-results.json](book-incident-fresh-results.json). The large
original capture was streamed once. Selected temporary data used less than
300 MiB; the retained rerun fixtures are much smaller.

The portable saved-window validation commands are:

```sh
python3 research/experiments/book-incident-fresh.py --prepare-saved --output-dir /tmp/submission-replay/fresh-saved
BOOK_INCIDENT_DIRECTORY=/tmp/submission-replay/fresh-saved timeout 120s cargo test --test book_incidents saved_raw_windows_match_decimal_before_every_refresh -- --ignored --test-threads=1
```

Recreating the selection from the stopped capture uses:

```sh
python3 research/experiments/book-incident-fresh.py --capture-dir /tmp/submission-m5/live --output-dir /tmp/submission-replay/fresh
python3 research/experiments/book-incident-fresh.py --output-dir /tmp/submission-replay/fresh --shrink-existing
BOOK_INCIDENT_DIRECTORY=/tmp/submission-replay/fresh/small BOOK_INCIDENT_LIVE_SBE=/tmp/submission-m5/live/books.sbe timeout 120s cargo test --test book_incidents saved_raw_windows_match_decimal_before_every_refresh -- --ignored --test-threads=1
python3 research/experiments/book-incident-fresh.py --output-dir /tmp/submission-replay/fresh --verify-rust
```

The Rust test asserts that each entire fixture fits in its worker and writer
queues and that periodic snapshot counts are exact. The wall-clock timeout
also bounds failures independently of Tokio's paused clock. The live SBE audit
runs after replay has finished, when no worker timer remains active.
