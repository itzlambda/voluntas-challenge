# Saved book incident replay

The current decoder, worker, and SBE writer agree with an independent Python
Decimal reconstruction before all 42 repeated books in the three saved
connection prefixes. N=1,2,4 give 126 complete comparisons with zero
disagreements. Each comparison includes both full sides and the timestamps of
the latest applied event. Source fingerprints are in
[book-incident-results.json](book-incident-results.json).

The raw prefixes contain 3,712 received frames, 5,398 initial books, and 7,286
applied absolute level updates per worker count. Thirty-six refreshes match the
held book. Six refreshes each differ at one level, across two exchange timestamps.
All six are the first refresh of their respective assets after the bulk baseline.

- `subscription-behavior-20261025/frames.jsonl`, connection `bulk-4`, lines 74
  and 76: baseline line 36, array entries 203 and 275. The held quantities are
  5100; the incoming ask at 0.97 and bid at 0.03 contain 6100.
- `subscription-behavior-format-20261025/frames.jsonl`, connections
  `bulk-format-2` and `bulk-format-1`, lines 3679/3680 and 3684/3685: baselines
  at lines 168 and 19. The held quantities are 6887.75; the incoming bid at
  0.18 and ask at 0.82 contain 7443.3.

The test inserts a 60,001 ms advance of the paused clock immediately before each
singleton refresh frame. The worker emits its ordinary periodic snapshots before
that frame reaches the decoder. The test reads those snapshots through the
supplied SBE reader and compares them with the Decimal state. It then checks the
written correction deltas separately. An incoming book cannot supply its own
expected before state.

Every original received frame string and its array order are retained. Initial
subscriptions become `AddMarket` controls before their first frames; market IDs
come from the captured books. The two format feeds replay separately because the
base collector owns each market on one connection. No trade subtraction,
deduplication, event sorting, complementary inference, or startup exclusion is
applied. Sorting map keys only canonicalizes comparison output.

Two assets in the 1,800-asset `bulk-4` subscription have no book or price change
in its prefix. The replay retains all 1,798 observed books and all 43 raw frames.
The checkpoints change periodic output cadence, so these results say nothing
about runtime latency. The recordings cannot establish exchange intent or rule
out upstream or capture loss. No production change is indicated by these six
windows; no production test hook was needed.

The validation command is:

```sh
timeout 120s cargo test --test book_incidents -- --include-ignored --test-threads=1
python3 research/experiments/book-incident-replay.py --output-dir /tmp/submission-replay --verify-rust --results research/experiments/book-incident-results.json
rustfmt --check --edition 2024 tests/book_incidents.rs
```

The test command passes four tests, including two existing helper tests.
The final run completed in 33.57 seconds. Both queues hold the entire bounded
fixture, and the test asserts exact periodic snapshot counts. An unrelated
bid corrupted in the SBE-derived before output was rejected by the verifier. The raw
test requires the local recordings and Python 3 and is ignored by default. The
compact synthetic regression runs by default and checks ordered equal-time
absolute changes, zero removal, ignored trades, complete before books, refresh
corrections, and an unchanged refresh. Temporary SBE files are deleted after each
run; at most one 64 MiB output file is retained at once. No load benchmark,
network capture, order, commit, or publication is part of this check.
