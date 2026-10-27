# Older full-book filter validation, 2026-10-26

The worker rejects a full book only while its outcome book is synchronized and
its exchange timestamp is strictly below the maximum timestamp of applied depth
in that synchronized period. Equal or newer books remain accepted. Older absolute
price changes still apply in arrival order, without lowering the maximum. The
first full book after an unsync establishes a fresh baseline.

Rejection precedes level replacement, source and receive timestamp updates,
timer changes, corrections, and disagreement counting. Aggregate metrics expose
skip count and timestamp lag. No trade subtraction, message sorting, buffering
for ordering, or connection-layout change is part of this implementation.

## Deterministic regression

The retained six-market incident contains 9,572 frames and 994 repeated-book
comparisons. A separate Rust source copy with only rejection disabled reproduces
four disagreement flags. The changed Rust implementation skips exactly two books,
each 8 ms behind the applied maximum. Its 992 accepted comparisons have no
remaining disagreement. Both controls pass for 1, 2, and 4 workers through JSON
decoding, routing, book mutation, the SBE writer, and clean SBE reading. The
changed optimized test executable also passes on AWS.

The focused synthetic regression covers strictly older, equal and newer books,
independent outcome maxima, older absolute deltas, unchanged rejected-book output
metadata and timer cadence, dropped unsynced deltas, and reconnect baseline reset.
The metrics integration test checks worker publication of skip count and lag.
The full ordinary Rust suite passes, including the fake-exchange lifecycle and
shutdown tests. Large benchmarks and private-capture tests remain opt-in.

## Fifteen-minute AWS validation

The existing London server ran the changed release collector for 900.336 seconds
with six currently high-volume eligible binary markets, two outcome assets each,
one socket per market, four workers, raw fixture capture, and exact-depth checking.
The collector retained its production 60-second heartbeat. A separate guarded
15-minute capture used independent market sockets and application PING every
10 seconds. Both collections used time, memory or byte, and free-disk guards.
The server remains running.

The Rust collection finished with exit code zero and a clean shutdown. It captured
77,528 frames and applied 127,868 absolute level assignments. Seven older books
were skipped: one at 5 ms lag and two each at 6, 11, and 19 ms. The lag sum is
0.077 seconds. Of 8,991 accepted repeated books, two differed from held depth.
Both had timestamps equal to the applied maximum and are intentionally accepted
by this rule. The unfiltered model on the same input has 13 disagreement flags.

Independent reconstruction verifies every live output update and all 176 periodic
snapshots across 12 books, including full depth and accepted source metadata.
The complete capture also passes the changed Rust replay with 1, 2, and 4 workers.
There are zero decode errors and an empty decode-error file. All seven skip events
and both mismatch events match independent counts and exposed metrics. This
checks the relevant telemetry events; it does not infer an unexposed global
telemetry-drop total. Peak sampled resident memory was 27,732 KiB. This is a
correctness run, not a performance comparison.

The independent 10-second-PING control recorded 80,310 frames and 534 answered
application heartbeats. Integrity checks verify frame sequences, completion
markers, byte counts, and file hashes. Its changed Rust replay passes with
1, 2, and 4 workers: five skips, 9,373 accepted comparisons, and four accepted
equal-timestamp disagreements. Its unfiltered model has 11 flags. Different
sockets have different arrival histories, so the two runs are separate evidence.

## Provenance and reproduction

[Machine-readable results](older-full-book-filter-results-2026-10-26.json) retain
production source hashes, executed collector and test-executable SHA-256 values,
capture and SBE hashes, counts, and limits. Private raw captures, source archive,
and exact executed binary remain outside version control. The release collector
was built with `cargo build --release --locked` in a fresh dedicated target.
Its frozen production inputs remained unchanged throughout validation. The copied
AWS executable was hashed before execution and again afterward.

An initial debug control reused a target across source copies, which caused a
later metrics check to execute stale package code. Those checks were discarded.
The baseline was rebuilt in a separate fresh target, and the changed-source
package was cleaned and its full suite rebuilt. The AWS collector used its
separate release artifact throughout. Building the optimized test executable
later replaced the local Cargo target's collector with a different feature build;
the recorded AWS executable was retained separately. Cargo fingerprints are not
used as proof of executed binary identity.

With private evidence available, convert a completed guarded capture using
`research/experiments/older-full-book-fixture.py CAPTURE OUTPUT`. Set
`OLDER_BOOK_FIXTURE=OUTPUT` and run
`cargo test --test older_full_books captured_depth -- --ignored --nocapture`.
Optional `OLDER_BOOK_EXPECT_SKIPS` and `OLDER_BOOK_EXPECT_MISMATCHES` assert
control counts. `research/experiments/older-full-book-audit.py FIXTURE REPORT`
compares independent Decimal models with and without rejection. Its bounded
`--follow SECONDS` mode checks a growing raw fixture capture.

For the live SBE audit, append an `advance` event of one millisecond to a copy of
the completed raw Rust fixture and set `OLDER_BOOK_LIVE_SBE` to the retained SBE
file when running that Rust test. Use separate Cargo target directories for the
baseline and changed source copies, and retain executed artifacts before another
build can replace them.

## Follow-up on the two accepted-book disagreements

Retained-frame tracing identifies one episode across the two outcome books.
Each full book reflected a quantity reduction before the matching absolute
`price_change` arrived. The incremental update's exchange timestamp was 5 ms
later, and it assigned exactly the quantity already supplied by the full book.
The immediately preceding update had the same timestamp as the full books but
changed other levels. These books supplied useful depth ahead of the incremental
updates; they did not roll back newer applied quantities.

For this episode, the flags reflect harmless Polymarket feed sequencing rather
than a collector correctness defect. The capture establishes receipt order and
message contents, not the exchange's internal cause. No further collector fix is
needed. The counter remains an observation of temporary depth disagreement,
not proof of data loss. See the
[design decision](../design-journal.md#full-books-ahead-of-incremental-updates-2026-10-26).

## Remaining limits

Exchange timestamps do not establish a complete order. Equal-time stale books
can overwrite levels; older deltas can still overwrite levels; and a rejected
older book might contain a useful correction. The live accepted-book disagreements
show that benign sequencing can also increment the disagreement counter. The filter
prevents the demonstrated older-full-book rollback without claiming upstream completeness,
complete trade-to-depth coverage, or universal ordering safety.
