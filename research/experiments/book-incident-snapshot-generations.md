# Snapshot disagreements across market generations

All 48 reported snapshot disagreements match the first captured full book after
explicit `RemoveMarket` and `AddMarket` controls. They concern 40 symbols in 20
markets. None matches a later full book within the same generation.

- 28 occur at receive time `2026-10-26 11:44:25.079886 UTC`, after split
  400 to 1675. They are generation 2, book 1, in raw frame line 2544185.
- 12 occur at `2026-10-26 11:51:12.350041 UTC`, after split 400 to 2401.
  They are generation 2, book 1, in frame line 7690131.
- 8 occur at `2026-10-26 11:52:30.854004 UTC`, after split 1675 to 2402.
  They are generation 3, book 1, in frame line 9116033.

Every case has a matching removal before its new add, a changed connection,
and no earlier full book in the new generation. The independent Decimal model
also finds that each fresh initial book differs from its final old-generation
book. The standalone verifier's original comparison crossed these generation
boundaries. This lifecycle cause is supported by the captured controls and raw
books, rather than inferred from shared timestamps or split telemetry alone.

The compact proof manifest is
[book-incident-snapshot-baselines.json](book-incident-snapshot-baselines.json).
It contains 48 unique symbol/receive-time keys and all 2,958 bid and ask levels
from their fresh raw baselines. The full corrected capture SHA256 is
`f6240bd21d4d73b3049f105620ed8cffa44e07bb48d7be41a9833767a46ce4fc`,
matching an independently computed hash. Each of the three original
frame strings also has its own hash. This is a set of specific proven baseline
occurrences; timestamps are not a general deduplication key.

[book-incident-snapshot-generations.json](book-incident-snapshot-generations.json)
records every original control line, frame line, array index, market, old/new
connection, raw lifetime book ordinal, and generation book ordinal. The raw
capture was scanned once, retaining original frame and array order. The filter
selected affected symbols plus removal and unsync controls. Python retained
only affected book states. The initial ripgrep scan used mmap; future reruns
explicitly use `--no-mmap` to bound its resident memory as well.

The raw proof is complete. Establishing that the output is valid additionally
requires the SBE checker to consume the first SBE snapshot for each
manifest key exactly once and compare both complete sides with this manifest.
The checker must reject mismatching quantities and unused markers. No claim
about all output contents is made from lifecycle matching alone.

The raw classification and manifest command is:

```sh
python3 research/experiments/book-incident-snapshot-generations.py --results research/experiments/book-incident-snapshot-generations.json --source-sha256 f6240bd21d4d73b3049f105620ed8cffa44e07bb48d7be41a9833767a46ce4fc
```

Regenerating only the manifest avoids rereading the large capture:

```sh
python3 research/experiments/book-incident-snapshot-generations.py --manifest-only research/experiments/book-incident-snapshot-generations.json --source-sha256 f6240bd21d4d73b3049f105620ed8cffa44e07bb48d7be41a9833767a46ce4fc
```

Negative controls confirmed that the manifest builder rejects a later book
ordinal and a removal that occurs after its add.
