#!/usr/bin/env python3
"""Classify SBE snapshot disagreements against captured market generations.

The large raw capture is read exactly once by ripgrep's multi-pattern filter.
Python retains only affected books and controls. It never edits collector code.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess

spec = importlib.util.spec_from_file_location("book_reference", Path(__file__).with_name("book-incident-replay.py"))
reference = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reference)


def expected_cases(path, expected_count):
    cases = []
    for text in path.read_text().splitlines():
        match = re.fullmatch(r"differs: (\d+) at (.+) UTC", text)
        if match:
            received = reference.micros(match[2].replace(" ", "T", 1) + "+00:00")
            cases.append(dict(asset=match[1], received_us=received, received_at=match[2] + " UTC"))
    assert len(cases) == expected_count, f"need all {expected_count} verbose disagreements, found {len(cases)}"
    assert len({(p["asset"], p["received_us"]) for p in cases}) == len(cases)
    return cases


def write_manifest(report, path, source_sha256=None):
    assert report["matched"] == report["expected_disagreements"] == 48
    assert report["classifications"] == {"first_book_after_remove_add": 48}
    baselines = []
    for case in report["cases"]:
        assert case["book_ordinal_in_generation"] == 1
        assert case["remove_control"]["line"] < case["add_control"]["line"] < case["frame_line"]
        assert case["previous_generation"]["connection"] == case["remove_control"]["connection"]
        assert case["previous_generation"]["connection"] != case["connection"]
        levels = case["incoming_complete_book"]
        baselines.append(dict(symbol=case["asset"], received_us=case["received_us"],
                              bids=levels["bids"], asks=levels["asks"]))
    assert len({(b["symbol"], b["received_us"]) for b in baselines}) == 48
    manifest = dict(
        source_sha256=source_sha256,
        source_sha256_scope="Full corrected raw capture when populated; null until the parent's independent full-file hash is attached.",
        source_bytes=report["source_bytes"],
        selected_stream_sha256=report["matched_stream_sha256"],
        selected_stream_sha256_scope="ripgrep-matched original capture lines prefixed by their original line numbers; includes affected assets, removals, and unsync controls; not the full capture.",
        raw_frame_sha256={str(c["frame_line"]): c["frame_sha256"] for c in report["cases"]},
        baselines=baselines)
    path.write_text(json.dumps(manifest, separators=(",", ":")) + "\n")
    print(json.dumps(dict(manifest_baselines=len(baselines), complete_levels=sum(len(b["bids"]) + len(b["asks"]) for b in baselines),
                         source_sha256=source_sha256, groups=dict(Counter(
                             f'{c["previous_generation"]["connection"]}->{c["connection"]}' for c in report["cases"])))), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, default=Path("/tmp/submission-m5/full-catalog/frames.jsonl"))
    parser.add_argument("--telemetry", type=Path, default=Path("/tmp/submission-m5/full-catalog/collector.jsonl"))
    parser.add_argument("--verbose", type=Path, default=Path("/tmp/submission-m5/readback-verbose.log"))
    parser.add_argument("--expected", type=int, default=48)
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/submission-replay/snapshot-generations"))
    parser.add_argument("--results", type=Path)
    parser.add_argument("--manifest-only", type=Path, help="Reuse classification JSON without rescanning raw capture")
    parser.add_argument("--manifest", type=Path, default=Path("research/experiments/book-incident-snapshot-baselines.json"))
    parser.add_argument("--source-sha256", help="Independent SHA256 of the entire raw capture, supplied by parent")
    args = parser.parse_args()
    if args.manifest_only:
        write_manifest(json.loads(args.manifest_only.read_text()), args.manifest, args.source_sha256)
        return
    cases = expected_cases(args.verbose, args.expected)
    wanted = {(p["asset"], p["received_us"]): p for p in cases}
    assets = {p["asset"] for p in cases}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    patterns = args.output_dir / "patterns.txt"
    patterns.write_text("\n".join(sorted(assets)) + '\n{"type":"remove_market"\n{"type":"mark_unsynced"\n')
    splits = []
    with args.telemetry.open() as stream:
        for text in stream:
            row = json.loads(text)
            if row.get("event") == "connection_split":
                splits.append(row)

    active, by_asset, generation = {}, {}, Counter()
    history = defaultdict(list)
    books, previous = {}, {}
    ordinal = Counter()
    counters = Counter()
    classified = {}
    matched_digest = hashlib.sha256()
    before_stat = args.capture.stat()

    def preserve(book):
        return dict(levels={side: dict(book["levels"][side]) for side in ("bids", "asks")},
                    latest=book["latest"], last_book=book["last_book"],
                    generation=book["generation"], connection=book["connection"])

    process = subprocess.Popen(
        ["rg", "--no-mmap", "--line-number", "--no-heading", "--color", "never",
         "--fixed-strings", "--file", str(patterns), "--", str(args.capture)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None
    with (args.output_dir / "control-evidence.jsonl").open("w") as evidence:
        for raw in process.stdout:
            matched_digest.update(raw)
            line_text, payload = raw.split(b":", 1)
            line = int(line_text)
            row = json.loads(payload)
            kind = row["type"]
            counters["filtered_rows"] += 1
            if kind == "add_market":
                selected = [a for a in row["order_books"] if a in assets]
                if not selected:
                    continue
                market, conn = row["market"], row["connection"]
                generation[market] += 1
                active[market] = dict(connection=conn, assets=selected)
                control = dict(line=line, type=kind, market=market, connection=conn,
                               generation=generation[market], order_books=row["order_books"])
                history[market].append(control)
                evidence.write(json.dumps(control) + "\n")
                for asset in selected:
                    by_asset[asset] = market
                    books[asset] = dict(levels={"bids": {}, "asks": {}}, synced=False,
                                        book_ordinal=0, generation=generation[market],
                                        connection=conn, latest=None, last_book=None)
                counters["affected_add_controls"] += 1
            elif kind == "remove_market":
                market = row["market"]
                if market not in active:
                    continue
                held = active.pop(market)
                control = dict(line=line, type=kind, market=market,
                               connection=held["connection"], generation=generation[market])
                history[market].append(control)
                evidence.write(json.dumps(control) + "\n")
                for asset in held["assets"]:
                    previous[asset] = preserve(books[asset])
                    del books[asset]
                counters["affected_remove_controls"] += 1
            elif kind == "mark_unsynced":
                conn = row["connection"]
                for market, held in active.items():
                    if held["connection"] == conn:
                        control = dict(line=line, type=kind, market=market,
                                       connection=conn, reason=row["reason"])
                        history[market].append(control)
                        for asset in held["assets"]:
                            books[asset]["synced"] = False
                        evidence.write(json.dumps(control) + "\n")
                counters["filtered_unsync_controls"] += 1
            elif kind == "frame":
                received = reference.micros(row["received_at"])
                try:
                    payload = json.loads(row["text"])
                except ValueError:
                    counters["non_json_selected_frames"] += 1
                    continue
                objects = payload if isinstance(payload, list) else [payload]
                for event_index, event in enumerate(objects):
                    event_type = event.get("event_type")
                    if event_type == "book" and event.get("asset_id") in assets:
                        asset = event["asset_id"]
                        market = event["market"]
                        held = active.get(market)
                        if not held or held["connection"] != row["connection"] or asset not in books:
                            counters["unsubscribed_selected_books"] += 1
                            continue
                        book = books[asset]
                        ordinal[asset] += 1
                        book["book_ordinal"] += 1
                        incoming = reference.levels(event)
                        key = (asset, received)
                        if key in wanted:
                            controls = list(history[market])
                            add = next(c for c in reversed(controls) if c["type"] == "add_market")
                            removes = [c for c in controls if c["type"] == "remove_market" and c["line"] < add["line"]]
                            prior = previous.get(asset)
                            if book["book_ordinal"] == 1 and book["generation"] > 1 and removes and prior:
                                classification = "first_book_after_remove_add"
                            elif book["book_ordinal"] == 1 and book["generation"] > 1:
                                classification = "first_book_after_add_without_proven_remove"
                            else:
                                classification = "not_a_fresh_generation_book"
                            related = [s for s in splits if s["new_connection"] == row["connection"]]
                            result = wanted[key] | dict(
                                classification=classification, market=market, frame_line=line,
                                frame_event_index=event_index, connection=row["connection"],
                                generation=book["generation"], book_ordinal_in_generation=book["book_ordinal"],
                                raw_book_ordinal_lifetime=ordinal[asset], synced_before=book["synced"],
                                add_control=add, remove_control=removes[-1] if removes else None,
                                previous_generation=({k: v for k, v in prior.items() if k != "levels"} if prior else None),
                                old_generation_to_new_initial_differences=(
                                    reference.differences(prior["levels"], incoming) if prior else []),
                                incoming_complete_book=reference.export(incoming),
                                incoming_exchange_us=int(event["timestamp"]) * 1000,
                                frame_sha256=hashlib.sha256(row["text"].encode()).hexdigest(),
                                incoming_hash=event.get("hash"), split_telemetry=related,
                                controls=controls)
                            assert key not in classified, "ambiguous asset/time in captured frames"
                            classified[key] = result
                            evidence.write(json.dumps({k: v for k, v in result.items()
                                                       if k not in ("incoming_complete_book", "controls")}) + "\n")
                            if len(classified) <= 3 or len(classified) % 12 == 0:
                                print(json.dumps(dict(classified=len(classified), classification=classification,
                                                      generation=book["generation"], ordinal=book["book_ordinal"],
                                                      remove_line=removes[-1]["line"] if removes else None,
                                                      add_line=add["line"], frame_line=line,
                                                      old_connection=prior["connection"] if prior else None,
                                                      new_connection=row["connection"])), flush=True)
                        book["levels"] = incoming
                        book["synced"] = True
                        book["latest"] = dict(line=line, event_index=event_index,
                                              received_us=received, exchange_us=int(event["timestamp"]) * 1000,
                                              event_type="book")
                        book["last_book"] = dict(book["latest"])
                    elif event_type == "price_change":
                        from decimal import Decimal
                        for change_index, change in enumerate(event["price_changes"]):
                            asset = change.get("asset_id")
                            if asset not in assets:
                                continue
                            book = books.get(asset)
                            if not book or not book["synced"] or book["connection"] != row["connection"]:
                                counters["selected_updates_ignored_unsynced_or_unplaced"] += 1
                                continue
                            assert by_asset[asset] == event["market"]
                            side = "bids" if change["side"] == "BUY" else "asks"
                            price, qty = Decimal(change["price"]), Decimal(change["size"])
                            if qty:
                                book["levels"][side][price] = qty
                            else:
                                book["levels"][side].pop(price, None)
                            book["latest"] = dict(line=line, event_index=event_index,
                                                  change_index=change_index, received_us=received,
                                                  exchange_us=int(event["timestamp"]) * 1000,
                                                  event_type="price_change")
                            counters["selected_absolute_updates"] += 1
                            key = (asset, received)
                            if key in wanted:
                                classified[key] = wanted[key] | dict(
                                    classification="snapshot_freshness_matches_price_change",
                                    frame_line=line, frame_event_index=event_index,
                                    market=event["market"], connection=row["connection"],
                                    generation=book["generation"], controls=list(history[event["market"]]))
    stderr = process.stderr.read().decode() if process.stderr is not None else ""
    assert process.wait() == 0, stderr
    after_stat = args.capture.stat()
    assert (before_stat.st_size, before_stat.st_mtime_ns) == (after_stat.st_size, after_stat.st_mtime_ns)
    missing = [p for key, p in wanted.items() if key not in classified]
    result = dict(
        source="corrected-full-catalog/frames.jsonl", source_bytes=before_stat.st_size,
        source_mtime_ns=before_stat.st_mtime_ns,
        matched_stream_sha256=matched_digest.hexdigest(),
        verbose_sha256=hashlib.sha256(args.verbose.read_bytes()).hexdigest(),
        expected_disagreements=len(cases), matched=len(classified), unmatched=missing,
        classifications=dict(Counter(p["classification"] for p in classified.values())),
        counts=dict(counters), split_telemetry=splits,
        cases=[classified[key] for key in wanted if key in classified],
        method="Single raw-capture scan by ripgrep multi-pattern matching of every affected asset plus all removal and unsync controls; matching frame strings and array order unchanged. Decimal model retains the final old-generation book and resets only on AddMarket. SBE disagreement receive times are matched exactly to captured input events.",
        limitations="A new-generation first book is a permitted authoritative Snapshot. This capture/control classification does not yet independently check the SBE snapshot's full contents or its SBE snapshot ordinal.")
    path = args.results or args.output_dir / "classification.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: result[k] for k in ("expected_disagreements", "matched", "classifications", "counts")}), flush=True)
    assert not missing, f"unmatched disagreements: {missing}"
    write_manifest(result, args.manifest, args.source_sha256)


if __name__ == "__main__":
    main()
