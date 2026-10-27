#!/usr/bin/env python3
"""Extract a few conditions from the stopped, resource-confounded M5 capture.

The large source is scanned once, line by line. Unrelated frames are not parsed.
Retained frame strings and arrays are unchanged. Run only after status.json says
the capture completed. This script makes no capacity or venue-failure claim.
"""
import argparse
from collections import Counter
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location("reference", Path(__file__).with_name("book-incident-replay.py"))
reference = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reference)


def shrink(args):
    """Reduce the retained selection, never rescan the large original."""
    index = json.loads((args.output_dir / "source-index.json").read_text())
    summary = json.loads((args.output_dir / "decimal-summary.json").read_text())
    incidents = {p["line"]: p for p in summary["incidents"]}
    wanted_assets = {p["asset"] for p in incidents.values()}
    last_book, controls, anchors = {}, {}, {}
    with (args.output_dir / "selected-original.jsonl").open() as stream:
        for meta, text in zip(index, stream, strict=True):
            row = json.loads(text)
            if row["type"] == "add_market":
                controls[row["market"]] = row
            if row["type"] != "frame":
                continue
            payload = json.loads(row["text"])
            for event in payload if isinstance(payload, list) else [payload]:
                if event.get("event_type") != "book" or event["asset_id"] not in wanted_assets:
                    continue
                asset = event["asset_id"]
                if meta["source_line"] in incidents and incidents[meta["source_line"]]["asset"] == asset:
                    assert asset in last_book
                    anchors[meta["source_line"]] = last_book[asset]
                last_book[asset] = meta["source_line"]
    plans = {}
    for line, incident in incidents.items():
        conn = incident["connection"]
        plan = plans.setdefault(conn, dict(connection=conn, market=incident["market"],
                                          start=anchors[line], end=line))
        plan["start"] = min(plan["start"], anchors[line])
        plan["end"] = max(plan["end"], line)
    handles = {}
    for conn, plan in plans.items():
        directory = args.output_dir / f"case-{conn}"
        directory.mkdir(exist_ok=True)
        (directory / "status.json").write_text(json.dumps(dict(completed_utc="saved-window")))
        (directory / "collector.jsonl").write_text("".join(
            json.dumps(row) + "\n" for row in summary["telemetry"] if str(row["connection"]) == conn))
        handles[conn] = (directory / "frames.jsonl").open("w")
        handles[conn].write(json.dumps(controls[plan["market"]], separators=(",", ":")) + "\n")
    source_lines = {conn: [] for conn in plans}
    try:
        with (args.output_dir / "selected-original.jsonl").open() as stream:
            for meta, text in zip(index, stream, strict=True):
                for conn, plan in plans.items():
                    if plan["start"] <= meta["source_line"] <= plan["end"] and plan["market"] in text:
                        handles[conn].write(text)
                        source_lines[conn].append(meta["source_line"])
    finally:
        for handle in handles.values():
            handle.close()
    import subprocess
    import sys
    combined = []
    small_summary = []
    for conn, plan in plans.items():
        directory = args.output_dir / f"case-{conn}"
        subprocess.run([sys.executable, __file__, "--capture-dir", str(directory),
                        "--output-dir", str(directory), "--connections", conn], check=True)
        window = json.loads((directory / "expectations.json").read_text())[0]
        window["fixture"] = f"case-{conn}/selected.jsonl"
        window["connection"] = f"fresh-{conn}"
        combined.append(window)
        for checkpoint in window["checkpoints"]:
            if not checkpoint["differences"]:
                continue
            # The mini-window must reproduce the complete original before book,
            # not just its reported differing levels.
            original = next(p for p in json.loads((args.output_dir / "expectations.json").read_text())[0]["checkpoints"]
                            if p["asset"] == checkpoint["asset"]
                            and p["incoming_received_us"] == checkpoint["incoming_received_us"])
            assert original["before"] == checkpoint["before"]
            assert original["incoming"] == checkpoint["incoming"]
        small_summary.append(plan | dict(source_lines=source_lines[conn],
                             bytes=(directory / "frames.jsonl").stat().st_size))
    small = args.output_dir / "small"
    small.mkdir(exist_ok=True)
    # References remain relative to the parent fresh directory.
    (args.output_dir / "small-expectations.json").write_text(json.dumps(combined, indent=2) + "\n")
    for window in combined:
        window["fixture"] = "../" + window["fixture"]
    (small / "expectations.json").write_text(json.dumps(combined, indent=2) + "\n")
    (args.output_dir / "small-window-provenance.json").write_text(json.dumps(small_summary, indent=2) + "\n")
    print(json.dumps(dict(small_windows=len(combined), bytes=sum(p["bytes"] for p in small_summary),
                         complete_original_before_books_reproduced=len(incidents))))


def verify_selected(args):
    root = Path(__file__).resolve().parents[2]
    summary = json.loads((args.output_dir / "decimal-summary.json").read_text())
    full = json.loads((args.output_dir / "expectations.json").read_text())[0]
    expected = {p["asset"]: p for p in full["checkpoints"] if p["differences"]}
    assert len(expected) == 6
    small = args.output_dir / "small"
    digests = []
    for n in (1, 2, 4):
        actual = json.loads((small / f"rust-before-n{n}.json").read_text())
        assert len(actual) == 6
        for row in actual:
            checkpoint = expected[row["asset"]]
            assert row["before"] == checkpoint["before"]
            assert row["received_us"] == checkpoint["before_received_us"]
            assert row["exchange_us"] == checkpoint["before_exchange_us"]
        digests.append(dict(workers=n, complete_before_books=6, disagreements=0,
                            sha256=hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()))
    live = json.loads((small / "live-sbe-before.json").read_text())
    assert live["selected_before_comparisons"] == 6 and live["disagreements"] == 0
    for row in live["comparisons"]:
        assert dict(bids=row["bids"], asks=row["asks"]) == expected[row["asset"]]["before"]
    windows = json.loads((small / "expectations.json").read_text())
    fixture_hashes = {}
    for window in windows:
        conn = window["connection"].removeprefix("fresh-")
        name = f"book-incidents-fresh-{conn}.jsonl"
        payload = (small / window["fixture"]).read_bytes()
        (root / "tests/fixtures" / name).write_bytes(payload)
        window["fixture"] = name
        fixture_hashes[name] = hashlib.sha256(payload).hexdigest()
    (root / "tests/fixtures/book-incidents-fresh-expectations.json").write_text(json.dumps(windows, indent=2) + "\n")
    result = summary | dict(
        original_prefix_complete_before_books=len(full["checkpoints"]),
        rust_comparisons=digests,
        live_sbe={k: v for k, v in live.items() if k != "comparisons"},
        live_before_sha256=hashlib.sha256(json.dumps(live["comparisons"], sort_keys=True).encode()).hexdigest(),
        small_windows=json.loads((args.output_dir / "small-window-provenance.json").read_text()),
        saved_fixture_sha256=fixture_hashes,
        interpretation="The six selected actual live SBE books immediately before corrective refresh records equal the independent raw Decimal books. Current offline Rust also agrees at N=1,2,4. A worker omission of a captured update does not explain these six incoming differences. This resource-confounded run cannot establish venue behavior, network reliability, or capacity.",
        source_code_sha256={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in sorted((root / "src").rglob("*.rs"))})
    path = args.results or root / "research/experiments/book-incident-fresh-results.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(dict(complete_before_comparisons=18, live_before_comparisons=6,
                         disagreements=0, resource_confounded=True)))


def prepare_saved(args):
    import shutil
    root = Path(__file__).resolve().parents[2]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    fixture_root = root / "tests/fixtures"
    shutil.copyfile(fixture_root / "book-incidents-fresh-expectations.json", args.output_dir / "expectations.json")
    windows = json.loads((args.output_dir / "expectations.json").read_text())
    for window in windows:
        shutil.copyfile(fixture_root / window["fixture"], args.output_dir / window["fixture"])
    print(json.dumps(dict(windows=len(windows), complete_before_books=6, resource_confounded=True)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-dir", type=Path, default=Path("/tmp/submission-m5/live"))
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/submission-replay/fresh"))
    parser.add_argument("--connections", default="264,400,661")
    parser.add_argument("--shrink-existing", action="store_true")
    parser.add_argument("--verify-rust", action="store_true")
    parser.add_argument("--results", type=Path)
    parser.add_argument("--prepare-saved", action="store_true")
    args = parser.parse_args()
    if args.verify_rust:
        verify_selected(args)
        return
    if args.prepare_saved:
        prepare_saved(args)
        return
    if args.shrink_existing:
        shrink(args)
        return
    status = json.loads((args.capture_dir / "status.json").read_text())
    assert status.get("completed_utc"), "wait for the parent to finish capture"
    requested = {int(x) for x in args.connections.split(",")}
    selected, telemetry = {}, []
    with (args.capture_dir / "collector.jsonl").open() as stream:
        for text in stream:
            row = json.loads(text)
            if row.get("event") != "mismatch" or row.get("connection") not in requested:
                continue
            conn = row["connection"]
            selected.setdefault(conn, row["market"])
            if selected[conn] == row["market"]:
                telemetry.append(row)
    assert selected, "no requested mismatch conditions"
    targets = set(selected.values())
    args.output_dir.mkdir(parents=True, exist_ok=True)
    fixture_path = args.output_dir / "selected.jsonl"
    retained_path = args.output_dir / "selected-original.jsonl"
    counts = Counter()
    books, markets, initial, latest = {}, {}, {}, {}
    synced, emitted, ordinals = {}, {}, Counter()
    refreshes, sessions, session_books, total_books = Counter(), Counter(), Counter(), Counter()
    checkpoints, incidents, index = [], [], []
    source_digest = hashlib.sha256()
    source_bytes = 0

    def write(output, row):
        output.write(json.dumps(row, separators=(",", ":")) + "\n")

    with (args.capture_dir / "frames.jsonl").open("rb") as stream, fixture_path.open("w") as output, retained_path.open("wb") as retained:
        for line, raw in enumerate(stream, 1):
            source_digest.update(raw)
            source_bytes += len(raw)
            # Every source line is visited once. Only target-market frames and
            # controls, plus unsync marks for selected sockets, are decoded.
            relevant = any(market.encode() in raw for market in targets)
            if not relevant and not raw.startswith(b'{"type":"mark_unsynced"'):
                continue
            row = json.loads(raw)
            kind = row["type"]
            if kind == "mark_unsynced":
                conn = row["connection"]
                if conn not in selected:
                    continue
                counts["unsync_marks"] += 1
                sessions[conn] += 1
                for asset, held in markets.items():
                    if held[1] == conn:
                        synced[asset] = False
                        session_books[asset] = 0
            elif kind == "add_market":
                if row["market"] not in targets:
                    continue
                counts["markets"] += 1
                for asset in row["order_books"]:
                    markets[asset] = (row["market"], row["connection"])
                    books.pop(asset, None)
                    synced[asset] = False
                    emitted[asset] = False
                    refreshes[asset] = 0
                    session_books[asset] = 0
                    total_books[asset] = 0
            elif kind == "remove_market":
                if row["market"] not in targets:
                    continue
                counts["removals"] += 1
                for asset in list(markets):
                    if markets[asset][0] == row["market"]:
                        del markets[asset]
                        books.pop(asset, None)
                        synced.pop(asset, None)
                        emitted.pop(asset, None)
            elif kind == "frame":
                if not relevant:
                    continue
                conn = row["connection"]
                payload = json.loads(row["text"])
                events = payload if isinstance(payload, list) else [payload]
                repeated = [e for e in events if e.get("event_type") == "book"
                            and e.get("asset_id") in books and synced[e["asset_id"]]
                            and markets.get(e["asset_id"]) == (e.get("market"), conn)]
                if repeated:
                    counts["checkpoint_frames"] += 1
                    assert counts["checkpoint_frames"] < 1000, "bound optional diagnostic scope"
                    write(output, dict(type="advance", millis=60_001))
                    for asset in books:
                        if synced[asset] and emitted[asset]:
                            ordinals[asset] += 1
                            counts["periodic_snapshots"] += 1
                touched = set()
                for event_index, event in enumerate(events):
                    if event.get("market") not in targets:
                        continue
                    event_type = event.get("event_type")
                    counts[event_type or "unknown"] += 1
                    if event_type == "book":
                        asset = event["asset_id"]
                        if markets.get(asset) != (event["market"], conn):
                            counts["unsubscribed_books"] += 1
                            continue
                        assert asset not in touched, "cannot checkpoint a repeated asset within one frame without splitting it"
                        touched.add(asset)
                        target = reference.levels(event)
                        total_books[asset] += 1
                        session_books[asset] += 1
                        if emitted[asset] and synced[asset]:
                            diff = reference.differences(books[asset], target)
                            refreshes[asset] += 1
                            checkpoint = dict(
                                ordinal=ordinals[asset], line=line, event_index=event_index,
                                connection=str(conn), market=event["market"], asset=asset,
                                initial=initial[asset], refresh_number=refreshes[asset],
                                session=sessions[conn] + 1, session_book_number=session_books[asset],
                                book_number=total_books[asset],
                                before=reference.export(books[asset]), incoming=reference.export(target),
                                before_received_us=latest[asset][0], before_exchange_us=latest[asset][1],
                                incoming_received_us=reference.micros(row["received_at"]),
                                incoming_exchange_us=int(event["timestamp"]) * 1000,
                                incoming_hash=event.get("hash"), differences=diff)
                            checkpoints.append(checkpoint)
                            if diff:
                                incidents.append(checkpoint)
                        elif emitted[asset]:
                            counts["catch_up_books"] += 1
                            counts["catch_up_levels"] += len(reference.differences(books[asset], target))
                        else:
                            counts["first_snapshots"] += 1
                            emitted[asset] = True
                            # Zero is the first snapshot's ordinal. Later
                            # AddMarket lifetimes keep the per-symbol count.
                            if asset in initial:
                                ordinals[asset] += 1
                            initial[asset] = dict(line=line, event_index=event_index,
                                                 received_at=row["received_at"],
                                                 exchange_ms=int(event["timestamp"]), hash=event.get("hash"))
                        books[asset] = target
                        synced[asset] = True
                        latest[asset] = (reference.micros(row["received_at"]), int(event["timestamp"]) * 1000)
                    elif event_type == "price_change":
                        for change in event["price_changes"]:
                            asset = change["asset_id"]
                            if markets.get(asset) != (event["market"], conn):
                                continue
                            touched.add(asset)
                            if not synced.get(asset):
                                counts["updates_before_baseline"] += 1
                                continue
                            side = "bids" if change["side"] == "BUY" else "asks"
                            assert change["side"] in ("BUY", "SELL")
                            price, qty = Decimal(change["price"]), Decimal(change["size"])
                            if qty:
                                books[asset][side][price] = qty
                            else:
                                books[asset][side].pop(price, None)
                            latest[asset] = (reference.micros(row["received_at"]), int(event["timestamp"]) * 1000)
                            counts["applied_level_updates"] += 1
                counts["frames"] += 1
            else:
                continue
            retained.write(raw)
            index.append(dict(source_line=line, type=kind))
            write(output, row)
        write(output, dict(type="advance", millis=1))

    different_levels = sum(len(p["differences"]) for p in incidents)
    signs = Counter("increase" if d["incoming"] > d["held"] else "decrease"
                    for p in incidents for d in p["differences"])
    window = dict(source="resource-confounded-live/frames.jsonl", connection="selected-conditions",
                  fixture=fixture_path.name, source_sha256=source_digest.hexdigest(),
                  source_bytes=source_bytes, books=counts["first_snapshots"], counts=dict(counts),
                  checkpoints=checkpoints)
    (args.output_dir / "expectations.json").write_text(json.dumps([window], indent=2) + "\n")
    (args.output_dir / "source-index.json").write_text(json.dumps(index, indent=2) + "\n")
    summary = dict(resource_confounded=True, reason="The process reached RLIMIT_NOFILE=1024. Connection failures and coverage are not venue evidence.",
                   selected_conditions={str(k): v for k, v in selected.items()},
                   source_sha256=source_digest.hexdigest(), source_bytes=source_bytes,
                   selected_original_bytes=retained_path.stat().st_size,
                   fixture_sha256=hashlib.sha256(fixture_path.read_bytes()).hexdigest(),
                   counts=dict(counts), repeat_books=len(checkpoints), different_books=len(incidents),
                   different_levels=different_levels, quantity_directions=dict(signs),
                   telemetry=telemetry,
                   incidents=[{k: v for k, v in p.items() if k not in ("before", "incoming")} for p in incidents])
    (args.output_dir / "decimal-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: summary[k] for k in ("resource_confounded", "source_bytes", "selected_original_bytes", "repeat_books", "different_books", "different_levels", "quantity_directions") }))


if __name__ == "__main__":
    main()
