#!/usr/bin/env python3
"""Build raw-order fixtures and independent Decimal expectations for saved windows.

Run from the repository root. Raw payload strings are copied intact. Only virtual
clock checkpoints are inserted; they expose the worker's pre-refresh state via
its ordinary periodic snapshots, without applying the incoming full book first.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

SCALE = Decimal(1_000_000)



def fixed(value):
    scaled = Decimal(value) * SCALE
    assert scaled == scaled.to_integral_value(), value
    return int(scaled)


def micros(value):
    delta = datetime.fromisoformat(value.replace("Z", "+00:00")) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def levels(event):
    result = {}
    for side in ("bids", "asks"):
        result[side] = {}
        for entry in event[side]:
            price, qty = Decimal(entry["price"]), Decimal(entry["size"])
            if qty:
                result[side][price] = qty
            else:
                result[side].pop(price, None)
    return result


def export(book):
    # Canonical map presentation only: event arrays are never sorted/reordered.
    return {side: [[fixed(p), fixed(q)] for p, q in sorted(book[side].items())]
            for side in ("bids", "asks")}


def differences(before, incoming):
    result = []
    for side in ("bids", "asks"):
        for price in sorted(before[side].keys() | incoming[side].keys()):
            old, new = before[side].get(price, Decimal(0)), incoming[side].get(price, Decimal(0))
            if old != new:
                result.append(dict(side="bid" if side == "bids" else "ask", price=fixed(price),
                                   held=fixed(old), incoming=fixed(new)))
    return result


def prepare(root, out, folder, connection, end):
    path = root / "research/recordings" / folder / "frames.jsonl"
    rows = []
    with path.open() as stream:
        for line, text in enumerate(stream, 1):
            if line > end:
                break
            row = json.loads(text)
            if row.get("connection") == connection:
                rows.append((line, row))
    asset_markets = {}
    for _, row in rows:
        if row["kind"] != "received":
            continue
        try:
            payload = json.loads(row["raw"])
        except (ValueError, TypeError):
            continue
        for event in payload if isinstance(payload, list) else [payload]:
            if event.get("event_type") == "book":
                asset_markets[event["asset_id"]] = event["market"]
    fixture, checkpoints = [], []
    books, latest, initial = {}, {}, {}
    refreshes = Counter()
    subscribed = set()
    counts = Counter()
    subscription = None
    for line, row in rows:
        kind = row["kind"]
        if kind == "opened":
            assert not books, "unexpected reconnect inside incident window"
            continue
        if kind == "sent":
            request = json.loads(row["raw"])
            assert request.get("type") == "market", "unexpected subscription change; extend control translation"
            assert not subscribed
            subscription = dict(line=line, received_at=row["received_at"], assets=len(request["assets_ids"]))
            grouped = {}
            for asset in request["assets_ids"]:
                subscribed.add(asset)
                if asset not in asset_markets:
                    counts["subscription_assets_without_window_book"] += 1
                    continue
                grouped.setdefault(asset_markets[asset], []).append(asset)
            for market, assets in grouped.items():
                fixture.append(dict(type="add_market", market=market, order_books=assets, connection=1))
            counts["subscriptions"] += 1
            counts["markets"] += len(grouped)
            continue
        assert kind == "received", (line, kind)
        raw = row["raw"]
        try:
            payload = json.loads(raw)
        except (ValueError, TypeError):
            payload = []
        events = payload if isinstance(payload, list) else [payload]
        repeats = [e for e in events if e.get("event_type") == "book" and e["asset_id"] in books]
        if repeats:
            # Current windows have singleton refresh frames. Do not silently
            # split/reorder an array to make a checkpoint possible.
            assert len(events) == 1 and len(repeats) == 1 and not isinstance(payload, list)
            fixture.append(dict(type="advance", millis=60_001))
        for index, event in enumerate(events):
            event_type = event.get("event_type")
            counts[event_type or "unknown"] += 1
            if event_type == "book":
                asset = event["asset_id"]
                assert asset in subscribed
                target = levels(event)
                if asset in books:
                    refreshes[asset] += 1
                    ordinal = len(checkpoints) + 1
                    # All initial bulk books precede the first checkpoint.
                    assert len(books) == len(asset_markets)
                    checkpoints.append(dict(
                        ordinal=ordinal, line=line, event_index=index, asset=asset,
                        refresh_number=refreshes[asset],
                        initial=initial[asset], subscription=subscription,
                        before=export(books[asset]), incoming=export(target),
                        before_received_us=latest[asset][0], before_exchange_us=latest[asset][1],
                        incoming_received_us=micros(row["received_at"]),
                        incoming_exchange_us=int(event["timestamp"]) * 1000,
                        incoming_hash=event.get("hash"), differences=differences(books[asset], target)))
                else:
                    assert not checkpoints, "late baseline changes periodic snapshot ordinal"
                    initial[asset] = dict(line=line, event_index=index, received_at=row["received_at"],
                                          exchange_ms=int(event["timestamp"]), hash=event.get("hash"))
                books[asset] = target
                latest[asset] = (micros(row["received_at"]), int(event["timestamp"]) * 1000)
            elif event_type == "price_change":
                for change in event["price_changes"]:
                    asset = change["asset_id"]
                    if asset not in subscribed:
                        counts["unsubscribed_changes"] += 1
                        continue
                    if asset not in books:
                        counts["updates_before_baseline"] += 1
                        continue
                    side = "bids" if change["side"] == "BUY" else "asks"
                    assert change["side"] in ("BUY", "SELL")
                    price, qty = Decimal(change["price"]), Decimal(change["size"])
                    if qty:
                        books[asset][side][price] = qty
                    else:
                        books[asset][side].pop(price, None)
                    latest[asset] = (micros(row["received_at"]), int(event["timestamp"]) * 1000)
                    counts["applied_level_updates"] += 1
        fixture.append(dict(type="frame", connection=1, received_at=row["received_at"], text=raw))
        counts["frames"] += 1
    fixture.append(dict(type="advance", millis=1))
    fixture_path = out / f"{connection}.jsonl"
    fixture_path.write_text("".join(json.dumps(e, separators=(",", ":")) + "\n" for e in fixture))
    prefix_digest = hashlib.sha256()
    with path.open("rb") as stream:
        for number, raw in enumerate(stream, 1):
            if number > end:
                break
            prefix_digest.update(raw)
    return dict(source=path.relative_to(root).as_posix(), connection=connection, end_line=end,
                source_prefix_sha256=prefix_digest.hexdigest(),
                fixture=fixture_path.name, fixture_sha256=hashlib.sha256(fixture_path.read_bytes()).hexdigest(),
                counts=dict(counts), books=len(books), checkpoints=checkpoints)


def verify_rust(root, args, windows, summary):
    expected = {(w["connection"], p["line"], p["asset"]): p
                for w in windows for p in w["checkpoints"]}
    by_workers = []
    previous = None
    for n in (1, 2, 4):
        actual = json.loads((args.output_dir / f"rust-before-n{n}.json").read_text())
        assert len(actual) == len(expected) == 42
        assert len({(r["connection"], r["line"], r["asset"]) for r in actual}) == 42
        for row in actual:
            checkpoint = expected[(row["connection"], row["line"], row["asset"])]
            assert row["before"] == checkpoint["before"]
            assert row["received_us"] == checkpoint["before_received_us"]
            assert row["exchange_us"] == checkpoint["before_exchange_us"]
        if previous is not None:
            assert actual == previous, "worker-count-dependent before books"
        previous = actual
        by_workers.append(dict(workers=n, complete_before_books=42, rust_decimal_disagreements=0,
                               before_book_sha256=hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()))
    code = json.loads((args.output_dir / "code-at-generation.json").read_text())
    assert all(hashlib.sha256((root / p).read_bytes()).hexdigest() == h for p, h in code.items()), "production code changed during replay"
    rust = json.loads((args.output_dir / "rust-summary.json").read_text())
    assert len(rust) == 9
    for row in rust:
        assert row["rust_decimal_disagreements"] == 0
    evidence = dict(
        method="Unmodified raw frame strings; all per-connection events and arrays retain arrival order. Before each singleton repeated full-book frame, advance the paused clock by 60001ms and read the worker's ordinary periodic snapshot through the supplied SBE reader. Compare both complete sides and freshness timestamps before applying the incoming full book.",
        restrictions="No trade subtraction, event sorting, event deduplication, complementary inference, or warmup exclusion. Map keys are canonicalized only for comparing complete book contents.",
        limitations=["Only saved prefixes; cannot identify exchange intent or prove absence of upstream/capture loss.",
                     "Duplicate feeds replay independently because the base collector has one connection per market.",
                     "Two bulk-4 subscription assets have no book or price change in the prefix; no market can be mapped for them. All 1798 observed books and every raw received frame are retained.",
                     "Virtual checkpoints change periodic output cadence, not source events or their receive/exchange timestamps. This is a correctness check, not a latency measurement.",
                     "Raw recordings are local and untracked; the raw test is explicitly ignored by default. The compact synthetic regression runs by default."],
        windows=summary, rust_runs=rust, comparisons=by_workers,
        totals=dict(complete_before_comparisons=126, distinct_repeat_books=42, exact_to_incoming=36,
                    divergent_to_incoming=6, divergent_levels=6, distinct_exchange_timestamps=2,
                    rust_decimal_disagreements=0),
        source_code_sha256=code,
        harness_sha256={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in [root / "tests/book_incidents.rs", root / "tests/fixtures/book-incidents-regression.jsonl",
                                  root / "tests/fixtures/book-incidents-windows.json", Path(__file__).resolve()]})
    path = args.results or args.output_dir / "verified-results.json"
    path.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(dict(verified_rust_before_books=126, worker_counts=[1, 2, 4], disagreements=0)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/submission-replay"))
    parser.add_argument("--verify-rust", action="store_true")
    parser.add_argument("--results", type=Path, help="write compact verified evidence here")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((root / "tests/fixtures/book-incidents-windows.json").read_text())
    windows = []
    for expected in manifest:
        folder = Path(expected["source"]).parent.name
        window = prepare(root, args.output_dir, folder, expected["connection"], expected["end_line"])
        assert window["source_prefix_sha256"] == expected["source_prefix_sha256"], "saved source prefix changed"
        assert len(window["checkpoints"]) == expected["repeat_books"]
        incidents = [{k: p[k] for k in ("line", "event_index", "asset", "initial", "incoming_exchange_us", "incoming_hash", "differences")}
                     for p in window["checkpoints"] if p["differences"]]
        assert incidents == expected["incidents"], "saved incident signatures changed"
        windows.append(window)
    assert sum(len(w["checkpoints"]) for w in windows) == 42
    different = [p for w in windows for p in w["checkpoints"] if p["differences"]]
    assert len(different) == 6 and sum(len(p["differences"]) for p in different) == 6
    assert all(p["refresh_number"] == 1 for p in different)
    assert len({p["incoming_exchange_us"] for p in different}) == 2
    (args.output_dir / "expectations.json").write_text(json.dumps(windows, indent=2) + "\n")
    summary = [{k: v for k, v in w.items() if k != "checkpoints"} | dict(
        repeat_books=len(w["checkpoints"]),
        different_books=sum(bool(p["differences"]) for p in w["checkpoints"]),
        incidents=[{k: v for k, v in p.items() if k not in ("before", "incoming")}
                   for p in w["checkpoints"] if p["differences"]]) for w in windows]
    (args.output_dir / "decimal-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if args.verify_rust:
        verify_rust(root, args, windows, summary)
    else:
        code = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted((root / "src").rglob("*.rs"))}
        (args.output_dir / "code-at-generation.json").write_text(json.dumps(code, indent=2) + "\n")
    print(json.dumps(dict(windows=len(windows), repeat_books=42, different_books=6, different_levels=6)))


if __name__ == "__main__":
    main()
