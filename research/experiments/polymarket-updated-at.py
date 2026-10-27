"""Bounded read-only test of Gamma updatedAt listings; no collector changes.

Run from the repository root. Raw observations go into ignored data/.
"""
import argparse
import concurrent.futures
import datetime as dt
import importlib.util
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://gamma-api.polymarket.com/markets/keyset"
FLAGS = ("active", "closed", "archived", "acceptingOrders", "enableOrderBook")
FIELDS = ("id", "version", *FLAGS, "updatedAt",
          "acceptingOrdersTimestamp", "closedTime", "createdAt", "endDate")


def now():
    return dt.datetime.now(dt.timezone.utc)


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def compact(row):
    return {key: row.get(key) for key in FIELDS}


def cached_seed(path):
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location(
        "adoption_reader", Path(__file__).with_name("polymarket-protocol-adoption.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = {}
    with open(path) as stream:
        reader = module.Reader(stream)
        for key in reader.members():
            if key != "catalog":
                reader.value()
                continue
            for section in reader.members():
                assert section == "markets"
                for market_id in reader.members():
                    result[market_id] = compact(reader.value()["metadata"])
                return result
    raise ValueError("seed has no market catalog")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=120)
    parser.add_argument("--interval", type=float, default=20)
    parser.add_argument("--seed", default="data/markets.json")
    parser.add_argument("--fresh", action="store_true",
                        help="Append a unique experimental query to avoid cached responses")
    args = parser.parse_args()
    tag = now().strftime("%Y%m%dT%H%M%SZ")
    raw_path = Path("data") / ("updated-at-" + tag + ".jsonl")
    raw_path.parent.mkdir(exist_ok=True)
    summary = {"started_at": now().isoformat(), "requests": 0, "http_errors": [],
               "ordering_checks": [], "selectors": [], "rounds": [],
               "raw_observations": str(raw_path), "v1_only": True,
               "fresh_query_control": args.fresh}
    observed = {}
    previous = {}
    changes = []

    with raw_path.open("x") as raw, concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        baseline_future = pool.submit(cached_seed, args.seed)

        def get(params, kind):
            params = params + ([("_research_poll", str(time.time_ns()))] if args.fresh else [])
            for attempt in range(3):
                summary["requests"] += 1
                try:
                    request = urllib.request.Request(BASE + "?" + urllib.parse.urlencode(params),
                                                     headers={"User-Agent": "engineering-challenge-research/0.1"})
                    with urllib.request.urlopen(request, timeout=25) as response:
                        page = json.load(response)
                        headers = {k: response.headers.get(k) for k in
                                   ("Date", "Age", "Cache-Control", "X-Cache", "CF-Cache-Status")}
                    rows = [compact(row) for row in page["markets"]]
                    assert all(row["version"] == "v1" for row in rows)
                    raw.write(json.dumps({"received_at": now().isoformat(), "kind": kind,
                                          "query": params, "headers": headers, "rows": rows}) + "\n")
                    raw.flush()
                    observed.update((row["id"], row) for row in rows)
                    return rows, page.get("next_cursor")
                except urllib.error.HTTPError as error:
                    summary["http_errors"].append({"kind": kind, "status": error.code})
                    if error.code != 429 and error.code < 500:
                        raise
                    if attempt == 2:
                        raise
                    time.sleep(0.5 * (attempt + 1))

        def listing(closed, field, pages, kind):
            params = [("version", "v1"), ("closed", str(closed).lower()),
                      ("order", field + ",id"), ("ascending", "false"), ("limit", "100")]
            rows, cursors = [], set()
            cursor = None
            for _ in range(pages):
                page, cursor = get(params + ([("after_cursor", cursor)] if cursor else []), kind)
                rows.extend(page)
                if not cursor:
                    break
                assert cursor not in cursors
                cursors.add(cursor)
            values = [timestamp(row[field]) for row in rows if row.get(field)]
            summary["ordering_checks"].append({"kind": kind, "field": field,
                "rows": len(rows), "unique_ids": len({r["id"] for r in rows}),
                "null_timestamps": sum(not r.get(field) for r in rows),
                "time_inversions": sum(a < b for a, b in zip(values, values[1:])),
                "newest": rows[0].get(field) if rows else None,
                "oldest": rows[-1].get(field) if rows else None,
                "has_more": cursor is not None})
            return rows

        # Establish listing behavior and include known older closures.
        listing(False, "updatedAt", 10, "initial_updated_open")
        listing(True, "updatedAt", 10, "initial_updated_closed")
        listing(True, "closedTime", 5, "recent_closures")

        # Select outcomes likely to close during this bounded observation.
        window_start, window_end = now() - dt.timedelta(seconds=120), now() + dt.timedelta(seconds=args.duration)
        params = [("version", "v1"), ("closed", "false"), ("limit", "100"),
                  ("order", "endDate,id"), ("ascending", "true"),
                  ("end_date_min", window_start.isoformat()), ("end_date_max", window_end.isoformat())]
        ending, _ = get(params, "ending_soon_candidates")
        summary["selectors"].append({"kind": "ending_soon", "rows": len(ending),
            "all_in_requested_window": all(r.get("endDate") and window_start <= timestamp(r["endDate"]) <= window_end for r in ending)})
        pending, _ = get([("version", "v1"), ("closed", "false"), ("limit", "100"),
                          ("order", "acceptingOrders,id"), ("ascending", "true")], "pending_candidates")
        busy, _ = get([("version", "v1"), ("closed", "false"), ("limit", "100"),
                       ("order", "volume24hr,id"), ("ascending", "false")], "busy_candidates")
        tracked = sorted({r["id"] for r in ending + pending + busy}, key=int)
        summary["tracked_ids"] = tracked
        started = time.monotonic()
        round_index = 0
        while True:
            round_started = now().isoformat()
            for start in range(0, len(tracked), 100):
                ids = tracked[start:start + 100]
                rows, cursor = get([("version", "v1"), ("closed", "any"), ("limit", "100")]
                                   + [("id", mid) for mid in ids], "tracked_status")
                # Exactly 100 results can carry an otherwise empty next page.
                assert {r["id"] for r in rows} == set(ids)
                for row in rows:
                    old = previous.get(row["id"])
                    if old:
                        fields = [f for f in FLAGS if old[f] != row[f]]
                        if fields:
                            changes.append({"observed_at": now().isoformat(), "fields": fields,
                                            "old": old, "new": row})
                    previous[row["id"]] = row
            listing(False, "updatedAt", 2, "round_updated_open")
            listing(True, "updatedAt", 2, "round_updated_closed")
            summary["rounds"].append({"round": round_index, "started_at": round_started,
                                      "completed_at": now().isoformat(), "flag_changes_so_far": len(changes)})
            print(json.dumps({"event": "observation_round", **summary["rounds"][-1]}), flush=True)
            elapsed = time.monotonic() - started
            if elapsed >= args.duration:
                break
            round_index += 1
            time.sleep(max(0, min(args.interval * round_index - elapsed, args.duration - elapsed)))

        baseline = baseline_future.result()
        comparisons = []
        for mid, row in observed.items():
            old = baseline.get(mid)
            if old:
                fields = [f for f in FLAGS if old[f] != row[f]]
                if fields:
                    comparisons.append({"fields": fields, "old": old, "new": row})
        summary["observed_unique_markets"] = len(observed)
        summary["seed_markets"] = len(baseline)
        summary["seed_status_changes"] = comparisons
        summary["live_status_changes"] = changes
        summary["completed_at"] = now().isoformat()
        summary["complete"] = True
    output = Path("research/experiments/polymarket-updated-at-" +
                  ("fresh-" if args.fresh else "") + "results.json")
    output.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"event": "complete", "output": str(output),
                      "requests": summary["requests"], "seed_changes": len(comparisons),
                      "live_changes": len(changes)}), flush=True)


if __name__ == "__main__":
    main()
