#!/usr/bin/env python3
"""Bounded event-discovery checks against independent Gamma market samples.

Run from the repository root. Uses only public GETs and the Python standard
library. Saves counts, IDs, and flags; never saves raw market descriptions.
This is a response/coverage probe, not a complete-catalog or speed benchmark.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request


BASE = "https://gamma-api.polymarket.com"


class Client:
    def __init__(self, requests_per_second):
        self.nonce = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.spacing = 1 / requests_per_second
        self.next_request = 0.0
        self.requests = []

    def get(self, path, params):
        params = list(params) + [("_nonce", "events-discovery-" + self.nonce)]
        url = BASE + path + "?" + urllib.parse.urlencode(params)
        for attempt in range(5):
            time.sleep(max(0, self.next_request - time.monotonic()))
            self.next_request = time.monotonic() + self.spacing
            request = urllib.request.Request(url, headers={
                "User-Agent": "orderbook-collector-research/0.1",
                "Accept": "application/json",
            })
            try:
                with urllib.request.urlopen(request, timeout=35) as response:
                    raw = response.read()
                    status = response.status
            except urllib.error.HTTPError as error:
                status, raw = error.code, error.read()
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                self.requests.append({"path": path, "transport_error": type(error).__name__})
                if attempt < 4:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f"transport failed after retries for {path}") from error
            entry = {
                "path": path,
                "params": [[k, v] for k, v in params
                           if k not in ("after_cursor", "_nonce")],
                "has_input_cursor": any(k == "after_cursor" for k, _ in params),
                "status": status, "response_bytes": len(raw),
            }
            self.requests.append(entry)
            if status == 429 or status >= 500:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                    continue
            data = json.loads(raw)
            if status != 200:
                entry["error"] = data
            return status, data
        raise RuntimeError("request attempts exhausted")


def walk(client, kind, params, max_pages):
    rows, seen, cursor = [], set(), None
    for page in range(max_pages):
        query = list(params) + [("limit", "100")]
        if cursor:
            query.append(("after_cursor", cursor))
        status, data = client.get("/" + kind + "/keyset", query)
        if status != 200 or not isinstance(data, dict):
            raise RuntimeError(f"{kind} keyset returned status {status}")
        chunk = data.get(kind)
        if not isinstance(chunk, list):
            raise RuntimeError(f"missing {kind} array")
        rows.extend(chunk)
        following = data.get("next_cursor")
        if not following:
            return rows, {"pages": page + 1, "exhausted": True}
        if not chunk or following in seen:
            raise RuntimeError(f"empty page with cursor or cursor cycle in {kind}")
        seen.add(following)
        cursor = following
    return rows, {"pages": max_pages, "exhausted": False}


def desired(market):
    """Mirror current collector eligibility without an activity filter."""
    try:
        tokens = json.loads(market.get("clobTokenIds") or "[]")
    except (ValueError, TypeError):
        return False
    condition = market.get("conditionId") or ""
    return (
        market.get("version") == "v1"
        and market.get("active") is True
        and market.get("closed") is False
        and market.get("archived") is not True
        and market.get("enableOrderBook") is True
        and len(condition) == 66 and condition.startswith("0x")
        and all(c in "0123456789abcdefABCDEF" for c in condition[2:])
        and isinstance(tokens, list) and len(tokens) == 2
        and len(set(str(token) for token in tokens)) == 2
        and all(isinstance(token, str) and token.isascii() and token.isdecimal()
                for token in tokens)
    )


def event_summary(events):
    children = [m for event in events for m in event.get("markets") or []]
    eligible = {str(m["id"]) for m in children if desired(m)}
    return {
        "events": len(events), "nested_market_occurrences": len(children),
        "distinct_market_ids": len({str(m["id"]) for m in children}),
        "desired_v1_market_ids": len(eligible),
        "closed_child_occurrences": sum(m.get("closed") is True for m in children),
        "child_versions": dict(Counter(m.get("version", "missing") for m in children)),
        "max_children_per_event": max((len(e.get("markets") or []) for e in events), default=0),
        "desired_children_in_closed_events": sum(
            desired(m) for e in events if e.get("closed") is True
            for m in e.get("markets") or []),
    }


def full_catalog(client, kind, checkpoint_dir):
    """Keep only eligible market identities, not full response bodies."""
    identities, seen_cursors, cursor = {}, set(), None
    counts = Counter()
    completed_pages = 0
    checkpoint = checkpoint_dir / (kind + ".json.gz")
    if checkpoint.exists():
        with gzip.open(checkpoint, "rt") as stream:
            saved = json.load(stream)
        client.nonce, client.requests = saved["nonce"], saved["requests"]
        identities, counts = saved["identities"], Counter(saved["counts"])
        seen_cursors, cursor = set(saved["seen_cursors"]), saved["cursor"]
        completed_pages = saved["pages"]
        print(json.dumps({"resumed": kind, "pages": completed_pages,
                          "exhausted": saved["exhausted"]}), flush=True)
        if saved["exhausted"]:
            return identities, {"pages": completed_pages, "exhausted": True, **counts}

    def save(exhausted):
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        temporary = checkpoint.with_suffix(".tmp")
        with gzip.open(temporary, "wt", compresslevel=1) as stream:
            json.dump({"nonce": client.nonce, "requests": client.requests,
                       "identities": identities, "counts": counts,
                       "seen_cursors": sorted(seen_cursors), "cursor": cursor,
                       "pages": completed_pages, "exhausted": exhausted}, stream)
        temporary.replace(checkpoint)

    for page in range(completed_pages + 1, 10001):
        params = [("closed", "false"), ("version", "v1"), ("limit", "100"),
                  ("order", "id"), ("ascending", "false")]
        if cursor:
            params.append(("after_cursor", cursor))
        try:
            status, data = client.get("/" + kind + "/keyset", params)
        except Exception:
            save(False)
            raise
        if status != 200 or not isinstance(data, dict) or not isinstance(data.get(kind), list):
            raise RuntimeError(f"invalid {kind} response on page {page}")
        rows = data[kind]
        counts["rows"] += len(rows)
        markets = rows if kind == "markets" else [m for e in rows for m in e.get("markets") or []]
        counts["market_occurrences"] += len(markets)
        counts["closed_market_occurrences"] += sum(m.get("closed") is True for m in markets)
        for market in markets:
            if desired(market):
                identity = hashlib.sha256(json.dumps([
                    market.get("conditionId"), market.get("clobTokenIds")
                ]).encode()).hexdigest()
                key = str(market["id"])
                if key in identities and identities[key] != identity:
                    counts["conflicting_identity_occurrences"] += 1
                identities[key] = identity
        following = data.get("next_cursor")
        completed_pages = page
        if page == 1 or page % 100 == 0 or not following:
            print(json.dumps({"full_scan": kind, "pages": page,
                              "rows": counts["rows"], "desired": len(identities),
                              "exhausted": not following}), flush=True)
        if not following:
            save(True)
            return identities, {"pages": page, "exhausted": True, **counts}
        if not rows or following in seen_cursors:
            raise RuntimeError(f"empty page with cursor or cursor cycle in {kind}")
        seen_cursors.add(following)
        cursor = following
        if page % 100 == 0:
            save(False)
    raise RuntimeError(f"{kind} scan exceeded 10,000 pages")


def full_comparison(client, checkpoint_dir):
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "scope": "Sequential exhausted v1 open-market and open-event keyset walks, with current-status checks of set differences. Not an atomic snapshot or a controlled speed benchmark.",
        "requests_per_second": 1 / client.spacing,
    }
    markets, market_counts = full_catalog(client, "markets", checkpoint_dir)
    events, event_counts = full_catalog(client, "events", checkpoint_dir)
    market_only = sorted(markets.keys() - events.keys())
    event_only = sorted(events.keys() - markets.keys())
    rechecked = {}
    differences = market_only + event_only
    for start in range(0, len(differences), 100):
        params = [("closed", "any"), ("version", "v1")] + [
            ("id", key) for key in differences[start:start + 100]]
        rows, paging = walk(client, "markets", params, 3)
        if not paging["exhausted"]:
            raise RuntimeError("difference lookup did not exhaust its ID-filtered results")
        rechecked.update((str(m["id"]), m) for m in rows)
    def status_groups(keys):
        return {
            "still_desired_ids": [key for key in keys if key in rechecked and desired(rechecked[key])],
            "no_longer_desired_ids": [key for key in keys if key in rechecked and not desired(rechecked[key])],
            "not_returned_unknown_ids": [key for key in keys if key not in rechecked],
        }
    report.update({
        "markets_scan": {**market_counts, "desired_market_ids": len(markets)},
        "events_scan": {**event_counts, "desired_market_ids": len(events)},
        "intersection": len(markets.keys() & events.keys()),
        "markets_only": {"count": len(market_only), **status_groups(market_only)},
        "events_only": {"count": len(event_only), **status_groups(event_only)},
        "identity_difference_ids": sorted(key for key in markets.keys() & events.keys()
                                          if markets[key] != events[key]),
        "desired_id_set_sha256": {
            "markets": hashlib.sha256("\n".join(sorted(markets)).encode()).hexdigest(),
            "events": hashlib.sha256("\n".join(sorted(events)).encode()).hexdigest(),
        },
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "requests": client.requests,
    })
    return report


def inspect_differences(client, comparison_path):
    comparison = json.loads(comparison_path.read_text())
    keys = comparison["markets_only"]["still_desired_ids"]
    markets, parents = {}, {}
    for start in range(0, len(keys), 100):
        params = [("closed", "any"), ("version", "v1")] + [
            ("id", key) for key in keys[start:start + 100]]
        rows, paging = walk(client, "markets", params, 3)
        if not paging["exhausted"]:
            raise RuntimeError("difference market lookup did not finish")
        markets.update((str(m["id"]), m) for m in rows)
    parent_ids = sorted({str(e["id"]) for m in markets.values() for e in m.get("events") or []})
    for start in range(0, len(parent_ids), 50):
        params = [("closed", "any"), ("version", "v1")] + [
            ("id", key) for key in parent_ids[start:start + 50]]
        rows, paging = walk(client, "events", params, 3)
        if not paging["exhausted"]:
            raise RuntimeError("difference event lookup did not finish")
        parents.update((str(e["id"]), e) for e in rows)
    listed_parent_ids = set(parents)
    direct_parent_statuses = {}
    for parent_id in sorted(set(parent_ids) - listed_parent_ids):
        status, data = client.get("/events/" + parent_id, [])
        direct_parent_statuses[parent_id] = status
        if status == 200 and isinstance(data, dict):
            parents[parent_id] = data
    probe_ids = sorted(listed_parent_ids)[:1] + sorted(set(parent_ids) - listed_parent_ids)[:2]
    filter_checks = probe_parent_filters(client, probe_ids)
    checks, classifications = [], Counter()
    flags = ("version", "active", "closed", "archived", "enableOrderBook")
    for key in keys:
        market = markets.get(key)
        links = market.get("events") or [] if market else []
        parent_checks = []
        for link in links:
            parent_id = str(link["id"])
            parent = parents.get(parent_id)
            child = next((m for m in parent.get("markets") or [] if str(m["id"]) == key), None) if parent else None
            parent_checks.append({
                "id": parent_id, "returned": parent is not None,
                "returned_by_listing": parent_id in listed_parent_ids,
                "active": parent.get("active") if parent else None,
                "archived": parent.get("archived") if parent else None,
                "closed": parent.get("closed") if parent else None,
                "child_found": child is not None,
                "child_desired": desired(child) if child else False,
                "child_flags": {f: child.get(f) for f in flags} if child else None,
            })
        if market is None:
            classification = "market_lookup_missing"
        elif not desired(market):
            classification = "no_longer_desired"
        elif not links:
            classification = "no_parent_link"
        elif any(p["closed"] is False and p["child_desired"]
                 and p["returned_by_listing"] for p in parent_checks):
            classification = "present_in_open_event_now"
        elif any(p["closed"] is False and p["child_desired"] for p in parent_checks):
            classification = "eligible_child_in_unlisted_open_event"
        elif any(p["closed"] is True and p["child_desired"] for p in parent_checks):
            classification = "eligible_child_only_in_closed_events"
        elif any(p["closed"] is False and p["child_found"] for p in parent_checks):
            classification = "child_ineligible_in_open_event"
        elif not any(p["returned"] for p in parent_checks):
            classification = "parent_lookup_missing"
        else:
            classification = "market_missing_from_parent_payload"
        classifications[classification] += 1
        checks.append({"market_id": key, "classification": classification,
                       "market_flags": {f: market.get(f) for f in flags} if market else None,
                       "parents": parent_checks})
    return {"checked_at": datetime.now(timezone.utc).isoformat(),
            "source_comparison": comparison_path.name,
            "scope": "Recheck every still-eligible market missed by the completed open-event walk, using parent lookups with closed=any.",
            "classifications": dict(classifications), "checks": checks,
            "direct_parent_statuses": direct_parent_statuses,
            "filter_checks": filter_checks,
            "requests": client.requests}


def probe_parent_filters(client, ids):
    base = [("closed", "any"), ("version", "v1"), ("limit", "100")] + [("id", key) for key in ids]
    cases = [
        ("/events/keyset", []),
        ("/events/keyset", [("include_children", "true")]),
        ("/events/keyset", [("active", "false")]),
        ("/events/keyset", [("active", "false"), ("archived", "true")]),
        ("/events/keyset", [("version", "v2")]),
        ("/events", [("active", "false"), ("archived", "true")]),
    ]
    results = []
    for path, extra in cases:
        status, data = client.get(path, base + extra)
        rows = (data.get("events", []) if isinstance(data, dict) else data) if status == 200 else []
        results.append({"path": path, "extra": extra, "requested_ids": ids,
                        "status": status, "returned_events": [
                            {k: event.get(k) for k in ("id", "active", "closed", "archived")}
                            for event in rows]})
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pages", type=int, default=3,
                        help="pages per endpoint per ID-order direction")
    parser.add_argument("--requests-per-second", type=float, default=4)
    parser.add_argument("--full-comparison", action="store_true",
                        help="exhaust both listings instead of bounded samples; use a separate --output")
    parser.add_argument("--checkpoint-dir", type=Path,
                        help="resume full scans from this directory; use a fresh directory for a fresh comparison")
    parser.add_argument("--inspect-differences", type=Path,
                        help="investigate still-eligible market-only IDs from a full-comparison JSON")
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name(
        "polymarket-events-discovery-results.json"))
    args = parser.parse_args()
    if args.pages < 1 or args.requests_per_second <= 0:
        parser.error("pages and requests-per-second must be positive")
    client = Client(args.requests_per_second)
    if args.inspect_differences:
        if args.full_comparison or args.output == Path(__file__).with_name("polymarket-events-discovery-results.json"):
            parser.error("--inspect-differences requires a separate --output and excludes --full-comparison")
        report = inspect_differences(client, args.inspect_differences)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"classifications": report["classifications"],
                          "requests": len(client.requests)}), flush=True)
        return
    if args.full_comparison:
        if args.output == Path(__file__).with_name("polymarket-events-discovery-results.json"):
            parser.error("--full-comparison requires a separate --output")
        checkpoint_dir = args.checkpoint_dir or args.output.with_suffix(".checkpoints")
        report = full_comparison(client, checkpoint_dir)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({key: value for key, value in report.items() if key != "requests"}), flush=True)
        return
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "scope": "Oldest/newest bounded samples, not a full-catalog coverage proof or speed benchmark.",
        "pages_per_direction": args.pages, "samples": [],
    }
    sampled_markets, sampled_events = {}, {}
    for ascending in ("false", "true"):
        for kind in ("markets", "events"):
            params = [("closed", "false"), ("order", "id"), ("ascending", ascending)]
            if kind == "markets":
                params.append(("version", "v1"))
            rows, paging = walk(client, kind, params, args.pages)
            summary = {"kind": kind, "ascending": ascending, **paging, "rows": len(rows)}
            if kind == "events":
                summary.update(event_summary(rows))
                sampled_events.update((str(row["id"]), row) for row in rows)
            else:
                summary["desired"] = sum(desired(row) for row in rows)
                sampled_markets.update((str(row["id"]), row) for row in rows)
            report["samples"].append(summary)
            print(json.dumps(summary), flush=True)

    eligible = {key: m for key, m in sampled_markets.items() if desired(m)}
    parent_ids = sorted({str(e["id"]) for m in eligible.values() for e in m.get("events") or []})
    parents = {}
    for start in range(0, len(parent_ids), 50):
        params = [("closed", "any")] + [("id", key) for key in parent_ids[start:start + 50]]
        rows, paging = walk(client, "events", params, 3)
        if not paging["exhausted"]:
            raise RuntimeError("parent lookup did not exhaust its ID-filtered results")
        parents.update((str(row["id"]), row) for row in rows)
    open_children = {str(m["id"]) for event in parents.values()
                     if event.get("closed") is False for m in event.get("markets") or []}
    desired_open_children = {str(m["id"]): m for event in parents.values()
                             if event.get("closed") is False
                             for m in event.get("markets") or [] if desired(m)}
    all_children = {str(m["id"]) for event in parents.values() for m in event.get("markets") or []}
    no_parent = [key for key, m in eligible.items() if not m.get("events")]
    report["independent_market_check"] = {
        "desired_market_ids": len(eligible), "referenced_parent_ids": len(parent_ids),
        "returned_parent_ids": len(parents),
        "desired_markets_without_parent_ids": no_parent,
        "parent_ids_not_returned": sorted(set(parent_ids) - parents.keys()),
        "desired_market_ids_missing_from_all_parent_payloads": sorted(set(eligible) - all_children),
        "desired_market_ids_missing_from_open_parent_payloads": sorted(set(eligible) - open_children),
        "desired_market_ids_missing_or_ineligible_in_open_parent_payloads": sorted(
            set(eligible) - desired_open_children.keys()),
        "condition_or_token_ids_differ": [key for key, market in eligible.items()
            if key in desired_open_children and any(
                market.get(field) != desired_open_children[key].get(field)
                for field in ("conditionId", "clobTokenIds"))],
        "parent_summary": event_summary(list(parents.values())),
    }
    status, error = client.get("/markets", [("closed", "false"), ("limit", "1"), ("offset", "100000")])
    report["deep_offset_probe"] = {"status": status, "body": error if status != 200 else "accepted"}
    original = ["A", "B", "C", "D"]
    first_page = original[:2]
    after_removal = [x for x in original if x != "A"]
    report["deterministic_offset_example"] = {
        "original": original, "first_page": first_page,
        "after_removing_A": after_removal,
        "next_offset_page": after_removal[2:4],
        "cursor_after_B_page": after_removal[after_removal.index("B") + 1:][:2],
    }
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report["requests"] = client.requests
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"independent_market_check": report["independent_market_check"],
                      "deep_offset_probe": report["deep_offset_probe"],
                      "requests": len(client.requests)}), flush=True)


if __name__ == "__main__":
    main()
