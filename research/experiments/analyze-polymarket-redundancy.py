"""Offline comparison of independently captured public market streams."""

import collections
import hashlib
import json
import math
import sys
from decimal import Decimal
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def distribution(values):
    return {
        "count": len(values),
        "median": percentile(values, 0.5),
        "p95": percentile(values, 0.95),
        "max": max(values) if values else None,
    }


def book_digest(book):
    if book is None:
        return None
    return fingerprint({
        side: [[str(p.normalize()), str(q.normalize())] for p, q in sorted(levels.items())]
        for side, levels in book.items()
    })


def analyze(directory):
    root = Path(directory)
    meta = json.loads((root / "metadata.json").read_text())
    asset = meta["market"]["yes_asset_id"]
    count = meta["connections"]
    start = meta["started_mono_ms"] + meta["warmup_seconds"] * 1000
    end = start + meta["observation_seconds"] * 1000
    by_fingerprint = collections.defaultdict(lambda: collections.defaultdict(list))
    streams = []
    summaries = []
    timestamp_groups = collections.defaultdict(set)
    event_payloads = {}
    hash_values = collections.defaultdict(set)
    wire_counts = collections.defaultdict(lambda: collections.Counter())
    for connection in range(1, count + 1):
        book = None
        initial = None
        stream = []
        types = collections.Counter()
        unrelated_types = collections.Counter()
        occurrences = collections.Counter()
        regressions = []
        previous_timestamp = None
        parse_errors = []
        checkpoint_differences = []
        row_count = 0
        for line in (root / f"connection-{connection}.jsonl").read_text().splitlines():
            row = json.loads(line)
            if row["kind"] != "received":
                continue
            row_count += 1
            if row["raw"].strip() == "PONG":
                types["PONG"] += 1
                continue
            try:
                payload = json.loads(row["raw"])
            except ValueError:
                parse_errors.append(row["raw"])
                continue
            wire_counts[hashlib.sha256(row["raw"].encode()).hexdigest()][connection] += 1
            for event in payload if isinstance(payload, list) else [payload]:
                kind = event.get("event_type", "unknown")
                types[kind] += 1
                relevant = (
                    event.get("asset_id") == asset
                    or kind == "price_change" and any(c.get("asset_id") == asset for c in event.get("price_changes", []))
                    or kind == "market_resolved" and event.get("market") == meta["market"]["condition_id"]
                )
                if not relevant:
                    unrelated_types[kind] += 1
                    continue
                is_initial = False
                if kind == "book" and event.get("asset_id") == asset:
                    replacement = {
                        side: {Decimal(level["price"]): Decimal(level["size"])
                               for level in event[side] if Decimal(level["size"]) != 0}
                        for side in ("bids", "asks")
                    }
                    if initial is None:
                        is_initial = True
                        initial = {"receipt_ms": row["mono_ms"], "exchange_timestamp": event.get("timestamp"),
                                   "hash": event.get("hash"), "state": book_digest(replacement)}
                    elif book_digest(book) != book_digest(replacement):
                        changes = [
                            {"side": side, "price": str(price), "before": str(book[side].get(price, 0)),
                             "after": str(replacement[side].get(price, 0))}
                            for side in ("bids", "asks")
                            for price in sorted(book[side].keys() | replacement[side].keys())
                            if book[side].get(price, 0) != replacement[side].get(price, 0)
                        ]
                        checkpoint_differences.append({"timestamp": event.get("timestamp"), "changes": changes})
                    book = replacement
                elif kind == "price_change":
                    if book is None:
                        parse_errors.append("price_change before initial snapshot")
                    else:
                        for change in event["price_changes"]:
                            if change["asset_id"] != asset:
                                continue
                            side = {"BUY": "bids", "SELL": "asks"}[change["side"]]
                            price, quantity = Decimal(change["price"]), Decimal(change["size"])
                            if quantity == 0:
                                book[side].pop(price, None)
                            else:
                                book[side][price] = quantity
                event_id = fingerprint(event)
                occurrences[event_id] += 1
                token = f"{event_id}:{occurrences[event_id]}"
                item = {"connection": connection, "time_ms": row["mono_ms"], "event_id": event_id,
                        "token": token, "kind": kind, "timestamp": event.get("timestamp"),
                        "initial": is_initial, "state": book_digest(book)}
                stream.append(item)
                by_fingerprint[event_id][connection].append(item)
                event_payloads[event_id] = event
                if not is_initial and event.get("timestamp") is not None:
                    timestamp = int(event["timestamp"])
                    if previous_timestamp is not None and timestamp < previous_timestamp:
                        regressions.append({"previous": previous_timestamp, "current": timestamp, "kind": kind})
                    previous_timestamp = timestamp
                    timestamp_groups[(kind, event["timestamp"], event.get("asset_id", event.get("market")))].add(event_id)
                if kind == "price_change":
                    for change in event["price_changes"]:
                        hash_values[change.get("hash")].add(fingerprint({
                            "timestamp": event.get("timestamp"), "market": event.get("market"), **change,
                        }))
        streams.append(stream)
        summaries.append({"connection": connection, "frames": row_count, "event_counts": dict(types),
                          "unrelated_event_counts": dict(unrelated_types),
                          "initial": initial, "final_state": book_digest(book),
                          "final_levels": {side: len(levels) for side, levels in (book or {}).items()},
                          "timestamp_regressions": regressions, "parse_errors": parse_errors,
                          "later_snapshot_disagreements": checkpoint_differences,
                          "repeated_exact_events": sum(n - 1 for n in occurrences.values() if n > 1)})
    # Pair repeated payloads by occurrence, rather than silently dropping repeats.
    # This is an analysis convention, not proof of a globally unique event ID.
    eligible = []
    instances = {}
    for event_id, per_connection in by_fingerprint.items():
        for occurrence in range(max(len(items) for items in per_connection.values())):
            items = [items[occurrence] for items in per_connection.values() if len(items) > occurrence]
            if any(item["initial"] for item in items):
                continue
            first = min(item["time_ms"] for item in items)
            if start <= first < end:
                token = f"{event_id}:{occurrence + 1}"
                instances[token] = items
                eligible.append(token)
    eligible_set = set(eligible)
    shared = {token for token in eligible if len(instances[token]) == count}
    sequences = [[item["token"] for item in stream if item["token"] in shared] for stream in streams]
    reference = {token: i for i, token in enumerate(sequences[0])}
    missing = []
    disagreements = []
    spreads = []
    winners = collections.Counter()
    for token in eligible:
        items = instances[token]
        if len(items) < count:
            missing.append({"kind": items[0]["kind"], "timestamp": items[0]["timestamp"],
                            "seen_on": sorted(item["connection"] for item in items), "event_id": items[0]["event_id"]})
        else:
            spreads.append(max(item["time_ms"] for item in items) - min(item["time_ms"] for item in items))
            winners[min(items, key=lambda item: item["time_ms"])["connection"]] += 1
            if len({item["state"] for item in items}) > 1:
                disagreements.append({"kind": items[0]["kind"], "timestamp": items[0]["timestamp"], "event_id": items[0]["event_id"]})
    for i, summary in enumerate(summaries):
        summary["core_events"] = sum(item["token"] in eligible_set for item in streams[i])
        summary["core_missing"] = len(eligible) - summary["core_events"]
        summary["shared_order_matches_connection_1"] = sequences[i] == sequences[0]
        ranks = [reference[token] for token in sequences[i]]
        summary["shared_adjacent_order_inversions_vs_connection_1"] = sum(a > b for a, b in zip(ranks, ranks[1:]))
        lags = []
        for token in shared:
            own = next(item for item in instances[token] if item["connection"] == i + 1)
            lags.append(own["time_ms"] - min(item["time_ms"] for item in instances[token]))
        summary["lag_behind_first_callback_ms"] = distribution(lags)
        summary["first_for_shared_events"] = winners[i + 1]
    timestamp_collisions = [
        {"kind": key[0], "timestamp": key[1], "different_payloads": len(ids), "event_ids": sorted(ids)}
        for key, ids in timestamp_groups.items() if len(ids) > 1
    ]
    repeated_hashes = [{"hash": key, "different_changes": len(values)} for key, values in hash_values.items()
                       if key is not None and len(values) > 1]
    healthy = [s["connection"] for s in meta["status"] if s["opened"] and not s["errors"] and not s["unexpected_closes"]]
    healthy_shared = {
        token for token in eligible
        if all(any(item["connection"] == connection for item in instances[token]) for connection in healthy)
    } if healthy else set()
    healthy_sequences = {
        connection: [item["token"] for item in streams[connection - 1] if item["token"] in healthy_shared]
        for connection in healthy
    }
    healthy_spreads = []
    healthy_winners = collections.Counter()
    healthy_disagreements = []
    first_arrival_sequence = []
    for token in healthy_shared:
        items = [item for item in instances[token] if item["connection"] in healthy]
        healthy_spreads.append(max(item["time_ms"] for item in items) - min(item["time_ms"] for item in items))
        first = min(items, key=lambda item: item["time_ms"])
        healthy_winners[first["connection"]] += 1
        first_arrival_sequence.append((first["time_ms"], token))
        if len({item["state"] for item in items}) > 1:
            healthy_disagreements.append(token)
    first_arrival_sequence.sort()
    healthy_reference = healthy_sequences[healthy[0]] if healthy else []
    healthy_summary = {
        "connections": healthy, "shared_core_events": len(healthy_shared),
        "same_order": all(sequence == healthy_reference for sequence in healthy_sequences.values()),
        "arrival_spread_ms": distribution(healthy_spreads),
        "first_arrival_merge_matches_reference_order": [token for _, token in first_arrival_sequence] == healthy_reference,
        "state_disagreements_at_shared_event": len(healthy_disagreements),
        "distinct_final_states": len({summaries[i - 1]["final_state"] for i in healthy}),
        "first_arrival_counts": dict(sorted(healthy_winners.items())),
    }
    level_versions = collections.defaultdict(dict)
    hash_examples = collections.defaultdict(dict)
    for event_id, event in event_payloads.items():
        if event.get("event_type") != "price_change":
            continue
        for change in event["price_changes"]:
            if change.get("asset_id") != asset:
                continue
            level_versions[(event.get("timestamp"), change["side"], str(Decimal(change["price"]).normalize()))][
                str(Decimal(change["size"]).normalize())] = {"event_id": event_id, **change}
            descriptor = {"timestamp": event.get("timestamp"), "side": change["side"], "price": change["price"], "size": change["size"]}
            hash_examples[change.get("hash")][canonical(descriptor)] = descriptor
    ambiguous_levels = [
        {"timestamp": key[0], "side": key[1], "price": key[2], "versions": list(versions.values())}
        for key, versions in level_versions.items() if len(versions) > 1
    ]
    result = {
        "market": meta["market"], "started_at": meta["started_at"], "connections": count,
        "observation_seconds": meta["observation_seconds"], "warmup_seconds": meta["warmup_seconds"],
        "grace_seconds": meta["grace_seconds"], "core_event_instances": len(eligible),
        "core_event_types": dict(collections.Counter(instances[token][0]["kind"] for token in eligible)),
        "core_events_seen_on_all_connections": len(shared), "core_events_not_seen_everywhere": missing,
        "shared_event_arrival_spread_ms": distribution(spreads),
        "shared_event_book_state_disagreements": disagreements,
        "distinct_initial_states": len({s["initial"]["state"] for s in summaries if s["initial"]}),
        "distinct_final_states": len({s["final_state"] for s in summaries}),
        "timestamp_collisions": timestamp_collisions, "hash_reuse_across_different_changes": repeated_hashes,
        "all_shared_events_same_order": all(s["shared_order_matches_connection_1"] for s in summaries),
        "surviving_connections": healthy_summary,
        "same_level_same_timestamp_different_sizes": ambiguous_levels,
        "selected_asset_reused_hash_examples": [
            {"hash": key, "occurrences": list(examples.values())}
            for key, examples in hash_examples.items() if key is not None and len(examples) > 1
        ],
        "connections_detail": summaries, "capture_errors": meta["errors"], "capture_status": meta["status"],
        "cpu_ms": meta["cpu_ms"], "event_loop_delay_ms": meta["event_loop_delay_ms"],
        "maximum_queued_output_bytes": meta["maximum_queued_output_bytes"],
        "matching_convention": "Canonical complete JSON payload plus occurrence count; not a guaranteed exchange event ID.",
        "timing_scope": "Same-process socket callbacks, measured using one monotonic clock; not wire or matching-engine latency.",
    }
    (root / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    if missing or timestamp_collisions or disagreements:
        interesting_ids = {v["event_id"] for v in missing + disagreements}
        for collision in timestamp_collisions:
            interesting_ids.update(collision["event_ids"])
        (root / "interesting-events.json").write_text(json.dumps(
            {key: event_payloads[key] for key in sorted(interesting_ids)}, indent=2) + "\n")
    return result


if __name__ == "__main__":
    result = analyze(sys.argv[1])
    print(json.dumps(result, indent=2))
