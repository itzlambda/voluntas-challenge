"""Analyze saved updatedAt experiments without additional network requests."""
from collections import Counter
from datetime import datetime
import json
from pathlib import Path

FLAGS = ("active", "closed", "archived", "acceptingOrders", "enableOrderBook")


def stamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def analyze(path):
    summary = json.loads(path.read_text())
    assert summary["complete"]
    seen, changes, regressions = {}, [], []
    repeated, responses, metadata_updates = 0, 0, 0
    ages, cache_hits = Counter(), Counter()
    for line in open(summary["raw_observations"]):
        record = json.loads(line)
        responses += 1
        ages[str(record["headers"].get("Age"))] += 1
        cache_hits[str(record["headers"].get("X-Cache"))] += 1
        for row in record["rows"]:
            row = {k: v for k, v in row.items() if k != "question"}
            old = seen.get(row["id"])
            newtime = stamp(row.get("updatedAt"))
            if old:
                repeated += 1
                oldtime = stamp(old.get("updatedAt"))
                fields = [f for f in FLAGS if old.get(f) != row.get(f)]
                if fields:
                    changes.append({"id": row["id"], "fields": fields,
                                    "observed_at": record["received_at"],
                                    "old": old, "new": row,
                                    "updatedAt_advanced": bool(newtime and oldtime and newtime > oldtime)})
                elif newtime and oldtime and newtime > oldtime:
                    metadata_updates += 1
                if newtime and oldtime and newtime < oldtime:
                    regressions.append({"id": row["id"], "old_updatedAt": old["updatedAt"],
                                        "new_updatedAt": row["updatedAt"]})
            seen[row["id"]] = row
    assert responses + len(summary["http_errors"]) == summary["requests"]
    baseline_changes = summary["seed_status_changes"]
    return {
        "result": path.name,
        "requests": summary["requests"], "http_errors": summary["http_errors"],
        "unique_observed": len(seen), "repeated_observations": repeated,
        "all_raw_flag_changes": len(changes), "changed_unique_markets": len({c["id"] for c in changes}),
        "flag_changes_with_later_updatedAt": sum(c["updatedAt_advanced"] for c in changes),
        "nonclosure_flag_changes": sum("closed" not in c["fields"] for c in changes),
        "changes": changes,
        "updatedAt_regressions": len(regressions), "regression_examples": regressions[:3],
        "timestamp_updates_without_flag_changes": metadata_updates,
        "seed_changes": len(baseline_changes),
        "seed_changes_updatedAt_advanced": sum(
            bool(stamp(c["new"].get("updatedAt")) and stamp(c["old"].get("updatedAt"))
                 and stamp(c["new"]["updatedAt"]) > stamp(c["old"]["updatedAt"]))
            for c in baseline_changes),
        "cached_response_ages": dict(ages), "cache_hits": dict(cache_hits),
        "timestamp_order_inversions": sum(c["time_inversions"] for c in summary["ordering_checks"]),
        "null_timestamp_observations": sum(c["null_timestamps"] for c in summary["ordering_checks"]),
        "selectors": summary["selectors"],
        "note": "The pinned cohort alone had no flag changes; changes above include repeated IDs across every captured listing and lookup.",
    }


if __name__ == "__main__":
    root = Path("research/experiments")
    results = [analyze(root / name) for name in
               ("polymarket-updated-at-results.json", "polymarket-updated-at-fresh-results.json")]
    (root / "polymarket-updated-at-analysis.json").write_text(json.dumps(results, indent=2) + "\n")
    for row in results:
        print(json.dumps({k: v for k, v in row.items()
                          if k not in ("changes", "regression_examples", "cached_response_ages")}))
