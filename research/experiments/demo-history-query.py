#!/usr/bin/env python3
"""Compare cold All loads and chart values against an earlier demo revision."""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
import types
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "demo"))
import server as current


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prometheus-url", required=True)
    parser.add_argument("--baseline-ref", default="c321996")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    baseline = types.ModuleType("baseline")
    baseline.__file__ = str(REPO / "demo/server.py")
    source = subprocess.check_output(
        ["git", "show", f"{args.baseline_ref}:demo/server.py"], cwd=REPO)
    exec(compile(source, "baseline/server.py", "exec"), baseline.__dict__)
    fixed_now = time.time()
    records = []
    expected = None
    for run in range(args.runs):
        for name, module in [("baseline", baseline), ("optimized", current)]:
            reader = module.Reader(args.prometheus_url)
            queries = []

            def query(endpoint, parameters, limit=1_048_576, timeout_seconds=4):
                # Same transport and deadline for both query/decoder implementations.
                started = time.perf_counter()
                payload = current.Reader.query(reader, endpoint, {**parameters, "stats": "all"},
                                               limit=limit, timeout_seconds=4)
                if payload.get("status") != "success":
                    raise AssertionError("Prometheus query failed")
                queries.append({"endpoint": endpoint, "seconds": time.perf_counter() - started,
                                "bytes": len(json.dumps(payload).encode()),
                                "rows": len(payload["data"]["result"]),
                                "stats": payload["data"].get("stats")})
                return payload

            reader.query = query
            with patch.object(module.time, "time", return_value=fixed_now):
                started = time.perf_counter()
                result = reader.history("all")
                elapsed = time.perf_counter() - started
            if result["status"] != "ok":
                raise AssertionError(f"{name} history failed: {result['status']}")
            if expected is None:
                expected = result
            if result != expected:
                raise AssertionError(f"{name} chart response differs from the baseline")
            records.append({"run": run + 1, "candidate": name, "seconds": elapsed,
                            "points": len(result["timestamps"]), "queries": queries})
            print(json.dumps({"run": run + 1, "candidate": name, "seconds": round(elapsed, 3)}),
                  flush=True)
    summary = {}
    for name in ("baseline", "optimized"):
        times = [record["seconds"] for record in records if record["candidate"] == name]
        summary[name] = {"median_seconds": statistics.median(times),
                         "min_seconds": min(times), "max_seconds": max(times)}
    output = {"at": fixed_now, "runs_per_side": args.runs, "failures": 0,
              "responses_equal": True, "summary": summary, "runs": records}
    if args.output:
        args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps({"failures": 0, "responses_equal": True, "summary": summary}))


if __name__ == "__main__":
    main()
