#!/usr/bin/env python3
"""Reclaim old SBE blocks without changing the active writer's file offset."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import time

CHUNK = 64 * 1024 * 1024


def reclaim(path, state_path, keep_bytes, minimum_age, now=None):
    now = time.monotonic() if now is None else now
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "r+b", buffering=0) as output:
        info = os.fstat(output.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("output must be a regular file")
        with open(str(state_path) + ".lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            identity = [info.st_dev, info.st_ino]
            state = json.loads(state_path.read_text()) if state_path.exists() else {}
            if state.get("identity") != identity or info.st_size < state.get("size", 0):
                state = {"identity": identity, "punched": 0, "observations": []}
            observations = state["observations"] + [[now, info.st_size]]
            old = [row for row in observations if row[0] <= now - minimum_age]
            cutoff = min(old[-1][1] if old else 0, max(0, info.st_size - keep_bytes))
            cutoff = cutoff // CHUNK * CHUNK
            previous = state["punched"]
            if cutoff > previous:
                subprocess.run(["fallocate", "--punch-hole", "--keep-size", "--offset", str(previous),
                                "--length", str(cutoff - previous), f"/proc/self/fd/{output.fileno()}"],
                               pass_fds=(output.fileno(),), check=True)
                state["punched"] = cutoff
            state["size"] = info.st_size
            state["observations"] = (old[-1:] if old else []) + [row for row in observations if row[0] > now - minimum_age]
            temporary = state_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(state) + "\n")
            temporary.replace(state_path)
            return max(0, cutoff - previous)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--keep-gib", type=int, default=4)
    parser.add_argument("--minimum-age-seconds", type=int, default=300)
    args = parser.parse_args()
    if args.keep_gib < 1 or args.minimum_age_seconds < 60:
        parser.error("retain at least 1 GiB and 60 seconds")
    if args.output.exists():
        released = reclaim(args.output, args.state, args.keep_gib * 1024**3, args.minimum_age_seconds)
        print(json.dumps({"reclaimed_bytes": released}))
