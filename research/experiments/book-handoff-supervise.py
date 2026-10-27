#!/usr/bin/env python3
"""Bound one follow-up run, preserving a separate file set per A socket epoch.

Usage: python3 -B -I research/experiments/book-handoff-supervise.py PRIVATE_ROOT
This run attempts ten additional joins, at most four segments and 15 minutes.
"""
import json
from pathlib import Path
import subprocess
import sys
import time

root = Path(sys.argv[1])
root.mkdir(mode=0o700, exist_ok=False)
script = Path('research/experiments/book-handoff-capture.mjs')
(root / 'collector-source.mjs').write_bytes(script.read_bytes())
(root / 'collector-source.mjs').chmod(0o600)
started, deadline = time.monotonic(), time.monotonic() + 900
attempted, segments = 0, []
for segment in range(1, 5):
    remaining = 10 - attempted
    if remaining <= 0 or time.monotonic() + 70 >= deadline:
        break
    config = {'joins': remaining, 'baseline_seconds': 30, 'join_seconds': 40,
              'control_gap_seconds': 7, 'tail_seconds': 20, 'pilot_seconds': 10, 'market_id': '682705'}
    run = root / f'segment-{segment:02d}'
    command = ['node', str(script), str(run), json.dumps(config, separators=(',', ':'))]
    with open(root / f'segment-{segment:02d}.log', 'xb') as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        timed_out = False
        try:
            returncode = child.wait(timeout=max(1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            timed_out = True
            child.terminate()
            try:
                returncode = child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                returncode = child.wait(timeout=5)
    count = 0
    if (run / 'frames.jsonl').exists():
        for line in (run / 'frames.jsonl').read_text().splitlines():
            if json.loads(line)['kind'] == 'join_start':
                count += 1
    attempted += count
    record = {'segment': segment, 'directory': run.name, 'joins_attempted': count,
              'returncode': returncode, 'timed_out': timed_out, 'command': command,
              'elapsed_seconds': time.monotonic() - started}
    segments.append(record)
    (root / 'manifest.json').write_text(json.dumps({'attempted': attempted, 'segments': segments}, indent=2))
    (root / 'manifest.json').chmod(0o600)
    print(json.dumps({key: record[key] for key in ['segment', 'joins_attempted', 'returncode', 'timed_out', 'elapsed_seconds']}), flush=True)
    if timed_out:
        break
print(json.dumps({'finished': True, 'joins_attempted': attempted, 'segments': len(segments), 'elapsed_seconds': time.monotonic() - started}), flush=True)
