#!/usr/bin/env python3
"""Convert a guarded independent-socket capture into a private Rust replay fixture.

Usage: python3 research/experiments/older-full-book-fixture.py CAPTURE OUTPUT
Full raw text frames are retained. Each full-book frame is preceded by a timer
advance so the Rust regression checks complete held depth and source metadata.
The output can contain sensitive capture data; keep it outside source control.
"""
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('capture', type=Path)
parser.add_argument('output', type=Path)
args = parser.parse_args()
status = json.loads((args.capture / 'status.json').read_text())
assert status['complete'], 'Capture must finish cleanly'
markets = json.loads((args.capture / 'markets.json').read_text())['markets']
frames = 0
with args.output.open('w') as output:
    def emit(row):
        output.write(json.dumps(row, separators=(',', ':')) + '\n')
    for connection, market in enumerate(markets):
        path = args.capture / ('market-' + market['id'] + '.jsonl')
        expected = status['markets'][market['id']]
        assert path.stat().st_size == expected['file_bytes']
        with path.open('rb') as source:
            assert hashlib.file_digest(source, 'sha256').hexdigest() == expected['sha256']
        emit(dict(type='add_market', market=market['market'], order_books=market['assets'], connection=connection))
        sequence = pings = pongs = 0
        last = None
        for line in path.open():
            row = json.loads(line)
            last = row
            if row['type'] == 'sent':
                pings += row['payload'] == 'PING'
            if row['type'] != 'frame':
                continue
            sequence += 1
            assert sequence == row['sequence']
            pongs += row['text'] == 'PONG'
            if row['text'] != 'PONG':
                payload = json.loads(row['text'])
                if any(e.get('event_type') == 'book' for e in (payload if isinstance(payload, list) else [payload])):
                    emit(dict(type='advance', millis=60001))
            emit(dict(type='frame', connection=connection, received_at=row['received_at'], text=row['text']))
        assert last['type'] == 'complete'
        assert sequence == expected['frames']
        assert pings == expected.get('pings_sent', pings) and pongs == expected['event_counts']['PONG']
        assert pings == pongs, 'All application heartbeats must be answered'
        frames += sequence
        # Remove completed sockets to keep subsequent timer/output work bounded.
        emit(dict(type='remove_market', market=market['market']))
    emit(dict(type='advance', millis=1))
print(json.dumps(dict(frames=frames, markets=len(markets), integrity=True)))
