#!/usr/bin/env python3
"""Compare arrival-order and older-full-book-filter models on fixture JSONL.

With --follow, publish exact-depth comparison counts while a Rust raw capture
is being written. This audits received data, not upstream completeness or SBE.
The Rust integration test separately checks decoding, workers and SBE output.
"""
import argparse
from collections import Counter
from decimal import Decimal
import json
from pathlib import Path
import time

class Model:
    def __init__(self, filtered):
        self.filtered = filtered
        self.books, self.synced, self.high, self.connections = {}, set(), {}, {}
        self.counts = Counter()
        self.lags = Counter()

    def apply(self, row):
        kind = row['type']
        if kind == 'add_market':
            for asset in row['order_books']:
                self.connections[asset] = row['connection']
                self.books.pop(asset, None)
                self.synced.discard(asset)
            return
        if kind == 'mark_unsynced':
            self.synced.difference_update(a for a,c in self.connections.items() if c == row['connection'])
            return
        if kind != 'frame':
            return
        self.counts['frames'] += 1
        if row['text'] == 'PONG':
            self.counts['pongs'] += 1
            return
        payload = json.loads(row['text'], parse_float=Decimal)
        for event in payload if isinstance(payload, list) else [payload]:
            kind = event.get('event_type')
            if kind not in ['book', 'price_change']:
                self.counts['ignored'] += 1
                continue
            ts = int(event['timestamp'])
            if kind == 'book':
                asset = event['asset_id']
                incoming = {(side,Decimal(x['price'])):Decimal(x['size']) for side in ['bids','asks'] for x in event[side] if Decimal(x['size'])}
                if self.filtered and asset in self.synced and ts < self.high[asset]:
                    self.counts['skipped'] += 1
                    self.lags[self.high[asset]-ts] += 1
                    continue
                if asset in self.synced:
                    self.counts['accepted_comparisons'] += 1
                    if self.books[asset] != incoming:
                        self.counts['mismatches'] += 1
                        self.counts['equal_timestamp_mismatches' if ts == self.high[asset] else 'newer_timestamp_mismatches' if ts > self.high[asset] else 'older_timestamp_mismatches'] += 1
                self.high[asset] = max(self.high[asset],ts) if asset in self.synced else ts
                self.books[asset] = incoming
                self.synced.add(asset)
            else:
                for change in event['price_changes']:
                    asset = change['asset_id']
                    if asset not in self.synced:
                        self.counts['prebaseline_updates'] += 1
                        continue
                    key = ({'BUY':'bids','SELL':'asks'}[change['side']],Decimal(change['price']))
                    qty = Decimal(change['size'])
                    if qty:self.books[asset][key] = qty
                    else:self.books[asset].pop(key,None)
                    self.high[asset] = max(self.high[asset],ts)
                    self.counts['updates'] += 1

    def summary(self):
        return dict(counts=dict(self.counts), skipped_lag_ms=dict(sorted(self.lags.items())), initialized_books=len(self.books))

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('fixture',type=Path)
    parser.add_argument('output',type=Path)
    parser.add_argument('--follow',type=int,default=0,help='Bounded follow duration in seconds')
    args=parser.parse_args()
    models=[Model(False), Model(True)]
    deadline=time.monotonic()+args.follow
    with args.fixture.open() as source:
        while True:
            position=source.tell()
            line=source.readline()
            if line.endswith('\n'):
                row=json.loads(line)
                for model in models:model.apply(row)
                continue
            source.seek(position)
            report=dict(baseline=models[0].summary(),filtered=models[1].summary())
            temporary=args.output.with_suffix('.tmp')
            temporary.write_text(json.dumps(report,indent=2)+'\n');temporary.replace(args.output)
            if time.monotonic()>=deadline:break
            time.sleep(2)
