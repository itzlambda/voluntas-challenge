"""Validate every claimed accepted subscription against saved bid/ask snapshots."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
catalog = json.loads((root / 'catalog.json').read_text())
probe_assets = {a for m in catalog['markets'] if m['id'] in catalog['probe_market_ids'] for a in m['assets']}
verified = []
for result_file in sorted(root.glob('*/result.json')):
    result = json.loads(result_file.read_text())
    selected = json.loads((result_file.parent / 'requested.json').read_text())
    feeds = []
    for stat in result['stats']:
        requested = probe_assets if stat['label'] == 'control' else {a for m in selected for a in m['assets']}
        received, malformed = set(), 0
        with (result_file.parent / (stat['label'] + '.jsonl')).open() as source:
            for line in source:
                row = json.loads(line)
                if row['kind'] != 'received' or row['raw'] == 'PONG':
                    continue
                decoded = json.loads(row['raw'])
                for event in decoded if isinstance(decoded, list) else [decoded]:
                    if event.get('event_type') != 'book' or event.get('asset_id') not in requested:
                        continue
                    if isinstance(event.get('bids'), list) and isinstance(event.get('asks'), list):
                        received.add(event['asset_id'])
                    else:
                        malformed += 1
                if len(received) == len(requested):
                    break
        assert not malformed, 'Malformed book snapshot'
        if result['accepted']:
            assert received == requested, 'Claimed acceptance lacks complete bid/ask snapshots'
        feeds.append(dict(label=stat['label'], requested=len(requested), valid_snapshot_assets=len(received), malformed=malformed))
    verified.append(dict(id=result['id'], accepted=result['accepted'], feeds=feeds))
(root / 'snapshot-verification.json').write_text(json.dumps(dict(trials=verified), indent=2) + '\n')
print(json.dumps(dict(verified_trials=len(verified), accepted_trials=sum(t['accepted'] for t in verified), malformed_snapshots=0)))
