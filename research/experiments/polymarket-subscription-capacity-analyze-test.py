"""Check full-entry fingerprints, duplicate multiplicity, order, and global traffic."""
import json
import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory() as temporary:
    root = pathlib.Path(temporary)
    trial = root / 'trial'
    trial.mkdir()
    (root / 'catalog.json').write_text(json.dumps(dict(probe_market_ids=['p'], markets=[dict(id='p', assets=['a', 'b'])])))
    change = dict(asset_id='a', price='0.5', size='10', side='BUY', hash='same')
    other = {**change, 'size': '11'}

    def event(changes):
        return dict(event_type='price_change', market='m', timestamp='42', price_changes=changes)

    def frame(t, payload):
        return dict(kind='received', mono_ms=t, raw=json.dumps(payload))

    control = [frame(11000, event([change, other])), frame(12000, event([change])),
               frame(13000, dict(event_type='new_market', id='unrelated'))]
    candidate = [frame(11007, event([other, change])), frame(12009, event([change])),
                 frame(13001, dict(event_type='new_market', id='unrelated'))]
    for name, rows in [('control', control), ('candidate', candidate)]:
        (trial / f'{name}.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))
    result = dict(id='fixture', markets=1, assets=2, seconds=10, purpose='fixture', accepted=True,
                  abort_reason=None, interrupted=False, started_ms=0, ended_ms=23000, window_start_ms=10000,
                  window_end_ms=20000, event_loop_ms={}, max_rss_bytes=0, cpu_ms={},
                  stats=[dict(label=s, initialization_ms=1, per_second={}) for s in ['control', 'candidate']])
    (trial / 'result.json').write_text(json.dumps(result))
    subprocess.run([sys.executable, 'experiments/polymarket-subscription-capacity-analyze.py', str(root)], check=True, capture_output=True)
    actual = json.loads((root / 'analysis.json').read_text())['trials'][0]
    assert actual['control_probe_occurrences'] == 3
    assert actual['matched_control'] == 3
    assert actual['candidate_minus_control_ms']['p50'] == 7
    assert actual['candidate_minus_control_ms']['maximum'] == 9
    assert actual['repeated_fingerprints_control'] == 1
    assert actual['per_asset_adjacent_order_reversals'] == 1
    assert actual['candidate_traffic']['global_new_market'] == 1
    assert actual['control_coverage'] == actual['candidate_coverage'] == 1
print('PASS: full payload fields, repeated occurrences, per-asset ordering, and unrelated global traffic')
