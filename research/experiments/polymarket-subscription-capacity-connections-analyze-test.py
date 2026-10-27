"""Verify that resolved markets cannot masquerade as a full concurrent hold."""
import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory() as directory:
    root = pathlib.Path(directory)
    source = b'// offline fixture\n'
    (root / 'harness.mjs').write_bytes(source)
    (root / 'events.jsonl').write_text('')
    rows = []
    for index, receipt in [(1, 1000), (2, 2000), (3, 121500)]:
        raw = json.dumps([dict(event_type='book', asset_id=f'{index}-{side}', bids=[], asks=[]) for side in ['a', 'b']])
        rows.append(dict(connection=index, mono_ms=receipt, raw=raw))
    for index, start, end in [(1, 1500, 422000), (2, 2500, 119000), (3, 122000, 422000)]:
        for receipt in range(start, end + 1, 10000):
            rows.append(dict(connection=index, mono_ms=receipt, raw='PONG'))
    rows.sort(key=lambda r: r['mono_ms'])
    (root / 'frames.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in rows))
    connections = []
    for index, complete in [(1, 1000), (2, 2000), (3, 121500)]:
        resolved = index == 2
        connections.append(dict(index=index, market=str(index), assets=[f'{index}-a', f'{index}-b'],
            created_ms=complete-200, opened_ms=complete-100, complete_ms=complete,
            retired=resolved, lifecycle_revalidation=dict(closed=True, acceptingOrders=False) if resolved else {},
            closes=[dict(mono_ms=120000 if resolved else 423000, code=1000,
                reason='all subscribed assets resolved' if resolved else 'Experiment complete', expected=not resolved)]))
    result = dict(connections=connections, bytes_received=sum(len(r['raw'].encode()) for r in rows),
        protocol=dict(script_sha256=hashlib.sha256(source).hexdigest(), stage_hold_seconds=300),
        stages=[dict(passed=True, target=2, connection_indices=[1, 3], hold_start_ms=122001,
            end_ms=422002, hold_seconds=300.001)], maximum_passed=2,
        egress=dict(distinct_local_websocket_addresses=1))

    def run(value):
        (root / 'result.json').write_text(json.dumps(value))
        return subprocess.run([sys.executable, 'experiments/polymarket-subscription-capacity-connections-analyze.py', str(root)], capture_output=True)

    assert run(result).returncode == 0, 'Valid replacement population was rejected'
    verified = json.loads((root / 'verification.json').read_text())
    assert verified['maximum_passed_from_raw'] == 2
    assert len(verified['resolved_market_closes']) == 1 and not verified['unexpected_closes']
    maintained = json.loads(json.dumps(result))
    maintained['stages'][0].update(population_policy='maintained', hold_start_ms=120000, end_ms=420001, hold_seconds=300.001)
    assert run(maintained).returncode == 0, 'Maintained observation was rejected'
    observed = json.loads((root/'verification.json').read_text())['verified_stages'][0]['population']
    assert observed['initialized_population']['minimum'] == 1
    assert observed['initialized_population']['maximum'] == 2
    assert abs(observed['initialized_population']['seconds_at_or_above_target'] - 298.001) < 1e-6
    failed = json.loads(json.dumps(maintained))
    failed['stages'][0].update(passed=False, healthy=1)
    failed['maximum_passed'] = 0
    assert run(failed).returncode == 0, 'Completed failed health gate lost its observation'
    verification = json.loads((root/'verification.json').read_text())
    assert verification['maximum_passed_from_raw'] == 0 and not verification['verified_stages']
    assert verification['failed_completed_observations'][0]['target'] == 2
    assert verification['failed_completed_observations'][0]['final_health_gate_passed'] is False
    discordant = json.loads(json.dumps(result))
    discordant['connections'][1]['lifecycle_revalidation'] = dict(closed=False, acceptingOrders=True, agrees_with_stream=False)
    assert run(discordant).returncode == 0, 'Metadata disagreement incorrectly invalidated the replacement population'
    verified = json.loads((root / 'verification.json').read_text())
    assert len(verified['resolved_market_closes']) == 1 and not verified['unexpected_closes']
    broken = json.loads(json.dumps(result))
    broken['stages'][0]['connection_indices'] = [1, 2]
    assert run(broken).returncode != 0, 'Resolved socket counted as concurrently healthy'
    broken = json.loads(json.dumps(result))
    broken['stages'][0]['hold_seconds'] = 299
    assert run(broken).returncode != 0, 'Short hold counted as a five-minute pass'
print('PASS: replacement population, metadata disagreement, resolved-market exclusion, and complete five-minute duration')
