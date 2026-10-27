"""Verify concurrent sockets from raw snapshots, heartbeats, and close records."""
import bisect
import collections
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
result = json.loads((root / 'result.json').read_text())
connections = {c['index']: c for c in result['connections']}
books = collections.defaultdict(set)
complete = {}
pongs = collections.defaultdict(list)
received_bytes = 0
frames = 0
event_types = collections.Counter()
for line in (root / 'frames.jsonl').open():
    row = json.loads(line)
    index, raw, receipt = row['connection'], row['raw'], row['mono_ms']
    assert index in connections, 'Unknown connection in raw frames'
    received_bytes += len(raw.encode())
    frames += 1
    if raw == 'PONG':
        pongs[index].append(receipt)
        continue
    decoded = json.loads(raw)
    for event in decoded if isinstance(decoded, list) else [decoded]:
        kind = event.get('event_type', 'unknown')
        event_types[kind] += 1
        if kind == 'book' and event.get('asset_id') in connections[index]['assets']:
            assert isinstance(event.get('bids'), list) and isinstance(event.get('asks'), list), 'Malformed snapshot'
            books[index].add(event['asset_id'])
            if len(books[index]) == 2 and index not in complete:
                complete[index] = receipt
recorded_bytes = result.get('retained_payload_bytes', result['bytes_received'])
assert received_bytes == recorded_bytes, 'Retained byte count differs from saved frames'
assert received_bytes <= result['bytes_received'], 'Recorded bytes exceed received payload bytes'
assert len({c['market'] for c in connections.values()}) == len(connections), 'Repeated market across sockets'

def population_summary(intervals, start, end, target):
    deltas = collections.Counter()
    for a, b in intervals:
        a, b = max(start, a), min(end, b)
        if b > a:
            deltas[a] += 1
            deltas[b] -= 1
    count, previous, gap, longest_gap = 0, start, 0, 0
    durations = collections.Counter()
    for point in sorted(set(deltas) | {start, end}):
        elapsed = point - previous
        if elapsed:
            durations[count] += elapsed
            gap = gap + elapsed if count < target else 0
            longest_gap = max(longest_gap, gap)
        count += deltas[point]
        previous = point
    return dict(minimum=min(durations), maximum=max(durations),
        seconds_at_or_above_target=sum(v for k, v in durations.items() if k >= target)/1000,
        fraction_at_or_above_target=sum(v for k, v in durations.items() if k >= target)/(end-start),
        longest_below_target_seconds=longest_gap/1000,
        seconds_by_population={str(k):v/1000 for k,v in sorted(durations.items())})


def verify_maintained_window(start, end, target):
    assert end-start >= 300000, 'Maintained observation shorter than five minutes'
    initialized, upgraded, timely = [], [], []
    stale = []
    non_lifecycle_closes=[]
    participating = set()
    for index, c in connections.items():
        for closed in c['closes']:
            known_lifecycle=c.get('retired') and closed['code']==1000 and closed['reason']=='all subscribed assets resolved'
            if start<=closed['mono_ms']<end and not closed['expected'] and not known_lifecycle:
                non_lifecycle_closes.append(dict(connection=index,market=c['market'],close=closed))
        close = min((x['mono_ms'] for x in c['closes']), default=end)
        if c['opened_ms'] is not None:
            upgraded.append((c['opened_ms'], close))
        if index not in complete or not pongs[index]:
            continue
        a, b = max(start, complete[index], pongs[index][0]), min(end, close)
        if b <= a:
            continue
        assert books[index] == set(c['assets']), 'Incomplete participant books'
        participating.add(index)
        before_end = bisect.bisect_right(pongs[index], b)
        before_start = bisect.bisect_right(pongs[index], a)
        relevant = pongs[index][max(0, before_start-1):before_end]
        covered = []
        for receipt in relevant:
            left,right=max(a,receipt),min(b,receipt+15000)
            if right<=left:
                continue
            if covered and left<=covered[-1][1]:
                covered[-1]=(covered[-1][0],max(covered[-1][1],right))
            else:
                covered.append((left,right))
        timely.extend(covered)
        stale_ms=(b-a)-sum(y-x for x,y in covered)
        if stale_ms>0:
            stale.append(dict(connection=index,market=c['market'],seconds_without_fresh_pong=stale_ms/1000))
        initialized.append((a,b))
    errors = []
    for line in (root/'events.jsonl').open():
        e = json.loads(line)
        if start <= e['mono_ms'] < end and e['kind'] in ['socket_error','transport_error','experiment_error']:
            errors.append(e)
    healthy = population_summary(initialized,start,end,target)
    assert healthy['maximum'] >= target, 'Target population never reached'
    return dict(target=target,start_ms=start,end_ms=end,seconds=(end-start)/1000,
        participating_connections=len(participating),initialized_population=healthy,
        timely_heartbeat_population=population_summary(timely,start,end,target),stale_heartbeat_connections=stale,
        upgraded_population=population_summary(upgraded,start,end,target),transport_errors=errors,non_lifecycle_closes=non_lifecycle_closes)

verified_stages = []
failed_completed_observations = []
for stage in result['stages']:
    if not stage['passed']:
        if stage.get('population_policy') == 'maintained' and stage['end_ms']-stage['hold_start_ms'] >= 300000:
            observed = verify_maintained_window(stage['hold_start_ms'],stage['end_ms'],stage['target'])
            observed['final_health_gate_passed'] = False
            observed['final_healthy_connections'] = stage.get('healthy')
            failed_completed_observations.append(observed)
        continue
    if stage.get('population_policy') == 'maintained':
        observed = verify_maintained_window(stage['hold_start_ms'],stage['end_ms'],stage['target'])
        verified_stages.append(dict(connections=stage['target'],hold_seconds=stage['hold_seconds'],population_policy='maintained',population=observed))
        continue
    indices = stage.get('connection_indices', list(range(1, stage['target'] + 1)))
    assert len(indices) == len(set(indices)) == stage['target'], 'Claimed hold lacks full distinct connection population'
    assert stage['hold_seconds'] >= result['protocol']['stage_hold_seconds'], 'Hold is shorter than configured duration'
    for index in indices:
        assert complete[index] <= stage['hold_start_ms'], 'Snapshot incomplete before hold'
        assert books[index] == set(connections[index]['assets']), 'Wrong outcome IDs'
        before_end = bisect.bisect_right(pongs[index], stage['end_ms'])
        assert before_end and stage['end_ms'] - pongs[index][before_end - 1] <= 15000, 'Missing fresh heartbeat'
        before_start = bisect.bisect_right(pongs[index], stage['hold_start_ms'])
        relevant = pongs[index][max(0, before_start - 1):before_end] + [stage['end_ms']]
        assert relevant and all(b-a <= 15000 for a, b in zip(relevant, relevant[1:])), 'Heartbeat gap during hold'
        assert not any(c['mono_ms'] <= stage['end_ms'] for c in connections[index]['closes']), 'Socket closed during claimed concurrent hold'
    verified_stages.append(dict(connections=stage['target'], distinct_markets=stage['target'],
        asset_books=stage['target']*2, hold_seconds=stage['hold_seconds'], fresh_pongs=True))
assert result['protocol']['script_sha256'] == hashlib.sha256((root / 'harness.mjs').read_bytes()).hexdigest(), 'Archived harness differs from measured version'
intervals = [b['created_ms'] - a['created_ms'] for a, b in zip(result['connections'], result['connections'][1:])]
initialization = sorted(c['complete_ms'] - c['opened_ms'] for c in connections.values() if c['complete_ms'] is not None)
closes = collections.Counter(str(c['code']) for item in connections.values() for c in item['closes'])
def resolved_lifecycle(c, close):
    return c.get('retired') and close['code'] == 1000 and close['reason'] == 'all subscribed assets resolved'

unexpected = [dict(connection=c['index'], close=close) for c in connections.values() for close in c['closes'] if not close['expected'] and not resolved_lifecycle(c, close)]
lifecycle_closes = [dict(connection=c['index'], market=c['market'], close=close, metadata=c['lifecycle_revalidation']) for c in connections.values() for close in c['closes'] if not close['expected'] and resolved_lifecycle(c, close)]
verification = dict(verified_stages=verified_stages, failed_completed_observations=failed_completed_observations,
    received_frames=frames, received_payload_bytes=received_bytes,
    all_received_payload_bytes=result['bytes_received'], frame_recording=result['protocol'].get('frame_recording','Every received frame'),
    total_pongs=sum(map(len, pongs.values())), event_types=dict(event_types),
    distinct_markets=len({c['market'] for c in connections.values()}),
    distinct_assets=len({a for c in connections.values() for a in c['assets']}),
    minimum_connection_creation_interval_ms=min(intervals, default=None),
    snapshot_initialization_ms=dict(minimum=min(initialization, default=None),
        median=initialization[len(initialization)//2] if initialization else None, maximum=max(initialization, default=None)),
    close_codes=dict(closes), unexpected_closes=unexpected, resolved_market_closes=lifecycle_closes,
    unavailable_initial_markets=[dict(connection=c['index'],market=c['market'],reason=c['retirement_reason'],metadata=c.get('lifecycle_revalidation'),books=c.get('book_revalidation')) for c in connections.values() if c.get('retirement_reason') in ['metadata_ineligible_before_initial_snapshot','book_api_unavailable_before_initial_snapshot']],
    maximum_passed_from_raw=max((s['connections'] for s in verified_stages), default=0),
    archived_harness_sha256_verified=True)
measurement_end=max((s['end_ms'] for s in result['stages']),default=0)
if measurement_end>0:
    upgraded_intervals=[(c['opened_ms'],min(measurement_end,min((x['mono_ms'] for x in c['closes']),default=measurement_end))) for c in connections.values() if c['opened_ms'] is not None]
    initialized_intervals=[(max(complete[i],pongs[i][0]),min(measurement_end,min((x['mono_ms'] for x in c['closes']),default=measurement_end))) for i,c in connections.items() if i in complete and pongs[i]]
    verification['peak_upgraded_connections']=population_summary(upgraded_intervals,0,measurement_end,1)['maximum']
    verification['peak_initialized_connections']=population_summary(initialized_intervals,0,measurement_end,1)['maximum']
window_file = root/'observation-window.json'
if window_file.exists():
    window = json.loads(window_file.read_text())
    verification['maintained_observations'] = [verify_maintained_window(window['start_ms'],window['end_ms'],window['target'])]
    observations=verification['maintained_observations']
else:
    observations=[s['population'] for s in verified_stages if 'population' in s]
observations=observations+failed_completed_observations
for observed in observations:
    previous=None;buckets=[]
    for line in (root/'events.jsonl').open():
        row=json.loads(line)
        if row['kind']!='traffic_sample':
            continue
        if previous and observed['start_ms']<=previous['mono_ms'] and row['mono_ms']<=observed['end_ms']:
            elapsed=(row['mono_ms']-previous['mono_ms'])/1000
            buckets.append((elapsed,row['bytes_per_second'],row['market_updates_per_second']))
        previous=row
    covered=sum(b[0] for b in buckets)
    observed['interior_traffic_buckets']=dict(seconds=covered,buckets=len(buckets),
        bytes_per_second=sum(b[0]*b[1] for b in buckets)/covered if covered else None,
        market_updates_per_second=sum(b[0]*b[2] for b in buckets)/covered if covered else None,
        minimum_bytes_per_second=min((b[1] for b in buckets),default=None),
        maximum_bytes_per_second=max((b[1] for b in buckets),default=None))
assert verification['maximum_passed_from_raw'] == result['maximum_passed']
(root / 'verification.json').write_text(json.dumps(verification, indent=2) + '\n')
summary = {k:v for k,v in result.items() if k != 'connections'}
summary['stages'] = [{k:v for k,v in s.items() if k not in ['connection_indices','per_market_updates']} for s in result['stages']]
export = dict(recording_directory='recordings/' + root.name, measurement=summary, verification=verification,
    evidence='Full per-connection receipts and stage cohorts remain in the recording directory; the shareable export contains aggregates.',
    interpretation='Observed concurrent admissions; maintained windows report actual population dips and heartbeat freshness separately. No absolute IP cap is established.',
    caveats=['One ramp, not independent repeated saturation trials.',
        'Observed live traffic does not establish a true worst case.',
        'Volume24hr ranks historical trading volume, not instantaneous order-book update frequency.' if result['protocol'].get('market_selection') else 'Quiet or idle market sockets do not establish capacity at equal high update rates.'] +
        (['Initial harness per-WebSocket address extraction was unavailable; its all-transport local-address count and before/after exchange-edge public-egress checks remain recorded.']
         if result['egress']['distinct_local_websocket_addresses'] == 0 else []),
    sources=['https://docs.polymarket.com/market-data/realtime-data','https://docs.polymarket.com/api-reference/rate-limits'])
notes_file=root/'measurement-notes.json'
if notes_file.exists():
    export['measurement_notes']=json.loads(notes_file.read_text())
if len(sys.argv) > 2:
    pathlib.Path(sys.argv[2]).write_text(json.dumps(export, indent=2) + '\n')
print(json.dumps(dict(maximum_verified=verification['maximum_passed_from_raw'],
    maintained_observation_targets=[s['target'] for s in verification.get('maintained_observations',[])],
    distinct_markets=verification['distinct_markets'], distinct_assets=verification['distinct_assets'],
    total_pongs=verification['total_pongs'], close_codes=verification['close_codes'],
    unexpected_closes=len(unexpected), egress=result['egress'])))
