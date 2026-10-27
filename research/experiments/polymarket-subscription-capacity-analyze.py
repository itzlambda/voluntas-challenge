"""Recompute matching payload latency from saved frames, retaining multiplicity."""
import collections
import hashlib
import json
import math
import pathlib
import statistics
import sys

root = pathlib.Path(sys.argv[1])
catalog = json.loads((root / 'catalog.json').read_text())
probes = {a for m in catalog['markets'] if m['id'] in catalog['probe_market_ids'] for a in m['assets']}


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, math.floor(len(values) * p))]


def distribution(values):
    return dict(n=len(values), p50=percentile(values, .5), p95=percentile(values, .95),
                p99=percentile(values, .99), minimum=min(values, default=None), maximum=max(values, default=None))


def read_feed(file, start, end):
    events = collections.defaultdict(list)
    total = dict(bytes=0, frames=0, events=0, global_new_market=0)
    seq = 0
    kinds = collections.Counter()
    for line in file.open():
        row = json.loads(line)
        if row['kind'] != 'received':
            continue
        t = row['mono_ms']
        raw = row['raw']
        inside = start <= t < end
        if inside:
            total['bytes'] += len(raw.encode())
            total['frames'] += 1
        if raw == 'PONG':
            continue
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for event in decoded if isinstance(decoded, list) else [decoded]:
            kind = event.get('event_type', 'unknown')
            if inside:
                total['events'] += 1
                kinds[kind] += 1
                if kind == 'new_market':
                    total['global_new_market'] += 1
            # Include a common 3s matching margin; count coverage only for interior events.
            if t < start - 3000 or t >= end + 3000 or kind == 'new_market':
                continue
            atoms = []
            if kind == 'price_change':
                for change in event.get('price_changes', []):
                    if change.get('asset_id') in probes:
                        atoms.append((change['asset_id'], {**event, 'price_changes': [change]}))
            elif event.get('asset_id') in probes:
                atoms.append((event['asset_id'], event))
            for asset, atom in atoms:
                # Every field participates, including size, price, side, hash and timestamp.
                payload = json.dumps(atom, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
                fingerprint = hashlib.sha256(payload.encode()).hexdigest()
                events[fingerprint].append((t, seq, asset, kind, inside))
                seq += 1
    seconds = (end - start) / 1000
    return events, {**total, 'bytes_per_second': total['bytes'] / seconds,
                    'events_per_second': total['events'] / seconds, 'event_types': dict(kinds)}


analyses = []
for file in sorted(root.glob('*/result.json')):
    result = json.loads(file.read_text())
    start, end = result['window_start_ms'], result['window_end_ms']
    if start is None or end is None or result['ended_ms'] < end or result['interrupted']:
        continue
    control, control_traffic = read_feed(file.parent / 'control.jsonl', start, end)
    candidate, candidate_traffic = read_feed(file.parent / 'candidate.jsonl', start, end)
    lags, pairs = [], collections.defaultdict(list)
    per_asset = collections.defaultdict(lambda: dict(control=0, candidate=0, matched_control=0))
    for occurrences in control.values():
        for e in occurrences:
            if e[4]:
                per_asset[e[2]]['control'] += 1
    for occurrences in candidate.values():
        for e in occurrences:
            if e[4]:
                per_asset[e[2]]['candidate'] += 1
    control_count = sum(e[4] for occurrences in control.values() for e in occurrences)
    candidate_count = sum(e[4] for occurrences in candidate.values() for e in occurrences)
    matched_control = matched_candidate = 0
    for key in control.keys() | candidate.keys():
        for c, x in zip(control.get(key, []), candidate.get(key, [])):
            if c[4]:
                matched_control += 1
                lags.append(x[0] - c[0])
                pairs[c[2]].append((c[1], x[1]))
                per_asset[c[2]]['matched_control'] += 1
            if x[4]:
                matched_candidate += 1
    reversals = 0
    for asset_pairs in pairs.values():
        ordered = sorted(asset_pairs)
        reversals += sum(b[1] < a[1] for a, b in zip(ordered, ordered[1:]))
    analysis = dict(id=result['id'], markets=result['markets'], assets=result['assets'], seconds=result['seconds'],
                    purpose=result['purpose'], accepted=result['accepted'], abort_reason=result['abort_reason'],
                    opening_order=[s['label'] for s in result['stats']], per_probe_asset_counts=dict(per_asset),
                    control_probe_occurrences=control_count, candidate_probe_occurrences=candidate_count,
                    matched_control=matched_control, matched_candidate=matched_candidate,
                    control_coverage=matched_control / control_count if control_count else None,
                    candidate_coverage=matched_candidate / candidate_count if candidate_count else None,
                    candidate_minus_control_ms=distribution(lags),
                    per_asset_adjacent_order_reversals=reversals,
                    repeated_fingerprints_control=sum(len(v)-1 for v in control.values()),
                    control_traffic=control_traffic, candidate_traffic=candidate_traffic,
                    event_loop_ms=result['event_loop_ms'], max_rss_bytes=result['max_rss_bytes'],
                    steady_event_loop_ms=result.get('steady_event_loop_ms'), steady_cpu_ms=result.get('steady_cpu_ms'),
                    cpu_ms=result['cpu_ms'], initialization_ms={s['label']:s['initialization_ms'] for s in result['stats']})
    # Discard partially overlapping one-second buckets for callback-work summaries.
    callback = {}
    for feed in result['stats']:
        buckets = [b for sec, b in feed['per_second'].items()
                   if start <= result['started_ms'] + int(sec)*1000
                   and result['started_ms'] + (int(sec)+1)*1000 <= end]
        callback[feed['label']] = dict(frames=sum(b['frames'] for b in buckets),
            work_ms=sum(b.get('callback_total_ms', 0) for b in buckets),
            maximum_ms=max((b.get('callback_max_ms', 0) for b in buckets), default=0),
            measured=any('callback_total_ms' in b for b in buckets))
    analysis['steady_callback_work'] = callback
    (file.parent / 'analysis.json').write_text(json.dumps(analysis, indent=2) + '\n')
    analyses.append(analysis)

summary = dict(method='Canonical complete per-asset payload SHA256; ordinal occurrence pairing; candidate minus control monotonic callback receipt; 5s warmup and 3s matching margins', trials=analyses)
groups = {}
for count in sorted({a['markets'] for a in analyses if a['purpose'].startswith('balanced-')}):
    runs = [a for a in analyses if a['markets'] == count and a['purpose'].startswith('balanced-')]
    p50s = [a['candidate_minus_control_ms']['p50'] for a in runs]
    p99s = [a['candidate_minus_control_ms']['p99'] for a in runs]
    groups[str(count)] = dict(markets=count, assets=count*2, repetitions=len(runs), seconds_per_run=[a['seconds'] for a in runs],
        matched_occurrences=sum(a['matched_control'] for a in runs),
        control_occurrences=sum(a['control_probe_occurrences'] for a in runs),
        p50_lag_ms=dict(median=statistics.median(p50s), minimum=min(p50s), maximum=max(p50s)),
        p99_lag_ms=dict(median=statistics.median(p99s), minimum=min(p99s), maximum=max(p99s)),
        traffic_bytes_per_second=dict(minimum=min(a['candidate_traffic']['bytes_per_second'] for a in runs), maximum=max(a['candidate_traffic']['bytes_per_second'] for a in runs)),
        events_per_second=dict(minimum=min(a['candidate_traffic']['events_per_second'] for a in runs), maximum=max(a['candidate_traffic']['events_per_second'] for a in runs)),
        order_reversals=sum(a['per_asset_adjacent_order_reversals'] for a in runs),
        trial_ids=[a['id'] for a in runs])
summary['balanced_repetitions'] = groups
(root / 'analysis.json').write_text(json.dumps(summary, indent=2) + '\n')
if len(sys.argv) > 2:
    results = [json.loads(p.read_text()) for p in sorted(root.glob('*/result.json'))]
    exported = dict(recording_directory='recordings/' + root.name,
        recommendation=dict(markets_per_connection=100, asset_subscriptions_per_connection=200,
            status='provisional under observed traffic; no proven universal latency or throughput limit'),
        acceptance_search=json.loads((root / 'search.json').read_text()),
        initial_delivery_search=json.loads((root / 'delivery-search.json').read_text()),
        delivery_search_reproduced=False,
        catalog=dict(markets=len(catalog['markets']), assets=sum(len(m['assets']) for m in catalog['markets']),
            versions=dict(collections.Counter(m['version'] for m in catalog['markets'])), probe_market_ids=catalog['probe_market_ids']),
        trial_count=len(results),
        trials=[{k:r[k] for k in ['id','purpose','markets','assets','accepted','interrupted','abort_reason','missing_assets','errors','max_rss_bytes','max_output_queue_bytes','cpu_ms','event_loop_ms','stats']} for r in results],
        analysis=summary,
        sources=['https://docs.polymarket.com/market-data/realtime-data',
                 'https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination'],
        script_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in pathlib.Path('experiments').glob('polymarket-subscription-capacity*') if p.suffix in ['.mjs','.py']})
    profile = root / 'profile-summary.json'
    if profile.exists():
        exported['excluded_profile'] = json.loads(profile.read_text())
    verification = root / 'snapshot-verification.json'
    if verification.exists():
        exported['snapshot_verification'] = json.loads(verification.read_text())
    revalidation = root / 'probe-revalidation.json'
    if revalidation.exists():
        exported['final_probe_revalidation'] = json.loads(revalidation.read_text())
    pathlib.Path(sys.argv[2]).write_text(json.dumps(exported, indent=2) + '\n')
for a in analyses:
    print(json.dumps({k: a[k] for k in ['id', 'markets', 'seconds', 'control_probe_occurrences', 'control_coverage', 'candidate_coverage', 'candidate_minus_control_ms', 'candidate_traffic', 'per_asset_adjacent_order_reversals']}))
