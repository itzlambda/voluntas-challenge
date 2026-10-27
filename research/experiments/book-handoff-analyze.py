#!/usr/bin/env python3
"""Replay captured application messages in file/array order, using exact decimals.

Usage: python3 -I research/experiments/book-handoff-analyze.py PRIVATE_RUN OUTPUT_JSON
No timestamp/hash deduplication, timestamp sorting, trade subtraction, or mirrors.
"""
import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from decimal import Decimal
from difflib import SequenceMatcher
import hashlib
import json
from pathlib import Path

ZERO = Decimal(0)


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def text(number):
    return format(number, 'f')


def book_state(event):
    state, duplicates = {}, []
    for side, field in [('BUY', 'bids'), ('SELL', 'asks')]:
        seen = set()
        for level in event[field]:
            key = side, Decimal(level['price'])
            quantity = Decimal(level['size'])
            if key in seen:
                duplicates.append({'side': side, 'price': text(key[1])})
            seen.add(key)
            if quantity:
                state[key] = quantity
            else:
                state.pop(key, None)
    return state, duplicates


def differences(before, after):
    # Preserve map insertion order; never sort received data.
    keys = list(before) + [key for key in after if key not in before]
    return [{'side': side, 'price': text(price),
             'before': text(before.get((side, price), ZERO)),
             'after': text(after.get((side, price), ZERO))}
            for side, price in keys
            if before.get((side, price), ZERO) != after.get((side, price), ZERO)]


def reference(row, array_index, event, change_index=None):
    result = {key: row[key] for key in ['index', 'feed', 'frame', 'received_utc', 'received_unix_ms', 'received_mono_ns']}
    result.update(array_index=array_index, exchange_timestamp=event.get('timestamp'), hash=event.get('hash'))
    if change_index is not None:
        result['change_index'] = change_index
        result['hash'] = event['price_changes'][change_index].get('hash')
    return result


def bounded_changes(changes, start, end):
    times = [int(change['received_mono_ns']) for change in changes]
    return changes[bisect_left(times, start):bisect_right(times, end)]


def compare_sequences(a, b):
    """Occurrence-aware alignment of content, never identity by time or hash.

    Sequence alignment is descriptive: repeated content cannot uniquely identify
    exchange events. Each unmatched occurrence remains in the result.
    """
    def signature(item):
        return item['side'], Decimal(item['price']), Decimal(item['size'])
    matcher = SequenceMatcher(a=[signature(x) for x in a], b=[signature(x) for x in b], autojunk=False)
    a_unmatched, b_unmatched, matched = [], [], 0
    for tag, i, j, k, l in matcher.get_opcodes():
        if tag == 'equal':
            matched += j - i
        else:
            a_unmatched.extend(a[i:j])
            b_unmatched.extend(b[k:l])
    return {'A_occurrences': len(a), 'B_occurrences': len(b), 'matched_content_occurrences': matched,
            'A_unmatched_occurrences': len(a_unmatched), 'B_unmatched_occurrences': len(b_unmatched),
            'A_unmatched_examples': a_unmatched[:8], 'B_unmatched_examples': b_unmatched[:8],
            'A_unmatched_all': a_unmatched, 'B_unmatched_all': b_unmatched}


def analyze(run):
    meta = json.loads((run / 'meta.json').read_text())
    assets = set(meta['asset_ids'])
    models, initial, previous_book, initial_states = {}, {}, {}, {}
    book_counts = Counter()
    stats = defaultdict(Counter)
    changes = defaultdict(list)
    comparisons, incidents, cross = [], [], []
    quality = Counter()
    controls = []
    joins = {}
    last_frames, last_receive, last_market, opened_at, last_receive_ref = {}, {}, {}, {}, {}
    max_gaps = defaultdict(lambda: {'application_ms': 0, 'market_ms': 0})
    gap_examples = []
    previous_index, previous_mono = 0, 0
    phase = None
    open_b = {}
    close_rows = []
    close_requests = {}
    a_open_index, a_close_index = None, None
    lag_rows = []

    def new_phase(name, row):
        nonlocal phase
        if phase is not None:
            phase['end_mono_ns'] = row['received_mono_ns']
            phase['end_utc'] = row['received_utc']
            phase['seconds'] = (int(row['received_mono_ns']) - int(phase['start_mono_ns'])) / 1e9
        phase = {'name': name, 'start_mono_ns': row['received_mono_ns'], 'start_utc': row['received_utc'], 'counts': Counter()}
        controls.append(phase)

    with open(run / 'frames.jsonl') as source:
        for line_number, line in enumerate(source, 1):
            row = json.loads(line)
            quality['capture_records'] += 1
            if row['index'] != previous_index + 1:
                quality['capture_index_discontinuities'] += 1
            previous_index = row['index']
            mono = int(row['received_mono_ns'])
            if mono < previous_mono:
                quality['receive_monotonic_regressions'] += 1
            previous_mono = mono
            feed, kind = row.get('feed'), row['kind']
            if kind == 'collector_lag':
                lag_rows.append({key: row[key] for key in ['index', 'received_utc', 'lag_ms']})
            if feed == 'pilot':
                quality['pilot_records_excluded'] += 1
                continue
            if feed:
                stats[feed][kind] += 1
            if kind in ['experiment_error', 'hard_timeout', 'non_json']:
                quality[kind] += 1
            if kind == 'join_start':
                joins[f"B{row['join']:02d}"] = {'join': row['join'], 'start_utc': row['received_utc'], 'initial_books': {}, 'first_refreshes': {}}
            if kind == 'join_end':
                joins[f"B{row['join']:02d}"]['collector_initial_books'] = row['initial_books']
            if kind == 'sent':
                if row.get('raw') == 'PING':
                    stats[feed]['PING_sent'] += 1
                else:
                    subscription = json.loads(row['raw'])
                    stats[feed]['subscriptions_sent'] += 1
                    if subscription.get('type') != 'market' or subscription.get('initial_dump') is not True or set(subscription.get('assets_ids', [])) != assets:
                        quality['unexpected_subscriptions'] += 1
            if kind == 'close_requested':
                close_requests[feed] = row['index']
            if kind == 'open':
                opened_at[feed] = mono
                if feed == 'A':
                    a_open_index = row['index']
                    new_phase('established_control_0', row)
                elif feed and feed.startswith('B'):
                    open_b[feed] = True
                    if len(open_b) > 1:
                        quality['overlapping_B_open_events'] += 1
                    quality['maximum_open_B_sockets'] = max(quality['maximum_open_B_sockets'], len(open_b))
                    joins[feed]['open'] = {key: row[key] for key in ['index', 'received_utc', 'received_mono_ns']}
                    new_phase('joining_' + '+'.join(open_b), row)
            if kind == 'close':
                if feed in last_receive:
                    stats[feed]['last_receive_to_close_ms'] = (mono - last_receive[feed]) / 1e6
                if feed not in close_requests:
                    quality['unexpected_closes'] += 1
                if feed == 'A':
                    a_close_index = row['index']
                    if feed not in close_requests:
                        quality['A_continuity_failure'] += 1
                close_rows.append({key: row[key] for key in ['feed', 'index', 'received_utc', 'code', 'was_clean', 'reason']})
                if feed == 'A':
                    if phase:
                        phase['end_mono_ns'] = row['received_mono_ns']
                        phase['end_utc'] = row['received_utc']
                        phase['seconds'] = (mono - int(phase['start_mono_ns'])) / 1e9
                    phase = None
                    quality['A_closes'] += 1
                elif feed and feed.startswith('B'):
                    joins[feed]['close'] = {**close_rows[-1], 'received_mono_ns': row['received_mono_ns']}
                    open_b.pop(feed, None)
                    if a_close_index is None:
                        if open_b:
                            new_phase('joining_' + '+'.join(open_b), row)
                        else:
                            new_phase(f'established_control_{joins[feed]["join"]}', row)
            if kind != 'received':
                continue
            frame = row['frame']
            if frame != last_frames.get(feed, 0) + 1:
                quality['feed_frame_discontinuities'] += 1
            last_frames[feed] = frame
            if feed not in last_receive and feed in opened_at:
                stats[feed]['open_to_first_receive_ms'] = (mono - opened_at[feed]) / 1e6
            if feed in last_receive:
                gap = (mono - last_receive[feed]) / 1e6
                max_gaps[feed]['application_ms'] = max(max_gaps[feed]['application_ms'], gap)
                if gap > 1000:
                    stats[feed]['application_gaps_gt_1s'] += 1
                if gap > 5000:
                    stats[feed]['application_gaps_gt_5s'] += 1
                    gap_examples.append({'feed': feed, 'frame': frame, 'index': row['index'], 'received_utc': row['received_utc'], 'gap_ms': gap, 'start_mono_ns': str(last_receive[feed]), 'end_mono_ns': row['received_mono_ns'], 'previous_receive': last_receive_ref[feed]})
            last_receive[feed] = mono
            last_receive_ref[feed] = {key: row[key] for key in ['index', 'frame', 'received_utc', 'received_mono_ns']}
            raw = row['raw']
            if raw == 'PONG':
                stats[feed]['PONG'] += 1
                continue
            if not isinstance(raw, str):
                quality['non_text_messages'] += 1
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                quality['json_errors'] += 1
                continue
            if feed in last_market:
                gap = (mono - last_market[feed]) / 1e6
                max_gaps[feed]['market_ms'] = max(max_gaps[feed]['market_ms'], gap)
                if gap > 5000:
                    stats[feed]['market_gaps_gt_5s'] += 1
            last_market[feed] = mono
            events = value if isinstance(value, list) else [value]
            if isinstance(value, list):
                stats[feed]['array_frames'] += 1
            for array_index, event in enumerate(events):
                event_type = event.get('event_type')
                stats[feed][f'event_{event_type}'] += 1
                if feed == 'A' and phase is not None:
                    phase['counts'][f'event_{event_type}'] += 1
                ref = reference(row, array_index, event)
                if event.get('timestamp') is None:
                    quality['events_without_exchange_timestamp'] += 1
                if event_type == 'book':
                    asset = event['asset_id']
                    if asset not in assets:
                        quality['unexpected_assets'] += 1
                        continue
                    key = feed, asset
                    incoming, duplicates = book_state(event)
                    quality['duplicate_book_price_entries'] += len(duplicates)
                    ordinal = book_counts[key]
                    if key in models:
                        diff = differences(models[key], incoming)
                        comp = {**ref, 'asset_id': asset, 'refresh_number': ordinal,
                                'refresh_kind': 'FIRST' if ordinal == 1 else 'later',
                                'initial': initial[key], 'previous_book': previous_book[key],
                                'changed_levels': len(diff), 'differences': diff,
                                'phase': phase['name'] if feed == 'A' and phase else None}
                        comparisons.append(comp)
                        stats[feed]['comparisons'] += 1
                        stats[feed]['first_refresh_comparisons' if ordinal == 1 else 'later_refresh_comparisons'] += 1
                        if diff:
                            incidents.append(comp)
                            stats[feed]['differing_comparisons'] += 1
                            stats[feed]['first_refresh_differences' if ordinal == 1 else 'later_refresh_differences'] += 1
                            if feed == 'A' and phase:
                                phase['counts']['differing_comparisons'] += 1
                        if feed == 'A' and phase:
                            phase['counts']['comparisons'] += 1
                    else:
                        initial[key] = ref
                        initial_states[key] = incoming.copy()
                        stats[feed]['initial_books'] += 1
                        if feed.startswith('B'):
                            joins[feed]['initial_books'][asset] = ref
                    if feed.startswith('B') and ordinal == 1:
                        joins[feed]['first_refreshes'][asset] = ref
                    if feed.startswith('B') and ('A', asset) in models:
                        asof_diff = differences(models[('A', asset)], incoming)
                        cross.append({**ref, 'asset_id': asset, 'B_book_number': ordinal,
                                      'A_last_book': previous_book[('A', asset)],
                                      'different_levels_from_A_as_received': len(asof_diff),
                                      'differences': asof_diff})
                        stats[feed]['cross_asof_comparisons'] += 1
                        if asof_diff:
                            stats[feed]['cross_asof_differences'] += 1
                    models[key] = incoming
                    previous_book[key] = ref
                    book_counts[key] += 1
                elif event_type == 'price_change':
                    for change_index, change in enumerate(event['price_changes']):
                        asset = change['asset_id']
                        if asset not in assets:
                            quality['unexpected_assets'] += 1
                            continue
                        side = change['side']
                        if side not in ['BUY', 'SELL']:
                            raise ValueError('Unknown depth side')
                        price, size = Decimal(change['price']), Decimal(change['size'])
                        change_ref = {**reference(row, array_index, event, change_index),
                                      'asset_id': asset, 'side': side, 'price': text(price), 'size': text(size)}
                        changes[(feed, asset)].append(change_ref)
                        stats[feed]['absolute_level_entries'] += 1
                        if feed == 'A' and phase:
                            phase['counts']['absolute_level_entries'] += 1
                        if (feed, asset) not in models:
                            stats[feed]['entries_before_initial_book'] += 1
                            continue
                        if size:
                            models[(feed, asset)][(side, price)] = size
                        else:
                            models[(feed, asset)].pop((side, price), None)
                # Trade notifications and other events never mutate depth.

    # Every join gets an occurrence-aware A/B comparison for each outcome's
    # initial-to-FIRST-refresh receive interval. Edge-expanded A windows are
    # diagnostic only; neither matching content nor time proves event identity.
    for feed, join in joins.items():
        windows = []
        for asset in meta['asset_ids']:
            begin = join['initial_books'].get(asset)
            end = join['first_refreshes'].get(asset)
            if begin is None or end is None:
                windows.append({'asset_id': asset, 'status': 'no_initial_or_no_first_refresh'})
                continue
            lo, hi = int(begin['received_mono_ns']), int(end['received_mono_ns'])
            a = bounded_changes(changes[('A', asset)], lo, hi)
            b = [x for x in changes[(feed, asset)] if (begin['index'], begin['array_index']) < (x['index'], x['array_index']) < (end['index'], end['array_index'])]
            expanded_a = bounded_changes(changes[('A', asset)], lo - 2_000_000_000, hi + 2_000_000_000)
            comparison = compare_sequences(a, b)
            audit = []
            for occurrence in comparison['A_unmatched_all']:
                matches = [candidate for candidate in changes[(feed, asset)]
                           if candidate['side'] == occurrence['side']
                           and Decimal(candidate['price']) == Decimal(occurrence['price'])
                           and Decimal(candidate['size']) == Decimal(occurrence['size'])]
                before = [candidate for candidate in matches if (candidate['index'], candidate['array_index']) < (begin['index'], begin['array_index'])]
                after = [candidate for candidate in matches if (candidate['index'], candidate['array_index']) > (end['index'], end['array_index'])
                         and abs(int(candidate['received_mono_ns']) - int(occurrence['received_mono_ns'])) <= 2_000_000_000]
                audit.append({'A_occurrence': occurrence, 'equal_content_B_occurrences_before_initial': before,
                              'equal_content_B_occurrences_after_FIRST_within_2s_of_A': after,
                              'equal_content_B_occurrences_anywhere_in_session': matches,
                              'B_initial_quantity': text(initial_states[(feed, asset)].get((occurrence['side'], Decimal(occurrence['price'])), ZERO))})
            windows.append({'asset_id': asset, 'status': 'compared', 'initial': begin, 'FIRST_refresh': end,
                            'window_ms': (hi - lo) / 1e6, 'same_receive_window': comparison,
                            'A_unmatched_boundary_audit': audit,
                            'A_expanded_by_2s_diagnostic': compare_sequences(expanded_a, b)})
        join['first_refresh_windows'] = windows
        close = join.get('close', {})
        join['observed_seconds'] = (int(close.get('received_mono_ns', join['open']['received_mono_ns'])) - int(join['open']['received_mono_ns'])) / 1e9
        join['A_open_throughout'] = (a_open_index is not None and a_open_index < join['open']['index']
                                    and (a_close_index is None or a_close_index > close.get('index', previous_index)))
        join['successful'] = (len(join['initial_books']) == 2 and close.get('code') == 1000
                              and close.get('was_clean') and stats[feed]['error'] == 0
                              and close_requests.get(feed, previous_index + 1) < close.get('index', 0)
                              and join['observed_seconds'] >= meta.get('join_seconds', 0)
                              and join['A_open_throughout'])

    # For any changed refresh, retain every relevant level occurrence. Look for
    # the same numerical old/new transition on A in a +/-2s receive window;
    # retain occurrences rather than treating hashes or times as unique keys.
    enriched = []
    for incident in incidents:
        item = dict(incident)
        details = []
        for diff in incident['differences']:
            asset, feed = incident['asset_id'], incident['feed']
            lo = int(incident['initial']['received_mono_ns'])
            hi = int(incident['received_mono_ns'])
            a = bounded_changes(changes[('A', asset)], lo - 2_000_000_000, hi + 2_000_000_000)
            b = [x for x in changes[(feed, asset)] if (incident['initial']['index'], incident['initial']['array_index']) < (x['index'], x['array_index']) < (incident['index'], incident['array_index'])]
            relevant = lambda xs: [x for x in xs if x['side'] == diff['side'] and Decimal(x['price']) == Decimal(diff['price'])]
            a_level, b_level = relevant(a), relevant(b)
            matching_a_refreshes = []
            for candidate in incidents:
                if candidate['feed'] != 'A' or candidate['asset_id'] != asset:
                    continue
                if abs(int(candidate['received_mono_ns']) - hi) > 2_000_000_000:
                    continue
                if diff in candidate['differences']:
                    matching_a_refreshes.append({key: candidate[key] for key in ['index', 'frame', 'array_index', 'received_utc', 'received_mono_ns', 'exchange_timestamp', 'refresh_kind']})
            details.append({**diff, 'A_level_occurrences_initial_minus_2s_to_refresh_plus_2s': a_level,
                            'own_level_occurrences_initial_to_refresh': b_level,
                            'A_matching_book_only_changes_within_2s': matching_a_refreshes,
                            'A_target_quantity_occurrences': [x for x in a_level if Decimal(x['size']) == Decimal(diff['after'])],
                            'own_target_quantity_occurrences': [x for x in b_level if Decimal(x['size']) == Decimal(diff['after'])]})
        item['level_evidence'] = details
        enriched.append(item)

    for comparison in cross:
        if not comparison['different_levels_from_A_as_received']:
            continue
        hi = int(comparison['received_mono_ns'])
        candidates = bounded_changes(changes[('A', comparison['asset_id'])], hi, hi + 2_000_000_000)
        evidence = []
        for diff in comparison['differences']:
            matching = [x for x in candidates if x['side'] == diff['side']
                        and Decimal(x['price']) == Decimal(diff['price'])
                        and Decimal(x['size']) == Decimal(diff['after'])]
            evidence.append({**diff, 'later_A_target_quantity_occurrences_within_2s': matching})
        comparison['level_evidence'] = evidence
        comparison['every_differing_level_target_later_observed_on_A_within_2s'] = all(
            x['later_A_target_quantity_occurrences_within_2s'] for x in evidence)

    for gap in gap_examples:
        counts = Counter()
        lo, hi = int(gap['start_mono_ns']), int(gap['end_mono_ns'])
        for (feed, asset), entries in changes.items():
            if feed != gap['feed']:
                counts[feed] += len(bounded_changes(entries, lo, hi))
        gap['other_feed_absolute_level_entries_during_gap'] = dict(counts)

    feeds = {feed: {**dict(counts), 'maximum_receive_gaps_ms': max_gaps[feed]} for feed, counts in stats.items()}
    summary = {
        'schema_version': 1, 'complete_capture': meta.get('completed', False),
        'started_utc': meta['started_utc'], 'main_started_utc': meta['main_started_utc'],
        'finished_utc': meta.get('finished_utc'), 'capture_meta': meta,
        'source_fingerprints': {'frames.jsonl': digest(run / 'frames.jsonl'), 'meta.json': digest(run / 'meta.json'),
                                'gamma.json': digest(run / 'gamma.json'), 'analyzer': digest(Path(__file__))},
        'quality': dict(quality), 'feed_statistics': feeds,
        'totals': {'joins_started': len(joins), 'joins_successful': sum(join['successful'] for join in joins.values()),
                   'main_feeds': len(feeds), 'main_closes': len(close_rows), 'main_errors': sum(x['error'] for x in stats.values()), 'close_timeouts': sum(x['close_timeout'] for x in stats.values()),
                   'initial_to_FIRST_refresh_windows_compared': sum(window['status'] == 'compared' for join in joins.values() for window in join['first_refresh_windows']),
                   'initial_to_FIRST_refresh_windows_without_refresh': sum(window['status'] != 'compared' for join in joins.values() for window in join['first_refresh_windows']),
                   'A_unmatched_content_occurrences_in_same_receive_windows': sum(window['same_receive_window']['A_unmatched_occurrences'] for join in joins.values() for window in join['first_refresh_windows'] if window['status'] == 'compared'),
                   'A_unmatched_with_target_already_in_B_initial_and_no_boundary_match': sum(not (item['equal_content_B_occurrences_before_initial'] or item['equal_content_B_occurrences_after_FIRST_within_2s_of_A']) and Decimal(item['B_initial_quantity']) == Decimal(item['A_occurrence']['size']) for join in joins.values() for window in join['first_refresh_windows'] if window['status'] == 'compared' for item in window['A_unmatched_boundary_audit']),
                   'A_unmatched_without_boundary_match_or_initial_quantity_match': sum(not (item['equal_content_B_occurrences_before_initial'] or item['equal_content_B_occurrences_after_FIRST_within_2s_of_A']) and Decimal(item['B_initial_quantity']) != Decimal(item['A_occurrence']['size']) for join in joins.values() for window in join['first_refresh_windows'] if window['status'] == 'compared' for item in window['A_unmatched_boundary_audit']),
                   'A_unmatched_with_equal_B_content_before_initial_or_after_FIRST_within_2s': sum(bool(item['equal_content_B_occurrences_before_initial'] or item['equal_content_B_occurrences_after_FIRST_within_2s_of_A']) for join in joins.values() for window in join['first_refresh_windows'] if window['status'] == 'compared' for item in window['A_unmatched_boundary_audit']),
                   'B_unmatched_content_occurrences_in_same_receive_windows': sum(window['same_receive_window']['B_unmatched_occurrences'] for join in joins.values() for window in join['first_refresh_windows'] if window['status'] == 'compared'),
                   'repeat_book_comparisons': len(comparisons), 'differing_comparisons': len(incidents),
                   'differing_levels': sum(x['changed_levels'] for x in incidents),
                   'FIRST_refresh_comparisons': sum(x['refresh_kind'] == 'FIRST' for x in comparisons),
                   'FIRST_refresh_differences': sum(x['refresh_kind'] == 'FIRST' for x in incidents),
                   'later_refresh_comparisons': sum(x['refresh_kind'] == 'later' for x in comparisons),
                   'later_refresh_differences': sum(x['refresh_kind'] == 'later' for x in incidents),
                   'cross_asof_comparisons': len(cross),
                   'cross_asof_differences': sum(bool(x['different_levels_from_A_as_received']) for x in cross),
                   'cross_asof_differences_all_level_targets_later_on_A_within_2s': sum(bool(x.get('every_differing_level_target_later_observed_on_A_within_2s')) for x in cross),
                   'cross_asof_INITIAL_differences': sum(x['B_book_number'] == 0 and bool(x['different_levels_from_A_as_received']) for x in cross),
                   'cross_asof_FIRST_differences': sum(x['B_book_number'] == 1 and bool(x['different_levels_from_A_as_received']) for x in cross),
                   'cross_asof_later_differences': sum(x['B_book_number'] > 1 and bool(x['different_levels_from_A_as_received']) for x in cross)},
        'control_intervals': controls, 'joins': joins, 'close_records': close_rows,
        'receive_gaps_over_5s': gap_examples, 'collector_lag_over_100ms': lag_rows,
        'differing_refreshes': enriched, 'cross_asof_differences': [x for x in cross if x['different_levels_from_A_as_received']],
        'limitations': ['Application messages, not TCP packets; automatic protocol control frames unavailable.',
                        'Shared process, runtime, host, network route and upstream can hide correlated gaps.',
                        'Receive silence is not proof of missing data; cross-feed as-of comparisons include delivery skew.',
                        'Content sequence alignment is occurrence-aware but cannot prove unique event identity.',
                        'No negative observation establishes handoff correctness or causes of earlier incidents.']}
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    result = analyze(args.run)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    args.output.chmod(0o600)
    print(json.dumps({'totals': result['totals'], 'quality': result['quality']}))


if __name__ == '__main__':
    main()
