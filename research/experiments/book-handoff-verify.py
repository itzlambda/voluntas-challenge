#!/usr/bin/env python3
"""Behavioral controls for the handoff analyzer; scratch files stay under /tmp."""
import importlib.util
import json
from pathlib import Path
import tempfile

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('handoff_analyze', HERE / 'book-handoff-analyze.py')
analyzer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analyzer)


def run_fixture(events):
    with tempfile.TemporaryDirectory(prefix='book-handoff-verify-', dir='/tmp') as tmp:
        root = Path(tmp)
        meta = {'asset_ids': ['1', '2'], 'started_utc': '2026-01-01T00:00:00Z',
                'main_started_utc': '2026-01-01T00:00:00Z', 'finished_utc': '2026-01-01T00:00:01Z', 'completed': True}
        (root / 'meta.json').write_text(json.dumps(meta))
        (root / 'gamma.json').write_text('[]')
        frames, per_feed = [], {}
        for number, event in enumerate(events, 1):
            event = dict(event)
            row = {'index': number, 'received_utc': f'2026-01-01T00:00:00.{number:03d}Z',
                   'received_unix_ms': 1767225600000 + number, 'received_mono_ns': str(number * 1_000_000), **event}
            if event['kind'] == 'received':
                feed = event['feed']
                per_feed[feed] = per_feed.get(feed, 0) + 1
                row['frame'] = per_feed[feed]
                row['raw'] = json.dumps(row.pop('events'))
            frames.append(row)
        (root / 'frames.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in frames))
        return analyzer.analyze(root)


def book(asset='1', size='10', timestamp='1000'):
    return {'event_type': 'book', 'asset_id': asset, 'bids': [{'price': '0.4', 'size': size}],
            'asks': [], 'timestamp': timestamp, 'hash': 'shared-hash'}


def pc(sizes, timestamp='1000', asset='1'):
    return {'event_type': 'price_change', 'timestamp': timestamp,
            'price_changes': [{'asset_id': asset, 'side': 'BUY', 'price': '0.4', 'size': size, 'hash': 'shared-hash'} for size in sizes]}


def received(feed, events):
    return {'kind': 'received', 'feed': feed, 'events': events}


def prefix():
    return [{'kind': 'open', 'feed': 'A'}, received('A', [book(), book('2')]),
            {'kind': 'join_start', 'join': 1}, {'kind': 'open', 'feed': 'B01'},
            received('B01', [book(), book('2')])]


def suffix():
    return [{'kind': 'join_end', 'join': 1, 'initial_books': 2}, {'kind': 'close_requested', 'feed': 'B01'},
            {'kind': 'close', 'feed': 'B01', 'code': 1000, 'was_clean': True, 'reason': 'done'},
            {'kind': 'close_requested', 'feed': 'A'}, {'kind': 'close', 'feed': 'A', 'code': 1000, 'was_clean': True, 'reason': 'done'}]


def check_missing_handoff_update():
    result = run_fixture(prefix() + [received('A', pc(['12'])), received('A', book(size='12')),
                                    received('B01', book(size='12'))] + suffix())
    assert result['totals']['differing_comparisons'] == 1
    diff = result['differing_refreshes'][0]
    assert diff['feed'] == 'B01' and diff['refresh_kind'] == 'FIRST'
    evidence = diff['level_evidence'][0]
    assert len(evidence['A_target_quantity_occurrences']) == 1
    assert evidence['own_target_quantity_occurrences'] == []
    assert evidence['A_matching_book_only_changes_within_2s'] == []


def check_shared_book_only_change():
    result = run_fixture(prefix() + [received('A', book(size='12')), received('B01', book(size='12'))] + suffix())
    assert result['totals']['differing_comparisons'] == 2
    evidence = next(x for x in result['differing_refreshes'] if x['feed'] == 'B01')['level_evidence'][0]
    assert len(evidence['A_matching_book_only_changes_within_2s']) == 1
    assert evidence['A_target_quantity_occurrences'] == []


def check_original_order_equal_keys_and_old_timestamps():
    result = run_fixture(prefix() + [received('A', [pc(['11', '12']), book(size='12'), pc(['9'], timestamp='999'), book(size='9', timestamp='998')]),
                                    received('B01', [pc(['11', '12']), book(size='12'), pc(['9'], timestamp='999'), book(size='9', timestamp='998')])] + suffix())
    assert result['totals']['differing_comparisons'] == 0
    assert result['feed_statistics']['A']['absolute_level_entries'] == 3
    assert result['totals']['FIRST_refresh_comparisons'] == 2
    assert result['totals']['later_refresh_comparisons'] == 2
    assert result['joins']['B01']['first_refresh_windows'][0]['same_receive_window']['B_occurrences'] == 2


def check_reversed_array_fails():
    result = run_fixture(prefix() + [received('B01', [pc(['12', '11']), book(size='12')])] + suffix())
    assert result['totals']['differing_comparisons'] == 1
    assert result['differing_refreshes'][0]['differences'][0]['before'] == '11'


def check_no_trade_subtraction_or_mirror():
    trade = {'event_type': 'last_trade_price', 'asset_id': '1', 'side': 'BUY', 'price': '0.4', 'size': '3', 'timestamp': '1001'}
    result = run_fixture(prefix() + [received('A', [pc(['7']), trade, book(size='7'), book('2')])] + suffix())
    assert result['totals']['differing_comparisons'] == 0


def check_prebaseline_and_same_frame_window_edges():
    rows = [{'kind': 'open', 'feed': 'A'}, received('A', [pc(['99']), book(), book('2')]),
            {'kind': 'join_start', 'join': 1}, {'kind': 'open', 'feed': 'B01'},
            received('B01', [pc(['99']), book(), pc(['11', '12']), book(size='12'), pc(['14']), book('2')])]
    result = run_fixture(rows + suffix())
    assert result['totals']['differing_comparisons'] == 0
    assert result['feed_statistics']['A']['entries_before_initial_book'] == 1
    assert result['feed_statistics']['B01']['entries_before_initial_book'] == 1
    window = result['joins']['B01']['first_refresh_windows'][0]
    assert window['same_receive_window']['B_occurrences'] == 2
    assert window['same_receive_window']['B_unmatched_occurrences'] == 2


def check_repeated_content_not_deduplicated():
    a = [{'side': 'BUY', 'price': '0.4', 'size': '10'}] * 3
    b = [{'side': 'BUY', 'price': '0.4', 'size': '10'}] * 2
    comparison = analyzer.compare_sequences(a, b)
    assert comparison['matched_content_occurrences'] == 2
    assert comparison['A_unmatched_occurrences'] == 1


def check_cross_asof_skew_resolves_and_continuity():
    result = run_fixture(prefix() + [received('B01', pc(['12'])), received('B01', book(size='12')),
                                    received('A', pc(['12'])), received('A', book(size='12'))] + suffix())
    assert result['totals']['differing_comparisons'] == 0
    assert result['totals']['cross_asof_differences'] == 1
    assert result['totals']['cross_asof_differences_all_level_targets_later_on_A_within_2s'] == 1
    assert result['joins']['B01']['successful']
    interrupted = prefix() + [{'kind': 'close', 'feed': 'A', 'code': 1006, 'was_clean': False, 'reason': 'injected'}]
    result = run_fixture(interrupted + suffix()[:-2])
    assert not result['joins']['B01']['successful']
    assert result['quality']['A_continuity_failure'] == 1


def check_delayed_close_overlap_is_not_control():
    close = lambda feed: [{'kind': 'close_requested', 'feed': feed},
                          {'kind': 'close', 'feed': feed, 'code': 1000, 'was_clean': True, 'reason': 'done'}]
    rows = prefix() + [{'kind': 'join_start', 'join': 2}, {'kind': 'open', 'feed': 'B02'},
                       received('B02', [book(), book('2')])]
    rows += close('B01') + [received('A', pc(['12']))] + close('B02')
    rows += [received('A', pc(['13']))] + close('A')
    result = run_fixture(rows)
    controls = [x for x in result['control_intervals'] if x['name'].startswith('established_control_')]
    assert sum(x['counts'].get('absolute_level_entries', 0) for x in controls) == 1
    assert result['quality']['maximum_open_B_sockets'] == 2
    assert result['quality']['overlapping_B_open_events'] == 1


def main():
    checks = [check_missing_handoff_update, check_shared_book_only_change,
              check_original_order_equal_keys_and_old_timestamps, check_reversed_array_fails,
              check_no_trade_subtraction_or_mirror, check_prebaseline_and_same_frame_window_edges,
              check_repeated_content_not_deduplicated, check_cross_asof_skew_resolves_and_continuity, check_delayed_close_overlap_is_not_control]
    for check in checks:
        check()
        print('PASS', check.__name__)
    print(f'{len(checks)} behavioral controls passed')


if __name__ == '__main__':
    main()
