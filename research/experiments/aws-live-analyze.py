#!/usr/bin/env python3
"""Summarize completed live runs from Prometheus samples, never shutdown log statistics."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re

ROW = re.compile(r'^([^ {]+)(?:\{([^}]*)\})?\s+(\S+)$')


def parse(path):
    values = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith('#'):
            continue
        match = ROW.match(line)
        if match:
            labels = tuple(sorted(re.findall(r'(\w+)="([^"]*)"', match[2] or '')))
            values[match[1], labels] = float(match[3])
    return values


def value(values, name, **labels):
    return values.get((name, tuple(sorted(labels.items()))))


def delta(end, start, name):
    a, b = value(start, name), value(end, name)
    if a is None or b is None:
        raise ValueError('Required metric is absent: ' + name)
    if not math.isfinite(a) or not math.isfinite(b) or b < a:
        raise ValueError('Counter reset or invalid metric: ' + name)
    return b - a


def histogram(end, start, name):
    count = delta(end, start, name + '_count')
    total = delta(end, start, name + '_sum')
    result = {'count': int(count), 'mean_seconds': total / count if count else None}
    buckets = sorted((float(dict(labels)['le']), n - start.get((metric, labels), 0))
                     for (metric, labels), n in end.items() if metric == name + '_bucket')
    if any(n < 0 for _, n in buckets):
        raise ValueError('Histogram counter reset: ' + name)
    for quantile in [0.5, 0.95, 0.99]:
        bound = next((bound for bound, n in buckets if n >= quantile * count), math.inf) if count else None
        result['p' + str(int(quantile * 100)) + '_bucket_upper_seconds'] = bound if bound is None or math.isfinite(bound) else None
        result['p' + str(int(quantile * 100)) + '_overflow'] = bound is not None and not math.isfinite(bound)
    return result


def summarize(directory, window):
    if (directory / 'full' / 'status.json').exists():
        directory = directory / 'full'
    status = json.loads((directory / 'status.json').read_text())
    if 'exit_code' not in status:
        raise ValueError('Run is not complete.')
    samples = [json.loads(line) for line in (directory / 'samples.jsonl').read_text().splitlines()]
    observations = []
    for path in sorted((directory / 'metrics').glob('*.prom')):
        elapsed = float(path.stem)
        sample = min(samples, key=lambda row: abs(row['elapsed_s'] - elapsed))
        observations.append((sample, parse(path)))
    if not observations:
        raise ValueError('No retained Prometheus scrapes; legacy log summaries are unsupported.')
    first, last = observations[0], observations[-1]
    if window[0] < first[0]['elapsed_s'] - 2 or window[1] > last[0]['elapsed_s'] + 2:
        raise ValueError('Requested window is outside retained scrapes. Select a matched window.')
    begin = min(observations, key=lambda entry: abs(entry[0]['elapsed_s'] - window[0]))
    end = min(observations, key=lambda entry: abs(entry[0]['elapsed_s'] - window[1]))
    duration = end[0]['elapsed_s'] - begin[0]['elapsed_s']
    if duration <= 0:
        raise ValueError('Window must contain at least two distinct scrapes.')
    last_sample, final = last
    markets = {state: value(final, 'collector_markets', state=state)
               for state in ['desired', 'synced', 'standby_ready']}
    if any(number is None for number in markets.values()):
        raise ValueError('Required logical market gauges are absent.')
    return {'run': directory.parent.name if directory.name == 'full' else directory.name,
            'status': {key: status.get(key) for key in ['started_at', 'ended_at', 'seconds_requested',
                       'elapsed_s', 'exit_code', 'stop_reason', 'binary_sha256', 'raw_capture', 'nofile_runner']},
            'last_scrape': {'elapsed_s': last_sample['elapsed_s'], 'utc': last_sample['utc'],
                           'markets': markets,
                           'missing_publishers': markets['desired'] - markets['synced'],
                           'publishing_without_standby': markets['synced'] - markets['standby_ready'],
                           'books': {state: value(final, 'collector_books', state=state)
                                     for state in ['desired', 'synced']}},
            'window': {'requested_elapsed_seconds': window,
                       'actual_elapsed_seconds': [begin[0]['elapsed_s'], end[0]['elapsed_s']],
                       'seconds': duration,
                       'decoded_events_per_second': delta(end[1], begin[1], 'collector_events_total') / duration,
                       'cpu_cores': (end[0]['cpu_seconds'] - begin[0]['cpu_seconds']) / duration,
                       'internal': histogram(end[1], begin[1], 'collector_receive_to_encode_seconds'),
                       'exchange_age': histogram(end[1], begin[1], 'collector_exchange_to_receive_seconds'),
                       'writer_blocked_seconds': delta(end[1], begin[1], 'collector_writer_blocked_seconds_total')},
            'peak_sampled_rss_mib_whole_run': max(row.get('rss_kb', 0) for row in samples) / 1024,
            'observation_errors': [row['observation_error'] for row in samples if 'observation_error' in row],
            'counters_at_last_scrape': {name: value(final, name) for name in
                ['collector_decode_errors_total', 'collector_exchange_age_negative_total',
                 'collector_book_mismatches_total', 'collector_older_full_books_skipped_total']},
            'source_files': {name: hashlib.file_digest((directory / name).open('rb'), 'sha256').hexdigest()
                             for name in ['status.json', 'samples.jsonl', 'metrics.prom']
                             if (directory / name).exists()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', type=Path, nargs='+')
    parser.add_argument('--window', type=float, nargs=2, default=[595, 895], metavar=('START', 'END'))
    parser.add_argument('--json', type=Path, required=True)
    args = parser.parse_args()
    if args.window[0] >= args.window[1]:
        parser.error('Window start must precede end.')
    result = {'runs': [summarize(directory, args.window) for directory in args.directories],
              'populations': {'internal': 'Emitted publisher price-change levels only; snapshot-derived output excluded.',
                              'events_and_exchange_age': 'Both physical feeds.',
                              'percentiles': 'Upper histogram bucket bounds; no percentile subtraction.'},
              'limits': 'Historical live comparisons are descriptive, not causal. Removal is not book recovery. Observed input is not capacity.'}
    args.json.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    print(json.dumps({'runs': len(result['runs']), 'output': args.json.name}))


if __name__ == '__main__':
    main()
