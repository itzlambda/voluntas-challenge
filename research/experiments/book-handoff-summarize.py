#!/usr/bin/env python3
"""Write a whitelisted local result summary; never copy raw payloads or metadata.

Usage: python3 -B -I research/experiments/book-handoff-summarize.py PRIVATE_ANALYSIS [PRIVATE_ANALYSIS ...] PRIVATE_SOURCES OUTPUT_PREFIX
"""
import argparse
import hashlib
from datetime import datetime
import json
from pathlib import Path


def summarize(analysis, sources):
    meta = analysis['capture_meta']
    feeds = analysis['feed_statistics']
    joins = []
    for feed, join in analysis['joins'].items():
        windows = []
        for window in join['first_refresh_windows']:
            if window['status'] != 'compared':
                windows.append({'asset_id': window['asset_id'], 'status': window['status']})
                continue
            comparison = window['same_receive_window']
            windows.append({'asset_id': window['asset_id'], 'status': window['status'], 'window_ms': window['window_ms'],
                            **{key: comparison[key] for key in ['A_occurrences', 'B_occurrences', 'matched_content_occurrences', 'A_unmatched_occurrences', 'B_unmatched_occurrences']}})
        joins.append({'feed': feed, 'join': join['join'], 'successful': join['successful'],
                      'initial_books': len(join['initial_books']), 'observed_seconds': join['observed_seconds'], 'A_open_throughout': join['A_open_throughout'], 'assets_with_FIRST_refresh': len(join['first_refreshes']),
                      'repeat_comparisons': feeds[feed].get('comparisons', 0),
                      'FIRST_comparisons': feeds[feed].get('first_refresh_comparisons', 0),
                      'FIRST_differences': feeds[feed].get('first_refresh_differences', 0),
                      'later_comparisons': feeds[feed].get('later_refresh_comparisons', 0),
                      'later_differences': feeds[feed].get('later_refresh_differences', 0),
                      'first_refresh_windows': windows})
    quality = {key: analysis['quality'].get(key, 0) for key in [
        'capture_records', 'pilot_records_excluded', 'capture_index_discontinuities',
        'feed_frame_discontinuities', 'receive_monotonic_regressions', 'duplicate_book_price_entries',
        'unexpected_assets', 'non_text_messages', 'json_errors', 'events_without_exchange_timestamp',
        'experiment_error', 'hard_timeout', 'non_json', 'unexpected_closes', 'A_continuity_failure', 'unexpected_subscriptions', 'overlapping_B_open_events', 'maximum_open_B_sockets']}
    control = [phase for phase in analysis['control_intervals'] if phase['name'].startswith('established_control_')]
    control_counts = {}
    for phase in control:
        for key, value in phase['counts'].items():
            control_counts[key] = control_counts.get(key, 0) + value
    return {
        'complete_capture': analysis['complete_capture'],
        'conclusion': 'inconclusive: no repeat-book reconstruction incident observed' if not analysis['totals']['differing_comparisons'] else 'changed refreshes observed; inspect private level evidence before causal attribution',
        'period': {key: analysis[key] for key in ['started_utc', 'main_started_utc', 'finished_utc']},
        'elapsed_seconds_including_pilot': meta.get('elapsed_seconds'),
        'market': {key: meta['selected'][key] for key in ['id', 'conditionId', 'endDate', 'active', 'closed', 'acceptingOrders']},
        'asset_ids': meta['asset_ids'], 'pilot_absolute_level_entries': meta['pilot_updates'],
        'subscription': {'type': 'market', 'assets_ids': meta['asset_ids'], 'initial_dump': True},
        'schedule': {key: meta[key] for key in ['joins_planned', 'baseline_seconds', 'join_seconds', 'control_gap_seconds', 'tail_seconds']},
        'totals': analysis['totals'], 'quality': quality,
        'feed_statistics': feeds, 'joins': joins,
        'established_only_control': {'intervals': len(control), 'seconds': sum(x.get('seconds', 0) for x in control), 'counts': control_counts},
        'control_intervals': analysis['control_intervals'],
        'close_records': [{key: row[key] for key in ['feed', 'index', 'received_utc', 'code', 'was_clean']} for row in analysis['close_records']],
        'main_close_codes': {str(code): sum(row['code'] == code for row in analysis['close_records']) for code in set(row['code'] for row in analysis['close_records'])},
        'receive_gaps_over_5s': analysis['receive_gaps_over_5s'],
        'collector_lag': {'over_100ms': len(analysis['collector_lag_over_100ms']), 'maximum_ms': meta.get('max_event_loop_lag_ms')},
        'remaining_sockets': meta.get('remaining_sockets'),
        'source_fingerprints': {**analysis['source_fingerprints'], 'capture_script': meta['source_sha256']},
        'script_fingerprints': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('book-handoff-*') if p.suffix in ['.py', '.mjs']},
        'public_documentation': [{key: source[key] for key in ['url', 'http_status', 'retrieved_utc', 'sha256', 'bytes']} for source in sources],
        'limitations': analysis['limitations'],
        'publication': 'Local repository artifacts only; no external publication. Raw application messages retained privately under /tmp.',
    }


def aggregate(runs):
    totals = {}
    quality = {}
    for run in runs:
        for key, value in run['totals'].items():
            totals[key] = totals.get(key, 0) + value
        for key, value in run['quality'].items():
            quality[key] = max(quality.get(key, 0), value) if key.startswith('maximum_') else quality.get(key, 0) + value
    start = runs[0]['period']['started_utc']
    end = runs[-1]['period']['finished_utc']
    return {'conclusion': 'Inconclusive: no repeat-book reconstruction incident observed.' if not totals['differing_comparisons'] else 'Changed refreshes observed; causal review required.',
            'baseline_epochs': len(runs), 'totals': totals, 'quality': quality,
            'period': {'started_utc': start, 'finished_utc': end,
                       'wall_seconds': (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(),
                       'capture_seconds_including_pilots': sum(run['elapsed_seconds_including_pilot'] for run in runs)},
            'runs': runs,
            'scope': 'Public feed only. No orders, production edits, commits, performance benchmark, or external publication.',
            'limitations': ['An established socket failed; the baseline epochs are separate. There was no uninterrupted 20-join baseline.',
                            'Shared collector, runtime, host, network and upstream can hide correlated gaps.',
                            'Application messages are retained, not wire fragments or automatic protocol control frames.',
                            'No observed incident establishes neither handoff correctness nor the cause of earlier divergences.']}


def markdown(result, prefix):
    runs = result['runs']
    t = result['totals']
    quality = result['quality']
    feeds = [feed for run in runs for feed in run['feed_statistics'].values()]
    closes = [row for run in runs for row in run['close_records']]
    close_codes = {str(code): sum(x['code'] == code for x in closes) for code in set(x['code'] for x in closes)}
    controls = [run['established_only_control'] for run in runs]
    first = sum(join['FIRST_comparisons'] for run in runs for join in run['joins'])
    later = sum(join['later_comparisons'] for run in runs for join in run['joins'])
    lag_count = sum(run['collector_lag']['over_100ms'] for run in runs)
    max_lag = max(run['collector_lag']['maximum_ms'] for run in runs)
    run_lines = []
    for i, run in enumerate(runs, 1):
        rt = run['totals']
        schedule = run['schedule']
        run_lines.append(f"- Epoch {i}: {run['period']['started_utc']} to {run['period']['finished_utc']}; "
                         f"completed schedule: {run['complete_capture']}; {rt['joins_successful']}/{rt['joins_started']} successful joins; "
                         f"{rt['repeat_book_comparisons']} repeat comparisons, {rt['differing_comparisons']} differences. "
                         f"Baseline {schedule['baseline_seconds']} s, each joining feed {schedule['join_seconds']} s, "
                         f"control gaps {schedule['control_gap_seconds']} s, planned tail {schedule['tail_seconds']} s.")
    unclean = [f"`{row['feed']}` at {row['received_utc']} (code {row['code']})" for row in closes if row['code'] != 1000]
    commands = """node research/experiments/book-handoff-capture.mjs /tmp/submission-handoff/live
node research/experiments/book-handoff-capture.mjs /tmp/submission-handoff/continuation '{"joins":14,"baseline_seconds":30,"join_seconds":35,"control_gap_seconds":7,"tail_seconds":25,"pilot_seconds":10,"market_id":"682705"}'
python3 -B -I research/experiments/book-handoff-supervise.py /tmp/submission-handoff/followup
python3 -B -I research/experiments/book-handoff-verify.py
python3 -B -I research/experiments/book-handoff-analyze.py /tmp/submission-handoff/live /tmp/submission-handoff/analysis-initial.json
python3 -B -I research/experiments/book-handoff-analyze.py /tmp/submission-handoff/continuation /tmp/submission-handoff/analysis-continuation.json
"""
    commands += 'python3 -B -I research/experiments/book-handoff-analyze.py /tmp/submission-handoff/followup/segment-01 /tmp/submission-handoff/analysis-followup-01.json\n'
    commands += f"python3 -B -I research/experiments/book-handoff-summarize.py /tmp/submission-handoff/analysis-initial.json /tmp/submission-handoff/analysis-continuation.json /tmp/submission-handoff/analysis-followup-*.json /tmp/submission-handoff/sources.json research/experiments/{prefix.name}"
    return f"""# Established-feed / joining-feed experiment, 2026-10-26

{result['conclusion']} No production change is justified by this negative observation. This sample does not establish subscription-handoff correctness or explain earlier captures.

The public-feed experiment ran from {result['period']['started_utc']} to {result['period']['finished_utc']}. The {result['baseline_epochs']} bounded continuous-segment captures totalled {result['period']['capture_seconds_including_pilots']:.3f} s including their pilots; elapsed wall time including the interruption was {result['period']['wall_seconds']:.3f} s. An established socket failed, so these are separate baseline epochs, not an uninterrupted 20-join baseline.

{chr(10).join(run_lines)}

Market `{runs[0]['market']['id']}` was active, not closed, accepting orders, and had a future end date in all fresh Gamma responses. The selection pilot observed {runs[0]['pilot_absolute_level_entries']:,} absolute level entries in 25 s; the final 10 s segment pilot observed {runs[-1]['pilot_absolute_level_entries']:,}. Both asset IDs were subscribed explicitly:

- `{runs[0]['asset_ids'][0]}`
- `{runs[0]['asset_ids'][1]}`

Each connection sent `{{"type":"market","assets_ids":["<asset-1>","<asset-2>"],"initial_dump":true}}`; custom features were omitted. Feed A retained its socket until the end of each epoch or an observed failure. Each B was a fresh socket. Later epochs reused the same active pair with a newly established A. No reconnect is represented as continuous observation.

Results count received observations across sockets and outcomes, not unique exchange actions:

- Successful joins: {t['joins_successful']}/{t['joins_started']}; censored or otherwise unsuccessful joins: {t['joins_started'] - t['joins_successful']}. Success requires both initial books, the full requested observation duration, a requested clean close, no B error, and A open through B's observed close. A delayed B close after A fails is conservatively censored even if the intended observation duration had elapsed.
- Repeat-book comparisons: {t['repeat_book_comparisons']}; differing comparisons: {t['differing_comparisons']}; differing levels: {t['differing_levels']}.
- FIRST refreshes: {t['FIRST_refresh_comparisons']} comparisons, {t['FIRST_refresh_differences']} differences. Later refreshes: {t['later_refresh_comparisons']} comparisons, {t['later_refresh_differences']} differences. Joining feeds contributed {first} FIRST and {later} later comparisons; the remaining comparisons came from A.
- Initial-to-FIRST-refresh per-outcome windows compared: {t['initial_to_FIRST_refresh_windows_compared']}; outcome/join windows with no FIRST refresh: {t['initial_to_FIRST_refresh_windows_without_refresh']}. Same-receive-window content alignment left {t['A_unmatched_content_occurrences_in_same_receive_windows']} A and {t['B_unmatched_content_occurrences_in_same_receive_windows']} B occurrences unmatched. These include interval boundaries, repeated content ambiguity, delivery skew and censored intervals; these counts alone do not establish a handoff loss. All A-unmatched occurrences were checked against B's complete captured session. {t['A_unmatched_with_equal_B_content_before_initial_or_after_FIRST_within_2s']} have equal-content B occurrences before its initial book or after its FIRST refresh within 2 s of A receipt. A further {t['A_unmatched_with_target_already_in_B_initial_and_no_boundary_match']} occurrences already had their target quantity in B's initial book, leaving {t['A_unmatched_without_boundary_match_or_initial_quantity_match']} without either observation. Boundary and quantity matches describe observed content, not unique event identities or proof of completeness.
- Established-feed-only controls: {sum(x['intervals'] for x in controls)} intervals, {sum(x['seconds'] for x in controls):.3f} s, {sum(x['counts'].get('absolute_level_entries', 0) for x in controls):,} absolute entries, {sum(x['counts'].get('comparisons', 0) for x in controls)} repeat comparisons, {sum(x['counts'].get('differing_comparisons', 0) for x in controls)} differences.
- Main socket closes: {t['main_closes']} ({json.dumps(close_codes)}); errors: {t['main_errors']}. Abnormal closes: {', '.join(unclean) or 'none'}. Pilot sockets are excluded from main counts. Close-handshake timeouts after 3 s: {t['close_timeouts']}. Maximum simultaneously open B sockets: {quality['maximum_open_B_sockets']}; extra B opens while an older B was still closing: {quality['overlapping_B_open_events']}. Closing sockets remain captured and modeled separately, and their overlap is excluded from established-feed-only controls. Empty error messages and code 1006 do not establish a cause.
- Main application messages: {sum(x.get('received', 0) for x in feeds):,}; absolute level entries: {sum(x.get('absolute_level_entries', 0) for x in feeds):,}. Application receive gaps over 5 s: {sum(len(run['receive_gaps_over_5s']) for run in runs)}; maximum application gap: {max(x['maximum_receive_gaps_ms']['application_ms'] for x in feeds):.3f} ms; maximum market-message gap: {max(x['maximum_receive_gaps_ms']['market_ms'] for x in feeds):.3f} ms. Per-feed initial and terminal receive gaps are retained separately in JSON.
- Capture-index discontinuities: {quality['capture_index_discontinuities']}; per-feed message-index discontinuities: {quality['feed_frame_discontinuities']}; JSON failures: {quality['json_errors']}; duplicate prices inside received books: {quality['duplicate_book_price_entries']}; unexpected subscriptions: {quality['unexpected_subscriptions']}.
- Collector timer delays over 100 ms: {lag_count}; maximum timer delay: {max_lag:.3f} ms. These indicate shared-host/event-loop scheduling limits and prevent treating receive time as packet-arrival time.
- Cross-feed books differing from A's locally received state at B receipt: {t['cross_asof_differences']}/{t['cross_asof_comparisons']}. In {t['cross_asof_differences_all_level_targets_later_on_A_within_2s']} differing comparisons, every differing level's target quantity was subsequently observed in an A update within 2 s. Initial-dump, FIRST-refresh and later-refresh as-of differences were {t['cross_asof_INITIAL_differences']}, {t['cross_asof_FIRST_differences']} and {t['cross_asof_later_differences']}, respectively. These receive-time comparisons include delivery skew and are not missing-update incidents; repeated content does not establish event identity.

Each socket is reconstructed independently with exact decimal price keys and absolute quantities. Zero removes a level. Every array entry and message is applied in original received order. Before replacing a book, replay compares the entire incoming book with that socket's current model. It ignores trades for depth, does not synthesize complementary updates, and never sorts or deduplicates messages by time or hash. Local index, epoch, feed, message number, array index, change index, exchange timestamp, UTC and monotonic receipt times identify evidence positions. Hashes and exchange times are diagnostic fields only.

For each outcome on each join, private analysis retains its initial-to-FIRST-refresh window and occurrence-aware A/B update-content alignment in the same receive interval. A second A window expanded by 2 s is diagnostic for delivery skew. Repeated content occurrences remain represented. Full B books are also compared against A's reconstruction as locally received. If a changed refresh occurs, the analyzer retains the affected level's history and looks for the same numerical book-only change on A within 2 s; supporting observation is distinct from event identity and cause.

The sample contains no observed changed refresh to classify as a joining-feed-only omitted update or a matching established-feed book-only correction. All sockets shared a collector, runtime, host, network route and potentially upstream infrastructure. Correlated omissions and buffered delivery remain possible. Node's built-in WebSocket exposes complete application messages, not TCP packets, wire fragmentation, or automatic protocol ping/pong. Application `PING`/`PONG`, subscription sends, opens, errors, requested closes, observed close codes and timer delays were recorded. Quiet intervals do not prove missing frames, and contiguous local indices prove only that this collector's own records are contiguous.

Run from the repository root (the capture refuses an existing output directory):

```sh
{commands}
```

The initial command used the earlier source version fingerprinted in epoch 1; its exact source is retained privately as `/tmp/submission-handoff/capture-initial.mjs`. The current script preserves its default schedule and adds bounded configuration for the continuation. Each collector has a 20 minute hard stop and a 512 MiB raw-message cap. The final supervisor attempts ten additional joins, with at most four separate A epochs and a 15 minute deadline; it never represents reconnects as continuous observation. Raw messages, initial source copy, detailed analyses and official source copies remain private under `/tmp/submission-handoff/`. The sibling JSON contains per-feed/per-join counts, first-refresh content-alignment counts, control intervals, quality results and SHA-256 fingerprints of inputs, scripts and retrieved official documentation. Nine behavioral controls cover omitted updates, shared book-only changes, array order, repeated keys and content, stale timestamps, trade/mirror exclusion, within-frame window boundaries, delivery skew, established-socket continuity, and delayed-close overlap.

The official [WebSocket guide](https://github.com/Polymarket/agent-skills/blob/91ee44ae113e958affd20cd505c6e9d9d6100e0b/websocket.md) documents market subscriptions, zero-size deletion and heartbeats. The current [real-time data page](https://docs.polymarket.com/market-data/realtime-data) describes public market events. Neither is used as a completeness guarantee; startup handoff behavior is the empirical question. No orders, production edits, commits, load benchmarks or external publication occurred.
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('analysis', type=Path, nargs='+')
    parser.add_argument('sources', type=Path)
    parser.add_argument('prefix', type=Path)
    args = parser.parse_args()
    sources = json.loads(args.sources.read_text())
    runs = [summarize(json.loads(path.read_text()), sources) for path in args.analysis]
    for run, source in zip(runs, args.analysis):
        run['analysis_label'] = source.stem
    if any(run['period']['finished_utc'] is None for run in runs):
        raise SystemExit('Refusing a final summary while a capture is running')
    result = aggregate(runs)
    if result['totals']['differing_comparisons']:
        raise SystemExit('Changed refreshes require reviewed evidence and a revised narrative')
    args.prefix.with_suffix('.json').write_text(json.dumps(result, indent=2) + '\n')
    args.prefix.with_suffix('.md').write_text(markdown(result, args.prefix))
    print(json.dumps(result['totals']))


if __name__ == '__main__':
    main()
