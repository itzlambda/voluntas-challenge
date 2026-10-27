#!/usr/bin/env python3
"""Run one bounded collector validation; retain Prometheus scrapes and drain our child."""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def clock():
    try:
        result = subprocess.run(['chronyc', '-n', 'tracking'], capture_output=True,
                                text=True, timeout=5)
        return {'returncode': result.returncode, 'tracking': result.stdout}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {'error': type(error).__name__}


def cpu_ticks():
    values = [int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
    return sum(values), values[3] + values[4]


def check_idle(binary, metrics_bind, max_busy_cores):
    for item in Path('/proc').iterdir():
        if not item.name.isdigit():
            continue
        try:
            command = (item / 'cmdline').read_bytes().split(b'\0')[0].decode()
            if command and Path(command).name in {'collector', 'engineering-challenge', binary.name}:
                raise RuntimeError('Another collector is running; leave its workload untouched.')
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    host, port = metrics_bind.rsplit(':', 1)
    with socket.socket() as probe:
        probe.bind((host, int(port)))
    before = cpu_ticks()
    time.sleep(1)
    after = cpu_ticks()
    total = after[0] - before[0]
    busy = (1 - (after[1] - before[1]) / total) * os.cpu_count() if total else 0
    if busy > max_busy_cores:
        raise RuntimeError(f'Host is busy ({busy:.2f} cores); no collector started.')
    return {'busy_cores_before_launch': busy, 'metrics_port_free': True,
            'no_other_collector': True, 'cpu_count': os.cpu_count()}


def sample(pid, started, directory):
    proc = Path('/proc') / str(pid)
    fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
    status = dict(line.split(':', 1) for line in (proc / 'status').read_text().splitlines())
    return {'utc': utc(), 'elapsed_s': time.monotonic() - started,
            'rss_kb': int(status.get('VmRSS', '0 kB').split()[0]),
            'cpu_seconds': (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
            'open_fds': len(list((proc / 'fd').iterdir())),
            'free_bytes': shutil.disk_usage(directory).free,
            'sbe_bytes': (directory / 'books.sbe').stat().st_size
            if (directory / 'books.sbe').exists() else 0}


def fetch(base, path):
    with urllib.request.urlopen(base + path, timeout=2) as response:
        return response.status, response.read().decode()


def drain(child, seconds):
    if child.poll() is not None:
        return False
    child.send_signal(signal.SIGINT)
    try:
        child.wait(timeout=seconds)
        return False
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
        return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--out-dir', required=True, type=Path)
    parser.add_argument('--seconds', type=float, default=900)
    parser.add_argument('--scrape-seconds', type=float, default=5)
    parser.add_argument('--shutdown-seconds', type=float, default=15)
    parser.add_argument('--markets')
    parser.add_argument('--metrics-bind', default='127.0.0.1:19091')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--markets-per-connection', type=int, default=256)
    parser.add_argument('--gamma-requests-per-second', type=float, default=10)
    parser.add_argument('--connection-opens-per-second', type=float, default=20)
    parser.add_argument('--lifecycle-connections', type=int, default=3)
    parser.add_argument('--scan-interval-secs', type=int, default=900)
    parser.add_argument('--min-free-gib', type=float, default=10)
    parser.add_argument('--max-output-gib', type=float, default=12)
    parser.add_argument('--max-rss-mib', type=float, default=6500)
    parser.add_argument('--max-busy-cores', type=float, default=0.5)
    parser.add_argument('--lock-file', type=Path)
    args = parser.parse_args()
    if min(args.seconds, args.shutdown_seconds) <= 0 or args.scrape_seconds < 1:
        parser.error('Durations must be positive and scrape interval at least one second.')
    directory = args.out_dir.resolve()
    binary = args.binary.resolve(strict=True)
    if directory.exists():
        parser.error('Output directory already exists; evidence is never overwritten.')
    directory.parent.mkdir(parents=True, exist_ok=True)
    lock_path = args.lock_file or directory.parent / 'validation.lock'
    with lock_path.open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        preflight = check_idle(binary, args.metrics_bind, args.max_busy_cores)
        if shutil.disk_usage(directory.parent).free < args.min_free_gib * 2**30:
            raise RuntimeError('Insufficient free space; no collector started.')
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if hard != resource.RLIM_INFINITY and hard < 65536:
            raise RuntimeError('Open-file hard limit must allow 65536.')
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, hard))
        directory.mkdir()
        (directory / 'metrics').mkdir()
        command = [str(binary), '--output', str(directory / 'books.sbe'),
                   '--decode-error-log', str(directory / 'decode-errors.jsonl')]
        for name in ['metrics_bind', 'workers', 'markets_per_connection',
                     'gamma_requests_per_second', 'connection_opens_per_second',
                     'lifecycle_connections', 'scan_interval_secs']:
            command += ['--' + name.replace('_', '-'), str(getattr(args, name))]
        if args.markets:
            command += ['--markets', args.markets]
        state = {'started_at': utc(), 'seconds_requested': args.seconds,
                 'binary_sha256': hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest(),
                 'command': command, 'raw_capture': False, 'faults': [],
                 'scrape_interval_seconds': args.scrape_seconds,
                 'nofile_runner': resource.getrlimit(resource.RLIMIT_NOFILE),
                 'preflight': preflight, 'clock_before': clock()}
        status_file = directory / 'status.json'
        status_file.write_text(json.dumps(state, indent=2) + '\n')
        child = None
        reason = 'early_exit'
        started = time.monotonic()
        state['started_at'] = utc()
        def interrupted(*_):
            raise KeyboardInterrupt
        previous_term = signal.signal(signal.SIGTERM, interrupted)
        try:
            with (directory / 'collector.jsonl').open('wb') as log, \
                    (directory / 'samples.jsonl').open('w') as samples:
                child = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=log,
                                         start_new_session=True)
                state['pid'] = child.pid
                state['collector_limits'] = Path(f'/proc/{child.pid}/limits').read_text()
                status_file.write_text(json.dumps(state, indent=2) + '\n')
                while child.poll() is None:
                    if time.monotonic() - started >= args.seconds:
                        reason = 'duration_complete'
                        break
                    try:
                        row = sample(child.pid, started, directory)
                    except (FileNotFoundError, ProcessLookupError, KeyError):
                        break
                    try:
                        _, metrics = fetch('http://' + args.metrics_bind, '/metrics')
                        (directory / 'metrics' / f"{int(row['elapsed_s']):06d}.prom").write_text(metrics)
                        (directory / 'metrics.prom').write_text(metrics)
                        try:
                            row['ready_status'] = fetch('http://' + args.metrics_bind, '/readyz')[0]
                        except urllib.error.HTTPError as error:
                            row['ready_status'] = error.code
                    except (OSError, ValueError) as error:
                        row['observation_error'] = type(error).__name__ + ': ' + str(error)
                    samples.write(json.dumps(row) + '\n')
                    samples.flush()
                    (directory / 'progress.json').write_text(json.dumps(row, indent=2) + '\n')
                    if (row['free_bytes'] < args.min_free_gib * 2**30
                            or row['sbe_bytes'] > args.max_output_gib * 2**30
                            or row['rss_kb'] > args.max_rss_mib * 1024):
                        reason = 'resource_guard'
                        break
                    time.sleep(min(args.scrape_seconds,
                                   max(0, args.seconds - (time.monotonic() - started))))
        except KeyboardInterrupt:
            reason = 'interrupted'
        except Exception as error:
            reason = 'runner_error'
            state['runner_error'] = type(error).__name__ + ': ' + str(error)
        finally:
            if child is not None:
                if drain(child, args.shutdown_seconds):
                    reason = 'shutdown_timeout'
                state['exit_code'] = child.returncode
            state.update(ended_at=utc(), elapsed_s=time.monotonic() - started,
                         stop_reason=reason, clock_after=clock())
            state['files'] = {name: {'bytes': (directory / name).stat().st_size,
                                    'sha256': hashlib.file_digest((directory / name).open('rb'), 'sha256').hexdigest()}
                              for name in ['books.sbe', 'metrics.prom', 'collector.jsonl', 'decode-errors.jsonl']
                              if (directory / name).exists()}
            status_file.write_text(json.dumps(state, indent=2) + '\n')
            signal.signal(signal.SIGTERM, previous_term)
        print(json.dumps({key: state.get(key) for key in
                          ['exit_code', 'stop_reason', 'elapsed_s', 'binary_sha256']}))
        return 0 if state.get('exit_code') == 0 and reason == 'duration_complete' else 1


if __name__ == '__main__':
    raise SystemExit(main())
