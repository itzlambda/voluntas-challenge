"""Local checks for run ownership, clean drain, and metric-window calculations."""
import importlib.util
import json
import os
from pathlib import Path
import socket
import signal
import time
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


analyze = load('aws-live-analyze')


class MetricTests(unittest.TestCase):
    def test_histogram_subtracts_populations_and_reports_bucket_bounds(self):
        name = 'latency'
        start = {(name + '_count', ()): 10, (name + '_sum', ()): 1,
                 (name + '_bucket', (('le', '0.1'),)): 5,
                 (name + '_bucket', (('le', '+Inf'),)): 10}
        end = {(name + '_count', ()): 12, (name + '_sum', ()): 1.4,
               (name + '_bucket', (('le', '0.1'),)): 6,
               (name + '_bucket', (('le', '+Inf'),)): 12}
        result = analyze.histogram(end, start, name)
        self.assertEqual(result['count'], 2)
        self.assertAlmostEqual(result['mean_seconds'], 0.2)
        self.assertEqual(result['p50_bucket_upper_seconds'], 0.1)
        self.assertIsNone(result['p99_bucket_upper_seconds'])
        self.assertTrue(result['p99_overflow'])

    def test_counter_reset_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Counter reset'):
            analyze.delta({('events', ()): 1}, {('events', ()): 2}, 'events')

    def test_zero_population_has_no_percentile(self):
        values = {('latency_count', ()): 0, ('latency_sum', ()): 0}
        result = analyze.histogram(values, values, 'latency')
        self.assertIsNone(result['mean_seconds'])
        self.assertIsNone(result['p50_bucket_upper_seconds'])
        self.assertFalse(result['p50_overflow'])


@unittest.skipUnless(Path('/proc/self/stat').exists(), 'Linux validation harness')
class RunTests(unittest.TestCase):
    def test_runner_scrapes_and_drains_its_child_without_overwriting_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / 'fake-collector.py'
            binary.write_text('''#!/usr/bin/env python3
import argparse, http.server, pathlib, signal
p=argparse.ArgumentParser();p.add_argument('--output');p.add_argument('--metrics-bind');a,_=p.parse_known_args()
pathlib.Path(a.output).write_bytes(b'')
def stop(*_):
 pathlib.Path(a.output).with_name('drained').write_text('yes')
 raise SystemExit(0)
signal.signal(signal.SIGINT,stop)
class Handler(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  self.send_response(200);self.end_headers();self.wfile.write(b'collector_events_total 1\\n')
 def log_message(self,*_):pass
host,port=a.metrics_bind.rsplit(':',1)
http.server.HTTPServer((host,int(port)),Handler).serve_forever()
''')
            binary.chmod(0o755)
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]
            out = root / 'run'
            command = ['python3', str(ROOT / 'aws-live-run.py'), '--binary', str(binary),
                       '--out-dir', str(out), '--seconds', '3', '--scrape-seconds', '1',
                       '--min-free-gib', '0', '--max-busy-cores', '10000',
                       '--metrics-bind', f'127.0.0.1:{port}']
            result = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            state = json.loads((out / 'status.json').read_text())
            self.assertEqual(state['stop_reason'], 'duration_complete')
            self.assertEqual(state['exit_code'], 0)
            self.assertEqual(state['nofile_runner'][0], 65536)
            self.assertTrue((out / 'drained').exists())
            self.assertTrue(list((out / 'metrics').glob('*.prom')), (out / 'samples.jsonl').read_text() + (out / 'collector.jsonl').read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(state['pid'], 0)
            repeat = subprocess.run(command, capture_output=True, text=True, timeout=5)
            self.assertNotEqual(repeat.returncode, 0)
            self.assertIn('already exists', repeat.stderr)
            self.assertEqual(json.loads((out / 'status.json').read_text()), state)

    def test_terminating_runner_drains_the_owned_child(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / 'fake-collector.py'
            binary.write_text("""#!/usr/bin/env python3
import argparse, pathlib, signal, time
p=argparse.ArgumentParser();p.add_argument('--output');a,_=p.parse_known_args()
pathlib.Path(a.output).write_bytes(b'')
def stop(*_):
 pathlib.Path(a.output).with_name('drained').write_text('yes')
 raise SystemExit(0)
signal.signal(signal.SIGINT,stop)
while True:time.sleep(0.05)
""")
            binary.chmod(0o755)
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]
            out = root / 'run'
            command = ['python3', str(ROOT / 'aws-live-run.py'), '--binary', str(binary),
                       '--out-dir', str(out), '--seconds', '30', '--min-free-gib', '0',
                       '--max-busy-cores', '10000', '--metrics-bind', f'127.0.0.1:{port}']
            child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 10
                while not (out / 'books.sbe').exists():
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.05)
                child.send_signal(signal.SIGTERM)
                _, errors = child.communicate(timeout=5)
                self.assertEqual(child.returncode, 1, errors)
                state = json.loads((out / 'status.json').read_text())
                self.assertEqual(state['stop_reason'], 'interrupted')
                self.assertEqual(state['exit_code'], 0)
                self.assertTrue((out / 'drained').exists())
            finally:
                if child.poll() is None:
                    child.send_signal(signal.SIGTERM)
                    child.communicate(timeout=5)


if __name__ == '__main__':
    unittest.main()
