#!/usr/bin/env python3
"""Exercise real Prometheus scraping and the demo reader in an isolated test stack."""
import argparse
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--prometheus", required=True)
parser.add_argument("--demo", required=True)
args = parser.parse_args()
spec = importlib.util.spec_from_file_location("demo", args.demo)
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


class Fixture(BaseHTTPRequestHandler):
    requests = 0

    def do_GET(self):
        Fixture.requests += 1
        count = Fixture.requests * 10
        rows = [f'collector_oldest_required_component_publication_timestamp_seconds {time.time()}',
                f'collector_last_successful_scan_timestamp_seconds {time.time()}',
                'collector_markets{state="desired"} 10', 'collector_markets{state="synced"} 10',
                'collector_books{state="desired"} 20', 'collector_books{state="synced"} 20',
                f'collector_events_total {count}', 'collector_exchange_age_negative_total 0']
        for name in demo.HISTOGRAMS.values():
            rows += [f'{name}_bucket{{le="0.001"}} {count / 2}',
                     f'{name}_bucket{{le="0.01"}} {count}', f'{name}_bucket{{le="60"}} {count}',
                     f'{name}_bucket{{le="+Inf"}} {count}', f'{name}_count {count}', f'{name}_sum {count * .005}']
        body = ('\n'.join(rows) + '\n').encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


fixture = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
thread = threading.Thread(target=fixture.serve_forever, daemon=True)
thread.start()
with socket.socket() as reservation:
    reservation.bind(("127.0.0.1", 0))
    port = reservation.getsockname()[1]
process = None
try:
    with tempfile.TemporaryDirectory(prefix="voluntas-verify-") as directory:
        root = Path(directory)
        config = root / "prometheus.yml"
        config.write_text(f'''global:
  scrape_interval: 1s
  scrape_timeout: 900ms
scrape_configs:
  - job_name: collector
    static_configs:
      - targets: ['127.0.0.1:{fixture.server_port}']
''')
        process = subprocess.Popen([args.prometheus, f"--config.file={config}",
                                   f"--storage.tsdb.path={root / 'data'}",
                                   f"--web.listen-address=127.0.0.1:{port}", "--log.level=error"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        reader = demo.Reader(f"http://127.0.0.1:{port}")
        for _ in range(40):
            time.sleep(.25)
            if process.poll() is not None:
                raise RuntimeError(process.stderr.read().decode())
            if Fixture.requests >= 4:
                break
        assert Fixture.requests >= 4, "scrapes did not arrive"
        stats = reader.stats()
        assert stats["status"] == "live", stats
        assert stats["markets"]["synced"] == 10
        assert stats["events_per_second"] > 0
        assert stats["internal_latency"]["p99_ms"] > 0
        history = reader.history("15m")
        assert history["status"] == "ok"
        assert len(history["timestamps"]) == 901
        assert history["series"]["events_per_second"][0] is None
        assert any(value is not None for value in history["series"]["internal_p99"])
        fixture.shutdown()
        fixture.server_close()
        time.sleep(2)
        reader.refreshed = -float("inf")
        assert reader.stats()["status"] == "stale"
        assert reader.history("15m")["series"]["events_per_second"][-1] is None
        print(json.dumps({"result": "passed", "checks": ["real 1s Prometheus scraping", "live demo values",
              "rate and histogram queries", "901-point history with missing-data gaps", "scrape outage becomes stale"]}))
finally:
    fixture.shutdown()
    fixture.server_close()
    if process is not None and process.poll() is None:
        process.terminate()
        process.wait(timeout=10)
