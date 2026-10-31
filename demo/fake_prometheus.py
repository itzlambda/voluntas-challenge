#!/usr/bin/env python3
"""A fake Prometheus for demo tests and local runs. It answers the demo's fixed queries only.

It reads the field names from the demo's labelled queries and synthesizes values from a
scenario timeline, so every connection state can be shown without a collector:

    python3 -I demo/fake_prometheus.py --scenario live --port 9091
    curl http://127.0.0.1:9091/scenario/stopped     # switch while running

Values are shaped after the 2026-10-27 validation runs. They are not measurements.
"""
import argparse
import json
import math
import re
import threading
import time
import urllib.parse
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FIELD = re.compile(r'"field", "(\w+)"')
BUCKET = re.compile(r"\[(\d+)s\]\)\)")
SCAN_SECONDS = 260
BOOKS_SECONDS = 150
DESIRED = 258_591
SOCKETS = 2_022
WORKERS = 4
SESSION_REASONS = ("remote_close", "io_error", "decode_error", "protocol_error", "silence", "emptied", "shutdown")
SWITCH_REASONS = ("remote_close", "io_error", "silence", "decode_error", "sequence_gap")
HOUR = 3600

# name: offsets from the moment the scenario is selected
SCENARIOS = {
    "live": {"start": -6 * HOUR},
    "latency_miss": {"start": -6 * HOUR, "miss": True},
    "starting_scan": {"start": -90},
    "starting_books": {"start": -(SCAN_SECONDS + 40)},
    "degraded": {"start": -6 * HOUR, "missing_sockets": 3},
    "stale": {"start": -6 * HOUR, "frozen": -60},
    "stopped": {"start": -7 * HOUR, "stop": -2 * HOUR},
    "stopped_recent": {"start": -5 * HOUR, "stop": -300},
    "never_ran": {"start": None},
    "prometheus_down": {"start": -6 * HOUR, "down": True},
}


def noise(*key):
    """Deterministic value in [0, 1) for a key."""
    return zlib.crc32(repr(key).encode()) / 2**32


def wave(t, period, phase=0.0):
    return math.sin(2 * math.pi * (t / period) + phase)


class Timeline:
    def __init__(self, name, anchor=None):
        spec = SCENARIOS[name]
        anchor = time.time() if anchor is None else anchor
        self.name = name
        self.down = spec.get("down", False)
        self.miss = spec.get("miss", False)
        self.start = None if spec["start"] is None else anchor + spec["start"]
        self.stop = anchor + spec["stop"] if "stop" in spec else None
        self.frozen = anchor + spec["frozen"] if "frozen" in spec else None
        self.missing_sockets = spec.get("missing_sockets", 0)
        self.anchor = anchor

    def running(self, t):
        return self.start is not None and self.start <= t and (self.stop is None or t < self.stop)

    def last_running(self, t):
        if self.start is None or t < self.start:
            return None
        return min(t, self.stop - 1) if self.stop is not None else t

    def scanned(self, t):
        return self.start is not None and t - self.start >= SCAN_SECONDS

    def desired(self, t):
        if not self.scanned(t):
            return min(400, int((t - self.start) * 1.4))
        return DESIRED + int(40 * wave(t, 5400))

    def synced(self, t):
        desired = self.desired(t)
        if not self.scanned(t):
            return int(desired * 0.9)
        progress = min(1.0, (t - self.start - SCAN_SECONDS) / BOOKS_SECONDS)
        gap = 2 + int(6 * noise("gap", int(t) // 20))
        return int(min(desired - gap, desired * progress))

    def standby_ready(self, t):
        return max(0, self.synced(t) - 16 - int(40 * max(0, wave(t, 1800))) - int(8 * noise("sb", int(t) // 15)))

    def sockets(self, t):
        if not self.scanned(t):
            return 2, 2
        expected = SOCKETS
        connected = min(expected, int((t - self.start - SCAN_SECONDS) * 20))
        if self.missing_sockets and t > self.anchor - 120:
            connected = min(connected, expected - self.missing_sockets)
        return expected, connected

    def events(self, t):
        if not self.scanned(t):
            return 40.0
        ramp = min(1.0, (t - self.start - SCAN_SECONDS) / BOOKS_SECONDS)
        return ramp * (32_000 + 5_000 * wave(t, 2400) + 2_500 * wave(t, 170, 1.3) + 900 * noise("ev", int(t) // 5))

    def internal(self, q, t):
        base = 0.000024 if q == 50 else 0.00075
        burst = 0.0
        if q == 99:
            burst = 0.0012 * max(0, wave(t, 600)) ** 8
            if self.miss and (t % 480) < 40:
                burst = 0.16 * math.sin(math.pi * (t % 480) / 40)
        return base * (1 + 0.25 * noise("int", q, int(t) // 3)) + burst

    def exchange(self, q, t):
        base = 0.009 if q == 50 else 0.045
        return base * (1 + 0.3 * noise("ex", q, int(t) // 4)) + (0.02 * max(0, wave(t, 900)) ** 6 if q == 99 else 0)

    def published(self, t):
        if self.frozen is not None and t >= self.frozen:
            return self.frozen
        return float(int(t)) - 0.4

    def counter_increase(self, kind, reason, t, bucket):
        """Synthetic counter increase over (t - bucket, t]."""
        if not self.running(t) or not self.scanned(t):
            return 0.0
        rate = {"remote_close": 0.004, "io_error": 0.0012, "silence": 0.0004, "decode_error": 0.0, "protocol_error": 0.0001,
                "emptied": 0.0008, "shutdown": 0.0, "sequence_gap": 0.0}.get(reason, 0.0)
        if kind == "switch":
            rate *= 256
        elif kind == "discovery":
            rate = 0.0002 if reason == "remote_close" else 0.0
        elif kind in ("mismatch", "older", "decode"):
            rate = {"mismatch": 0.07, "older": 0.02, "decode": 0.0}[kind]
        spike = 3.0 if noise(kind, reason, int(t) // bucket) > 0.85 else 1.0
        return round(rate * bucket * spike * (0.4 + noise("c", kind, reason, int(t) // bucket)), 2)

    def series(self, field, t, bucket=None):
        """[(labels, value)] for one field at time t; an empty list means no sample."""
        last = self.last_running(t)
        if field == "first_data":
            return [({}, max(self.start, t - 7 * 24 * HOUR))] if last is not None else []
        if field == "first_scrape":
            return [({}, t)] if self.running(t) else []
        if field == "last_publication":
            return [({}, self.published(last))] if last is not None and t - last < 7 * 24 * HOUR else []
        if field in ("up", "up_recent", "up_max"):
            if self.start is None:
                return [({}, 0.0)]
            window = {"up": 0, "up_recent": 10, "up_max": bucket or 0}[field]
            running = self.running(t) or any(self.running(t - s) for s in range(1, window + 1))
            return [({}, 1.0 if running else 0.0)]
        if field == "scrape_at":
            return [({}, float(int(t)))]
        if field == "last_success":
            return [({}, float(int(last)))] if last is not None and t - last <= HOUR else []
        if field == "last_down_at":
            if not self.running(t):
                return [({}, float(int(t)))]
            return [({}, float(int(self.start)) - 1)] if t - self.start < 1800 else []
        if field == "first_up_at":
            if last is None or t - last > 1800:
                return []
            return [({}, float(max(int(self.start), int(t) - 1800)))]
        if not self.running(t):
            return []
        if self.frozen is not None and t >= self.frozen:
            t = self.frozen
        if field == "published_at":
            return [({}, self.published(t))]
        if field == "scan_at":
            if not self.scanned(t):
                return []
            done = self.start + SCAN_SECONDS
            return [({}, done + (t - done) // 900 * 900)]
        values = {
            "markets_desired": lambda: self.desired(t),
            "markets_synced": lambda: self.synced(t),
            "markets_standby_ready": lambda: self.standby_ready(t),
            "books_desired": lambda: 2 * self.desired(t),
            "books_synced": lambda: 2 * self.synced(t) + 1,
            "publisher_switches_total": lambda: float(int((t - self.start) / 20)),
            "workers": lambda: float(WORKERS),
            "events_per_second": lambda: self.events(t),
            "book_updates_per_second": lambda: 0.97 * self.events(t),
            "negative_rate": lambda: 0.0,
            "writer_busy": lambda: min(0.98, self.events(t) / 32_000 * 0.55 + 0.2 * max(0, wave(t, 600)) ** 8),
            "writer_blocked": lambda: 0.03 * max(0, wave(t, 600)) ** 8,
            "internal_samples": lambda: 0.97 * self.events(t) * 300,
            "internal_rate": lambda: 0.97 * self.events(t),
            "exchange_samples": lambda: self.events(t) * 300,
            "exchange_rate": lambda: self.events(t),
            "internal_finite_share": lambda: 1.0,
            "exchange_finite_share": lambda: 1.0,
        }
        for q in (50, 99):
            values[f"internal_p{q}"] = lambda q=q: self.internal(q, t)
            values[f"exchange_p{q}"] = lambda q=q: self.exchange(q, t)
        if field in ("internal_buckets", "exchange_buckets"):
            percentile = self.internal if field == "internal_buckets" else self.exchange
            return [({"le": str(bound)}, count) for bound, count in
                    [(percentile(50, t), 50), (percentile(99, t), 99), (60, 100), (math.inf, 100)]]
        if field in values:
            return [({}, float(values[field]()))]
        if field == "connections":
            expected, connected = self.sockets(t)
            return [({"kind": "orderbook", "state": "expected"}, float(expected)),
                    ({"kind": "orderbook", "state": "connected"}, float(connected)),
                    ({"kind": "market_discovery", "state": "expected"}, 3.0),
                    ({"kind": "market_discovery", "state": "connected"}, 3.0)]
        if field == "worker_busy":
            ramp = min(1.0, self.events(t) / 32_000)
            return [({"worker": str(w)}, ramp * (0.15 + 0.025 * w + 0.04 * noise("wb", w, int(t) // 5)))
                    for w in range(WORKERS)]
        bucket = bucket or 30
        if field == "session_ends":
            return [({"reason": r}, self.counter_increase("session", r, t, bucket)) for r in SESSION_REASONS]
        if field == "discovery_session_ends":
            return [({"reason": r}, self.counter_increase("discovery", r, t, bucket)) for r in SESSION_REASONS]
        if field == "publisher_switches":
            return [({"reason": r}, self.counter_increase("switch", r, t, bucket)) for r in SWITCH_REASONS]
        if field in ("book_mismatches", "older_full_books_skipped", "decode_errors"):
            kind = {"book_mismatches": "mismatch", "older_full_books_skipped": "older", "decode_errors": "decode"}[field]
            return [({}, self.counter_increase(kind, "", t, bucket))]
        return []


def number(value):
    return "NaN" if value is None or not math.isfinite(value) else repr(value)


class FakePrometheus(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, scenario="live"):
        self.timeline = Timeline(scenario)
        self.requests = 0
        self.lock = threading.Lock()
        super().__init__(address, FakeHandler)

    def select(self, scenario, anchor=None):
        with self.lock:
            self.timeline = Timeline(scenario, anchor)

    def evaluate(self, query, times):
        fields = FIELD.findall(query)
        buckets = BUCKET.findall(query)
        bucket = int(buckets[0]) if buckets else None
        timeline = self.timeline
        rows = {}
        for field in fields:
            for t in times:
                for labels, value in timeline.series(field, t, bucket):
                    key = (field, tuple(sorted(labels.items())))
                    rows.setdefault(key, []).append([t, number(value)])
        return [{"metric": {"field": field, **dict(labels)}, "values": values} for (field, labels), values in rows.items()]


class FakeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path.startswith("/scenario/"):
            name = parsed.path.removeprefix("/scenario/")
            if name not in SCENARIOS:
                self.reply(404, {"error": "unknown scenario", "scenarios": sorted(SCENARIOS)})
                return
            self.server.select(name)
            self.reply(200, {"scenario": name})
            return
        self.answer(urllib.parse.parse_qs(parsed.query))

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.answer(urllib.parse.parse_qs(self.rfile.read(length).decode()))

    def answer(self, form):
        path = urllib.parse.urlsplit(self.path).path
        self.server.requests += 1
        if self.server.timeline.down:
            self.reply(503, {"status": "error", "errorType": "unavailable", "error": "fake outage"})
            return
        query = form.get("query", [""])[0]
        if path == "/api/v1/query":
            at = float(form.get("time", [time.time()])[0])
            result = [{"metric": row["metric"], "value": row["values"][0]} for row in self.server.evaluate(query, [at])]
            self.reply(200, {"status": "success", "data": {"resultType": "vector", "result": result}})
        elif path == "/api/v1/query_range":
            start, end, step = (float(form[key][0]) for key in ("start", "end", "step"))
            times = [start + i * step for i in range(int((end - start) // step) + 1)]
            self.reply(200, {"status": "success", "data": {"resultType": "matrix", "result": self.server.evaluate(query, times)}})
        else:
            self.reply(404, {"status": "error", "error": "not found"})

    def reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", default="live", choices=sorted(SCENARIOS))
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", default=9091, type=int)
    args = parser.parse_args()
    FakePrometheus((args.bind, args.port), args.scenario).serve_forever()
