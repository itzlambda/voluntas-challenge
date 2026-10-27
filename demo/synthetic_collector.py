#!/usr/bin/env python3
"""Synthetic collector metrics for running the demo locally with a real Prometheus.

Serves the collector's 18 metric families on /metrics with invented but plausible values,
shaped after the 2026-10-27 validation runs (about 258,500 markets, 2,022 order-book
sockets, about 32,000 events per second). Nothing here is a measurement.

    python3 -I demo/synthetic_collector.py --port 19190 --backfill-hours 6 --backfill-out history.om

--backfill-out writes the simulated hours before startup as OpenMetrics, for
`promtool tsdb create-blocks-from openmetrics`. Switch states while running:

    curl 127.0.0.1:19190/control/degraded   # three order-book sockets stay down
    curl 127.0.0.1:19190/control/stale      # publication freezes; scrapes still succeed
    curl 127.0.0.1:19190/control/stopped    # /metrics fails, so Prometheus records up=0
    curl 127.0.0.1:19190/control/restart    # counters reset; the first scan runs again
    curl 127.0.0.1:19190/control/live
"""
import argparse
import math
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INTERNAL_BOUNDS = (0, .00001, .000025, .00005, .0001, .00025, .0005, .001, .0025, .005,
                   .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60)
EXCHANGE_BOUNDS = (0, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5, 10, 30, 60)
END_REASONS = ("remote_close", "io_error", "decode_error", "protocol_error", "silence", "emptied", "shutdown")
SWITCH_REASONS = ("remote_close", "io_error", "silence", "decode_error", "sequence_gap")
# Session-end reasons that move publishers, and the switch reason each records.
SWITCH_FOR_END = {"remote_close": "remote_close", "io_error": "io_error", "silence": "silence",
                  "decode_error": "decode_error", "protocol_error": "sequence_gap"}
# Session ends per second across all order-book sockets.
END_RATES = {"remote_close": 0.0045, "io_error": 0.0015, "silence": 0.0004, "decode_error": 0.0,
             "protocol_error": 0.00008, "emptied": 0.0012, "shutdown": 0.0}
MARKETS_PER_SOCKET = 256
WORKERS = 4
WORKER_SHARE = (1.08, 0.93, 1.04, 0.95)
SCAN_INTERVAL = 900
DISCOVERY_SOCKETS = 3


def phi(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def lognormal_cdf(x, median, sigma):
    return 0.0 if x <= 0 else phi(math.log(x / median) / sigma)


class Histogram:
    def __init__(self, bounds):
        self.bounds = bounds
        self.buckets = [0.0] * len(bounds)
        self.count = 0.0
        self.sum = 0.0

    def observe_many(self, n, components):
        """Adds n observations from a mixture of (weight, median, sigma) lognormals."""
        if n <= 0:
            return
        for i, bound in enumerate(self.bounds):
            self.buckets[i] += n * sum(w * lognormal_cdf(bound, m, s) for w, m, s in components)
        self.count += n
        self.sum += n * sum(w * m * math.exp(s * s / 2) for w, m, s in components)

    def samples(self, name):
        rows = [(f"{name}_bucket", {"le": format_bound(b)}, math.floor(v)) for b, v in zip(self.bounds, self.buckets)]
        rows.append((f"{name}_bucket", {"le": "+Inf"}, math.floor(self.count)))
        rows.append((f"{name}_count", {}, math.floor(self.count)))
        rows.append((f"{name}_sum", {}, self.sum))
        return rows


def format_bound(bound):
    return repr(float(bound)) if bound else "0"


class Collector:
    """A simulated collector process. step() advances time; families() reports cumulative state."""

    def __init__(self, now, seed=7, scan_seconds=260, history_seconds=0):
        self.random = random.Random(seed)
        self.scan_seconds = scan_seconds
        self.mode = "live"
        self.lock = threading.Lock()
        self.restart(now - history_seconds)

    def restart(self, now):
        self.started = now
        self.t = now
        self.internal = Histogram(INTERNAL_BOUNDS)
        self.exchange = Histogram(EXCHANGE_BOUNDS)
        self.older_lag = Histogram(EXCHANGE_BOUNDS)
        self.events = 0.0
        self.negative = 0.0
        self.decode_errors = 0.0
        self.mismatches = 0.0
        self.older_skipped = 0.0
        self.ends = {(kind, r): 0.0 for kind in ("orderbook", "market_discovery") for r in END_REASONS}
        self.switches = {r: 0.0 for r in SWITCH_REASONS}
        self.busy = [0.0] * WORKERS
        self.writer_blocked = 0.0
        self.writer_busy = 0.0
        self.outages = []          # [until, sockets] for reconnecting order-book sockets
        self.standby_gaps = []     # [recovered_at, markets] awaiting fresh standby baselines
        self.stall_until = -1.0
        self.stall_share = 0.0
        self.next_stall = now + self.random.uniform(1200, 4200)
        self.published = now
        self.frozen = None
        self.frozen_t = now

    # Shape of the catalog and load over time.
    def scanned(self, t):
        return t - self.started >= self.scan_seconds

    def desired(self, t):
        if not self.scanned(t):
            return min(420, int((t - self.started) * 1.4))
        return int(258_540 + 900 * math.sin(t / 7300) + 250 * math.sin(t / 1300 + 1))

    def expected_sockets(self, t):
        return 2 * math.ceil(self.desired(t) / MARKETS_PER_SOCKET)

    def opened_sockets(self, t):
        """Sockets opened so far: the collector opens about 20 per second after the scan."""
        expected = self.expected_sockets(t)
        if not self.scanned(t):
            return expected
        return min(expected, int((t - self.started - self.scan_seconds) * 20))

    def connected(self, t):
        down = sum(n for until, n in self.outages if until > t)
        if self.mode == "degraded":
            down += 3
        return max(0, self.opened_sockets(t) - down)

    def load(self, t):
        """Share of full traffic: rises as sockets open and baselines arrive."""
        if not self.scanned(t):
            return 0.002
        expected = self.expected_sockets(t)
        return min(1.0, self.opened_sockets(t) / expected) if expected else 0.0

    def event_rate(self, t):
        base = 32_000 + 4_500 * math.sin(t / 2900) + 2_000 * math.sin(t / 410 + 2)
        return self.load(t) * base * self.random.uniform(0.94, 1.06)

    def step(self, t):
        with self.lock:
            if self.mode == "stale":
                # Publication freezes: values stay at the freeze time while scrapes keep succeeding.
                if self.frozen is None:
                    self.frozen, self.frozen_t = self.published, self.t
                return
            self.frozen = None
            dt = max(0.0, t - self.t)
            self.t = t
            if dt == 0:
                return
            rate = self.event_rate(t)
            n = rate * dt
            self.events += n
            if t >= self.next_stall:
                # Writer stalls: usually short; about one in four lasts long enough to lift p99 past 100 ms.
                long = self.random.random() < 0.25
                self.stall_until = t + (self.random.uniform(80, 140) if long else self.random.uniform(15, 40))
                self.stall_share = 0.09 if long else 0.04
                self.next_stall = t + self.random.uniform(1800, 4800)
            stall = t < self.stall_until
            drift = 1 + 0.3 * math.sin(t / 700) + 0.15 * math.sin(t / 97 + 1)
            burst = 0.02 if (t % 433) < 25 else 0.0
            internal = [(0.985 - burst, 0.000026 * drift, 1.2 + 0.1 * math.sin(t / 1300)),
                        (0.015, 0.0011 * drift, 0.9), (burst, 0.004 * drift, 0.7)]
            if stall:
                share = self.stall_share
                internal = [(1 - share, 0.00007 * drift, 1.4), (share, 0.2, 0.6)]
            self.internal.observe_many(0.97 * n, internal)
            age = 1 + 0.25 * math.sin(t / 1100) + 0.1 * math.sin(t / 61)
            self.exchange.observe_many(n, [(0.97, 0.0085 * age, 0.5), (0.03, 0.03 * age, 0.8)])
            share = rate * 0.000022
            for w in range(WORKERS):
                self.busy[w] += dt * share / WORKERS * WORKER_SHARE[w] * self.random.uniform(0.9, 1.1) * (1.6 if stall else 1)
            self.writer_busy += dt * min(1.0, rate * 0.000017 + (0.4 if stall else 0))
            if stall:
                self.writer_blocked += dt * self.random.uniform(0.3, 0.9)
            self.mismatches += self.poisson(0.07 * dt * self.load(t))
            skipped = self.poisson(0.02 * dt * self.load(t))
            self.older_skipped += skipped
            self.older_lag.observe_many(skipped, [(1.0, 0.008, 0.7)])
            if self.scanned(t) and self.opened_sockets(t) == self.expected_sockets(t):
                for reason, per_second in END_RATES.items():
                    for _ in range(self.poisson(per_second * dt)):
                        self.end_session(reason, t)
                for _ in range(self.poisson(0.00005 * dt)):
                    self.ends[("market_discovery", "remote_close")] += 1
            self.outages = [o for o in self.outages if o[0] > t]
            self.standby_gaps = [g for g in self.standby_gaps if g[0] > t]
            self.published = t - self.random.uniform(0.1, 0.9)

    def poisson(self, mean):
        if mean <= 0:
            return 0
        limit, k, p = math.exp(-mean), 0, 1.0
        while True:
            p *= self.random.random()
            if p <= limit:
                return k
            k += 1

    def end_session(self, reason, t):
        self.ends[("orderbook", reason)] += 1
        if reason in ("emptied", "shutdown"):
            return
        reconnect = self.random.choice([0.4, 1.2, 1.8, 6.5])
        self.outages.append([t + reconnect, 1])
        # About half of a socket's markets were publishing on it; each switches to its ready peer.
        if reason in SWITCH_FOR_END:
            self.switches[SWITCH_FOR_END[reason]] += self.random.randint(100, 156)
        self.standby_gaps.append([t + reconnect + self.random.uniform(2, 12), self.random.randint(240, 256)])

    def families(self, now):
        """Exposition families: (name, type, help, [(sample name, labels, value)])."""
        with self.lock:
            t = self.frozen_t if self.mode == "stale" and self.frozen is not None else now
            desired = self.desired(t)
            missing = 2 + int(5 * (0.5 + 0.5 * math.sin(t / 830)))
            if not self.scanned(t):
                synced = int(desired * 0.9)
            else:
                synced = int(desired * self.load(t) ** 1.2) - missing
            synced = max(0, min(desired, synced))
            standby_gap = sum(n for until, n in self.standby_gaps if until > t)
            standby = max(0, synced - 14 - int(30 * (0.5 + 0.5 * math.sin(t / 1700))) - standby_gap)
            if self.scanned(t) and self.load(t) < 1:
                standby = int(standby * self.load(t) ** 2)
            published = self.frozen if self.mode == "stale" and self.frozen is not None else self.published
            families = [
                ("collector_exchange_to_receive_seconds", "histogram", "Exchange event age at application receipt.", self.exchange.samples("collector_exchange_to_receive_seconds")),
                ("collector_exchange_age_negative", "counter", "Exchange-age observations below zero.", [("collector_exchange_age_negative_total", {}, self.negative)]),
                ("collector_receive_to_encode_seconds", "histogram", "Receipt to completed encoding per emitted level-update record.", self.internal.samples("collector_receive_to_encode_seconds")),
                ("collector_markets", "gauge", "Logical markets by coverage state; states overlap.", [
                    ("collector_markets", {"state": "desired"}, desired), ("collector_markets", {"state": "synced"}, synced),
                    ("collector_markets", {"state": "standby_ready"}, standby)]),
                ("collector_books", "gauge", "Logical outcome books by coverage state.", [
                    ("collector_books", {"state": "desired"}, 2 * desired), ("collector_books", {"state": "synced"}, 2 * synced + (missing % 3))]),
                ("collector_events", "counter", "Decoded data events from both physical feeds.", [("collector_events_total", {}, math.floor(self.events))]),
                ("collector_connections", "gauge", "Expected and open connections by purpose; states overlap.", [
                    ("collector_connections", {"kind": "orderbook", "state": "expected"}, self.expected_sockets(t)),
                    ("collector_connections", {"kind": "orderbook", "state": "connected"}, self.connected(t)),
                    ("collector_connections", {"kind": "market_discovery", "state": "expected"}, DISCOVERY_SOCKETS),
                    ("collector_connections", {"kind": "market_discovery", "state": "connected"}, DISCOVERY_SOCKETS)]),
                ("collector_session_ends", "counter", "Established sessions ended by fixed reason.", [
                    ("collector_session_ends_total", {"kind": kind, "reason": reason}, value) for (kind, reason), value in self.ends.items()]),
                ("collector_publisher_switches", "counter", "Logical markets changing publisher connection after a connection failure.", [
                    ("collector_publisher_switches_total", {"reason": r}, v) for r, v in self.switches.items()]),
                ("collector_decode_errors", "counter", "Recognized frames that failed decoding.", [("collector_decode_errors_total", {}, self.decode_errors)]),
                ("collector_book_mismatches", "counter", "Accepted full books differing from the same source's synchronized image.", [("collector_book_mismatches_total", {}, self.mismatches)]),
                ("collector_older_full_books_skipped", "counter", "Full books rejected as older than applied depth.", [("collector_older_full_books_skipped_total", {}, self.older_skipped)]),
                ("collector_older_full_book_lag_seconds", "histogram", "Applied depth timestamp minus rejected full-book timestamp.", self.older_lag.samples("collector_older_full_book_lag_seconds")),
                ("collector_worker_busy_seconds", "counter", "Elapsed input handling time; not CPU utilization.", [
                    ("collector_worker_busy_seconds_total", {"worker": str(w)}, v) for w, v in enumerate(self.busy)]),
                ("collector_writer_busy_seconds", "counter", "Elapsed encoding and output write/flush time; not CPU utilization.", [("collector_writer_busy_seconds_total", {}, self.writer_busy)]),
                ("collector_writer_blocked_seconds", "counter", "Summed worker time waiting for writer channel space.", [("collector_writer_blocked_seconds_total", {}, self.writer_blocked)]),
                ("collector_oldest_required_component_publication_timestamp_seconds", "gauge", "Oldest last-publication time across required components.", [
                    ("collector_oldest_required_component_publication_timestamp_seconds", {}, published)]),
            ]
            if self.scanned(t):
                done = self.started + self.scan_seconds
                families.append(("collector_last_successful_scan_timestamp_seconds", "gauge", "UTC completion time of last successful full discovery scan.", [
                    ("collector_last_successful_scan_timestamp_seconds", {}, done + (t - done) // SCAN_INTERVAL * SCAN_INTERVAL)]))
            return families


def labels_text(labels):
    return "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}" if labels else ""


def value_text(value):
    return repr(float(value)) if isinstance(value, float) else str(value)


def exposition(families):
    lines = []
    for name, kind, help_text, samples in families:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")
        lines.extend(f"{sample}{labels_text(labels)} {value_text(value)}" for sample, labels, value in samples)
    return "\n".join(lines) + "\n"


def backfill(collector, end, step, path, instance):
    """Simulates up to `end` and writes OpenMetrics with one family block each, as promtool requires."""
    series = {}
    meta = {}
    t = collector.t
    while t < end:
        t = min(end, t + step)
        collector.step(t)
        for name, kind, help_text, samples in collector.families(t):
            meta[name] = (kind, help_text)
            for sample, labels, value in samples:
                key = (name, sample, tuple(labels.items()))
                series.setdefault(key, []).append((value, t))
        series.setdefault(("up", "up", ()), []).append((1, t))
    meta["up"] = ("gauge", "Synthetic scrape health for backfilled history.")
    target = {"job": "collector", "instance": instance}
    with open(path, "w") as out:
        for family, (kind, help_text) in meta.items():
            out.write(f"# HELP {family} {help_text}\n# TYPE {family} {kind}\n")
            keys = [k for k in series if k[0] == family]
            if kind == "histogram":
                # A histogram point carries all its buckets, count and sum at one timestamp.
                for i in range(len(series[keys[0]])):
                    for key in keys:
                        value, at = series[key][i]
                        out.write(f"{key[1]}{labels_text({**target, **dict(key[2])})} {value_text(value)} {at:.3f}\n")
            else:
                for key in keys:
                    for value, at in series[key]:
                        out.write(f"{key[1]}{labels_text({**target, **dict(key[2])})} {value_text(value)} {at:.3f}\n")
        out.write("# EOF\n")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        collector = self.server.collector
        if self.path == "/metrics":
            if collector.mode == "stopped":
                self.send_error(503)
                return
            body = exposition(collector.families(time.time())).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path.startswith("/control/"):
            mode = self.path.removeprefix("/control/")
            if mode not in ("live", "degraded", "stale", "stopped", "restart"):
                self.send_error(404)
                return
            with collector.lock:
                if mode == "restart":
                    collector.restart(time.time())
                    mode = "live"
                collector.mode = mode
            self.send_response(200)
            self.end_headers()
            self.wfile.write(f"{mode}\n".encode())
        else:
            self.send_error(404)

    def log_message(self, *_args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", default=19190, type=int)
    parser.add_argument("--backfill-hours", default=0, type=float)
    parser.add_argument("--backfill-out")
    parser.add_argument("--backfill-step", default=5, type=float)
    parser.add_argument("--scan-seconds", default=260, type=float, help="Length of the first discovery scan")
    parser.add_argument("--seed", default=7, type=int)
    args = parser.parse_args()
    now = time.time()
    collector = Collector(now, args.seed, args.scan_seconds, args.backfill_hours * 3600)
    if args.backfill_out:
        backfill(collector, now, args.backfill_step, args.backfill_out, f"{args.bind}:{args.port}")
    else:
        collector.t = now
    server = ThreadingHTTPServer((args.bind, args.port), Handler)
    server.collector = collector

    def tick():
        while True:
            collector.step(time.time())
            time.sleep(1)

    threading.Thread(target=tick, daemon=True).start()
    server.serve_forever()


if __name__ == "__main__":
    main()
