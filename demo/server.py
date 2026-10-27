#!/usr/bin/env python3
"""Serve the demo and read queries for its dedicated collector-only Prometheus."""
import argparse
import json
import math
import threading
import time
import urllib.parse
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SELECTOR = '{job="collector"}'
HISTOGRAMS = {
    "internal": "collector_receive_to_encode_seconds",
    "exchange": "collector_exchange_to_receive_seconds",
}
PUBLISHED = f"collector_oldest_required_component_publication_timestamp_seconds{SELECTOR}"
QUERIES = {
    "up": f"up{SELECTOR}",
    # A failed scrape is stale; stopped needs ten seconds without a successful scrape.
    "up_recent": f"max_over_time(up{SELECTOR}[10s])",
    "scrape_at": f"timestamp(up{SELECTOR})",
    "last_success": f"max_over_time((timestamp(up{SELECTOR} == 1))[1h:5s])",
    "published_at": PUBLISHED,
    "scan_at": f"collector_last_successful_scan_timestamp_seconds{SELECTOR}",
    "markets_desired": 'collector_markets{job="collector",state="desired"}',
    "markets_synced": 'collector_markets{job="collector",state="synced"}',
    "markets_standby_ready": 'collector_markets{job="collector",state="standby_ready"}',
    "publisher_switches_total": f"sum(collector_publisher_switches_total{SELECTOR})",
    "books_desired": 'collector_books{job="collector",state="desired"}',
    "books_synced": 'collector_books{job="collector",state="synced"}',
    "connections": f"collector_connections{SELECTOR}",
    "workers": f"count(collector_worker_busy_seconds_total{SELECTOR})",
    "events_per_second": f"sum(rate(collector_events_total{SELECTOR}[1m]))",
    # One observation per encoded price-level record: the collector's output update rate.
    "book_updates_per_second": f"sum(rate(collector_receive_to_encode_seconds_count{SELECTOR}[1m]))",
    "negative_rate": f"sum(rate(collector_exchange_age_negative_total{SELECTOR}[5m]))",
}
for prefix, name in HISTOGRAMS.items():
    for quantile in [50, 99]:
        QUERIES[f"{prefix}_p{quantile}"] = (
            f"histogram_quantile({quantile / 100}, sum by (le) (rate({name}_bucket{SELECTOR}[5m])))"
        )
    QUERIES[f"{prefix}_samples"] = f"sum(increase({name}_count{SELECTOR}[5m]))"
    QUERIES[f"{prefix}_rate"] = f"sum(rate({name}_count{SELECTOR}[5m]))"
    QUERIES[f"{prefix}_finite_share"] = (
        # Prometheus 3 stores classic-histogram bounds as floats ("60.0"); older versions keep "60".
        f'sum(rate({name}_bucket{{job="collector",le=~"60(\\\\.0)?"}}[5m]))'
        f" / sum(rate({name}_count{SELECTOR}[5m]))"
    )


def labelled(fields):
    """Join fixed expressions into one query; each result carries its field name."""
    return " or ".join(
        f'label_replace(({expression}), "field", "{field}", "__name__", ".*")'
        for field, expression in fields.items()
    )


# A single instant query evaluates all fixed expressions at the same timestamp.
QUERY = labelled(QUERIES)
# Read only when the collector is down; scanning retention is too costly to repeat every second.
LAST_DATA_QUERY = labelled({"last_publication": f"max_over_time({PUBLISHED}[7d])"})
LAST_KNOWN_CACHE_SECONDS = 60
# Values from just before the final publication avoid showing a shutdown drain as the last state.
LAST_KNOWN_OFFSET_SECONDS = 10
FRESH_SECONDS = 15

# window: (duration, line step, bar bucket)
HISTORY_WINDOWS = {"15m": (900, 1, 30), "1h": (3600, 5, 120), "6h": (21600, 30, 600)}
HISTORY_FIELDS = ("up", "published_at", "markets_desired", "markets_synced",
                  "markets_standby_ready", "publisher_switches_total",
                  "books_desired", "books_synced", "events_per_second", "book_updates_per_second",
                  "internal_p50", "internal_p99", "internal_finite_share",
                  "exchange_p50", "exchange_p99", "exchange_finite_share")
HISTORY_SERIES = {field: (QUERIES[field], ()) for field in HISTORY_FIELDS}
HISTORY_SERIES.update({
    "connections": (QUERIES["connections"], ("kind", "state")),
    "worker_busy": (f"rate(collector_worker_busy_seconds_total{SELECTOR}[1m])", ("worker",)),
    "writer_busy": (f"rate(collector_writer_busy_seconds_total{SELECTOR}[1m])", ()),
    "writer_blocked": (f"sum(rate(collector_writer_blocked_seconds_total{SELECTOR}[1m]))", ()),
})
HISTORY_QUERY = labelled({field: expression for field, (expression, _) in HISTORY_SERIES.items()})
MASK_FIELDS = ("up", "published_at", "internal_finite_share", "exchange_finite_share")


def bar_series(bucket):
    window = f"[{bucket}s]"
    return {
        "up_max": (f"max_over_time(up{SELECTOR}{window})", ()),
        "session_ends": (f'sum by (reason) (increase(collector_session_ends_total{{job="collector",kind="orderbook"}}{window}))', ("reason",)),
        "discovery_session_ends": (f'sum by (reason) (increase(collector_session_ends_total{{job="collector",kind="market_discovery"}}{window}))', ("reason",)),
        "publisher_switches": (f"sum by (reason) (increase(collector_publisher_switches_total{SELECTOR}{window}))", ("reason",)),
        "book_mismatches": (f"sum(increase(collector_book_mismatches_total{SELECTOR}{window}))", ()),
        "older_full_books_skipped": (f"sum(increase(collector_older_full_books_skipped_total{SELECTOR}{window}))", ()),
        "decode_errors": (f"sum(increase(collector_decode_errors_total{SELECTOR}{window}))", ()),
    }


def bar_query(bucket):
    return labelled({field: expression for field, (expression, _) in bar_series(bucket).items()})


def compact(value):
    """Six significant digits keep the history payload small without visible loss."""
    return None if value is None else float(f"{value:.6g}")


def matrix(payload, definitions):
    if payload.get("status") != "success" or payload.get("data", {}).get("resultType") != "matrix":
        raise ValueError("invalid history response")
    fields = {}
    for row in payload["data"]["result"]:
        field = row["metric"].get("field")
        if field not in definitions:
            continue
        key = ":".join([field, *(row["metric"].get(label, "") for label in definitions[field][1])])
        if key in fields:
            raise ValueError("ambiguous history metric")
        fields[key] = {int(t): finite(v) for t, v in row["values"]}
    return fields


def decode_history(payload, end, window):
    duration, step, _ = HISTORY_WINDOWS[window]
    fields = matrix(payload, HISTORY_SERIES)
    start = end - duration
    timestamps = list(range(start, end + 1, step))
    series = {key: [] for key in sorted(fields) if key not in MASK_FIELDS}
    for field in HISTORY_FIELDS:
        if field not in MASK_FIELDS:
            series.setdefault(field, [])
    for timestamp in timestamps:
        up = fields.get("up", {}).get(timestamp)
        published = fields.get("published_at", {}).get(timestamp)
        fresh = up == 1 and published is not None and 0 <= timestamp - published <= FRESH_SECONDS
        for key, values in series.items():
            value = fields.get(key, {}).get(timestamp) if fresh else None
            if key.startswith(("internal_", "exchange_")):
                prefix, quantile = key.split("_")
                share = fields.get(prefix + "_finite_share", {}).get(timestamp)
                if share is None or share < int(quantile[1:]) / 100:
                    value = None
                elif value is not None:
                    value *= 1000
            values.append(compact(value))
    return {"schema_version": 1, "status": "ok", "window": window, "start": start,
            "end": end, "step_seconds": step, "timestamps": timestamps, "series": series,
            "latency_unit": "ms", "rate_window_seconds": 60, "latency_window_seconds": 300}


def decode_bars(payload, end, window):
    """Counter increases per bucket. Timestamps are bucket starts; buckets without a scrape are null."""
    duration, _, bucket = HISTORY_WINDOWS[window]
    fields = matrix(payload, bar_series(bucket))
    ends = list(range(end - duration + bucket, end + 1, bucket))
    series = {key: [] for key in sorted(fields) if key != "up_max"}
    for timestamp in ends:
        running = fields.get("up_max", {}).get(timestamp) == 1
        for key, values in series.items():
            value = fields.get(key, {}).get(timestamp) if running else None
            values.append(None if value is None else round(max(value, 0), 2))
    return {"bucket_seconds": bucket, "timestamps": [t - bucket for t in ends], "series": series}


def finite(value):
    result = float(value)
    return result if math.isfinite(result) else None


def decode_stats(payload, now):
    if payload.get("status") != "success" or payload.get("data", {}).get("resultType") != "vector":
        raise ValueError("invalid metrics response")
    fields = {}
    for row in payload["data"]["result"]:
        field = row["metric"].get("field")
        if field in QUERIES:
            fields.setdefault(field, []).append(row)
    if len(fields.get("up", [])) > 1:
        raise ValueError("demo requires exactly one collector target")

    def number(field):
        rows = fields.get(field, [])
        if len(rows) > 1:
            raise ValueError("ambiguous metric")
        return finite(rows[0]["value"][1]) if rows else None

    def coverage(prefix):
        desired, synced = number(f"{prefix}_desired"), number(f"{prefix}_synced")
        return {"desired": desired, "synced": synced,
                "ratio": synced / desired if desired and synced is not None and synced <= desired else None}

    def latency(prefix):
        share = number(f"{prefix}_finite_share")
        result = {"samples": number(f"{prefix}_samples"), "window_seconds": 300}
        for q in [50, 99]:
            overflow = share is not None and share < q / 100
            seconds = number(f"{prefix}_p{q}")
            result[f"p{q}_ms"] = seconds * 1000 if seconds is not None and not overflow else None
            result[f"p{q}_above_60_seconds"] = overflow
        return result

    published, scraped, up = number("published_at"), number("scrape_at"), number("up")
    up_recent = number("up_recent")
    last_ok = scraped if up == 1 else number("last_success")
    markets = coverage("markets")
    connections = {}
    for row in fields.get("connections", []):
        kind, state = row["metric"].get("kind"), row["metric"].get("state")
        if kind in ("orderbook", "market_discovery") and state in ("expected", "connected"):
            values = connections.setdefault(kind, {})
            if state in values:
                raise ValueError("ambiguous connection metric")
            values[state] = finite(row["value"][1])
    if up != 1 and up_recent != 1:
        status = "stopped"
    elif up != 1:
        # The latest scrape failed: never live, but not yet a stopped collector.
        status = "stale"
    elif published is None or markets["desired"] is None:
        status = "starting"
    elif now - published > FRESH_SECONDS or last_ok is None or now - last_ok > FRESH_SECONDS:
        status = "stale"
    elif any(values.get("expected") != values.get("connected") for values in connections.values()):
        # Only feeds the collector reports as disconnected degrade the status; coverage gaps are shown as numbers.
        status = "degraded"
    else:
        status = "live"
    negative, nonnegative = number("negative_rate"), number("exchange_rate")
    population = (negative or 0) + (nonnegative or 0)
    return {
        "schema_version": 1, "query_at": now, "status": status,
        "retained": status == "stale", "values_at": (published or last_ok) if status == "stale" else last_ok,
        "last_successful_scrape_at": last_ok, "component_published_at": published,
        "last_successful_scan_at": number("scan_at"),
        "markets": markets, "books": coverage("books"), "connections": connections,
        "workers": number("workers"),
        "resilience": {"standby_ready_markets": number("markets_standby_ready"),
                       "publisher_switches_total": number("publisher_switches_total")},
        "events_per_second": number("events_per_second"),
        "book_updates_per_second": number("book_updates_per_second"), "rate_window_seconds": 60,
        "internal_latency": latency("internal"), "exchange_age": latency("exchange"),
        "negative_exchange_share": negative / population if negative is not None and population else None,
    }


def with_age(result, now):
    values_at = result.get("values_at")
    return {**result, "query_at": now, "age_seconds": max(0, now - values_at) if values_at is not None else None}


class Reader:
    def __init__(self, base_url, scan_interval=900):
        parsed = urllib.parse.urlparse(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("Prometheus URL must use HTTP or HTTPS")
        self.url = base_url.rstrip("/") + "/api/v1/query"
        self.scan_interval = scan_interval
        self.cached = None
        self.refreshed = -math.inf
        self.lock = threading.Lock()
        self.history_cache = {}
        self.bars_cache = {}
        self.history_lock = threading.Lock()
        self.last_known_cache = (-math.inf, None)

    def query(self, endpoint, parameters, limit=1_048_576):
        request = urllib.request.Request(
            self.url.removesuffix("query") + endpoint,
            data=urllib.parse.urlencode({**parameters, "timeout": "2s"}).encode())
        with urllib.request.urlopen(request, timeout=3) as response:
            raw = response.read(limit + 1)
        if len(raw) > limit:
            raise ValueError("metrics response exceeds limit")
        return json.loads(raw)

    def history_end(self, duration, step):
        """End at the current step, or at the last data when the collector stopped before this window."""
        end = int(time.time()) // step * step
        stats = self.stats()
        last = stats.get("values_at") if stats.get("status") == "stopped" else None
        if last is not None and last < end - duration:
            return int(math.ceil((last + duration * 0.05) / step) * step), last
        return end, None

    def history(self, window):
        if window not in HISTORY_WINDOWS:
            raise ValueError("unsupported history window")
        duration, step, bucket = HISTORY_WINDOWS[window]
        end, anchored = self.history_end(duration, step)
        if not self.history_lock.acquire(timeout=4):
            return {"schema_version": 1, "status": "unavailable", "window": window}
        try:
            cached = self.history_cache.get(window)
            if cached and cached[0] == end:
                return cached[1]
            bar_end = end // bucket * bucket
            try:
                payload = self.query("query_range", {"query": HISTORY_QUERY, "start": end - duration,
                                     "end": end, "step": step}, limit=4_194_304)
                result = decode_history(payload, end, window)
                bars = self.bars_cache.get(window)
                if not bars or bars[0] != bar_end:
                    bar_payload = self.query("query_range", {
                        "query": bar_query(bucket), "start": bar_end - duration + bucket,
                        "end": bar_end, "step": bucket}, limit=4_194_304)
                    bars = (bar_end, decode_bars(bar_payload, bar_end, window))
                    self.bars_cache[window] = bars
                result["bars"] = bars[1]
                result["anchored_to_last_data"] = anchored is not None
                result["last_data_at"] = anchored
            except (OSError, ValueError, KeyError, TypeError):
                result = {**(cached[1] if cached else {}), "schema_version": 1,
                          "status": "stale" if cached and cached[1].get("timestamps") else "unavailable",
                          "window": window}
            self.history_cache[window] = (end, result)
            return result
        finally:
            self.history_lock.release()

    def last_known(self, now):
        """Values from the collector's last run, read from Prometheus so they survive demo restarts."""
        fetched, value = self.last_known_cache
        if time.monotonic() - fetched < LAST_KNOWN_CACHE_SECONDS:
            return value
        payload = self.query("query", {"query": LAST_DATA_QUERY, "time": now})
        rows = payload.get("data", {}).get("result", [])
        last = finite(rows[0]["value"][1]) if len(rows) == 1 else None
        value = None
        if last is not None and last <= now:
            at = max(0, last - LAST_KNOWN_OFFSET_SECONDS)
            value = decode_stats(self.query("query", {"query": QUERY, "time": at}), at)
            value.update(values_at=last, component_published_at=last)
        self.last_known_cache = (time.monotonic(), value)
        return value

    def stats(self):
        if not self.lock.acquire(timeout=4):
            return {"schema_version": 1, "status": "metrics_unavailable", "retained": False}
        try:
            if time.monotonic() - self.refreshed < 1 and self.cached is not None:
                return self.cached
            now = time.time()
            try:
                result = decode_stats(self.query("query", {"query": QUERY, "time": now}), now)
                scan_at = result["last_successful_scan_at"]
                result["scan_overdue"] = bool(self.scan_interval and scan_at is not None
                                              and now - scan_at > self.scan_interval * 2)
                # In discovery mode the collector is starting until it reports a successful scan.
                if result["status"] in ("live", "degraded") and self.scan_interval and scan_at is None:
                    result["status"] = "starting"
                if result["status"] == "stopped":
                    if not (self.cached and self.cached.get("status") == "stopped"):
                        self.last_known_cache = (-math.inf, None)  # The collector just stopped.
                    last = self.last_known(now)
                    if last is not None:
                        result = {**last, "status": "stopped", "retained": True,
                                  "last_successful_scrape_at": result["last_successful_scrape_at"] or last["values_at"]}
                    else:
                        result = {**result, "retained": False, "values_at": None}
                self.cached = with_age(result, now)
            except (OSError, ValueError, KeyError, TypeError, IndexError):
                previous = self.cached or {}
                self.cached = with_age({**previous, "schema_version": 1, "status": "metrics_unavailable",
                                        "retained": previous.get("values_at") is not None,
                                        "source_error": "Metrics source unavailable"}, now)
            self.refreshed = time.monotonic()
            return self.cached
        finally:
            self.lock.release()


ROOT = Path(__file__).parent
# Pages accept any query string; assets and APIs match exactly.
PAGES = {"/": "index.html", "/live": "live.html"}
ASSETS = {"/chart-help.js": ("chart-help.js", "text/javascript; charset=utf-8"),
          "/vendor/uPlot.iife.min.js": ("vendor/uPlot.iife.min.js", "text/javascript; charset=utf-8"),
          "/vendor/uPlot.min.css": ("vendor/uPlot.min.css", "text/css; charset=utf-8"),
          "/site.css": ("site.css", "text/css; charset=utf-8"),
          "/status.js": ("status.js", "text/javascript; charset=utf-8"),
          "/diagram.js": ("diagram.js", "text/javascript; charset=utf-8"),
          "/overview.js": ("overview.js", "text/javascript; charset=utf-8"),
          "/charts.js": ("charts.js", "text/javascript; charset=utf-8")}
CSP = ("default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
       "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, reader):
        self.reader = reader
        self.slots = threading.BoundedSemaphore(16)
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        request.settimeout(5)
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


class Handler(BaseHTTPRequestHandler):
    def prometheus_query(self, query_string):
        try:
            fields = urllib.parse.parse_qs(query_string, keep_blank_values=True, strict_parsing=True, max_num_fields=8)
            if not fields.get("query", [""])[0] or any(len(values) != 1 for values in fields.values()):
                raise ValueError("provide one query expression")
            if set(fields) - {"query", "time", "start", "end", "step"}:
                raise ValueError("supported parameters: query, time, start, end, step")
            parameters = {key: values[0] for key, values in fields.items()}
            if len(parameters["query"]) > 8192:
                raise ValueError("query expression is too long")
        except ValueError as error:
            self.json_response(400, {"status": "error", "errorType": "bad_data", "error": str(error)})
            return
        endpoint = "query_range" if set(parameters) & {"start", "end", "step"} else "query"
        try:
            payload = self.server.reader.query(endpoint, parameters, limit=4_194_304)
        except urllib.error.HTTPError as error:
            try:
                payload = json.loads(error.read(65536))
            except (OSError, ValueError):
                payload = {"status": "error", "errorType": "upstream", "error": "Prometheus query failed"}
            self.json_response(error.code, payload)
        except (OSError, ValueError):
            self.json_response(502, {"status": "error", "errorType": "upstream", "error": "Prometheus is unavailable or its response exceeded the limit"})
        else:
            self.json_response(200, payload)

    def json_response(self, status, payload):
        body = json.dumps(payload, allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/api/prometheus/query":
            self.prometheus_query(parsed.query)
            return
        history_routes = {f"/api/demo/history?window={window}": window for window in HISTORY_WINDOWS}
        if self.path == "/api/demo/stats":
            self.json_response(200, self.server.reader.stats())
            return
        if self.path in history_routes:
            self.json_response(200, self.server.reader.history(history_routes[self.path]))
            return
        if self.path == "/healthz":
            self.send_body(b"ok\n", "text/plain")
            return
        if parsed.path.startswith("/api/"):
            self.send_error(404)
            return
        if parsed.path in PAGES:
            self.send_body((ROOT / PAGES[parsed.path]).read_bytes(), "text/html; charset=utf-8")
        elif self.path in ASSETS:
            filename, content_type = ASSETS[self.path]
            self.send_body((ROOT / filename).read_bytes(), content_type)
        elif parsed.path.rstrip("/") in PAGES and parsed.path != "/":
            self.send_response(301)
            self.send_header("Location", parsed.path.rstrip("/"))
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            self.send_error(404)

    def send_body(self, body, content_type):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if content_type.startswith("text/html"):
            self.send_header("Content-Security-Policy", CSP)
            self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:9091")
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", default=8000, type=int)
    parser.add_argument("--scan-interval-secs", default=900, type=int, help="Use 0 for fixed-market mode")
    args = parser.parse_args()
    Server((args.bind, args.port), Reader(args.prometheus_url, args.scan_interval_secs)).serve_forever()
