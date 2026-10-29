"""Bounded Prometheus reads for upstream socket traffic and selected history."""
import math
from http.client import HTTPException
import threading
import time
from collections import OrderedDict

SOCKET_SELECTOR = '{job="collector_sockets"}'
EVENTS = "collector_socket_events_total"
STARTED = "collector_socket_process_start_timestamp_seconds" + SOCKET_SELECTOR
WINDOWS = {"15m": (900, 15), "1h": (3600, 15), "6h": (21600, 30), "24h": (86400, 120)}
MAX_SAMPLE_AGE = 45


def number(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def labelled(fields):
    return " or ".join(f'label_replace(({expression}), "field", "{field}", "__name__", ".*")'
                       for field, expression in fields.items())


def expressions(socket_id=None):
    selector = SOCKET_SELECTOR if socket_id is None else f'{{job="collector_sockets",socket_id="{socket_id}"}}'
    events = EVENTS + selector
    return {
        "rate": f"rate({events}[1m]) and (count_over_time({events}[1m]) >= 4)",
        "connected": "collector_socket_connected" + selector,
        "markets": "collector_socket_markets" + selector,
        "sample_at": f"timestamp({events})",
        "up": "up" + SOCKET_SELECTOR,
        "scrape_at": "timestamp(up" + SOCKET_SELECTOR + ")",
        "started_at": STARTED,
    }


CURRENT_QUERY = labelled(expressions())


def parse_rows(payload):
    if payload.get("status") != "success":
        raise ValueError("socket query failed")
    rows = payload.get("data", {}).get("result", [])
    if len(rows) > 327_680:
        raise ValueError("too many socket samples")
    return rows


def socket_key(metric):
    raw = metric.get("socket_id", "")
    if not raw.isascii() or not raw.isdecimal() or not 0 <= int(raw) <= 65535:
        raise ValueError("invalid socket identity")
    socket_id = int(raw)
    pool = "primary" if socket_id % 2 == 0 else "replica"
    if metric.get("pool") != pool:
        raise ValueError("invalid socket pool")
    return socket_id, pool


def decode_current(payload, now):
    globals_ = {}
    sockets = {}
    for row in parse_rows(payload):
        metric = row["metric"]
        field = metric.get("field")
        value = number(row["value"][1])
        if field in ("up", "scrape_at", "started_at"):
            if field in globals_:
                raise ValueError("ambiguous socket collector")
            globals_[field] = value
        elif field in ("rate", "connected", "markets", "sample_at"):
            socket_id, pool = socket_key(metric)
            target = sockets.setdefault(socket_id, {"id": socket_id, "pool": pool})
            if field in target:
                raise ValueError("duplicate socket sample")
            target[field] = value
    started = globals_.get("started_at")
    source_at = globals_.get("scrape_at")
    fresh = (globals_.get("up") == 1 and source_at is not None
             and 0 <= now - source_at <= MAX_SAMPLE_AGE and started is not None)
    result = []
    sample_times = [source_at] if source_at is not None else []
    for socket in sockets.values():
        # Retired slots retain only counters and do not belong to the active fleet.
        if "markets" not in socket and "connected" not in socket:
            continue
        markets, connected, sample_at = socket.get("markets"), socket.get("connected"), socket.get("sample_at")
        if (markets is None or markets < 0 or markets != int(markets) or connected not in (0, 1)
                or sample_at is None or not 0 <= now - sample_at <= MAX_SAMPLE_AGE):
            fresh = False
        if sample_at is not None:
            sample_times.append(sample_at)
        rate = socket.get("rate")
        if rate is not None and rate < 0:
            rate = None
        # Prevent a rate window from mixing socket assignments across process restarts.
        warming_up = started is not None and 0 <= now - started < 60
        if started is None or warming_up:
            rate = None
        state = ("empty" if markets == 0 else "disconnected" if connected == 0
                 else "warming_up" if warming_up else "connected")
        result.append({"id": socket["id"], "pool": socket["pool"], "markets": int(markets) if markets is not None else None,
                       "connected": connected == 1, "events_per_second": rate, "state": state})
    return {"schema_version": 1, "status": "live" if fresh else "unavailable", "retained": False,
            "values_at": min(sample_times) if sample_times else None,
            "rate_window_seconds": 60, "scrape_interval_seconds": 15,
            "sockets": sorted(result, key=lambda socket: socket["id"])}


def decode_history(payload, socket_id, window, end):
    duration, step = WINDOWS[window]
    start = end - duration
    timestamps = list(range(start, end + 1, step))
    fields = {}
    for row in parse_rows(payload):
        metric = row["metric"]
        field = metric.get("field")
        if field not in expressions(socket_id) and field not in ("published_at", "aggregate_up", "aggregate_scrape_at"):
            continue
        if "socket_id" in metric and socket_key(metric)[0] != socket_id:
            raise ValueError("unexpected socket in history")
        if field in fields:
            raise ValueError("ambiguous socket history")
        fields[field] = {float(at): number(value) for at, value in row.get("values", [])}
    values = []
    restarts = set()
    for at in timestamps:
        def get(field):
            return fields.get(field, {}).get(at)
        started, sample_at, published, aggregate_scrape, socket_scrape = (
            get("started_at"), get("sample_at"), get("published_at"), get("aggregate_scrape_at"), get("scrape_at"))
        if started is not None and start <= started <= end:
            restarts.add(started)
        valid = (get("up") == 1 and get("aggregate_up") == 1 and get("connected") == 1
                 and (get("markets") or 0) > 0 and started is not None and at - started >= 60
                 and sample_at is not None and 0 <= at - sample_at <= MAX_SAMPLE_AGE
                 and socket_scrape is not None and 0 <= at - socket_scrape <= MAX_SAMPLE_AGE
                 and published is not None and 0 <= at - published <= 15
                 and aggregate_scrape is not None and 0 <= at - aggregate_scrape <= 15)
        rate = get("rate")
        values.append(rate if valid and rate is not None and rate >= 0 else None)
    return {"schema_version": 1, "status": "ok", "socket_id": socket_id,
            "pool": "primary" if socket_id % 2 == 0 else "replica", "window": window,
            "rate_window_seconds": 60, "start": start, "end": end, "step_seconds": step,
            "timestamps": timestamps, "events_per_second": values, "restarts": sorted(restarts)}


class SocketReader:
    def __init__(self, query, stats):
        self.query, self.stats = query, stats
        self.lock = threading.Lock()
        self.history_lock = threading.Lock()
        self.refreshed = -math.inf
        self.current = None
        self.last_good = None
        self.histories = OrderedDict()

    def retained(self, now):
        # Snapshots are replaced, never mutated, so readers can retain one without the query lock.
        previous = self.last_good
        at = previous.get("values_at") if previous is not None else None
        return {**(previous or {}), "schema_version": 1,
                "status": "stale" if previous is not None else "unavailable",
                "retained": previous is not None, "query_at": now, "values_at": at,
                "age_seconds": max(0, now - at) if at is not None else None}

    def sockets(self):
        now = time.time()
        if not self.lock.acquire(timeout=1):
            return self.retained(time.time())
        try:
            if time.monotonic() - self.refreshed >= 5:
                try:
                    self.current = decode_current(self.query("query", {"query": CURRENT_QUERY, "time": now},
                                                            limit=4_194_304), now)
                except (OSError, HTTPException, ValueError, KeyError, TypeError, IndexError):
                    self.current = None
                self.refreshed = time.monotonic()
            aggregate = self.stats()
            now = time.time()
            result = self.current
            if (result is not None and result["status"] == "live" and aggregate.get("status") in ("live", "degraded")
                    and result["values_at"] is not None and 0 <= now - result["values_at"] <= MAX_SAMPLE_AGE):
                self.last_good = result
            else:
                return self.retained(now)
            values_at = result.get("values_at")
            return {**result, "query_at": now, "age_seconds": max(0, now - values_at) if values_at is not None else None,
                    "sockets": result.get("sockets", [])}
        finally:
            self.lock.release()

    def history(self, socket_id, window):
        duration, step = WINDOWS[window]
        now = time.time()
        end = int(now) // step * step
        key = (socket_id, window)
        if not self.history_lock.acquire(timeout=1):
            return {"schema_version": 1, "status": "unavailable", "socket_id": socket_id, "window": window}
        try:
            cached = self.histories.get(key)
            if cached and now - cached[0] < 15:
                self.histories.move_to_end(key)
                return cached[1]
            fields = expressions(socket_id)
            fields.update({"published_at": 'collector_oldest_required_component_publication_timestamp_seconds{job="collector"}',
                           "aggregate_up": 'up{job="collector"}', "aggregate_scrape_at": 'timestamp(up{job="collector"})'})
            try:
                payload = self.query("query_range", {"query": labelled(fields), "start": end - duration,
                                                     "end": end, "step": step}, limit=4_194_304)
                result = decode_history(payload, socket_id, window, end)
                self.histories[key] = (now, result)
                self.histories.move_to_end(key)
                while len(self.histories) > 16:
                    self.histories.popitem(last=False)
            except (OSError, HTTPException, ValueError, KeyError, TypeError, IndexError):
                result = {**(cached[1] if cached else {}), "schema_version": 1,
                          "status": "stale" if cached else "unavailable", "socket_id": socket_id, "window": window}
            return result
        finally:
            self.history_lock.release()
