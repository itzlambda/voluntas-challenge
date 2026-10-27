#!/usr/bin/env python3
"""Independent audit of a controlled, fixed-membership paired collector run.

Run with a fresh collector, six fixed markets on one pair (capacity 256), both
replicas ready before each deliberate failure, and the other socket healthy.
Allow ordered controlled failures, including repeated connection IDs, with both
replicas ready before every failure. Stop cleanly after 300 seconds; retain raw fixture
capture, actual flushed SBE, switch JSON incidents and final Prometheus metrics.

Example (all paths are local; this program never connects to AWS):
  python3 research/experiments/standby-live-audit.py \
    data/aws-standby-validation/fixed --failure-order 1,0,1,0 \
    --metrics data/aws-standby-validation/fixed/final-metrics.prom
  python3 research/experiments/standby-live-audit.py --self-test

Defaults inside DIRECTORY: frames.jsonl, books.sbe, collector.jsonl,
final-metrics.prom. Incidents are structured publisher_switch events only;
operational log numbers are never used as evidence. --failure-order is the
externally controlled termination order, NOT an inferred ordering of frames.

Proof: exact ordered source-derived updates/corrections, exact initial and
replacement books at consistent source prefixes, ordinary snapshot equality
with SBE replay and source metadata, paired replacement adjacency, complete
switch-incident accounting against per-reason counters, and periodic presence.
Every source is reduced separately. No ordering between producer frame streams
is invented. Full-book filtering uses each source/outcome/session maximum.

Limits: this is consistency with captured input, not exchange truth, physical
failure timing, latency, exact timer cadence, or proof of the cross-producer
first-ready race. The controlled readiness/clean-shutdown preconditions require
external evidence. Silent events can leave equivalent processing prefixes.
Distinct complete assignments of source, replacement boundary, or replacement
prefix are AMBIGUOUS, never a verified pass. Resource limits also fail closed.
Unsupported: lifecycle changes, both-down/same-source recovery, extra failures,
legacy disconnected controls, malformed recognized data, incomplete captures.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
import re
import struct
import sys
import unittest

CHUNK = 64 << 20
REASONS = {"remote_close": "remote_close", "io_error": "io_error",
           "stalled": "silence", "decode_error": "decode_error"}
LIMITS = "No exchange-truth, latency, exact cadence, cross-producer first-ready, or readiness-barrier proof."


class AuditError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise AuditError(message)


def fixed(text):
    require(isinstance(text, str) and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", text),
            "invalid unsigned fixed-point wire string")
    whole, _, fraction = text.partition(".")
    fraction = fraction.rstrip("0")
    require(len(fraction) <= 6, "wire number is not exact fixed-six")
    number = int(whole) * 1_000_000 + int(fraction.ljust(6, "0") or "0")
    require(number <= (1 << 63) - 1, "wire number exceeds fixed-six i64")
    return number


def received_us(text):
    require(isinstance(text, str), "missing receipt timestamp")
    # datetime truncates fractional digits beyond microseconds, as SBE does.
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    require(value.tzinfo is not None, "receipt timestamp lacks UTC offset")
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def exchange_us(text):
    require(isinstance(text, str) and text.isascii() and text.isdigit(),
            "invalid exchange millisecond timestamp")
    result = int(text) * 1000
    require(0 <= result < (1 << 63), "exchange timestamp outside supported range")
    return result


def snapshot(asset, receipt, exchange, bids, asks):
    return ("S", asset, receipt, exchange,
            tuple(sorted(bids.items(), reverse=True)), tuple(sorted(asks.items())))


def update(asset, receipt, exchange, side, price, qty):
    return ("U", asset, receipt, exchange, side, price, qty)


def read_sbe(stream, chunk_size=CHUNK, max_records=2_000_000):
    """Decode schema 1/version 2 directly, including strict chunk padding."""
    records = []
    chunk_number = 0
    while chunk := stream.read(chunk_size):
        require(len(chunk) == chunk_size, "truncated SBE chunk / final flush missing")
        pos = 0

        def take(fmt):
            nonlocal pos
            size = struct.calcsize(fmt)
            require(pos + size <= chunk_size, "SBE message crosses chunk boundary")
            value = struct.unpack_from(fmt, chunk, pos)
            pos += size
            return value

        while True:
            require(pos + 8 <= chunk_size, "SBE chunk has no sentinel space")
            if chunk[pos:pos + 8] == bytes(8):
                require(not any(chunk[pos:]), "nonzero bytes after SBE padding sentinel")
                break
            position = chunk_number * chunk_size + pos
            block, template, schema, version = take("<HHHH")
            require((schema, version) == (1, 2), "unsupported SBE schema/version")
            require((template, block) in ((1, 16), (2, 33)),
                    "unknown SBE template/root block length")
            receipt, exchange = take("<QQ")
            if template == 1:
                sides = []
                for descending in (True, False):
                    width, count = take("<HH")
                    require(width == 16, "unexpected SBE level group width")
                    levels = [take("<qq") for _ in range(count)]
                    prices = [price for price, _ in levels]
                    require(prices == sorted(set(prices), reverse=descending),
                            "snapshot levels unsorted or duplicated")
                    require(all(price >= 0 and qty > 0 for price, qty in levels),
                            "snapshot contains negative price or nonpositive quantity")
                    sides.append(tuple(levels))
                payload = tuple(sides)
            else:
                price, qty, side = take("<qqB")
                require(side in (0, 1) and price >= 0 and qty >= 0,
                        "invalid SBE level update")
                payload = (side, price, qty)
            length, = take("<I")
            require(0 < length <= chunk_size - pos - 8, "invalid SBE symbol length")
            asset = chunk[pos:pos + length].decode("utf-8")
            pos += length
            record = (("S" if template == 1 else "U"), asset, receipt, exchange, *payload)
            records.append((position, record))
            require(len(records) <= max_records, "SBE record resource limit exceeded")
        chunk_number += 1
    require(records, "empty SBE output")
    return records


@dataclass
class Span:
    start: int
    end: int | None = None


@dataclass
class Book:
    bids: dict = field(default_factory=dict)
    asks: dict = field(default_factory=dict)
    initialized: bool = False
    maximum: int = 0
    receipt: int = 0
    exchange: int = 0
    active: tuple | None = None
    span: Span | None = None


@dataclass
class Session:
    assets: tuple
    wanted: set
    books: dict = field(init=False)
    normals: list = field(default_factory=list)  # (exact record, source event index)
    versions: dict = field(default_factory=lambda: defaultdict(list))
    pairs: dict = field(default_factory=lambda: defaultdict(list))
    ranges: dict = field(default_factory=dict)  # completed normal offset -> prefix range
    event: int = 0
    first_ready: int | None = None
    reset_reason: str | None = None
    older: int = 0
    mismatches: int = 0

    def __post_init__(self):
        self.books = {asset: Book() for asset in self.assets}

    def apply(self, event, receipt, max_records):
        kind = event.get("event_type")
        if kind not in ("book", "price_change"):
            return  # Non-book market-channel messages cannot mutate book state.
        exchange = exchange_us(event.get("timestamp"))
        self.event += 1
        changed = []
        emitted = []
        if kind == "book":
            asset = event.get("asset_id")
            if asset not in self.books:
                return
            book = self.books[asset]
            if book.initialized and exchange < book.maximum:
                self.older += 1
                return
            sides = []
            for name in ("bids", "asks"):
                rows = event.get(name)
                require(isinstance(rows, list), "recognized book lacks level arrays")
                levels = {}
                for row in rows:
                    price, qty = fixed(row["price"]), fixed(row["size"])
                    if qty:
                        levels[price] = qty
                    else:
                        levels.pop(price, None)
                sides.append(levels)
            if book.initialized:
                for side, before, after in ((0, book.bids, sides[0]), (1, book.asks, sides[1])):
                    # Independent union of price keys, not the collector's merge walk.
                    for price in sorted(before.keys() | after.keys()):
                        if before.get(price, 0) != after.get(price, 0):
                            emitted.append(update(asset, receipt, exchange, side, price,
                                                  after.get(price, 0)))
                self.mismatches += bool(emitted)
            book.bids, book.asks = sides
            book.initialized = True
            book.maximum = exchange
            changed.append(asset)
        else:
            changes = event.get("price_changes")
            require(isinstance(changes, list), "recognized price_change lacks changes")
            # Parse the entire recognized message before applying any entry.
            parsed = []
            for row in changes:
                side = row.get("side")
                require(side in ("BUY", "SELL"), "invalid wire side")
                parsed.append((row.get("asset_id"), int(side == "SELL"),
                               fixed(row["price"]), fixed(row["size"])))
            for asset, side, price, qty in parsed:
                if asset not in self.books or not self.books[asset].initialized:
                    continue
                book = self.books[asset]
                levels = book.asks if side else book.bids
                if qty:
                    levels[price] = qty
                else:
                    levels.pop(price, None)
                book.maximum = max(book.maximum, exchange)
                changed.append(asset)
                emitted.append(update(asset, receipt, exchange, side, price, qty))
        self.normals.extend((record, self.event) for record in emitted)
        require(len(self.normals) <= max_records, "source record resource limit exceeded")
        for asset in set(changed):
            book = self.books[asset]
            if book.span is not None:
                book.span.end = self.event
            book.receipt, book.exchange = receipt, exchange
            book.active = book.span = None
            key = (asset, receipt, exchange)
            if key in self.wanted:
                record = snapshot(asset, receipt, exchange, book.bids, book.asks)
                book.active, book.span = record, Span(self.event)
                self.versions[record].append(book.span)
        offset = len(self.normals)
        if offset in self.ranges:
            self.ranges[offset][1] = self.event
        else:
            self.ranges[offset] = [self.event, self.event]
        if all(book.initialized for book in self.books.values()):
            if self.first_ready is None:
                self.first_ready = self.event
            pair = tuple(self.books[asset].active for asset in self.assets)
            if all(record is not None for record in pair):
                self.pairs[pair].append((offset, self.event))


def load_jsonl(path):
    with open(path, encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if line.strip() and not line.lstrip().startswith("#"):
                try:
                    row = json.loads(line)
                    require(isinstance(row, dict), "JSONL row is not an object")
                    yield row
                except (ValueError, AuditError) as error:
                    raise AuditError(f"invalid JSONL at line {number}") from error


def reconstruct(rows, records, market_count, failure_order, max_records):
    registrations = {}
    pair = None
    sources = {}
    wanted = {(r[1], r[2], r[3]) for _, r in records if r[0] == "S"}
    resets = []
    first_receipt = last_receipt = None
    source_records = 0
    for row in rows:
        kind = row.get("type")
        if kind == "add_market":
            market, assets = row["market"], tuple(row["order_books"])
            current_pair = (row["connection"], row.get("standby"))
            require(len(assets) == 2 and len(set(assets)) == 2, "market needs two distinct assets")
            require(current_pair[1] is not None and current_pair[0] != current_pair[1],
                    "capture must contain mirrored registrations")
            require(market not in registrations, "market was re-added")
            require(pair is None or current_pair == pair, "markets do not share one ordered pair")
            require(not (set(assets) & {a for v in registrations.values() for a in v}),
                    "asset occurs in multiple markets")
            pair = current_pair
            registrations[market] = assets
            for connection in pair:
                require(not any(c == connection for c, _ in resets), "registration after source failure")
                sources[market, connection] = [Session(assets, wanted)]
        elif kind == "remove_market":
            raise AuditError("lifecycle removal outside controlled fixed-membership scope")
        elif kind == "mark_unsynced":
            connection, reason = row["connection"], row["reason"]
            require(pair is not None and connection in pair and reason in REASONS,
                    "unknown connection or legacy/unknown reset reason")
            resets.append((connection, reason))
            for market, assets in registrations.items():
                sessions = sources[market, connection]
                sessions[-1].reset_reason = reason
                sessions.append(Session(assets, wanted))
        elif kind == "frame":
            receipt = received_us(row["received_at"])
            first_receipt = receipt if first_receipt is None else min(first_receipt, receipt)
            last_receipt = receipt if last_receipt is None else max(last_receipt, receipt)
            text = row["text"]
            if text == "PONG":
                continue
            try:
                events = json.loads(text)
            except ValueError as error:
                raise AuditError("malformed raw JSON outside controlled scope") from error
            if isinstance(events, dict):
                events = [events]
            require(isinstance(events, list), "raw frame is neither object nor array")
            for event in events:
                require(isinstance(event, dict), "raw array contains a non-object")
                market = event.get("market")
                if market not in registrations:
                    continue
                connection = row["connection"]
                require(connection in pair, "registered market arrived on unknown source")
                session = sources[market, connection][-1]
                before = len(session.normals)
                session.apply(event, receipt, max_records)
                source_records += len(session.normals) - before
                require(source_records <= 2 * max_records, "aggregate source record resource limit exceeded")
        else:
            raise AuditError("unknown live capture control")
    require(len(registrations) == market_count, "unexpected fixed market count")
    require(tuple(c for c, _ in resets) == failure_order,
            "capture resets differ from externally controlled failure order")
    require(1 <= len(resets) <= 16,
            "scope supports one to sixteen controlled failure epochs")
    require(all(c in pair for c in failure_order), "failure not in registered pair")
    return registrations, pair, sources, resets, (last_receipt or 0) - (first_receipt or 0)


def metrics_switches(text):
    pattern = re.compile(r'^collector_publisher_switches_total\{reason="([a-z_]+)"\}\s+([^\s]+)\s*$')
    counts = {}
    for line in text.splitlines():
        match = pattern.fullmatch(line)
        if match:
            reason, value = match.groups()
            require(reason in REASONS.values() and reason not in counts,
                    "duplicate/unknown switch metric series")
            number = Decimal(value)
            require(number.is_finite() and number >= 0 and number == number.to_integral_value(),
                    "noninteger switch metric")
            counts[reason] = int(number)
    require(set(counts) == set(REASONS.values()), "missing final switch metric series")
    return counts


def plans(initial, pair, resets, incidents):
    current = initial
    generations = Counter()
    epochs = [(current, 0)]
    used = 0
    for failed, reason in resets:
        if current == failed:
            target = pair[1] if current == pair[0] else pair[0]
            if used >= len(incidents):
                return None
            incident = incidents[used]
            if (incident.get("old_connection"), incident.get("new_connection"), incident.get("reason")) != (current, target, reason):
                return None
            used += 1
            current = target
            epochs.append((current, generations[current]))
        generations[failed] += 1
    return epochs if used == len(incidents) else None


def epoch_valid(rows, start, end, session, checkpoint):
    """Match one source suffix exactly; periodic snapshots cannot hide updates."""
    offset, cursor = checkpoint
    held = {r[1]: [dict(r[4]), dict(r[5])] for _, r in rows[start:start + 2]}
    periodic = Counter()
    consumed = offset
    for _, record in rows[start + 2:end]:
        asset = record[1]
        if record[0] == "U":
            if consumed >= len(session.normals) or session.normals[consumed][0] != record:
                return None
            _, event = session.normals[consumed]
            cursor = max(cursor, event)
            consumed += 1
            side, price, qty = record[4:]
            levels = held[asset][side]
            if qty:
                levels[price] = qty
            else:
                levels.pop(price, None)
        else:
            if (tuple(sorted(held[asset][0].items(), reverse=True)),
                    tuple(sorted(held[asset][1].items()))) != record[4:]:
                return None
            interval = session.ranges.get(consumed)
            if interval is None:  # A timer cannot split one input's output group.
                return None
            candidates = []
            for span in session.versions.get(record, ()):
                low = max(cursor, interval[0], span.start)
                high = min(interval[1], (span.end - 1) if span.end is not None else session.event)
                if low <= high:
                    candidates.append(low)
            if not candidates:
                return None
            cursor = min(candidates)  # Earliest compatible prefix preserves all later choices.
            periodic[asset] += 1
    if consumed != len(session.normals):
        return None
    return periodic


def match_market(rows, assets, pair, sessions, resets, incidents, candidate_limit):
    require(len(rows) >= 2, "market missing initial snapshot pair")
    boundaries = []
    for index in range(len(rows) - 1):
        (position_a, a), (position_b, b) = rows[index:index + 2]
        if a[0] == b[0] == "S" and (a[1], b[1]) == assets:
            # Bytes are variable length; global record ordinal proves adjacency.
            if position_b == position_a + 1:
                boundaries.append(index)
    require(0 in boundaries, "initial snapshots are missing or not one adjacent outcome pair")
    solutions = []
    attempts = 0
    for initial in pair:
        plan = plans(initial, pair, resets, incidents)
        if plan is None:
            continue

        def search(epoch, start, proof, periodic):
            nonlocal attempts
            if len(solutions) >= 2:
                return
            source, generation = plan[epoch]
            session = sessions[source][generation]
            target = tuple(record for _, record in rows[start:start + 2])
            points = session.pairs.get(target, ())
            for point in points:
                if epoch == 0 and point[1] != session.first_ready:
                    continue
                attempts += 1
                require(attempts <= candidate_limit, "candidate search limit; provenance unresolved")
                ends = [len(rows)] if epoch + 1 == len(plan) else [i for i in boundaries if i >= start + 2]
                for end in ends:
                    attempts += 1
                    require(attempts <= candidate_limit, "candidate search limit; provenance unresolved")
                    result = epoch_valid(rows, start, end, session, point)
                    if result is None:
                        continue
                    witness = proof + [{"source": source, "session": generation,
                                        "snapshot_ordinals_1based": [sum(r[0] == "S" and r[1] == a for _, r in rows[:start + 2]) for a in assets],
                                        "sbe_record_ordinals_0based": [rows[start][0], rows[start + 1][0]],
                                        "market_record_index": start, "source_prefix": point[1],
                                        "source_normal_offset": point[0]}]
                    if epoch + 1 == len(plan):
                        if session.reset_reason is None:
                            solutions.append((witness, periodic + result))
                    else:
                        expected_reason = incidents[epoch]["reason"]
                        if session.reset_reason == expected_reason:
                            search(epoch + 1, end, witness, periodic + result)
                    if len(solutions) >= 2:
                        return
        search(0, 0, [], Counter())
    require(solutions, "no exact source/boundary assignment explains market output")
    return solutions, attempts


def audit(records, capture, incidents, metric_text, failure_order, market_count=6,
          min_periodics=3, min_seconds=290, candidate_limit=100_000, max_records=2_000_000):
    registrations, pair, sources, resets, duration = reconstruct(
        capture, records, market_count, failure_order, max_records)
    require(duration >= min_seconds * 1_000_000, "capture receipt span shorter than controlled run")
    incident_lists = defaultdict(list)
    observed = Counter()
    for incident in incidents:
        if incident.get("event") != "publisher_switch":
            continue
        market, reason = incident.get("market"), incident.get("reason")
        require(market in registrations and reason in REASONS, "unknown switch market/reason")
        observed[REASONS[reason]] += 1
        incident_lists[market].append(incident)
    expected = metrics_switches(metric_text)
    require(all(observed[reason] == count for reason, count in expected.items()),
            "switch incidents disagree with final per-reason metrics")
    require(sum(observed.values()) > 0, "controlled run produced no publisher switch")
    owner = {asset: market for market, assets in registrations.items() for asset in assets}
    per_market = defaultdict(list)
    for ordinal, (_, record) in enumerate(records):
        require(record[1] in owner, "SBE contains an unregistered asset")
        per_market[owner[record[1]]].append((ordinal, record))
    report = {"status": "verified", "markets": {}, "switches_by_reason": expected,
              "sbe_records": len(records),
              "sbe_snapshots": sum(r[0] == "S" for _, r in records),
              "sbe_level_updates": sum(r[0] == "U" for _, r in records),
              "capture_receipt_span_seconds": duration / 1_000_000,
              "checks_not_done": LIMITS,
              "external_preconditions": ["fresh process and final cumulative metrics", "capacity 256 and fixed selected markets",
                  "both-ready barrier before each termination; healthy peer", "complete raw capture and clean writer shutdown"],
              "scope": "controlled fixed membership; complete clean capture and final SBE"}
    for market, assets in registrations.items():
        try:
            solutions, attempts = match_market(per_market[market], assets, pair,
                {c: sources[market, c] for c in pair}, resets, incident_lists[market], candidate_limit)
        except AuditError as error:
            raise AuditError(f"market {market}: {error}") from error
        eligible = [(w, p) for w, p in solutions if all(p[a] >= min_periodics for a in assets)]
        require(eligible, "insufficient verified ordinary periodic snapshots")
        # Do not use the periodic minimum to silently resolve provenance collisions.
        if len(solutions) > 1:
            report["status"] = "ambiguous"
        witness, periodic = eligible[0]
        report["markets"][market] = {"witness": witness, "ordinary_snapshots": dict(periodic),
            "assignments": "at_least_two" if len(solutions) > 1 else 1,
            "search_attempts": attempts,
            "older_full_books": sum(s.older for c in pair for s in sources[market, c]),
            "same_source_mismatches": sum(s.mismatches for c in pair for s in sources[market, c])}
    report["verified_initial_snapshots"] = 2 * len(registrations)
    report["verified_replacement_snapshots"] = 2 * sum(observed.values())
    report["verified_ordinary_snapshots"] = sum(sum(entry["ordinary_snapshots"].values())
                                                for entry in report["markets"].values())
    require(report["sbe_snapshots"] == report["verified_initial_snapshots"]
            + report["verified_replacement_snapshots"] + report["verified_ordinary_snapshots"],
            "snapshot accounting is incomplete")
    return report


# Synthetic encoding is intentionally tiny and independent of the Rust writer.
def encode_test_sbe(records, chunk_size=4096):
    chunks, current = [], bytearray()
    for record in records:
        kind, asset, receipt, exchange, *payload = record
        if kind == "S":
            body = struct.pack("<QQ", receipt, exchange)
            for levels in payload:
                body += struct.pack("<HH", 16, len(levels))
                body += b"".join(struct.pack("<qq", *level) for level in levels)
            template, block = 1, 16
        else:
            side, price, qty = payload
            body = struct.pack("<QQqqB", receipt, exchange, price, qty, side)
            template, block = 2, 33
        symbol = asset.encode()
        message = struct.pack("<HHHH", block, template, 1, 2) + body + struct.pack("<I", len(symbol)) + symbol
        require(len(message) + 8 <= chunk_size, "test message exceeds chunk")
        if len(current) + len(message) + 8 > chunk_size:
            chunks.append(bytes(current).ljust(chunk_size, b"\0"))
            current.clear()
        current.extend(message)
    if current:
        chunks.append(bytes(current).ljust(chunk_size, b"\0"))
    return b"".join(chunks)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument("--capture", type=Path)
    parser.add_argument("--sbe", type=Path)
    parser.add_argument("--incidents", type=Path)
    parser.add_argument("--metrics", type=Path)
    parser.add_argument("--failure-order", help="externally controlled connection IDs, e.g. 1,0,1,0 (repeated IDs supported)")
    parser.add_argument("--min-periodics", type=int, default=3)
    parser.add_argument("--min-capture-seconds", type=int, default=290)
    parser.add_argument("--max-records", type=int, default=2_000_000)
    parser.add_argument("--candidate-limit", type=int, default=100_000)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    require(args.directory is not None and args.failure_order is not None,
            "DIRECTORY and --failure-order are required")
    require(args.min_periodics >= 1 and args.min_capture_seconds >= 0 and args.max_records > 0
            and args.candidate_limit > 0, "invalid audit bounds")
    root = args.directory
    with open(args.sbe or root / "books.sbe", "rb") as stream:
        records = read_sbe(stream, max_records=args.max_records)
    result = audit(records, load_jsonl(args.capture or root / "frames.jsonl"),
        load_jsonl(args.incidents or root / "collector.jsonl"),
        (args.metrics or root / "final-metrics.prom").read_text(),
        tuple(int(value) for value in args.failure_order.split(",")),
        min_periodics=args.min_periodics, min_seconds=args.min_capture_seconds,
        candidate_limit=args.candidate_limit, max_records=args.max_records)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "verified" else 2

class SelfTest(unittest.TestCase):
    """Known source values and explicit expected output; no production imports."""

    def scenario(self, twice=False):
        capture = [{"type": "add_market", "market": "m", "order_books": ["a", "b"],
                    "connection": 0, "standby": 1}]

        def raw(source, time, event):
            capture.append({"type": "frame", "connection": source,
                "received_at": f"1970-01-01T00:00:00.{time:03d}000Z", "text": json.dumps(event)})

        def book(source, asset, qty, time, rich=False):
            bids = [{"price": "0.4", "size": str(qty)}]
            asks = []
            if rich:
                bids.insert(0, {"price": "0.2", "size": "4"})
                asks = [{"price": "0.6", "size": "6"}]
            raw(source, time, {"event_type": "book", "market": "m", "asset_id": asset,
                "timestamp": str(time), "bids": bids, "asks": asks})

        def delta(source, asset, qty, time, extra=False):
            changes = [{"asset_id": asset, "side": "BUY", "price": "0.4", "size": str(qty)}]
            if extra:
                changes.append({"asset_id": asset, "side": "BUY", "price": "0.3", "size": "2"})
            raw(source, time, {"event_type": "price_change", "market": "m",
                              "timestamp": str(time), "price_changes": changes})

        book(0, "a", 1, 10)
        book(0, "b", 2, 11)
        delta(0, "a", 3, 12, extra=True)
        book(0, "a", 5, 13, rich=True)
        book(0, "a", 5, 14, rich=True)  # No diff, but metadata advances.
        book(0, "a", 999, 12)  # Strictly older than the greatest applied time.
        book(1, "a", 7, 5)
        book(1, "b", 8, 6)
        delta(1, "a", 9, 7)
        capture.append({"type": "mark_unsynced", "connection": 0, "reason": "remote_close"})
        delta(1, "a", 10, 8)
        book(1, "a", 999, 6)
        book(0, "a", 11, 1)  # Reset permits these lower timestamps.
        book(0, "b", 12, 2)
        incidents = [{"event": "publisher_switch", "market": "m", "old_connection": 0,
                      "new_connection": 1, "reason": "remote_close"}]
        S = lambda asset, qty, time: snapshot(asset, time * 1000, time * 1000,
                                             {400_000: qty * 1_000_000}, {})
        U = lambda asset, qty, time, price=400_000, side=0: update(
            asset, time * 1000, time * 1000, side, price, qty * 1_000_000)
        rich = lambda time: snapshot("a", time * 1000, time * 1000,
            {200_000: 4_000_000, 400_000: 5_000_000}, {600_000: 6_000_000})
        expected = [S("a", 1, 10), S("b", 2, 11),
            U("a", 3, 12), U("a", 2, 12, 300_000),
            U("a", 4, 13, 200_000), U("a", 0, 13, 300_000),
            U("a", 5, 13), U("a", 6, 13, 600_000, 1),
            rich(14), S("b", 2, 11),
            S("a", 9, 7), S("b", 8, 6), U("a", 10, 8),
            S("a", 10, 8), S("b", 8, 6)]
        if twice:
            capture.append({"type": "mark_unsynced", "connection": 1, "reason": "decode_error"})
            delta(0, "a", 13, 3)
            incidents.append({"event": "publisher_switch", "market": "m", "old_connection": 1,
                              "new_connection": 0, "reason": "decode_error"})
            expected += [S("a", 11, 1), S("b", 12, 2), U("a", 13, 3), S("a", 13, 3), S("b", 12, 2)]
        metrics = self.metric_text(remote_close=1, decode_error=int(twice))
        return capture, expected, incidents, metrics, ((0, 1) if twice else (0,))

    @staticmethod
    def metric_text(**counts):
        return "\n".join(f'collector_publisher_switches_total{{reason="{r}"}} {counts.get(r, 0)}'
                         for r in REASONS.values())

    def run_case(self, case):
        capture, expected, incidents, metrics, order = case
        binary = encode_test_sbe(expected, 256)
        records = read_sbe(io.BytesIO(binary), 256)
        self.assertEqual([r for _, r in records], expected)
        return audit(records, capture, incidents, metrics, order,
                     market_count=1, min_periodics=1, min_seconds=0)

    def test_one_switch_exact_corrections_metadata_and_older_filter(self):
        report = self.run_case(self.scenario())
        self.assertEqual(report["status"], "verified")
        self.assertEqual(report["markets"]["m"]["older_full_books"], 2)
        self.assertEqual(report["markets"]["m"]["same_source_mismatches"], 1)
        self.assertEqual(report["markets"]["m"]["ordinary_snapshots"], {"a": 2, "b": 2})

    def test_second_switch_uses_recovered_source_session(self):
        report = self.run_case(self.scenario(twice=True))
        self.assertEqual(report["status"], "verified")
        self.assertEqual([(w["source"], w["session"]) for w in report["markets"]["m"]["witness"]],
                         [(0, 0), (1, 0), (0, 1)])

    def test_interleaved_capture_does_not_impose_cross_source_frame_order(self):
        case = list(self.scenario())
        rows = case[0]
        # Move B's initial frames ahead of A while preserving each producer FIFO.
        b = [r for r in rows if r["type"] == "frame" and r["connection"] == 1][:3]
        case[0] = rows[:1] + b + [r for r in rows[1:] if r not in b]
        self.assertEqual(self.run_case(case)["status"], "verified")

    def assert_bad_output(self, mutate):
        case = list(self.scenario())
        case[1] = list(case[1])
        mutate(case[1])
        with self.assertRaises(AuditError):
            self.run_case(case)

    def test_missing_update_later_overwritten_is_rejected(self):
        self.assert_bad_output(lambda rows: rows.pop(2))

    def test_wrong_correction_order_is_rejected(self):
        def swap(rows):
            rows[4], rows[5] = rows[5], rows[4]
        self.assert_bad_output(swap)

    def test_standby_delta_leak_is_rejected(self):
        self.assert_bad_output(lambda rows: rows.insert(8, update("a", 7000, 7000, 0, 400_000, 9_000_000)))

    def test_unexplained_periodic_mismatch_is_rejected(self):
        self.assert_bad_output(lambda rows: rows.__setitem__(8,
            snapshot("a", 14000, 14000, {400_000: 999_000_000}, {})))

    def test_ordinary_snapshot_wrong_metadata_is_rejected(self):
        def corrupt(rows):
            row = rows[8]
            rows[8] = (*row[:2], row[2] + 1, *row[3:])
        self.assert_bad_output(corrupt)

    def test_update_wrong_receipt_timestamp_is_rejected(self):
        def corrupt(rows):
            row = rows[2]
            rows[2] = (*row[:2], row[2] + 1, *row[3:])
        self.assert_bad_output(corrupt)

    def test_one_outcome_from_wrong_source_at_switch_is_rejected(self):
        self.assert_bad_output(lambda rows: rows.__setitem__(11,
            snapshot("b", 11000, 11000, {400_000: 2_000_000}, {})))

    def test_reversed_pair_is_rejected(self):
        def swap(rows):
            rows[10], rows[11] = rows[11], rows[10]
        self.assert_bad_output(swap)

    def test_lost_switch_incident_cannot_be_excused_by_snapshot(self):
        case = list(self.scenario())
        case[2] = []
        with self.assertRaisesRegex(AuditError, "per-reason metrics"):
            self.run_case(case)

    def test_missing_metrics_and_wrong_reason_fail_closed(self):
        case = list(self.scenario())
        case[3] = self.metric_text(io_error=1)
        with self.assertRaisesRegex(AuditError, "per-reason metrics"):
            self.run_case(case)
        with self.assertRaises(AuditError):
            metrics_switches("")

    def test_uncontrolled_reset_is_rejected(self):
        case = list(self.scenario())
        case[0] = case[0] + [{"type": "mark_unsynced", "connection": 1, "reason": "stalled"}]
        with self.assertRaisesRegex(AuditError, "failure order"):
            self.run_case(case)

    def test_timestamp_and_content_collisions_are_ambiguous(self):
        rows = [{"type": "add_market", "market": "m", "order_books": ["a", "b"], "connection": 0, "standby": 1}]
        expected_pair = []
        for source in (0, 1):
            for asset, time in (("a", 10), ("b", 11)):
                event = {"event_type": "book", "market": "m", "asset_id": asset,
                         "timestamp": str(time), "bids": [{"price": "0.4", "size": "1"}], "asks": []}
                rows.append({"type": "frame", "connection": source,
                    "received_at": f"1970-01-01T00:00:00.{time:03d}Z", "text": json.dumps(event)})
                if source == 0:
                    expected_pair.append(snapshot(asset, time * 1000, time * 1000, {400_000: 1_000_000}, {}))
        rows.append({"type": "mark_unsynced", "connection": 0, "reason": "remote_close"})
        incidents = [{"event": "publisher_switch", "market": "m", "old_connection": 0,
                      "new_connection": 1, "reason": "remote_close"}]
        report = self.run_case((rows, expected_pair * 3, incidents, self.metric_text(remote_close=1), (0,)))
        self.assertEqual(report["status"], "ambiguous")

    def test_binary_chunk_integrity_and_schema(self):
        binary = encode_test_sbe(self.scenario()[1], 256)
        for damaged in (binary[:-1], b"\xff\xff" + binary[2:],
                        binary[:-1] + b"\x01"):
            with self.assertRaises(AuditError):
                read_sbe(io.BytesIO(damaged), 256)

    def test_four_failure_epochs_repeated_ids_first_failure_is_standby(self):
        capture = [{"type": "add_market", "market": "m", "order_books": ["a", "b"],
                    "connection": 0, "standby": 1}]
        def book(source, asset, qty, time):
            event = {"event_type": "book", "market": "m", "asset_id": asset,
                     "timestamp": str(time), "bids": [{"price": "0.4", "size": str(qty)}], "asks": []}
            capture.append({"type": "frame", "connection": source,
                "received_at": f"1970-01-01T00:00:00.{time:03d}Z", "text": json.dumps(event)})
        def delta(source, qty, time):
            event = {"event_type": "price_change", "market": "m", "timestamp": str(time),
                     "price_changes": [{"asset_id": "a", "side": "BUY", "price": "0.4", "size": str(qty)}]}
            capture.append({"type": "frame", "connection": source,
                "received_at": f"1970-01-01T00:00:00.{time:03d}Z", "text": json.dumps(event)})
        def reset(source, reason):
            capture.append({"type": "mark_unsynced", "connection": source, "reason": reason})
        book(0, "a", 1, 10); book(0, "b", 2, 11)
        book(1, "a", 3, 12); book(1, "b", 4, 13)
        reset(1, "io_error")  # First failure is standby; no publisher incident.
        book(1, "a", 5, 1); book(1, "b", 6, 2)
        reset(0, "io_error")
        book(0, "a", 7, 3); book(0, "b", 8, 4)
        delta(1, 9, 5)
        reset(1, "stalled")
        book(1, "a", 10, 6); book(1, "b", 11, 7)
        delta(0, 12, 8)
        reset(0, "stalled")
        delta(1, 13, 9)
        book(0, "a", 14, 1); book(0, "b", 15, 2)
        S = lambda asset, qty, t: snapshot(asset, t * 1000, t * 1000, {400_000: qty * 1_000_000}, {})
        U = lambda qty, t: update("a", t * 1000, t * 1000, 0, 400_000, qty * 1_000_000)
        expected = [S("a", 1, 10), S("b", 2, 11), S("a", 1, 10), S("b", 2, 11),
            S("a", 5, 1), S("b", 6, 2), U(9, 5), S("a", 9, 5), S("b", 6, 2),
            S("a", 7, 3), S("b", 8, 4), U(12, 8), S("a", 12, 8), S("b", 8, 4),
            S("a", 10, 6), S("b", 11, 7), U(13, 9), S("a", 13, 9), S("b", 11, 7)]
        incidents = [{"event": "publisher_switch", "market": "m", "old_connection": old,
                      "new_connection": new, "reason": reason}
                     for old, new, reason in [(0, 1, "io_error"), (1, 0, "stalled"), (0, 1, "stalled")]]
        case = (capture, expected, incidents, self.metric_text(io_error=1, silence=2), (1, 0, 1, 0))
        report = self.run_case(case)
        self.assertEqual(report["status"], "verified")
        witness = report["markets"]["m"]["witness"]
        self.assertEqual([(w["source"], w["session"]) for w in witness],
                         [(0, 0), (1, 1), (0, 1), (1, 2)])
        # A stale pre-reset image cannot replace a fresh source session.
        wrong = list(expected)
        wrong[14] = S("a", 5, 1)
        with self.assertRaises(AuditError):
            self.run_case((capture, wrong, incidents, case[3], case[4]))

    def test_six_markets_share_one_pair_and_one_connection_reset(self):
        capture, expected, incidents, _, order = self.scenario()
        combined_capture = []
        combined_output = []
        combined_incidents = []
        for row in capture:
            if row["type"] == "mark_unsynced":
                combined_capture.append(row)
                continue
            for index in range(6):
                renamed = dict(row)
                if row["type"] == "add_market":
                    renamed.update(market=f"m{index}", order_books=[f"{index}a", f"{index}b"])
                else:
                    event = json.loads(row["text"])
                    event["market"] = f"m{index}"
                    if event["event_type"] == "book":
                        event["asset_id"] = f"{index}" + event["asset_id"]
                    else:
                        for change in event["price_changes"]:
                            change["asset_id"] = f"{index}" + change["asset_id"]
                    renamed["text"] = json.dumps(event)
                combined_capture.append(renamed)
        for index in range(6):
            combined_output.extend((r[0], f"{index}" + r[1], *r[2:]) for r in expected)
            combined_incidents.extend(dict(r, market=f"m{index}") for r in incidents)
        binary = encode_test_sbe(combined_output, 256)
        report = audit(read_sbe(io.BytesIO(binary), 256), combined_capture, combined_incidents,
            self.metric_text(remote_close=6), order, min_periodics=1, min_seconds=0)
        self.assertEqual(report["status"], "verified")
        self.assertEqual(len(report["markets"]), 6)

    def test_fixed_six_and_nanosecond_receipts(self):
        self.assertEqual(fixed("0.0000010"), 1)
        self.assertEqual(received_us("2026-01-01T00:00:00.123456789Z") % 1_000_000, 123456)
        for value in ("0.0000001", "-1", "NaN", "9223372036854.775808"):
            with self.assertRaises(AuditError):
                fixed(value)
        with self.assertRaises(AuditError):
            fixed("0.1234560000000000000000000000000001")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AuditError, ValueError, KeyError, TypeError, OverflowError, OSError) as error:
        # Do not copy identifying absolute input paths into durable audit results.
        message = error.strerror if isinstance(error, OSError) else str(error)
        print(json.dumps({"status": "failed", "error": message,
                          "checks_not_done": LIMITS}), file=sys.stderr)
        sys.exit(1)
