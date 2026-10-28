import json
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import io
from unittest.mock import patch
from fake_prometheus import FakePrometheus
from server import (ASSETS, HISTORY_WINDOWS, PAGES, QUERY, Reader, Server, bar_query,
                    decode_bars, decode_history, decode_stats)


def fixture(**values):
    defaults = dict(up=1, scrape_at=100, last_success=100, published_at=100,
                    markets_desired=10, markets_synced=10, books_desired=20, books_synced=20,
                    internal_p50=0.001, internal_p99=0.005, internal_samples=100,
                    internal_finite_share=1, exchange_p50=0.02, exchange_p99=0.1,
                    exchange_samples=50, exchange_rate=2, negative_rate=0, exchange_finite_share=1,
                    markets_standby_ready=8, publisher_switches_total=0)
    defaults.update(values)
    return {"status": "success", "data": {"resultType": "vector", "result": [
        {"metric": {"field": field}, "value": [100, str(value)]} for field, value in defaults.items()
    ]}}


def connections(expected, connected):
    return [{"metric": {"field": "connections", "kind": "orderbook", "state": state}, "value": [100, str(value)]}
            for state, value in (("expected", expected), ("connected", connected))]


class Running:
    """Serves a Server and, optionally, a fake Prometheus on free ports for one test."""

    def __init__(self, scenario=None, reader=None):
        self.fake = FakePrometheus(("127.0.0.1", 0), scenario) if scenario else None
        url = f"http://127.0.0.1:{self.fake.server_port}" if self.fake else "http://127.0.0.1:9"
        self.reader = reader or Reader(url)
        self.server = Server(("127.0.0.1", 0), self.reader)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self):
        self.threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (self.server, self.fake) if s]
        for thread in self.threads:
            thread.start()
        return self

    def __exit__(self, *_):
        for s in (self.server, self.fake):
            if s:
                s.shutdown()
                s.server_close()
        for thread in self.threads:
            thread.join()

    def get(self, path):
        with urllib.request.urlopen(self.base + path) as response:
            return response.status, dict(response.headers), response.read()

    def json(self, path):
        return json.loads(self.get(path)[2])


class ReaderTests(unittest.TestCase):
    def test_units_coverage_and_status(self):
        stats = decode_stats(fixture(), 100)
        self.assertEqual(stats["status"], "live")
        self.assertEqual(stats["internal_latency"]["p99_ms"], 5)
        self.assertEqual(stats["markets"]["ratio"], 1)
        self.assertEqual(stats["resilience"], {"standby_ready_markets": 8, "publisher_switches_total": 0})
        # Only disconnected feeds degrade the status; a coverage gap is shown as numbers.
        self.assertEqual(decode_stats(fixture(markets_synced=9), 100)["status"], "live")
        degraded = fixture()
        degraded["data"]["result"] += connections(2022, 2019)
        self.assertEqual(decode_stats(degraded, 100)["status"], "degraded")
        self.assertEqual(decode_stats(fixture(), 116)["status"], "stale")
        self.assertTrue(decode_stats(fixture(), 116)["retained"])
        self.assertIsNone(decode_stats(fixture(markets_desired=0), 100)["markets"]["ratio"])
        self.assertEqual(decode_stats(fixture(last_success="NaN"), 100)["last_successful_scrape_at"], 100)

    def test_overflow_nan_and_negative_share(self):
        stats = decode_stats(fixture(internal_p99="NaN", exchange_finite_share=0.98, negative_rate=2), 100)
        self.assertIsNone(stats["internal_latency"]["p99_ms"])
        self.assertTrue(stats["exchange_age"]["p99_above_60_seconds"])
        self.assertIsNone(stats["exchange_age"]["p99_ms"])
        self.assertEqual(stats["negative_exchange_share"], 0.5)
        json.dumps(stats, allow_nan=False)

    def test_multiple_collectors_are_rejected(self):
        payload = fixture()
        payload["data"]["result"].append({"metric": {"field": "up"}, "value": [100, "1"]})
        with self.assertRaises(ValueError):
            decode_stats(payload, 100)

    def test_one_failed_scrape_is_stale_not_stopped(self):
        self.assertEqual(decode_stats(fixture(up=0, up_recent=1, last_success=99), 100)["status"], "stale")
        self.assertEqual(decode_stats(fixture(up=0, up_recent=0), 100)["status"], "stopped")
        self.assertEqual(decode_stats(fixture(up=0, up_recent=1, last_success=80), 100)["status"], "stale")

    def test_source_failure_retains_values_as_metrics_unavailable(self):
        reader = Reader("http://127.0.0.1:9091")
        reader.cached = decode_stats(fixture(), 100)
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("down")):
            stats = reader.stats()
        self.assertEqual(stats["status"], "metrics_unavailable")
        self.assertTrue(stats["retained"])
        self.assertEqual(stats["values_at"], 100)
        self.assertEqual(stats["markets"]["synced"], 10)
        fresh = Reader("http://127.0.0.1:9091")
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("down")):
            stats = fresh.stats()
        self.assertEqual(stats["status"], "metrics_unavailable")
        self.assertFalse(stats["retained"])
        self.assertNotIn("markets", stats, "no placeholder values without data")

    def test_queries_are_fixed_and_have_windowed_histograms(self):
        self.assertIn("histogram_quantile(0.99", QUERY)
        self.assertIn("[5m]", QUERY)
        self.assertNotIn("lifetime_max", QUERY)
        # Prometheus 3 normalizes classic-histogram bounds to "60.0"; earlier versions store "60".
        self.assertIn('le=~"60(\\\\.0)?"', QUERY)
        self.assertIn("increase(collector_session_ends_total", bar_query(30))

    def test_history_masks_outages_and_preserves_zero_and_latency_units(self):
        fields = {"up": [(100, 1), (101, 0), (102, 1)],
                  "published_at": [(100, 100), (101, 100), (102, 80)],
                  "events_per_second": [(100, 0), (101, 8), (102, 9)],
                  "internal_p99": [(100, .005)], "internal_finite_share": [(100, 1)],
                  "exchange_p99": [(100, 60)], "exchange_finite_share": [(100, .98)]}
        payload = {"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"field": name}, "values": values} for name, values in fields.items()]}}
        result = decode_history(payload, 1000, "15m")
        self.assertEqual(result["series"]["events_per_second"][:3], [0, None, None])
        self.assertEqual(result["series"]["internal_p99"][0], 5)
        self.assertIsNone(result["series"]["exchange_p99"][0])
        json.dumps(result, allow_nan=False)
        payload["data"]["result"].append(payload["data"]["result"][0])
        with self.assertRaises(ValueError):
            decode_history(payload, 1000, "15m")

    def test_writer_busy_is_empty_until_the_service_exports_it(self):
        rows = [
            {"metric": {"field": "up"}, "values": [[100, "1"], [101, "1"]]},
            {"metric": {"field": "published_at"}, "values": [[100, "100"], [101, "101"]]},
        ]
        payload = {"status": "success", "data": {"resultType": "matrix", "result": rows}}
        history = decode_history(payload, 1000, "15m")
        self.assertTrue(all(value is None for value in history["series"].get("writer_busy", [])))
        rows.append({"metric": {"field": "writer_busy"}, "values": [[100, "0"], [101, "0.75"]]})
        history = decode_history(payload, 1000, "15m")
        self.assertEqual(history["series"]["writer_busy"][:3], [0, 0.75, None])

    def test_history_splits_labels(self):
        payload = {"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"field": "up"}, "values": [[100, "1"]]},
            {"metric": {"field": "published_at"}, "values": [[100, "100"]]},
            {"metric": {"field": "worker_busy", "worker": "0"}, "values": [[100, "0.2"]]},
            {"metric": {"field": "worker_busy", "worker": "1"}, "values": [[100, "0.3"]]},
            {"metric": {"field": "connections", "kind": "orderbook", "state": "expected"}, "values": [[100, "2022"]]}]}}
        series = decode_history(payload, 1000, "15m")["series"]
        self.assertEqual(series["worker_busy:0"][0], 0.2)
        self.assertEqual(series["worker_busy:1"][0], 0.3)
        self.assertEqual(series["connections:orderbook:expected"][0], 2022)

    def test_bars_are_increases_with_gaps_when_not_scraped(self):
        bucket = HISTORY_WINDOWS["15m"][2]
        end = 900 * 10
        first, second = end - 900 + bucket, end - 900 + 2 * bucket
        payload = {"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"field": "up_max"}, "values": [[first, "1"], [second, "0"]]},
            {"metric": {"field": "session_ends", "reason": "remote_close"}, "values": [[first, "3"], [second, "0"]]},
            {"metric": {"field": "book_mismatches"}, "values": [[first, "0"], [second, "5"]]}]}}
        bars = decode_bars(payload, end, "15m")
        self.assertEqual(bars["bucket_seconds"], bucket)
        self.assertEqual(bars["timestamps"][0], first - bucket)
        self.assertEqual(bars["series"]["session_ends:remote_close"][:2], [3, None])
        self.assertEqual(bars["series"]["book_mismatches"][:2], [0, None], "zero stays zero; a missed scrape is a gap")

    def test_history_shares_cached_query_and_rejects_custom_windows(self):
        reader = Reader("http://127.0.0.1:9091")
        payload = {"status": "success", "data": {"resultType": "matrix", "result": []}}
        with patch.object(reader, "stats", return_value={"status": "live"}), \
                patch("urllib.request.urlopen", side_effect=lambda *a, **k: io.BytesIO(json.dumps(payload).encode())) as query:
            with patch("server.time.time", return_value=1000):
                first = reader.history("15m")
                self.assertEqual(reader.history("15m"), first)
            self.assertEqual(query.call_count, 2, "one line query and one bar query")
            body = urllib.parse.parse_qs(query.call_args_list[0].args[0].data.decode())
            self.assertEqual(body["step"], ["1"])
            self.assertTrue(query.call_args.args[0].full_url.endswith("/api/v1/query_range"))
        with self.assertRaises(ValueError):
            reader.history("100d")
        with patch.object(reader, "stats", return_value={"status": "live"}), \
                patch("server.time.time", return_value=1001), patch("urllib.request.urlopen", side_effect=OSError("offline")):
            self.assertEqual(reader.history("15m")["status"], "stale")


class ConnectionStateTests(unittest.TestCase):
    """Each connection state end to end: fake Prometheus, real reader, real HTTP server."""

    def stats(self, scenario):
        with Running(scenario) as running:
            return running.json("/api/demo/stats"), running

    def test_live(self):
        stats, _ = self.stats("live")
        self.assertEqual(stats["status"], "live")
        self.assertFalse(stats["retained"])
        self.assertEqual(stats["workers"], 4)
        self.assertGreater(stats["book_updates_per_second"], 0)
        self.assertLess(stats["age_seconds"], 5)

    def test_starting_until_the_first_scan(self):
        stats, _ = self.stats("starting_scan")
        self.assertEqual(stats["status"], "starting")
        self.assertIsNone(stats["last_successful_scan_at"])
        self.assertLess(stats["markets"]["desired"], 1000, "only confirmed hints before the scan")

    def test_degraded_when_sockets_are_disconnected(self):
        stats, _ = self.stats("degraded")
        self.assertEqual(stats["status"], "degraded")
        orderbook = stats["connections"]["orderbook"]
        self.assertEqual(orderbook["expected"] - orderbook["connected"], 3)

    def test_stale_when_publication_freezes(self):
        stats, _ = self.stats("stale")
        self.assertEqual(stats["status"], "stale")
        self.assertTrue(stats["retained"])
        self.assertGreater(stats["age_seconds"], 30)

    def test_stopped_shows_last_known_values_from_prometheus(self):
        stats, _ = self.stats("stopped")
        self.assertEqual(stats["status"], "stopped")
        self.assertTrue(stats["retained"])
        self.assertAlmostEqual(stats["age_seconds"], 7200, delta=30)
        self.assertGreater(stats["markets"]["synced"], 250_000)

    def test_stopped_with_no_data_has_no_values(self):
        stats, _ = self.stats("never_ran")
        self.assertEqual(stats["status"], "stopped")
        self.assertFalse(stats["retained"])
        self.assertIsNone(stats["values_at"])
        self.assertIsNone(stats["markets"]["synced"], "never a zero placeholder")

    def test_prometheus_unreachable(self):
        with Running("live") as running:
            self.assertEqual(running.json("/api/demo/stats")["status"], "live")
            running.fake.select("prometheus_down")
            running.reader.refreshed = -1e9
            stats = running.json("/api/demo/stats")
            self.assertEqual(stats["status"], "metrics_unavailable")
            self.assertTrue(stats["retained"])
            self.assertIn("markets", stats)
            history = running.json("/api/demo/history?window=1h")
            self.assertEqual(history["status"], "unavailable")
            # Pages and assets never depend on Prometheus.
            for path in [*PAGES, *ASSETS]:
                self.assertEqual(running.get(path)[0], 200, path)
            running.fake.select("live")
            running.reader.refreshed = -1e9
            self.assertEqual(running.json("/api/demo/stats")["status"], "live", "recovers without a restart")

    def test_history_is_anchored_to_the_stop(self):
        with Running("stopped") as running:
            history = running.json("/api/demo/history?window=15m")
            self.assertTrue(history["anchored_to_last_data"])
            self.assertLess(history["end"], time.time() - 3600)
            self.assertTrue(any(v is not None for v in history["series"]["events_per_second"]))
        with Running("stopped_recent") as running:
            history = running.json("/api/demo/history?window=15m")
            self.assertFalse(history["anchored_to_last_data"], "a recent stop stays in the current window")
            values = history["series"]["events_per_second"]
            self.assertIsNone(values[-1], "the stopped period is a gap")
            self.assertIsNotNone(values[0])
            self.assertTrue(all(v is None for v in history["bars"]["series"]["session_ends:remote_close"][-5:]))

    def test_all_history_covers_retained_data_and_stops_at_last_data(self):
        for scenario in ("live", "stopped", "never_ran"):
            with self.subTest(scenario=scenario), Running(scenario) as running:
                history = running.json("/api/demo/history?window=all")
                self.assertEqual(history["status"], "ok")
                self.assertEqual(history["window"], "all")
                self.assertLessEqual(len(history["timestamps"]), 902)
                self.assertLessEqual(len(history["bars"]["timestamps"]), 74)
                if scenario == "never_ran":
                    self.assertTrue(all(v is None for v in history["series"]["events_per_second"]))
                    continue
                self.assertAlmostEqual(history["start"], running.fake.timeline.start,
                                       delta=history["step_seconds"] + 1)
                self.assertTrue(any(v is not None for v in history["series"]["events_per_second"]))
                self.assertEqual(history["anchored_to_last_data"], scenario == "stopped")
                if scenario == "stopped":
                    self.assertAlmostEqual(history["end"], running.fake.timeline.stop,
                                           delta=history["step_seconds"] + 15)

    def test_history_windows_and_series(self):
        with Running("live") as running:
            for window, (duration, step, bucket) in HISTORY_WINDOWS.items():
                history = running.json(f"/api/demo/history?window={window}")
                self.assertEqual(history["status"], "ok")
                self.assertEqual(len(history["timestamps"]), duration // step + 1)
                self.assertEqual(len(history["bars"]["timestamps"]), duration // bucket)
                for key in ("internal_p99", "book_updates_per_second", "worker_busy:3", "writer_busy", "writer_blocked",
                            "connections:orderbook:connected", "markets_standby_ready"):
                    self.assertTrue(any(v is not None for v in history["series"][key]), key)
                for key in ("session_ends:remote_close", "publisher_switches:sequence_gap", "book_mismatches",
                            "older_full_books_skipped", "decode_errors", "discovery_session_ends:remote_close"):
                    self.assertIn(key, history["bars"]["series"])


class HttpTests(unittest.TestCase):
    def test_pages_assets_and_headers(self):
        reader = Reader("http://127.0.0.1:9091")
        reader.cached = decode_stats(fixture(), time.time())
        reader.refreshed = time.monotonic()
        with Running(reader=reader) as running:
            self.assertEqual(running.json("/api/demo/stats")["schema_version"], 1)
            status, headers, body = running.get("/")
            self.assertIn(b"Markets with a current book", body)
            self.assertIn("script-src 'self'", headers["Content-Security-Policy"])
            for path, needle in {"/live": b"Time inside the collector",
                                 "/live?range=1h": b'aria-describedby="help-internal"'}.items():
                self.assertIn(needle, running.get(path)[2], path)
            for path, (_, content_type) in ASSETS.items():
                status, headers, body = running.get(path)
                self.assertEqual(headers["Content-Type"], content_type)
                self.assertTrue(body)
            request = urllib.request.Request(running.base + "/live/")
            opener = urllib.request.build_opener(type("NoRedirect", (urllib.request.HTTPRedirectHandler,), {"redirect_request": lambda *a: None}))
            with self.assertRaises(urllib.error.HTTPError) as error:
                opener.open(request)
            self.assertEqual(error.exception.code, 301)
            self.assertEqual(error.exception.headers["Location"], "/live")
            for path in ["/api/demo/stats?query=anything", "/api/demo/history?window=2h", "/server.py",
                         "/fake_prometheus.py", "/synthetic_collector.py", "/test_ui.cjs", "/api/other", "/../server.py",
                         "/tradeoffs", "/tradeoffs.html"]:
                with self.assertRaises(urllib.error.HTTPError) as error:
                    urllib.request.urlopen(running.base + path)
                self.assertEqual(error.exception.code, 404, path)

    def test_read_query_endpoint_forwards_promql_and_native_errors(self):
        reader = Reader("http://127.0.0.1:9091")
        server = Server(("127.0.0.1", 0), reader)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}/api/prometheus/query"
        payload = {"status": "success", "data": {"resultType": "scalar", "result": [100, "42"]}}
        try:
            with patch.object(reader, "query", return_value=payload) as query:
                expression = 'sum(rate(collector_events_total{job="collector"}[1m]))'
                with urllib.request.urlopen(base + "?" + urllib.parse.urlencode({"query": expression})) as response:
                    self.assertEqual(json.load(response), payload)
                self.assertEqual(query.call_args.args, ("query", {"query": expression}))
                with urllib.request.urlopen(base + "?query=vector(42)&start=100&end=110&step=1") as response:
                    self.assertEqual(json.load(response), payload)
                self.assertEqual(query.call_args.args[0], "query_range")
                for parameters in ["query=up&url=http://example.com", "query=up&query=down", "query="]:
                    with self.assertRaises(urllib.error.HTTPError) as error:
                        urllib.request.urlopen(base + "?" + parameters)
                    self.assertEqual(error.exception.code, 400)
                self.assertEqual(query.call_count, 2)
            error_payload = {"status": "error", "errorType": "bad_data", "error": "invalid expression"}
            upstream = urllib.error.HTTPError("http://127.0.0.1:9091", 400, "bad query", {},
                                             io.BytesIO(json.dumps(error_payload).encode()))
            with patch.object(reader, "query", side_effect=upstream):
                with self.assertRaises(urllib.error.HTTPError) as error:
                    urllib.request.urlopen(base + "?query=invalid")
                self.assertEqual(json.load(error.exception), error_payload)
            with patch.object(reader, "query", side_effect=OSError("offline")):
                with self.assertRaises(urllib.error.HTTPError) as error:
                    urllib.request.urlopen(base + "?query=up")
                self.assertEqual(error.exception.code, 502)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


class SyntheticCollectorTests(unittest.TestCase):
    def test_exposes_the_eighteen_families(self):
        from synthetic_collector import Collector, exposition
        now = time.time()
        collector = Collector(now - 600, scan_seconds=60)
        collector.step(now)
        text = exposition(collector.families(now))
        families = {line.split()[2] for line in text.splitlines() if line.startswith("# TYPE")}
        self.assertEqual(len(families), 18)
        self.assertIn('collector_session_ends_total{kind="orderbook",reason="protocol_error"}', text)
        self.assertIn('collector_publisher_switches_total{reason="sequence_gap"}', text)
        self.assertIn('collector_writer_busy_seconds_total', text)
        self.assertGreater(collector.writer_busy, 0)
        self.assertLessEqual(collector.writer_busy, 600)


if __name__ == "__main__":
    unittest.main()
