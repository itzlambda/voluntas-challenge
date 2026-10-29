import copy
import json
import http.client
import unittest
from unittest.mock import Mock, patch

from socket_metrics import SocketReader, decode_current, decode_history
from test_server import Running
from server import Reader


def fixture(now=1000, rate=12.5, connected=1, markets=256, started=100, sample=None, up=1):
    rows = []
    for field, value in {"up": up, "scrape_at": now, "started_at": started,
                         "rate": rate, "connected": connected, "markets": markets,
                         "sample_at": now if sample is None else sample}.items():
        if value is None:
            continue
        metric = {"field": field}
        if field not in ("up", "scrape_at", "started_at"):
            metric.update(socket_id="0", pool="primary")
        rows.append({"metric": metric, "value": [now, str(value)]})
    return {"status": "success", "data": {"resultType": "vector", "result": rows}}


class SocketMetricsTests(unittest.TestCase):
    def test_zero_quiet_disconnected_empty_and_warming_are_distinct(self):
        for options, expected, rate in [({}, "connected", 12.5), ({"rate": 0}, "connected", 0),
                                        ({"connected": 0}, "disconnected", 12.5),
                                        ({"markets": 0}, "empty", 12.5),
                                        ({"rate": None}, "connected", None),
                                        ({"started": 980}, "warming_up", None)]:
            result = decode_current(fixture(**options), 1000)
            self.assertEqual(result["status"], "live")
            self.assertEqual(result["sockets"][0]["state"], expected)
            self.assertEqual(result["sockets"][0]["events_per_second"], rate)

    def test_source_timestamps_and_scrape_failure_control_freshness(self):
        self.assertEqual(decode_current(fixture(sample=954), 1000)["status"], "unavailable")
        self.assertEqual(decode_current(fixture(up=0), 1000)["status"], "unavailable")
        self.assertEqual(decode_current(fixture(), 1046)["status"], "unavailable")
        self.assertEqual(decode_current(fixture(sample=1001), 1000)["status"], "unavailable")

    def test_duplicate_collectors_and_bad_identity_are_rejected(self):
        payload = fixture()
        payload["data"]["result"].append(copy.deepcopy(payload["data"]["result"][0]))
        with self.assertRaises(ValueError):
            decode_current(payload, 1000)
        payload = fixture()
        payload["data"]["result"][-1]["metric"]["pool"] = "replica"
        with self.assertRaises(ValueError):
            decode_current(payload, 1000)

    def test_cache_preserves_source_age_and_global_staleness_overrides_scrape(self):
        query = Mock(return_value=fixture())
        aggregate = Mock(return_value={"status": "live"})
        reader = SocketReader(query, aggregate)
        with patch("socket_metrics.time.time", return_value=1000):
            first = reader.sockets()
        self.assertEqual(first["status"], "live")
        with patch("socket_metrics.time.time", return_value=1003):
            cached = reader.sockets()
        self.assertEqual(query.call_count, 1)
        self.assertEqual(cached["values_at"], 1000)
        self.assertEqual(cached["age_seconds"], 3)
        aggregate.return_value = {"status": "stale"}
        with patch("socket_metrics.time.time", return_value=1004):
            stale = reader.sockets()
        self.assertEqual(stale["status"], "stale")
        self.assertEqual(stale["sockets"], first["sockets"])
        aggregate.return_value = {"status": "live"}
        reader.refreshed = -float("inf")
        query.return_value = fixture(now=1015)
        with patch("socket_metrics.time.time", return_value=1015):
            self.assertEqual(reader.sockets()["status"], "live")

    def test_query_failure_retains_last_good_or_shows_unavailable(self):
        query = Mock(return_value=fixture())
        reader = SocketReader(query, lambda: {"status": "live"})
        with patch("socket_metrics.time.time", return_value=1000):
            reader.sockets()
            reader.refreshed = -float("inf")
            query.side_effect = OSError("down")
            self.assertEqual(reader.sockets()["status"], "stale")
            empty = SocketReader(query, lambda: {"status": "live"})
            self.assertEqual(empty.sockets()["status"], "unavailable")

    def test_lock_contention_retains_values_and_source_age(self):
        reader = SocketReader(Mock(return_value=fixture()), lambda: {"status": "live"})
        with patch("socket_metrics.time.time", return_value=1000):
            first = reader.sockets()
        reader.lock.acquire()
        try:
            with patch("socket_metrics.time.time", return_value=1005):
                result = reader.sockets()
        finally:
            reader.lock.release()
        self.assertEqual(result["status"], "stale")
        self.assertTrue(result["retained"])
        self.assertEqual(result["sockets"], first["sockets"])
        self.assertEqual(result["values_at"], 1000)
        self.assertEqual(result["query_at"], 1005)
        self.assertEqual(result["age_seconds"], 5)

    def test_truncated_responses_retain_current_and_historical_data(self):
        query = Mock(return_value=fixture())
        reader = SocketReader(query, lambda: {"status": "live"})
        with patch("socket_metrics.time.time", return_value=1000):
            reader.sockets()
            query.return_value = {"status": "success", "data": {"result": []}}
            reader.history(0, "15m")
        query.side_effect = http.client.IncompleteRead(b"partial")
        reader.refreshed = -float("inf")
        with patch("socket_metrics.time.time", return_value=1020):
            self.assertEqual(reader.sockets()["status"], "stale")
            self.assertEqual(reader.history(0, "15m")["status"], "stale")

    def test_truncated_http_body_is_a_recoverable_source_error(self):
        reader = Reader("http://127.0.0.1:9091")
        with patch("urllib.request.urlopen") as opened:
            opened.return_value.__enter__.return_value.read.side_effect = http.client.IncompleteRead(b"partial")
            with self.assertRaises(OSError):
                reader.query("query", {"query": "vector(1)"})
            self.assertEqual(reader.stats()["status"], "metrics_unavailable")

    def test_history_gaps_staleness_and_process_restart(self):
        ats = [900, 915, 930, 945, 960, 975, 990]
        fields = {"rate": [10, 11, 12, 13, 14, 15, 16], "connected": [1, 0, 1, 1, 1, 1, 1],
                  "markets": [256] * 7, "up": [1] * 7, "aggregate_up": [1] * 7,
                  "started_at": [100, 100, 100, 100, 960, 960, 960],
                  "sample_at": ats, "scrape_at": ats, "aggregate_scrape_at": ats,
                  "published_at": [900, 915, 900, 945, 960, 975, 990]}
        payload = {"status": "success", "data": {"result": [
            {"metric": {"field": field}, "values": [[at, str(value)] for at, value in zip(ats, values)]}
            for field, values in fields.items()]}}
        history = decode_history(payload, 0, "15m", 990)
        by_at = dict(zip(history["timestamps"], history["events_per_second"]))
        self.assertEqual([by_at[at] for at in ats], [10, None, None, 13, None, None, None])
        self.assertEqual(history["restarts"], [100, 960])
        json.dumps(history, allow_nan=False)

    def test_history_queries_only_selected_socket_and_cache_is_bounded(self):
        query = Mock(return_value={"status": "success", "data": {"result": []}})
        reader = SocketReader(query, lambda: {"status": "live"})
        for socket_id in range(20):
            reader.history(socket_id, "1h")
        self.assertEqual(len(reader.histories), 16)
        expression = query.call_args.args[1]["query"]
        self.assertIn('socket_id="19"', expression)
        self.assertNotIn('socket_id="18"', expression)
        calls = query.call_count
        reader.history(19, "1h")
        self.assertEqual(query.call_count, calls)

    def test_history_route_validates_parameters(self):
        with Running() as app:
            for path in ["?socket=-1&window=1h", "?socket=1&window=all", "?socket=1&socket=2&window=1h",
                         "?socket=65536&window=1h", "?socket=1", "?socket=1&window=1h&extra=1"]:
                with self.assertRaises(Exception) as error:
                    app.get("/api/demo/socket-history" + path)
                self.assertEqual(error.exception.code, 400)
            self.assertEqual(app.json("/api/demo/sockets")["status"], "unavailable")
            history = app.json("/api/demo/socket-history?socket=0&window=1h")
            self.assertEqual(history["status"], "unavailable")


if __name__ == "__main__":
    unittest.main()
