"""Check Caddy's adapted request routing, including handle precedence."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest


@unittest.skipUnless(shutil.which("caddy"), "requires Caddy")
class StreamProxyTests(unittest.TestCase):
    def test_health_directory_and_stream_routes_are_isolated(self):
        config = Path(__file__).with_name("voluntas-stream.caddy.example")
        adapted = json.loads(subprocess.check_output(
            ["caddy", "adapt", "--config", str(config), "--adapter", "caddyfile"],
            env={**os.environ, "STREAM_HOST": "stream.example.com", "DEMO_ORIGIN": "https://demo.example.com"},
            text=True,
        ))
        routes = adapted["apps"]["http"]["servers"]["srv0"]["routes"][0]["handle"][0]["routes"]
        def handlers(route):
            return route["handle"][0]["routes"][0]["handle"]
        def route_for(path):
            return next(route for route in routes if any(
                pattern == path or pattern.endswith("*") and path.startswith(pattern[:-1])
                for match in route.get("match", [{"path": ["*"]}]) for pattern in match["path"]
            ))
        health = handlers(route_for("/healthz"))
        self.assertEqual(health[0]["handler"], "static_response")
        self.assertEqual(health[0]["status_code"], 200)
        directory = handlers(route_for("/sockets/0/markets"))
        proxy = next(h for h in directory if h["handler"] == "reverse_proxy")
        self.assertEqual(proxy["upstreams"], [{"dial": "127.0.0.1:19090"}])
        self.assertEqual(proxy["transport"]["max_conns_per_host"], 8)
        headers = {k: v for h in directory if h["handler"] == "headers" for k, v in h["response"]["set"].items()}
        self.assertEqual(headers["Access-Control-Allow-Origin"], ["https://demo.example.com"])
        for path in ["/", "/metrics", "/metrics/sockets", "/readyz"]:
            self.assertEqual(handlers(route_for(path))[0]["upstreams"], [{"dial": "127.0.0.1:9100"}])


if __name__ == "__main__":
    unittest.main()
