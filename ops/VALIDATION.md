# Live metrics validation

The implementation was checked with the following commands:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
python3 -m unittest discover -s demo
node demo/test_ui.cjs
```

The binary integration test runs discovery, reconnects, removals, recovery, live
HTTP scraping, and independent SBE replay for both SIGINT and SIGTERM. Focused
metrics tests cover inclusive bucket boundaries, overflow, the exact family
allowlist, retirement without double-counting, partial synchronization, UTC
clock independence, exclusion of snapshot-derived output, slow HTTP clients,
readiness, and unexpected HTTP task loss.

An additional local end-to-end check used the release collector, the independent
synthetic exchange, Prometheus 3.15.1, and `demo/server.py`. Promtool accepted both
the scrape configuration and actual exposition. The fixed demo query produced
live statistics for 100 synchronized markets and 200 books, with two order-book
connections and three market-discovery connections. Both latency distributions
contained observations. SIGTERM drained the collector with exit 0. The independent
SBE checker passed.
After collector exit, the demo changed to stale and retained its last measurements.

Those initial checks did not include a browser inspection or a deployment
retention policy. They do not establish a production performance budget.

## Staged deployment validation, 2026-10-27

The deployment on the presentation host uses checksum-verified Prometheus 3.13.5 LTS and
locally served uPlot 1.6.32. Promtool and systemd accepted the installed
configuration. Both services on the presentation host are enabled at boot, bind to loopback,
and use the existing Caddy server for the demo. The demo subdomain has valid
HTTPS. The unused Prometheus DNS record and Caddy route were subsequently
removed. The original website still returns 200 directly from its origin.

The affected checks are:

```sh
python3 -m unittest discover -s demo
python3 -m unittest discover -s ops
node demo/test_ui.cjs
node --check demo/charts.js
```

The isolated integration script runs a synthetic exporter and a separate real
Prometheus instance. It does not add synthetic samples to the deployed database.
It verified one-second scraping, decoded live coverage, rates, histogram
percentiles, a 901-point history response, missing-data gaps, and an outage
becoming stale.

On a host with a matching Prometheus executable, run:

```sh
python3 ops/verify-demo-stack.py --prometheus /path/to/prometheus --demo demo/server.py
```

A headless Chromium check loaded the deployed demo, rendered four real uPlot
canvases, switched all three time ranges, rendered a browser-only fixture with
a missing point, and checked a mobile viewport. No JavaScript errors occurred.
T3 preview could not start because of host AppArmor restrictions, so that check
used a temporary Playwright client and the existing Chromium executable.

The regular-file cleanup test freed allocated blocks while preserving apparent
size, the retained tail, the open writer's position, and subsequent writes. It
also checked the minimum retention age and rejection of symbolic links.

The AWS security group permits TCP 19090 only from the presentation host. A temporary
15-second listener returned 503 to the presentation host while an unapproved source timed
out. The listener exported no metrics and closed after the test. The collector
and cleanup timer remain inactive, the collector is disabled, and the activation
marker is absent. No collector process, benchmark artifact, or host tuning was
changed for deployment validation.

Actual collector startup, its service shutdown and restart behavior, namespaced
journal retention, and live AWS collector history remain deferred until
benchmarking finishes. The current public demo correctly reports unavailable.
The external cleanup policy is not a comparative performance measurement.

The frontend-defined read-query endpoint was verified through the public demo
URL using `vector(42)` and an 11-point range query. Invalid PromQL and invalid
time ranges returned native Prometheus errors. Upstream URL overrides were
rejected, and an administrative API path returned 404. These expression queries
stored no synthetic samples. The Prometheus DNS record is absent, its Caddy
route is removed, and the server still listens only on loopback.
