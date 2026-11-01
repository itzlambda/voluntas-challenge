# Live metrics validation

The implementation was checked with the following commands:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
python3 -m unittest discover -s demo
node demo/test_ui.cjs
node demo/test_sockets.cjs
node demo/test_books.cjs
node demo/test_deployment.cjs
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
node demo/test_sockets.cjs
node demo/test_deployment.cjs
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

## Socket market view validation, 2026-11-01

Local validation passed formatting, Clippy with warnings denied, 94 Rust
library tests, and the 25 affected metrics, standby, and mock-exchange
integration tests. Dashboard checks passed 43 Python tests, 11 book wire and
lifecycle tests, 17 socket tests, 14 UI tests, and 14 deployment-notice tests.
Two Caddy tests verify adapted route precedence and actual health/CORS behavior
when the collector upstream returns 502.

The isolated Chromium check exercises the reused history chart, synthetic
markets and books, 45-market Gamma batches, snapshot waiting, exact quantities,
updates, reconnect baseline resets, dialog cleanup, focus restoration, and
refreshed assignments. It also checks long quantities on a 390 px viewport,
stable table nodes and status announcements, hidden-tab release/resume,
disabled streaming, and a missing book-view asset.

A public Gamma lookup with 40 condition IDs, including a v2 market, returned
all 40 rows with CORS `*` using explicit versions, `closed=any`, and a matching
page limit. The shared `demo/fixtures/books.sbe` fixture is checked byte-for-byte
against Rust's `with_message` encoder and independently decoded in JavaScript.
These checks establish local behavior and metadata compatibility. Collector
startup, live proxy configuration, and the deployed browser flow require
verification during rollout.

### WebSocket close regression

The live browser check exposed a missing reply flush when a client initiated
WebSocket close. The focused regression
`replies_to_client_initiated_close_before_dropping_connection` failed with
`Connection reset without closing handshake` before the fix and passed after
it. Formatting, Clippy, all 95 library tests, and all six mock-exchange
integration tests passed with the correction. The public Chromium close check
then returned code 1000 with a clean handshake and no WebSocket errors.
The native Node WebSocket client also closed cleanly without the earlier probe
error.

### Production rollout

The collector release `fd09a0b` and frontend release `90105fa` were activated
on 2026-11-01. Rollback copies of the prior collector and complete website
were retained. The deployment banner covered both collector restarts and was
cleared after validation.

The final collector passed readiness with no automatic restarts. Both
Prometheus scrape jobs were healthy. All 2,034 order-book sockets were connected,
and market coverage remained above 99.99% across three consecutive checks.
The public directory returned sorted assignments with the website's CORS
origin, while metrics routes remained private.

The final public-site Chromium run loaded real socket history and market
metadata, received both outcome snapshots, and rendered all four bid/ask
tables. After an intentional disconnect it received fresh snapshots again.
Mobile layout, focus restoration, subscription cleanup, and clean close
handshakes passed. It recorded zero page errors, WebSocket errors, and CSP
violations. This was an end-to-end behavior check, not a performance benchmark.
