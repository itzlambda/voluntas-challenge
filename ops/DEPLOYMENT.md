# Activate the staged collector after benchmarking

Host aliases, DNS names, and Cloudflare profile names in this runbook are neutral
placeholders. Substitute the deployment values before running the commands.

Leave the AWS collector stopped until benchmarking finishes. Prometheus and the
demo already run on the presentation host. The collector unit is disabled, its approval
marker is absent, and the cleanup timer is inactive.

## Select the final release

`/opt/voluntas/collector` points to the prior validated delayed-retry candidate,
with SHA-256 `5975874ba13924c2b5ec22c14cf05f07d0df78663286dd16e0dba66325f27948`.
Its [validation report](../research/experiments/delayed-retry-validation-2026-10-27.md)
describes that candidate. Later benchmark changes are not included automatically.

After selecting the final candidate, install its release executable on AWS.
Do not build on AWS during a benchmark or replace a benchmark's executable.

```sh
sudo install -d /opt/voluntas/releases/SELECTED_RELEASE
sudo install -m 755 /path/to/validated/collector /opt/voluntas/releases/SELECTED_RELEASE/collector
sha256sum /opt/voluntas/releases/SELECTED_RELEASE/collector
sudo ln -sfn /opt/voluntas/releases/SELECTED_RELEASE/collector /opt/voluntas/collector
```

Compare the installed hash with the candidate's validation record. Preserve the
record beside the executable, and keep the previous release for rollback.

## Start the collector when benchmarking finishes

On AWS, confirm that no benchmark collector or supervisor is active. The
launcher checks for collector processes and holds the benchmark coordination
lock for the service's lifetime.

```sh
pgrep -a -x collector
sudo systemctl cat voluntas-collector.service
sudo systemd-analyze verify /etc/systemd/system/voluntas-collector.service
sudo touch /etc/voluntas/collector-approved
sudo systemctl enable --now voluntas-collector.service
sudo systemctl status voluntas-collector.service voluntas-reclaim.timer
```

The prepared command uses four book workers, 256 markets per connection,
full discovery, and `LimitNOFILE=65536`. The staged release uses optimization
level 3, ThinLTO, and one codegen unit. This setup adds no CPU affinity, Tokio
thread override, sysctl change, raw capture, or fsync.

The service writes SBE to `/var/lib/voluntas-collector/books.sbe` through the
existing 64 MiB buffer. Its metrics port is TCP 19090. The AWS security group
permits that port only from the presentation host. The host firewall is inactive. Existing
SSH and outbound rules are unchanged.

```sh
curl -fsS http://127.0.0.1:19090/healthz
curl -fsS http://127.0.0.1:19090/readyz
sudo journalctl --namespace=voluntas -u voluntas-collector.service -n 50
```

Readiness requires fresh component publications and completed initial discovery.
It can remain false while the first scan runs.

## Verify Prometheus and the demo

On the presentation host, inspect the target and the demo API.

```sh
curl -fsS http://127.0.0.1:9091/api/v1/targets
curl -fsS http://127.0.0.1:8000/api/demo/stats
curl -fsS 'http://127.0.0.1:8000/api/demo/history?window=15m'
```

Confirm that the aggregate target is up with a `1s` scrape interval and `900ms`
timeout. Add the `collector_sockets` job from `ops/prometheus.yml` using the same
collector target address, `/metrics/sockets`, a `15s` scrape interval, and a `3s`
timeout. Confirm that this target is also up. Preserve the one-second job.
Install `demo/socket_metrics.py`, `demo/sockets.js`, and `demo/socket-demo.js`
alongside the existing demo files when updating the dashboard.
Confirm that coverage appears and history gains observations. Rates and
percentiles require multiple scrapes and observations. Traffic rates use one
minute, and latency percentiles use five minutes.

Open [the public demo](https://collector-demo.example.com).
Prometheus has no public hostname or Caddy route. The frontend can submit any
PromQL read expression through `/api/prometheus/query`, which returns Prometheus
JSON. Supply `query` and optional `time` for current values, or `query`, `start`,
`end`, and `step` for history. This API reads the dedicated collector-only
Prometheus instance. For example:

```sh
curl -fsSG https://collector-demo.example.com/api/prometheus/query --data-urlencode 'query=up{job="collector"}'
```

For local access to Prometheus itself through authenticated SSH, run:

```sh
ssh -N -L 9091:127.0.0.1:9091 presentation-host
```

Open `http://127.0.0.1:9091` while the tunnel runs. The demo and Prometheus
backends listen on loopback ports 8000 and 9091.

The demo DNS record is a DNS-only A record in the deployment Cloudflare account.
Pass `--profile deployment-profile` explicitly for every Cloudflare operation.
If an AWS stop/start changes its
public IP, update `/etc/voluntas/prometheus.yml` on the presentation host and reload Prometheus.

## Check output and log retention

The collector starts `voluntas-reclaim.timer` as a dependency. Once a minute,
the timer reclaims old blocks from only the service's output file. It retains
at least 4 GiB and five minutes of observed file growth, with reclamation rounded
to 64 MiB boundaries. It never truncates the active file or changes the writer's
offset. It does not touch benchmark artifacts.

```sh
sudo systemctl status voluntas-reclaim.timer voluntas-reclaim.service
sudo journalctl -u voluntas-reclaim.service -n 20
sudo du -h /var/lib/voluntas-collector/books.sbe
sudo du -h --apparent-size /var/lib/voluntas-collector/books.sbe
df -h /var/lib/voluntas-collector
```

Allocated space includes the retained tail, recent output, and growth between
timer runs. It is not a strict 4 GiB cap. After every cleanup attempt, the disk
guard stops only `voluntas-collector.service` if less than 5 GiB remains free.

The file becomes sparse. Its apparent length keeps growing, and reclaimed
ranges read as zeroes. Treat the file as disposable, not a complete recording
or an independently replayable retained tail. Reset its apparent length through
a planned collector restart before reaching the filesystem's maximum file size.
Startup truncates the output through the existing writer and resets cleanup
observations. Restarts rebuild collection state and create a collection gap.

Reclamation adds filesystem work. Retaining five minutes of observed growth
avoids deleting the newest writes immediately. This setup makes no claim of
identical performance to an unlimited output file.

Collector diagnostics use a dedicated journald namespace with 256 MiB retention,
32 MiB files, and a seven-day age limit. Prometheus uses persistent local storage
with seven-day and 2 GB retention limits. Leave disk room for its WAL, head chunks,
and compaction. Prometheus has a 768 MiB memory limit, and the demo has a 256 MiB limit.

## Stop or roll back

Before another AWS benchmark, stop the collector. This also stops its cleanup
timer and releases the coordination lock.

```sh
sudo systemctl disable --now voluntas-collector.service
sudo rm -f /etc/voluntas/collector-approved
sudo systemctl is-active voluntas-collector.service voluntas-reclaim.timer
```

To roll back the executable, restore `/opt/voluntas/collector` to a previous
validated release while the service is stopped. Repeat the activation checks
before restarting it.

On the presentation host, stop the demo and Prometheus without stopping Caddy or the main site.

```sh
sudo systemctl disable --now voluntas-demo.service voluntas-prometheus.service
```

To remove the routes, remove only the Voluntas import from `/etc/caddy/Caddyfile`,
validate Caddy, and reload it. Preserve unrelated sites. The pre-install copy is
`/etc/caddy/Caddyfile.before-voluntas`. To retire the deployment, delete only its
demo DNS record through the deployment profile and revoke only the TCP 19090 security-group rule for the presentation host. Preserve SSH access rules.
