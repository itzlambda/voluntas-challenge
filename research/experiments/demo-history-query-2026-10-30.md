# Retained dashboard history query

On 30 October 2026, the All history line query exceeded the demo's two-second
Prometheus deadline. It succeeded with a longer deadline. This experiment tests
whether the query can do less work while returning the same chart data.

## Change

The original query separately calculated p50, p99, and the share of observations
within 60 seconds for each of two histograms. That repeatedly evaluated the same
five-minute bucket rates. The revised query reads each histogram's bucket rates
once. The demo calculates both percentiles and applies the existing overflow
mask from those rates.

Interpolation follows the positive-duration classic histogram behavior in
[Prometheus](https://github.com/prometheus/prometheus/blob/main/promql/quantile.go),
including repair of decreasing counts and suppression of tiny relative bucket
differences. Empty or incomplete histograms remain missing data. The frontend
still receives the same percentile series, units, resolution, and freshness gaps.

The expanded intermediate response is larger, so the client also requests gzip.
The response limit applies to decompressed bytes. A live query confirmed that
Prometheus supplies gzip; one expanded histogram response used approximately
317 kB on the wire. The four-second line-query deadline remains as headroom.

## Method

Baseline: `c321996:demo/server.py`. Optimized: the working-tree implementation.
The benchmark executed locally through a read-only SSH tunnel to the existing
Prometheus. No code was uploaded and no service settings changed.

The host has two CPUs. Before profiling, load averages were 0.28, 0.19, and 0.12;
Prometheus reported 1.5% lifetime CPU usage. Individual-expression profiling ran
separately from the measurements below. It identified histogram evaluation as a
large cost. Prometheus evaluation statistics then confirmed the reduction in
samples read and negligible query queue time.

The final run began at 11:29:55 UTC. Five cold Reader instances per implementation
ran in alternating baseline/optimized order against one fixed evaluation time.
Both sides used the same gzip transport, four-second query deadline, and
five-second HTTP timeout. Each cold load included retained-range discovery,
current stats, lines, bars, and decoding. The retained data covered roughly
27 October through 30 October, with 900 line timestamps. The benchmark checked
successful responses and complete decoded-response equality on every run.

Reproduce against a private Prometheus endpoint or a read-only local tunnel:

```sh
python3 research/experiments/demo-history-query.py \
  --prometheus-url http://127.0.0.1:9091 \
  --baseline-ref c321996 --runs 5 --output /tmp/history-query-results.json
```

## Results and limits

The line query's median Prometheus evaluation time decreased from 2.143 seconds
(range 2.070–2.171) to 1.196 seconds (range 1.146–1.226): about 44% less time.
Samples read decreased from 21,378,591 to 10,388,241, about 51% fewer. Each side
completed all five runs without an error. All five complete All responses matched.
Separate comparisons of 15m, 1h, 12h, and 24h also matched exactly.

Cold-load medians over the tunnel were 4.103 seconds before (range 3.425–5.216)
and 3.793 seconds after (range 3.267–4.757). These ranges overlap substantially;
the experiment does not establish a reliable end-to-end latency improvement.
Transport variability and unchanged discovery/bar requests limit that result.
The query evaluation improvement is consistent across all five runs.

The decoded intermediate JSON grew from approximately 0.68 MB to 1.68 MB,
because it now contains bucket rates. The expanded matrix also increases peak
Prometheus result samples. Gzip reduces transfer size; the public chart response
has no added series. This measurement covers the current retained data and one
sequential caller, not a full seven-day or concurrent load test. No result here
explains or repairs the separate scrape outage.

The bucket-decoding regression failed before the change. The final Python suite
passes 42 tests, including empty histograms, overflow, monotonic repair,
freshness gaps, gzip payload limits, and truncated compressed responses. The 44
JavaScript checks also passed. The patch has not been deployed.
