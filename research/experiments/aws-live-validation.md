# Run a bounded AWS collector validation

Use the retained test host after confirming that it is idle. These tools do not provision resources, upload binaries, or deploy services. Run them on the host with a freshly built collector and retain the source manifest, compiler settings, and binary SHA alongside the evidence.

## Run the collector

Use a new output directory for every run:

```sh
python3 research/experiments/aws-live-run.py \
  --binary ./collector --out-dir ./validation-unique/full --seconds 900
```

The defaults match the recent validation configuration: four workers, 256 markets per connection, three lifecycle connections, 20 connection opens per second, 10 Gamma requests per second, and a 900-second scan interval. Scrapes run every five seconds at `127.0.0.1:19091`. Capture stays off. Pass `--markets` for fixed-market mode.

The runner acquires `validation.lock` in the output directory's parent, rejects another collector or an occupied metrics port, and samples host CPU before launch. It raises the open-file limit to 65,536 and fails if the hard limit cannot support that value. It never terminates another workload. Use the same `--lock-file` across concurrent validation tools.

The requested duration includes startup. On completion, SIGINT, or SIGTERM, the runner drains its own collector. It kills that child only if the 15-second drain deadline expires, and records that failure. Disk, output-size, and RSS guards can stop the run earlier. The directory retains status, process limits, samples, individual Prometheus scrapes, collector incidents, and output hashes. An existing output directory is never overwritten.

Use `--help` for limits and configuration overrides. The runner needs Linux `/proc`; Python 3.11 or later is required. Chrony information is recorded when available. The one-second idle check reduces accidental overlap but does not reserve host CPU against unrelated processes that start later.

## Compare matched windows

For two completed 15-minute runs:

```sh
python3 research/experiments/aws-live-analyze.py \
  ./previous/full ./current/full --window 595 895 --json comparison.json
```

For a comparison with a 10-minute run, select `--window 295 595` and compare coverage at matching checkpoints. The analyzer accepts a run directory containing `full/` as well as the `full/` directory itself. It reads the retained format from recent AWS runs too.

The analyzer uses Prometheus counters and histogram deltas, not numeric shutdown logs. It records actual scrape times, sample counts, bucket upper bounds, CPU, and RSS. A counter reset or unavailable window fails the analysis. Empty populations have no percentile. Overflow bounds are null with an explicit overflow flag. It never subtracts percentiles.

Internal latency covers emitted publisher price-change levels and excludes snapshot-derived output. Decoded-event and exchange-age populations include both physical feeds. Coverage is logical, not physical copy count. Inspect structured incidents separately for closure and retry causes. Distinguish removal of ineligible markets from successful baseline recovery.

Historical live runs have different catalogs, activity, and failures. Observed input is not processing capacity, and one run does not establish a causal speedup or regression.

## Validate the complete output separately

```sh
cargo build --release --example validate_collector_output
./target/release/examples/validate_collector_output ./validation-unique/full/books.sbe
```

Cargo can use a configured target directory instead of `target/`. The validator uses the supplied reader to check every record, baseline presence, Polymarket price and quantity domains, and sorted unique positive snapshot levels. It does not compare independent feeds or prove upstream completeness without raw input. Publisher switches can replace full books, so this structural check is separate from the stricter replay checks in `sbe_summary`.

Run the local harness tests without AWS access:

```sh
python3 -m unittest discover -s research/experiments -p test_aws_live_tools.py -v
```

The October 27 eligibility run used the frozen runner and temporary diagnostics retained with its evidence. These portable tools replace the older experiment scripts that depended on removed shutdown summaries. Temporary raw-frame diagnostics were removed from the collector after validation; normal recovery incidents remain.
