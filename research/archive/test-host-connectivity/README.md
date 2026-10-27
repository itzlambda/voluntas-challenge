# Original test-host connectivity incident

Archived on 2026-10-26. Treat the broad receive stalls as an incident affecting
the original test host and its network path. The earlier attribution to a
Polymarket-wide delivery pause is withdrawn. The failed component was not
identified.

## Why the collector has a long silence timeout

The original host repeatedly stopped receiving on many WebSockets together,
sometimes for almost two minutes. Some sessions resumed with queued events and
PONGs. We initially assumed the exchange was pausing delivery. Reconnecting
during those interruptions discarded sessions that might recover, so we
replaced the short PONG timeout with a conservative 180-second silence limit.
Book connections add up to 30 seconds of stagger, plus check scheduling.

Later controls changed that interpretation. Independent IPv6 ICMP and HTTP
traffic also stopped on the original host while IPv4 controls continued. A
two-host comparison reproduced those interruptions only on the original host,
which also had a separate prolonged IPv4 WebSocket failure. The comparison
host had no receipt gaps of at least 15 seconds during its two-hour probe.
This supports a problem with the original host or its network path. It does
not identify the failed hop or establish that every pause had the same cause.

## AWS follow-up and limits

The one-hour London run did not reproduce the broad receive-stall pattern.
After startup, no sampled 10-second aggregate health interval reported zero
frames. All 722 HTTP controls succeeded, and the longest interval between
ICMP replies was about two seconds. These observations support moving the
old incident out of the active exchange-behavior findings.

The run still recorded late events and retries for missing initial books.
It did not retain per-frame, per-connection receipt traces, so it cannot rule
out shorter individual connection pauses. It used an instrumented executable
and was not a controlled hardware comparison. Private run evidence is retained
under `data/aws-london-test/`, including the completed run's status, telemetry,
and analysis. No additional test is required for this archive decision.

## Current decision

Keep the existing timeout during this cleanup and document its cost: an
isolated stalled connection can take roughly three minutes to recover.
Timeout changes belong to the separate standby/replica connection work.
Do not use the original host's stalls as evidence of a normal Polymarket
delivery cycle, and do not add an IPv4-only workaround on that basis.

## Retained evidence

- [Original shared-pause observations](docs/polymarket-shared-pauses.md)
- [TCP and independent-control observations](docs/polymarket-tcp-pause-probe.md)
- [Two-host comparison](docs/polymarket-family-pause-comparison.md)
- Anonymized measurement summaries in `experiments/`

The diagnostic probe scripts, Rust probe examples, timeline generator,
timeline data, and generated SVGs were deleted at the user's request.
Historical source fingerprints in retained results refer to those deleted
tools. Raw network captures remain private and are excluded from Git.
