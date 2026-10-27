# Polymarket pause probe with kernel TCP state

Archived on 2026-10-26 as an original test-host connectivity incident.
The [archive summary](../README.md) supersedes the attribution and next steps below.
Historical measurements remain unchanged.

Completed a fresh 40-minute run on 2026-10-26, 08:46:16 to 09:26:16 UTC.
The probe exited successfully and its supervisor recorded the completion marker.
The capture contains 2 shared receive pauses of at least 15 seconds.

The first shared pause lasted about 60 seconds on both Nagle-on and both TCP_NODELAY sockets.
Acknowledged-byte and received-byte counters stayed flat while retransmissions increased.
Independent IPv6 pings to Cloudflare and Google also stopped receiving, while IPv4 controls continued.
This is evidence of an IPv6 connectivity problem affecting more than Polymarket.
The failing hop is not established by this test.

## Setup and coverage

- Four Polymarket market-channel sockets subscribed to the same asset. Each queued an application PING every 5 seconds.
- A and B used Nagle. C and D used TCP_NODELAY. Every observed Polymarket session used IPv6; Kraken used IPv4.
- IPv4 ping to 1.1.1.1, IPv6 ping to 2606:4700:4700::1111, kernel TCP state, IPv6 counters, and gateway state cover the full run.
- Google IPv6 ping to 2001:4860:4860::8888 began at 08:57:15 UTC, before the observed pause.
- HTTPS requests to Gamma, Google, and Cloudflare ran every 2 seconds. Their connection families were not logged.
- The collector implementation and its socket settings were unchanged. The test used the existing probe binary, fingerprinted in the JSON evidence.

## Shared receive pauses

| Event | Socket | Nagle | Receive silence, UTC | Duration | ACKed bytes inside | Added retransmissions |
|---|---|---|---|---:|---:|---:|
| 1 | A | on | 09:07:41 to 09:08:41 | 59.775 s | 0 B | 8 |
| 1 | B | on | 09:07:42 to 09:08:41 | 59.512 s | 0 B | 8 |
| 1 | C | off | 09:07:42 to 09:08:41 | 59.605 s | 0 B | 8 |
| 1 | D | off | 09:07:41 to 09:08:42 | 60.350 s | 0 B | 8 |
| 2 | A | on | 09:25:07 to 09:25:41 (reset) | ≥34.229 s | 0 B | 7 |
| 2 | B | on | 09:25:07 to 09:25:41 | 34.239 s | 0 B | 7 |
| 2 | C | off | 09:25:07 to 09:25:47 | 39.842 s | 0 B | 7 |
| 2 | D | off | 09:25:06 to 09:25:41 | 34.717 s | 0 B | 7 |

TCP deltas compare the first and last available kernel samples inside each receive silence, excluding its first second.
They exclude recovery traffic. Each affected socket also had outstanding unacknowledged data and queued unsent bytes.
The JSON evidence retains every interior sample, including retransmission timeout and backoff values.
`≥` marks a silence that ended with a reset rather than a receipt; its recorded duration is a lower bound.

A successful application PING write is not proof of delivery. Linux can keep unsent bytes queued while earlier data remains unacknowledged.
`bytes_sent` also includes retransmissions, so its increase is not a count of distinct delivered PINGs.
[Linux TCP counters](https://github.com/torvalds/linux/blob/master/net/ipv4/tcp.c),
[TCP transmit accounting](https://github.com/torvalds/linux/blob/master/net/ipv4/tcp_output.c),
[ss counter rendering](https://github.com/iproute2/iproute2/blob/main/misc/ss.c).

## Independent controls

| Control | Recorded receipts | Longest silence over its coverage |
|---|---:|---:|
| kraken | 4,100 | 1.634 s |
| icmp | 2,362 | 1.698 s |
| cloudflare | 1,200 | 2.720 s |
| google | 1,199 | 12.341 s |
| gamma | 1,199 | 12.329 s |
| cpu | 2,400 | 1.697 s |
| Cloudflare IPv6 ping | 2,302 | 40.135 s |
| Google IPv6 ping, added mid-run | 1,653 | 40.169 s |

During the first shared pause, Cloudflare IPv6 ping had a 40.135-second gap and Google IPv6 ping had a 40.169-second gap.
IPv4 ping, Kraken, and CPU sampling continued. This rules out an application-only Polymarket explanation for that event.
CPU steal increased by 0 jiffies over the run.
HTTP status counts, failures, and response maxima are retained in the JSON evidence.

The IPv6 gateway state moved from `REACHABLE` to `DELAY`, then `PROBE`, then back to `REACHABLE` near recovery.
This makes neighbour discovery a useful lead, but does not prove that the gateway caused the outage.
TCP retry intervals had grown to tens of seconds, consistent with WebSockets recovering later than ICMP.
That timing is an interpretation, not a measured identification of the failed component.

The IPv6 control-packet capture began after the first pause and covers the second.
Its 3,416 packets were independently read back with tcpdump, with matching neighbour-discovery counts.
The capture reported zero packets dropped by the kernel.

During the second outage, the VM sent three unicast neighbour solicitations for its default IPv6 gateway,
at 09:25:36.073, 09:25:37.098, and 09:25:38.136 UTC, with no corresponding advertisement captured.
It then sent a multicast solicitation at 09:25:39.787. An advertisement arrived about 1.9 ms later,
as replies from both IPv6 ping targets resumed. The advertised gateway MAC matched the previously cached MAC.
The [anonymized packet trace](../experiments/pause-probe-tcp-2026-10-26.json) retains these events without addresses.

This makes IPv6 next-hop reachability and neighbour discovery the next diagnostic target.
It does not establish whether the VM, gateway, or an intermediate virtual network caused the loss.

## Resets and delivery limits

- `polymarket_a`: 3 close/error events, 481 PING writes, 465 PONG receipts.
- `polymarket_b`: 0 close/error events, 480 PING writes, 480 PONG receipts.
- `polymarket_c_nodelay`: 0 close/error events, 480 PING writes, 480 PONG receipts.
- `polymarket_d_nodelay`: 1 close/error events, 481 PING writes, 481 PONG receipts.

Peer resets occurred with both Nagle settings. Socket A also reset after the first shared pause.
The probe records frame lengths, not market payloads, so this run cannot establish that every market update was preserved.
A delayed backlog arriving after a pause is not proof of lossless recovery across resets.

## Interrupted capture

The inherited run stopped unexpectedly at 08:40:37 UTC after 36.926 minutes and had no completion marker.
Its original samplers also stopped. The cause of termination was not established.
It is retained as interrupted evidence, not counted as a completed 40-minute test.

That capture observed two shared pauses, about 57 seconds and 30 to 33 seconds, affecting both Nagle settings.
All four sockets had flat acknowledged-byte counters and additional retransmissions during each.
Its late-added Cloudflare IPv6 ping covered only the second pause and also stopped receiving while IPv4 controls continued.
The fresh run above was supervised independently to complete the full requested duration.

## Completed follow-up

The [two-host family comparison](polymarket-family-pause-comparison.md) ran
for two hours on both hosts, with two sockets per family and one socket per
family using TCP_NODELAY. The original host again showed broad IPv6
interruptions, and also a separate 16–17 minute IPv4 WebSocket interruption
ending in TCP timeouts. The comparison host had no receipt gap of at least
15 seconds on either family.

These observations are retained as an original-host connectivity incident.
The failing components remain unidentified. No IPv4-only workaround was added.
The current attribution and deferred timeout work are in the
[archive summary](../README.md).

## Retained evidence

- [Completed-run counters, traces, coverage, and source fingerprints](../experiments/pause-probe-tcp-2026-10-26.json)
- [Interrupted-run evidence](../experiments/pause-probe-interrupted-2026-10-26.json)

The completed capture contained 19,543 valid JSON records. No individual
probe had timestamp regressions. The diagnostic scripts and generated figures
were deleted after the incident was archived. Their fingerprints in the
results describe the historical tools. Raw network captures remain private.
