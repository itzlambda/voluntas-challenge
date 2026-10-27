# Two-host IPv4 and IPv6 pause comparison

Archived on 2026-10-26 as an original test-host connectivity incident.
The [archive summary](../README.md) supersedes the attribution and next steps below.
Historical measurements remain unchanged.

Forcing IPv4 does not resolve every observed receive pause. The original host
shows two failure patterns: broad IPv6 interruptions and a separate prolonged
interruption on both IPv4 Polymarket sockets. The comparison host has no
receipt gaps of at least 15 seconds on either family during this two-hour run.
The collector's address-family default was kept unchanged. The incident is now archived.

## Capture and method

The two probes run simultaneously on 2026-10-26, approximately 12:57 to 14:57 UTC.
Host A is the original host. Host B is a separate VPS. These labels omit machine identities.
Both supervisors record completion and probe exit 0. Host A contains 149,648
application records, and host B contains 155,971. Neither has timestamp
regressions within an individual probe. Each host has 7,200 CPU samples and
zero increase in CPU steal time. This does not rule out all scheduling delays.

Each probe has four Polymarket sockets subscribed to the same asset: two
forced to IPv4 and two forced to IPv6. One socket per family uses TCP_NODELAY.
Each queues an application PING every five seconds. The controls are Gamma
HTTP requests every two seconds bound to each family, ICMP replies for each
family, and Kraken. Every logged Kraken session uses IPv4.
All logged Polymarket handshakes match the requested family.

A gap is time between consecutive receipts of a text frame, PONG, WebSocket
control message, HTTP response headers, or ICMP echo reply. Reconnect time is
included. Silence before the first receipt and after the last is excluded.
These are availability observations, not market-update latency measurements.
Frame payloads are not retained, so equal receipt counts do not prove equal content.

## Receipt gaps

The threshold is 15 seconds. Longest gap includes gaps below the threshold.
A close count includes all close or error events, even those without a long gap.

| Probe | Host A gaps | Host A longest | Host A closes | Host B gaps | Host B longest |
|---|---:|---:|---:|---:|---:|
| Polymarket IPv4, Nagle on | 1 | 1030.1 s | 1 | 0 | 5.0 s |
| Polymarket IPv4, TCP_NODELAY | 1 | 937.1 s | 3 | 0 | 5.0 s |
| Polymarket IPv6, Nagle on | 4 | 112.5 s | 12 | 0 | 5.0 s |
| Polymarket IPv6, TCP_NODELAY | 5 | 116.2 s | 13 | 0 | 5.1 s |
| Gamma IPv4 | 0 | 9.8 s | 0 | 0 | 2.1 s |
| Gamma IPv6 | 4 | 77.4 s | 0 | 0 | 2.0 s |
| ICMP IPv4 | 0 | 2.4 s | 0 | 0 | 2.0 s |
| ICMP IPv6 | 4 | 77.0 s | 0 | 0 | 2.0 s |
| Kraken, observed IPv4 | 0 | 5.4 s | 2 | 0 | 1.2 s |

Host B records no closes on any WebSocket. Each Polymarket socket records
30,053 receipts, but update completeness is not measured. Gamma returns HTTP
200 for all 3,600 IPv4 and 3,600 IPv6 responses on host B. On host A it returns
3,600 IPv4 and 3,581 IPv6 responses, all HTTP 200, with 19 IPv6 request failures.
The HTTP probe records response headers and does not consume the response body.

## Broad IPv6 interruptions on host A

Four periods have overlapping gaps on both IPv6 Polymarket sockets, IPv6
ICMP, and IPv6 Gamma. The IPv6 WebSocket gap envelopes are approximately
13:00:24 to 13:02:20, 13:47:59 to 13:48:56, 14:21:59 to 14:22:59, and
14:39:36 to 14:40:33 UTC. ICMP and HTTP recover sooner than the WebSockets.
The longest IPv6 ICMP gap is 77.0 seconds. The longest IPv6 WebSocket gap is
116.2 seconds. See the exact intervals in the JSON evidence.

IPv4 ICMP and Gamma continue through all four periods. Both IPv4 Polymarket
sockets continue through the first, third, and fourth. During the second,
they are already stalled in the separate interruption described below.
Host B has no qualifying gaps during any of these periods.

This supports an IPv6-path problem affecting host A's connectivity to
multiple services. It does not identify the failed hop, or establish a
provider-wide or service-wide outage. The previous experiment's neighbour
discovery observations remain a lead, not a proven cause.

One additional gap affects only the TCP_NODELAY IPv6 socket, at 13:12:10 to
13:12:38 UTC. Its interior TCP samples show 160 acknowledged bytes, no
received bytes, and zero added retransmissions while the controls continue.
That event has a different signature from the broad IPv6 interruptions.
TCP acknowledgements do not prove application-level processing by the server.

## Separate prolonged IPv4 interruption on host A

Both IPv4 Polymarket sockets stop receiving at about 13:38:51 UTC.
The TCP_NODELAY socket receives again at 13:54:28, a 937.1-second gap.
The Nagle-on socket receives again at 13:56:02, a 1,030.1-second gap.
Both sessions end with a TCP timeout before reconnecting.
IPv4 ICMP, IPv4 Gamma, and Kraken continue during the interruption.

Within each affected established session, kernel samples show no increase
in acknowledged or received bytes. Retransmissions increase by 15 on the
TCP_NODELAY socket and 16 on the Nagle-on socket. This supports a transport
failure affecting those flows. The capture cannot distinguish the remote
endpoint from an intermediate path or host networking problem.
The capture contains neighbour-discovery packets, not the IPv4 TCP or ARP
packets needed to locate that failure.

The raw probe has no collector-style 180-second silence timeout. It waits
for TCP to fail, which is why this gap reaches 16 to 17 minutes.
Do not present that duration as measured collector recovery time. Recovery
under the collector's current health rule needs its own service test. Book
connections use a 180-second base limit with up to 30 seconds of random
stagger, then wait for the next check. This is not an exact 180-second
recovery guarantee.

## Revised interpretation

The original host had both broad IPv6 interruptions and a separate prolonged
IPv4 WebSocket failure. Forcing IPv4 was not a general fix. The independent
host comparison and later AWS observations support treating this as an
original-host or network-path incident rather than an exchange-wide delivery
cycle. The failed component remains unproven. The [archive summary](../README.md)
records the timeout history and the separate standby/replica follow-up.

## Retained evidence

- [Host A anonymized counters, gap intervals, and TCP evidence](../experiments/pause-probe-family-host-a-2026-10-26.json)
- [Host B anonymized counters and gap intervals](../experiments/pause-probe-family-host-b-2026-10-26.json)
- [Earlier TCP and neighbour-discovery experiment](polymarket-tcp-pause-probe.md)

The probe sources, analysis scripts, timeline generator, and figures were
deleted after this incident was archived. Source fingerprints in the retained
JSON describe those historical tools. Raw captures remain private under
`data/pause-probe-family/`; Git ignores `data/`. Raw socket and packet captures
contain network identifiers and must not be published.
