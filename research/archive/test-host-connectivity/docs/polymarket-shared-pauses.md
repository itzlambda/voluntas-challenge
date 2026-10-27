# Shared receive pauses on the market channel

Archived on 2026-10-26 as an original test-host connectivity incident.
The [archive summary](../README.md) supersedes the attribution and next steps below.
Historical measurements remain unchanged.

Measured 2026-10-25 and 2026-10-26 from the original 6-vCPU test host.
These observations historically motivated the health rule in
[SPEC section 8](../../../../SPEC.md#8-connections).

## What happens

On the measured host, market WebSockets sometimes stopped delivering on all
observed connections together. Some sessions resumed with a backlog. Later
address-family controls found both broader IPv6 connectivity interruptions and
a separate IPv4 Polymarket interruption; silence on one host does not establish
a service-wide cause. See the [two-host comparison](polymarket-family-pause-comparison.md).

- **Frequency:** 15 pauses in 3.2 hours of a full-catalog run (2,500
  connections), 9–26 minutes apart. The intervals cluster near multiples of
  about 9 minutes (9.0, 9.2, 9.3, 9.5, 17.2, 17.3, 25.9).
- **Length:** 10–100 s at the 10 s resolution of the health log; 57 s and
  60 s in the probe below. The longest earlier observation was 119 s
  ([lifecycle sockets](../../../docs/polymarket-lifecycle-ws.md)).
- **Observed at different collection sizes:** pauses appear with 2 lightly loaded connections
  (probe below), with 3 idle lifecycle sockets, and with 2,500 busy ones.
- **Backlogs can arrive after recovery.** In the original two-socket probe,
  when a pause ended, the held market events and **all
  the `PONG`s for `PING`s sent during the pause** arrived within about 35 ms,
  on the same connection. In the full-catalog run, frames/s after a pause
  rose well above normal (for example 52,000/s against about 20,000/s) as
  backlogs arrived. The TCP follow-up also observed peer resets, so recovery
  does not establish that every market update was preserved.
- Connections resume a few seconds apart, not all at the same instant.



The overnight durations are lower bounds. They count only the 10 s health
intervals with zero frames on every connection, so a pause covers an interval
only if it fills it completely, and connections resuming a few seconds apart
end the count early. A 57 s pause would show as 40–50 s there. The probe
measures each socket's silence directly, to the millisecond.

## Where it comes from

A 40-minute probe ran independent streams side by side:

| During each pause (57 s, 60 s) | Longest silence |
|---|---|
| Two Polymarket market-channel sockets (`PING` every 5 s) | 57–60 s, no frames, no `PONG` |
| Kraken WebSocket (1 s heartbeats) | 1.1–1.3 s |
| ICMP to 1.1.1.1 every second | 1.2–1.3 s |
| HTTPS every 2 s to Cloudflare's trace endpoint and Google | about 2 s |
| HTTPS every 2 s to Gamma (Polymarket REST) | 2.3 s in one pause; in the other, one failed request shared with Google at the same moment (a brief local hiccup) |
| CPU steal time | 0 |

In that probe, both Polymarket market WebSockets stopped together while the
other controls continued. This initially suggested an upstream WebSocket
service pause. Those controls did not isolate IPv4 from IPv6, so they could
not establish that TCP connectivity to Polymarket stayed healthy.

The [TCP follow-up](polymarket-tcp-pause-probe.md) found flat acknowledged-byte
counters and rising retransmissions during shared pauses. All affected
Polymarket sockets used IPv6. An additional IPv6 ping control also paused
while IPv4 controls continued. This points to an IPv6 connectivity problem
affecting more than Polymarket; the failing hop remains unidentified.

## Consequence for the collector

The first health rule treated "no `PONG` within 30 s" as a failure when most
other connections were receiving. Because connections resume a few seconds
apart, the end of every pause looked like hundreds of individual stalls: one
run reconnected about 2,150 connections in 52 minutes, and each reconnect
abandoned the old session and any backlog that might still arrive. A fresh
book restores current state but cannot recover every event in the gap.

The conservative rule was retained: reconnect on close or error, or after
180 seconds with no received frames. Later controls undermined the initial
exchange-side attribution. Timeout changes are deferred to the separate
standby/replica work. See the [current archive summary](../README.md).

## Retained evidence

The diagnostic probes and generated figures were deleted after the incident
was archived. The [TCP summary](../experiments/pause-probe-tcp-2026-10-26.json)
and [two-host comparison](polymarket-family-pause-comparison.md) retain the
later measurements that changed the interpretation.
