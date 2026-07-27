# AdaptiveBDP VPN Validation Plan

## Purpose

This document defines how to prove that AdaptiveBDP provides high throughput
without unstable behavior across materially different VPN networks.

Unit tests are necessary but not sufficient. The validation must exercise a
real QUIC send/ACK path with a deterministic bottleneck, queue, loss model, and
capacity changes.

Use this plan after each repair work package in
`ADAPTIVE_BDP_FIX_INSTRUCTIONS.md`.

## Success Dimensions

Evaluate all of these dimensions. Do not optimize only average throughput.

- Goodput: application payload bytes delivered per second.
- Utilization: goodput divided by configured bottleneck capacity.
- Queue delay: observed RTT minus the drained-path RTT.
- Stability: pacing, cwnd, bandwidth, and queue oscillation.
- Recovery time: RTTs needed to approach capacity after a capacity increase.
- Downshift time: RTTs needed to stop oversending after a capacity decrease.
- Loss response: cwnd and pacing reduction per completed round.
- Fairness: bandwidth share between competing flows.
- Idle restart: burst size and time to useful throughput after inactivity.
- Migration safety: no stale model is reused after an explicit path change.

Record at least p50, p95, p99, minimum, and maximum where applicable.

## Required Telemetry

Sample `AdaptiveBDPDebugInfo` at least once per completed controller round and
on every state transition. Store:

- state and transition reason;
- cwnd and target cwnd;
- bytes in flight and BDP;
- active, max, short, and recovery-probe bandwidth;
- pacing rate and pacing gain;
- raw/latest RTT, smoothed RTT, min RTT, queue delay, queue state;
- round number and all persistent-round counters;
- loss ratio for the round and EWMA;
- lost and ACKed bytes for the round;
- ECN reaction round;
- active temporary pacing multiplier and expiration;
- warmup, idle-restart, ProbeUp, ProbeDown, and ProbeRTT status.

Also record application goodput, sent packets, retransmissions, dropped
packets, and bottleneck queue occupancy.

## Test Harness Requirements

The existing `testutils/simnet` package provides deterministic endpoints,
latency, MTU, packet drop callbacks, and variable-latency callbacks. It does
not currently model a bandwidth-limited queued bottleneck.

Extend the test harness with a deterministic link component that supports:

- independent forward and reverse capacities;
- token-bucket or serialization-delay bandwidth limiting;
- a finite byte or packet queue;
- tail-drop and optional deterministic AQM/ECN marking;
- fixed and variable latency;
- deterministic random loss with a fixed seed;
- scripted burst loss;
- packet reordering and duplication;
- capacity and base-RTT changes at a specified simulated time.

Prefer simulated/synthetic time. A CI test must not rely on scheduler timing or
`time.Sleep`.

If the in-process harness cannot model a scenario accurately, run that
scenario as a separate non-CI experiment using Linux network namespaces and
`tc netem`. Keep its configuration and result parser in the repository so it
is reproducible.

## Traffic Patterns

Run every relevant network scenario with these VPN-like traffic patterns:

1. Saturated unidirectional upload.
2. Saturated unidirectional download.
3. Simultaneous upload and download.
4. QUIC DATAGRAM traffic with small mixed-size packets.
5. Long stream transfer.
6. Interactive low-rate traffic for 10 seconds followed by a bulk transfer.
7. Bulk transfer, idle period, then another bulk transfer.
8. Many short bursts separated by idle gaps.

Do not infer upload behavior from download-only tests.

## Core Scenario Matrix

### Clean fixed-capacity paths

| ID | Capacity | Base RTT | Queue | Loss |
|---|---:|---:|---:|---:|
| C01 | 1 Mbit/s | 20 ms | 1 BDP | 0 |
| C02 | 10 Mbit/s | 50 ms | 1 BDP | 0 |
| C03 | 30 Mbit/s | 150 ms | 1 BDP | 0 |
| C04 | 100 Mbit/s | 20 ms | 2 BDP | 0 |
| C05 | 100 Mbit/s | 200 ms | 1 BDP | 0 |
| C06 | 1 Gbit/s | 100 ms | 1 BDP | 0 |

For C06, explicitly configure a cwnd ceiling large enough for the path, then
run it once more with defaults to demonstrate the default ceiling.

### Wireless-style loss without persistent queue

| ID | Capacity | RTT | Loss model |
|---|---:|---:|---|
| L01 | 30 Mbit/s | 50 ms | independent 0.1% |
| L02 | 30 Mbit/s | 100 ms | independent 1% |
| L03 | 30 Mbit/s | 150 ms | independent 2% |
| L04 | 100 Mbit/s | 40 ms | 10-packet burst every 5 s |
| L05 | 10 Mbit/s | 200 ms | Gilbert-Elliott burst loss |

These scenarios must not synthesize queue pressure merely to justify a loss
cutback.

### Queue and congestion

| ID | Capacity | RTT | Queue / signal |
|---|---:|---:|---|
| Q01 | 30 Mbit/s | 50 ms | tail drop, 0.25 BDP |
| Q02 | 30 Mbit/s | 50 ms | tail drop, 4 BDP |
| Q03 | 100 Mbit/s | 100 ms | persistent reverse-path ACK queue |
| Q04 | 30 Mbit/s | 50 ms | deterministic ECN marks above queue target |
| Q05 | 10 Mbit/s | 200 ms | complete outage long enough for persistent congestion |

Q02 is the bufferbloat test. It must prove that min-RTT expiry does not rebase
the standing queue to zero.

### Capacity and RTT changes without QUIC migration

| ID | Initial path | Changed path | Change time |
|---|---|---|---:|
| T01 | 100 Mbit/s, 30 ms | 10 Mbit/s, 30 ms | 10 s |
| T02 | 10 Mbit/s, 30 ms | 100 Mbit/s, 30 ms | 10 s |
| T03 | 30 Mbit/s, 30 ms | 30 Mbit/s, 150 ms | 10 s |
| T04 | 30 Mbit/s, 150 ms | 30 Mbit/s, 30 ms | 10 s |
| T05 | 100 Mbit/s, 20 ms | 2 Mbit/s, 200 ms | 10 s |
| T06 | 2 Mbit/s, 200 ms | 100 Mbit/s, 20 ms | 10 s |

The 5-tuple must remain unchanged. These scenarios validate aging and
adaptation rather than migration reset.

### Explicit migration

Run Wi-Fi-to-mobile and mobile-to-Wi-Fi migration with a new local path.
Verify that `sentPacketHandler.MigratedPath` creates a new controller and that
no prior `maxBw`, min RTT, loss EWMA, or probe state survives.

### Competing traffic

Run:

- two AdaptiveBDP flows with the same RTT;
- two AdaptiveBDP flows with 20 ms and 200 ms RTT;
- AdaptiveBDP against Cubic;
- AdaptiveBDP against Reno;
- a late-starting AdaptiveBDP flow against an established AdaptiveBDP flow.

Record per-flow goodput, Jain's fairness index, queue delay, and loss.

## F14 implementation status (2026-07-28)

Status legend: `[x]` real QUIC scenario executed in the deterministic-link
harness; `[~]` scenario coverage exists but its full validation-plan
acceptance gate is still pending or failed; `[ ]` not implemented. An executed
scenario is **not** a production-readiness claim.

| Area | Status | Notes |
|---|---|---|
| C01–C05 | `[~]` | Real upload runs exist. Convergence utilization and clean-path p95 gates are pending. |
| C06 | `[~]` | Default and enlarged cwnd configurations run, but the current transfer is not a saturated default-ceiling demonstration. |
| L01–L03 | `[~]` | Fixed-seed independent-loss runs exist with no tail-drop pressure; comparative no-queue-versus-queued-loss gate is pending. |
| L04 | `[~]` | Exact 10-packet deterministic burst is exercised once; the plan's every-5-second repetition is pending. |
| L05 | `[~]` | Fixed-seed Gilbert–Elliott run exists; full loss-response acceptance remains pending. |
| Q01 | `[~]` | Shallow tail-drop run and p99 queue-delay bound (12.5 ms) exist. |
| Q02 | `[x]` | Deep standing queue does not rebase min RTT. |
| Q03 | `[~]` | Reverse ACK-queue occupancy is exercised; full queue/state telemetry gate is pending. |
| Q04 | `[ ]` | Link marks ECN, but deterministic SimConn cannot deliver CE marks through the real QUIC ACK_ECN transport path. |
| Q05 | `[~]` | Complete outage/recovery is exercised; persistent-congestion model-reset proof is pending. |
| T01 | `[~]` | Executed, but currently fails the documented downshift target: observed final pacing was 50 Mbit/s, not below 15 Mbit/s. |
| T02 | `[~]` | Executed; capacity-increase recovery target is pending. |
| T03, T05 | `[x]` | Same-5-tuple upward min-RTT rebase is asserted. |
| T04, T06 | `[x]` | Lower same-5-tuple base RTT is asserted (min RTT <= 50 ms). |
| Idle download → upload | `[x]` | Real direction switch after virtual idle verifies payload and non-zero pacing. |
| Explicit migration | `[~]` | Real AddPath/Probe/Switch/new-path traffic is exercised; controller-reset evidence is pending. |
| Competing flows / fairness | `[ ]` | AdaptiveBDP/Cubic/Reno competition and Jain fairness are not implemented. |
| Bidirectional/DATAGRAM/interactive bursts | `[ ]` | Remaining traffic-pattern coverage is not implemented. |
| JSON / Markdown results | `[~]` | Artifacts exist, but do not yet contain the complete matrix. |
| Per-round telemetry and strict numeric gates | `[ ]` | Queue percentiles exist; per-round controller time series, recovery RTTs, and full cwnd/pacing/BW bounds remain pending. |

Current F14 result: **BLOCKED**. In addition to incomplete coverage, T01 is
an observed performance-acceptance failure. Do not start F15 or claim
production readiness from this status table.

## Correctness Gates

These are hard pass/fail conditions:

1. Upload warmup starts through the real send path after idle.
2. Ten packet-loss callbacks in one round produce at most one normal loss
   response.
3. Loss EWMA decreases during sufficiently sampled clean rounds.
4. Recovery bandwidth advances at most once per round.
5. Persistent congestion clears the old max-bandwidth and full-bandwidth
   state.
6. Temporary pacing cuts survive subsequent ACK processing until expiration.
7. An old max-bandwidth sample expires after the configured filter window.
8. Queue and congestion round counters advance at most once per round.
9. Repeated ECN callbacks in one round do not compound the cut.
10. A standing queue cannot disappear from the model only because the min-RTT
    filter expired.
11. App-limited rounds do not falsely complete Startup.
12. The controller never reaches a zero pacing rate or a permanent no-send
    state.

A failure of any correctness gate blocks performance tuning.

## Initial Performance Targets

These targets are starting acceptance thresholds. If a target is changed,
record the scenario evidence and reason in the review.

### Clean path

- After convergence, median utilization should be at least 90%.
- No clean fixed path may show recurring Startup/ProbeDown oscillation.
- p95 queue delay should remain below
  `max(2 * QueueTarget, 50 ms)` after convergence.

### Capacity decrease

- After a 100-to-10 Mbit/s transition, pacing should fall below 15 Mbit/s
  within three RTTs once congestion or filled-pipe evidence is available.
- Queue delay should return below twice the target within six RTTs.
- The controller must not remain pinned to the old 100 Mbit/s recovery goal
  after the bandwidth-filter window expires.

### Capacity increase

- After a 10-to-100 Mbit/s transition with saturated demand and an empty
  queue, goodput should reach at least 80 Mbit/s within 15 RTTs or within
  5 seconds, whichever limit is larger.
- No individual recovery step may exceed the configured per-round probe gain.

### Random loss without queue

- Independent loss at or below `LossGraceRatio` must not reduce cwnd.
- A normal no-queue loss reaction must not exceed the configured no-queue
  cwnd and pacing cut limits.
- One burst-loss round must not be counted as multiple persistent-loss rounds.

### Persistent congestion

- Cwnd reaches the configured minimum.
- Old `maxBw`, `shortBw`, full-bandwidth state, and recovery state are cleared.
- The first ACK after the outage cannot restore the pre-outage bandwidth.

### Idle restart

- The pacer must preserve its configured burst limit.
- App-limited pre-traffic must not cause false full-bandwidth detection.
- With saturated demand after idle, controlled probing starts no more than one
  step per round.

### Fairness

- Two equal-RTT AdaptiveBDP flows should reach Jain's fairness index of at
  least 0.90 after convergence.
- Against Cubic or Reno, investigate any sustained bandwidth ratio outside
  `[0.5, 2.0]`; do not automatically accept higher AdaptiveBDP throughput as a
  success if it is caused by persistent queue occupation.

## Regression Test Mapping

Each repair work package must add the smallest deterministic regression first:

| Work package | Minimum regression |
|---|---|
| F01 | Post-send bytes-in-flight starts warmup |
| F02 | Multiple losses in one round count once; zero-loss EWMA decay |
| F03 | Multiple ACKs in one round create one recovery step |
| F04 | First ACK after persistent congestion cannot restore old state |
| F05 | ACK recomputation cannot erase an active pacing cut |
| F06 | Old high bandwidth expires after the filter window |
| F07 | ACK frequency cannot advance round counters or compound ECN |
| F08 | Standing queue triggers ProbeRTT and cannot rebase min RTT |
| F09 | App-limited rounds do not complete Startup |
| F10 | Public zero/default/range semantics are explicit |

Then run the relevant network scenarios. Do not replace deterministic unit
regressions with long end-to-end tests.

## Result Report Template

For each scenario, report:

```text
Scenario:
Commit:
Configuration:
Seed:
Traffic pattern:

Capacity / base RTT / queue / loss:
Convergence time:
Goodput p50 / p95:
Utilization p50:
RTT p50 / p95 / p99:
Queue delay p50 / p95 / p99:
Loss rate:
State transition counts:
Minimum / maximum cwnd:
Minimum / maximum pacing:
Minimum / maximum active bandwidth:
Fairness index, if applicable:

Correctness gates:
Performance targets:
Unexpected state reasons:
Conclusion:
```

Store machine-readable results when possible. A graph is useful for pacing,
cwnd, bandwidth, RTT, and queue over time, but the pass/fail decision must also
be expressible from numeric assertions.

## Final Readiness Rule

AdaptiveBDP can be proposed as a general VPN default only after:

- all Critical and High work packages are complete;
- all correctness gates pass;
- clean-path, loss, queue, capacity-transition, idle-restart, and migration
  scenarios pass;
- no unresolved persistent oscillation or stale-model recovery remains;
- any throughput improvement is not obtained by unbounded queue growth or
  starvation of competing flows.
