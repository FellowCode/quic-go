# AdaptiveBDP F14 deterministic-link acceptance report

F14 passes the deterministic real-QUIC validation matrix. This unblocks F15;
it is not, by itself, a production-readiness decision.

The acceptance run used:

```powershell
go test -count=1 ./integrationtests/self -run TestAdaptiveBDPDeterministicLink -v
```

The tests use `testing/synctest` and a 100 microsecond virtual-time link tick.
Packet delivery, capacity changes, loss, ECN, outage, idle periods, and
migration checks do not depend on wall-clock sleeps. The machine-readable
counterpart is
`integrationtests/self/testdata/adaptive_bdp_f14_results.json`.

## Clean fixed-capacity paths

Every C01-C06 run asserts all of the following from application-delivery and
per-round controller telemetry:

- median post-convergence application utilization is at least 90%;
- post-convergence p95 queue delay is at most
  `max(2 * QueueTarget, 50 ms)`;
- cwnd remains within its configured minimum and maximum;
- pacing remains non-zero and completed controller rounds are monotonic;
- Startup is not re-entered and spontaneous ProbeDown oscillation does not
  recur. The separately labelled `probe_up_drain` transition is the bounded
  drain phase of an intentional capacity probe, not an uncontrolled cycle.

| Scenario | Capacity / RTT | Transfer-wide goodput | Queue-delay p95 | Outcome |
|---|---:|---:|---:|---|
| C01 | 1 Mbit/s / 20 ms | 0.925 Mbit/s | 20.480 ms | pass; low-BDP tuning uses two-packet initial/minimum windows, 20 ms queue target, and 0.90 no-congestion floor |
| C02 | 10 Mbit/s / 50 ms | 9.330 Mbit/s | 14.336 ms | pass |
| C03 | 30 Mbit/s / 150 ms | 22.003 Mbit/s | 43.691 ms | pass |
| C04 | 100 Mbit/s / 20 ms | 91.792 Mbit/s | 6.246 ms | pass |
| C05 | 100 Mbit/s / 200 ms | 64.284 Mbit/s | 60.621 ms | pass |
| C06 default ceiling | 1 Gbit/s / 100 ms | 368.831 Mbit/s | 2.867 ms | pass |
| C06 enlarged ceiling | 1 Gbit/s / 100 ms | 378.291 Mbit/s | 31.048 ms | pass |

Transfer-wide goodput includes handshake and ramp-up time; the 90% acceptance
gate is evaluated over sustained post-convergence application windows.

## Loss, queues, and congestion signals

- L01-L03 use fixed-seed independent loss without a finite bottleneck queue.
  The observed goodputs were 17.397, 9.161, and 5.670 Mbit/s. All retained
  non-zero pacing and produced zero tail drops.
- L04 delivered one exact ten-packet scripted burst with zero tail drops.
  L05 used fixed-seed Gilbert-Elliott loss and produced 34 correlated losses
  with zero tail drops.
- The comparative loss test proves that a 1% no-queue loss reaction is
  gentler than a queued-loss reaction by comparing telemetry-recorded cwnd
  multipliers.
- Q01 exercises a 0.25-BDP tail-drop queue. Q02 reaches the configured
  768,000-byte deep queue while retaining a 50.8 ms drained-path min RTT, so a
  standing queue is not rebased to zero.
- Q03 creates persistent reverse-path ACK queue occupancy.
- Q04 carries ECT bits through the real simulated PacketConn path, converts
  them to CE above the deterministic threshold, receives ACK_ECN, and records
  the ECN reaction round in controller telemetry.
- Q05 reports one persistent-congestion event. Its accumulated send-time loss
  span was 2.9 s against a 684.273 ms gate. The controller reaches minimum
  cwnd, clears old max/short/recovery/full-bandwidth state, and the first
  post-outage ACK cannot restore the pre-outage bandwidth.

## Capacity and RTT transitions

T01 proves that after filled-queue evidence on a 100-to-10 Mbit/s change,
pacing falls below 15 Mbit/s within three base RTTs and queue delay returns
below twice the target within six RTTs. Its final pacing was approximately
10.02 Mbit/s.

T02 proves both controller pacing and sustained application goodput reach at
least 80 Mbit/s within the documented five-second limit after a
10-to-100 Mbit/s change. The final pacing was approximately 100.15 Mbit/s.
The default 900 ms ProbeUp interval leaves deterministic margin before the
five-second deadline; the isolated transition passed 100 consecutive runs.

T03/T05 rebase min RTT upward on the same 5-tuple, while T04/T06 learn the
lower base RTT. The deterministic schedule is compressed relative to the
10-second narrative schedule in the plan; acceptance deadlines are measured
from the actual link-change/evidence timestamp and preserve the stated RTT and
five-second bounds.

## Idle, migration, fairness, and mixed traffic

- The download-to-idle-to-upload case preserves the pacer's 16 KiB burst
  limit after 500 ms of virtual idle and resumes with non-zero pacing.
- Explicit AddPath/Probe/Switch migration sends and confirms post-switch
  traffic, then observes a new controller history starting at round 1 in
  Startup. Old max bandwidth, short bandwidth, loss EWMA, recovery bandwidth,
  and full-bandwidth state are absent from the new path.
- Equal-RTT AdaptiveBDP flows achieved Jain fairness 1.0000.
- AdaptiveBDP/Cubic and AdaptiveBDP/Reno sustained ratios were 0.6888 and
  0.6934, inside the required `[0.5, 2.0]` review range.
- The 20/200 ms unequal-RTT run gave Jain 0.8248 and both flows made progress.
  No numeric unequal-RTT fairness threshold is defined by the plan.
- The late-start run gave Jain 1.0000 and proves the established flow was
  active when the second flow started.
- Bidirectional DATAGRAM, bidirectional stream, interactive, and post-idle
  bulk traffic all delivered in both directions with non-zero pacing.

## Repairs validated by F14

The completed work also fixes two harness/correctness defects discovered by
the matrix:

1. deterministic queue occupancy now ends at serializer departure rather than
   at propagation delivery, so propagation bytes are not misreported as
   bottleneck queue;
2. persistent congestion accumulates qualifying loss spans across loss
   detection batches and uses the pre-outage PTO gate.

Controller telemetry is opt-in and bounded to 2,048 samples. It records
completed rounds and state transitions with cwnd, BDP, bandwidth filters,
pacing, RTT/queue, loss, ECN, warmup/probe flags, and action multipliers.

## Verdict

**F14: PASS.** All deterministic correctness and initial performance gates in
scope pass. F15 and any remaining platform/manual production-readiness work
remain separate.
