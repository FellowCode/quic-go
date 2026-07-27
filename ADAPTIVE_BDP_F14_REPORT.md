# AdaptiveBDP F14 deterministic-link baseline

This is an actual baseline, not a synthetic or hand-filled performance
claim. It was generated on Windows with:

```powershell
go test -count=1 -v ./integrationtests/self -run TestAdaptiveBDPDeterministicLink
```

The machine-readable counterpart is
`integrationtests/self/testdata/adaptive_bdp_f14_results.json`. The tests use
`testing/synctest` and a 100 microsecond virtual-time link tick; no wall-clock
sleep or elapsed-time measurement drives packet delivery.

| Scenario | Goodput (payload / virtual elapsed) | Min RTT | Peak queue | Outcome |
|---|---:|---:|---:|---|
| bulk-transfer | 7.50 Mbit/s | 11 ms | 42,976 B | no-send invariant passed |
| T03 | 12.62 Mbit/s | 217 ms | 545,280 B | upward rebase passed |
| T05 | 0.44 Mbit/s | 210.4 ms | 69,120 B | upward rebase passed |
| Q02 | 2.14 Mbit/s | 50.8 ms | 769,385 B | standing queue did not rebase min RTT |

Wireless-loss baseline (1 MiB upload, no finite bottleneck queue): L01 at
0.1% loss delivered 17.39 Mbit/s; L02 at 1% delivered 9.81 Mbit/s; L03 at 2%
delivered 6.52 Mbit/s. Their fixed seeds were 1, 12, and 13 respectively;
all had zero tail drops and non-zero pacing.

L05 uses a fixed-seed Gilbert–Elliott model (seed 505) on the 10 Mbit/s /
200 ms path. Across three runs it produced exactly 34 correlated losses and
zero tail drops; application goodput was 3.776–3.778 Mbit/s, queue-delay p50
was 52.224 ms, and p99 was 72.732 ms.

Two equal-RTT AdaptiveBDP uploads sharing the same 30 Mbit/s bottleneck
achieved Jain fairness 0.9999–1.0000 across five runs (13.24–13.47 Mbit/s per
flow), exceeding the 0.90 target.

On the same bottleneck, one AdaptiveBDP flow versus Cubic measured 12.09
versus 17.00 Mbit/s (ratio 0.7110), and AdaptiveBDP versus Reno measured 11.95
versus 17.23 Mbit/s (ratio 0.6936). Both have deterministic assertions that
the ratio stays within [0.5, 2.0]; ten repeated test invocations passed. The
current p95 forward queue delays were 115.720 ms and 134.152 ms respectively,
with zero tail drops.

Two AdaptiveBDP flows with 20 ms and 200 ms RTT paths sharing one bottleneck
measured 25.82 and 7.91 Mbit/s respectively (Jain 0.7800); ten repeated test
invocations passed and both flows made progress. This is an observation, not a
fairness acceptance claim: queue/loss telemetry and a documented unequal-RTT
target are still missing.

The competing-flow helper also records aggregate queue/loss telemetry. Its
current equal-RTT AdaptiveBDP/AdaptiveBDP p95 queue delay was 96.256 ms;
unequal RTT was 19.806 ms. All ten repeated competing-flow runs had zero
tail drops after using a finite 1 MiB common queue.

For late start, the first AdaptiveBDP upload transfers 8 MiB before a second
flow starts 500 ms later in virtual time. All ten runs proved the first flow
was still active when the late flow began; goodput was 14.72–15.77 Mbit/s for
the established flow and 15.88–15.90 Mbit/s for the late flow, with Jain
fairness 0.9985–1.0000. This too remains telemetry rather than a final
acceptance target; its current p95 queue delay was 57.003 ms and tail drops
were zero.

Interactive traffic coverage sends 100 bidirectional 256-byte QUIC DATAGRAMs
at 100 ms virtual intervals (10 seconds), followed by bidirectional streams
and a 512 KiB client bulk upload. Three runs delivered 656,329–656,376 B
forward and 122,115–122,225 B reverse; forward goodput was 0.412–0.417 Mbit/s
and reverse goodput was 0.081–0.082 Mbit/s. Both controllers retained
non-zero pacing.

L04 drops exactly ten packets after virtual time 200 ms on the 100 Mbit/s /
40 ms path. Across three runs it delivered 40.54–41.89 Mbit/s, had zero tail
drops, and retained non-zero pacing. The exact loss count is deterministic;
the remaining metric range is an unresolved same-timestamp QUIC scheduling
issue, so it cannot yet serve as an exact numeric acceptance gate.

T01 (100 Mbit/s to 10 Mbit/s at virtual time 200 ms) now asserts final pacing
below 15 Mbit/s; the observed final pacing was 1,301,633 B/s (10.41 Mbit/s).
The per-round proof that this happens within three RTTs remains pending. T02
completed with 20.603 Mbit/s transfer-wide application goodput; this does not
meet or prove the required 80 Mbit/s within 15 RTTs after the upshift. C01
(1 Mbit/s / 20 ms / one BDP) measured 232,032 bit/s and 169 tail drops on its
short transfer, so it is likewise not a clean-path utilization pass. These are
measurements, not full acceptance claims.

The real-QUIC C01-C06 clean-path runs and L01-L05 runs are present in
`adaptive_bdp_simnet_test.go`. Q04 and controller-reset
evidence for migration/outage remain uncovered. It does not claim the
performance targets in `ADAPTIVE_BDP_VALIDATION_PLAN.md`; F14 remains blocked
until those scenarios have deterministic numeric assertions and recorded
results.
