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

L04 drops exactly ten packets after virtual time 200 ms on the 100 Mbit/s /
40 ms path. Across three runs it delivered 40.54–41.89 Mbit/s, had zero tail
drops, and retained non-zero pacing. The exact loss count is deterministic;
the remaining metric range is an unresolved same-timestamp QUIC scheduling
issue, so it cannot yet serve as an exact numeric acceptance gate.

Capacity-transition observations are currently an F14 blocker. On T01
(100 Mbit/s to 10 Mbit/s at virtual time 200 ms), the observed final pacing
was 6,250,000 B/s (50 Mbit/s), above the validation target of 15 Mbit/s after
the downshift. T02 completed with 20.56 Mbit/s application goodput. These are
measurements, not passing acceptance claims.

The real-QUIC C01-C06 clean-path runs and L01-L05 runs are present in
`adaptive_bdp_simnet_test.go`. The baseline does not yet cover Q04,
idle restart, explicit migration, or competing
flows. It does not claim the
performance targets in `ADAPTIVE_BDP_VALIDATION_PLAN.md`; F14 remains blocked
until those scenarios have deterministic numeric assertions and recorded
results.
