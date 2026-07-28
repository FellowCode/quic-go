# AdaptiveBDP Production Readiness Fix Plan

## Purpose

This document is the execution plan for the issues found by the second
AdaptiveBDP production audit. It extends `ADAPTIVE_BDP_FIX_INSTRUCTIONS.md`.

Execute every work package in a separate Codex / OpenAI Terra session. Do not
combine packages, because each package has its own correctness gate and review
scope.

The current production decision is **GO**: F11-F15 pass, with the release
evidence and limitations recorded in `ADAPTIVE_BDP_F15_RELEASE_REPORT.md`.

## Global Rules

1. Read these files before changing code:
   - `ADAPTIVE_BDP_ARCHITECTURE.md`
   - `ADAPTIVE_BDP_FIX_INSTRUCTIONS.md`
   - `ADAPTIVE_BDP_VALIDATION_PLAN.md`
   - this document
2. Preserve all user changes already present in the worktree.
3. Do not weaken an assertion, increase a timeout, or skip a test merely to
   obtain a green result.
4. Add a deterministic regression test before or together with every fix.
5. Use raw RTT only for ProbeRTT sampling. Do not replace it with smoothed RTT.
6. An app-limited or under-filled flow cannot prove that capacity decreased.
7. A standing queue cannot become the new min RTT merely because the min-RTT
   filter expired.
8. A permanent base-RTT increase must eventually be learned after the
   controller has deliberately drained its own inflight data.
9. Public configuration must either be honored exactly or rejected with a
   descriptive error.
10. Do not claim production readiness from unit tests alone.

## Required Order

| Order | Package | Purpose | Production priority |
|---:|---|---|---|
| 1 | F11 | Make ProbeRTT learn a permanent base-RTT increase safely | Critical |
| 2 | F12 | Bound `MaxWindowPackets` and remove integer overflow | Critical |
| 3 | F13 | Restore a fully green repository test suite | High |
| 4 | F14 | Implement deterministic VPN network validation | Critical |
| 5 | F15 | Add race, soak, and release gates | High |

Do not start F14 until F11 and F12 pass. F13 may be investigated in parallel
only in a separate worktree or by a separate developer.

---

## F11: Safe Upward Rebase of min RTT

### Defect

`isProbeRTTSampleDrained` accepts a ProbeRTT sample only when:

```text
raw RTT <= old min RTT + QueueTarget / 2
```

This protects against rebasing a standing queue, but it cannot learn a
permanent base-RTT increase on the same 5-tuple. For example, after a route
change from 30 ms to 150 ms, a drained raw RTT can never satisfy the old
threshold. ProbeRTT times out, preserves 30 ms, and later repeats.

### Required state

Add explicit ProbeRTT observation state to `adaptiveBDPSender`:

- minimum positive raw RTT observed during the current ProbeRTT;
- number of accepted raw observations;
- first and last observation rounds;
- whether local inflight was observed at or below the ProbeRTT cap;
- timeout/backoff information needed to prevent immediate retry loops.

Reset all of this state:

- on entry to ProbeRTT;
- on normal or fail-safe exit;
- after persistent congestion;
- when a new congestion controller is created after migration.

Expose new public debug fields only if existing state/reason fields cannot
diagnose the decision.

### Required algorithm

1. Keep the existing fast path:
   - a positive raw RTT at or below `oldMinRTT + QueueTarget/2` is a normal
     drained sample;
   - it may refresh min RTT after the minimum time and round conditions pass.
2. Record the minimum positive raw RTT seen throughout ProbeRTT, even when it
   is above the old drained threshold.
3. Do not use smoothed RTT as the upward-rebase candidate.
4. Permit an upward rebase only after all of these conditions are true:
   - ProbeRTT has lasted at least its minimum duration;
   - at least one new controller round completed;
   - raw RTT observations were collected in at least two distinct rounds;
   - local `priorInFlight` was observed at or below the ProbeRTT congestion
     window cap;
   - there is no fresh ECN event or material-loss round;
   - the candidate is the minimum raw RTT observed during the capped interval.
5. The upward rebase must set `minRTT` to the candidate, not to the latest or
   maximum sample.
6. If the evidence is insufficient at the maximum ProbeRTT duration:
   - preserve the old trusted min RTT;
   - exit without increasing cwnd abruptly;
   - apply a bounded retry backoff so the controller cannot enter ProbeRTT
     every ACK or every short interval;
   - record an explicit reason such as
     `probe_rtt_timeout_insufficient_drain_evidence`.
7. A successful upward rebase must record a distinct reason such as
   `probe_rtt_base_rtt_increased`.
8. Do not accept a high candidate merely because two seconds elapsed.

The implementation may pass `priorInFlight` into the ProbeRTT sampling helper
or record the evidence in the ACK path before calling the exit helper.

### Required deterministic tests

Add tests for:

1. 30 ms to 150 ms permanent base-RTT increase on the same controller:
   - own inflight drains to the ProbeRTT cap;
   - raw samples in at least two rounds stabilize around 150 ms;
   - min RTT becomes the minimum observed drained candidate;
   - state exits to ProbeBW.
2. A temporary 150 ms standing queue over a 30 ms path:
   - insufficient drain evidence does not rebase min RTT;
   - timeout preserves 30 ms;
   - retry backoff prevents an immediate loop.
3. A decreasing raw sequence such as 180, 160, 151, 150 ms:
   - the accepted candidate is 150 ms.
4. ECN or material loss during the capped interval:
   - upward rebase is rejected.
5. A normal 30 ms drained sample:
   - existing fast-path behavior remains unchanged.
6. App-limited / idle ACKs:
   - pacing remains non-zero;
   - ProbeRTT cannot deadlock.
7. Persistent-congestion reset clears every new ProbeRTT observation field.

### Acceptance criteria

- Validation-plan scenarios T03 and T05 can learn their new base RTT.
- Bufferbloat scenario Q02 cannot hide a standing queue by simple filter
  expiry.
- ProbeRTT cannot oscillate indefinitely between ProbeRTT and ProbeBW.

### Mandatory commands

```powershell
go test -count=1 ./internal/congestion -run ProbeRTT
go test -count=100 ./internal/congestion -run ProbeRTT
go test -count=1 ./internal/congestion ./internal/ackhandler .
go vet ./internal/congestion ./internal/ackhandler
```

---

## F12: Safe and Bounded MaxWindowPackets

### Defect

`MaxWindowPackets` accepts any `uint32`. The ackhandler computes:

```go
int(2 * maxWindow)
```

The multiplication occurs as `uint32` and wraps for values at or above
`2^31`. Large non-overflowing values can also permit unsafe per-connection
packet-history memory use.

### Required implementation

1. Define one documented AdaptiveBDP maximum window-packet limit.
2. Select the limit from an explicit memory budget:
   - measure or estimate packet-history memory per tracked packet;
   - include the `2 * cwnd` outstanding limit and `5 / 4` tracked multiplier;
   - document the expected worst-case per-connection memory.
3. A starting candidate is 1,000,000 packets, but do not adopt it without the
   memory calculation. A lower bound is acceptable if justified.
4. Reject `MaxWindowPackets` above the selected limit in
   `validateAdaptiveBDPCwndTuning`.
5. Perform all limit arithmetic as `uint64`.
6. Check conversion to `int` before converting.
7. Keep `MaxWindowPackets` exact for every accepted value.
8. Preserve the 10,000-packet default.
9. Update public Go documentation with:
   - exact hard-limit semantics;
   - default;
   - accepted maximum;
   - approximate memory/cwnd implications.
10. Do not silently clamp an oversized public value.

### Required tests

- Exact accepted values below and above 10,000 packets.
- The selected maximum accepted value.
- Selected maximum plus one is rejected.
- `math.MaxUint32`, `1<<31`, and nearby boundaries are rejected.
- `maxOutstandingSentPackets()` equals the expected checked value.
- `maxTrackedSentPackets()` is greater than the outstanding limit and does
  not overflow.
- Partial effective window settings still obey
  `min <= initial <= max`.

Add a small benchmark or allocation measurement for the dynamic packet-history
path if the repository test framework supports it.

### Acceptance criteria

No accepted public configuration can overflow packet-count arithmetic or
request an undocumented, unbounded packet-history allocation.

### Mandatory commands

```powershell
go test -count=1 . -run AdaptiveBDPCwndTuningValidation
go test -count=1 ./internal/ackhandler -run DynamicOutstandingPacketLimit
go test -count=1 ./internal/congestion -run MaxWindowPackets
go test -count=1 ./internal/congestion ./internal/ackhandler .
go vet ./...
```

---

## F13: Restore a Green Full Test Suite

### Known failures

- `TestGracefulShutdownLongLivedRequest` fails repeatedly on Windows because
  observed timing exceeds its tolerance.
- `TestMITCorruptPackets/towards_the_client` timed out once in a full run but
  passed five isolated repetitions.

Neither test currently points directly to AdaptiveBDP, but a red mandatory
suite blocks release.

### Investigation procedure

1. Confirm neither failing test selects AdaptiveBDP.
2. Run each test at least 50 times in isolation on the current platform.
3. Run each test under CPU load and with normal load.
4. Run the package with shuffled test order:

```powershell
go test -shuffle=on -count=20 ./integrationtests/self
```

5. Compare Windows and Linux CI results.
6. Determine whether the root cause is:
   - an actual shutdown/packet-corruption bug;
   - timer granularity;
   - scheduler contention;
   - test interference;
   - an invalid timing assertion.
7. Fix the product behavior when incorrect.
8. Change a test tolerance only when measurements prove the behavior is
   correct and the old assertion is platform-invalid.
9. Do not skip either test.

### Required evidence

The final report must contain:

- failure rate before the change;
- diagnosed root cause;
- failure rate after the change;
- Windows and Linux results;
- explanation of any modified timing bound.

### Acceptance criteria

These commands pass repeatedly:

```powershell
go test -count=20 ./integrationtests/self -run TestGracefulShutdownLongLivedRequest
go test -count=20 ./integrationtests/self -run TestMITCorruptPackets
go test -shuffle=on -count=10 ./integrationtests/self
go test -count=1 ./...
```

---

## F14: Deterministic VPN Network Validation

### Goal

Implement the missing harness and execute the scenario matrix in
`ADAPTIVE_BDP_VALIDATION_PLAN.md`. Unit tests cannot replace this package.

### Harness requirements

Extend `testutils/simnet` or add a narrowly scoped deterministic link layer
that supports:

- independent forward and reverse bandwidth;
- serialization delay or token-bucket shaping;
- finite queues and tail drop;
- deterministic ECN marking where supported;
- fixed and variable base latency;
- deterministic random loss with a fixed seed;
- scripted burst loss and outage;
- reordering and duplication;
- capacity and base-RTT changes at exact simulated times;
- per-direction counters and queue occupancy;
- simulated time without scheduler sleeps.

Do not add long wall-clock sleeps to CI.

### Execution phases

#### Phase A: correctness scenarios

Implement numeric assertions for:

- clean fixed-capacity paths;
- random wireless loss without queue;
- burst loss;
- shallow and deep queues;
- persistent congestion;
- idle download-to-upload restart;
- same-5-tuple capacity changes;
- same-5-tuple RTT changes;
- explicit migration reset.

#### Phase B: performance scenarios

Measure:

- application goodput;
- utilization;
- p50 / p95 / p99 queue delay;
- recovery and downshift time in RTTs;
- cwnd, pacing, and bandwidth oscillation;
- post-idle burst size;
- Jain fairness index.

#### Phase C: competing flows

Run:

- AdaptiveBDP versus AdaptiveBDP;
- AdaptiveBDP versus Cubic;
- AdaptiveBDP versus Reno;
- equal and unequal RTTs.

### Required traffic patterns

For relevant scenarios, run:

- continuous bulk upload;
- continuous bulk download;
- download, idle, then upload;
- small interactive bursts followed by bulk;
- bidirectional tunnel traffic;
- long-lived flow with capacity and RTT changes.

### Acceptance criteria

Use the numeric targets in `ADAPTIVE_BDP_VALIDATION_PLAN.md`. At minimum:

- no zero-rate or no-send deadlock;
- clean utilization meets the documented target;
- queue delay remains bounded;
- capacity decrease converges without persistent oversending;
- capacity increase recovers in the documented number of RTTs;
- random no-queue loss receives a smaller reaction than queued loss;
- equal-RTT AdaptiveBDP fairness reaches Jain index >= 0.90;
- T03 and T05 prove safe upward min-RTT rebasing;
- Q02 proves a standing queue is not silently rebased away.

Store machine-readable results and a short Markdown report.

---

## F15: Race, Soak, and Release Gates

### Required CI environment

Use Linux amd64 with CGO enabled for the Go race detector. Keep the existing
Windows test job.

### Required jobs

1. Core race:

```bash
CGO_ENABLED=1 go test -race ./internal/congestion ./internal/ackhandler .
```

2. Full static analysis:

```bash
go vet ./...
```

3. Full repository:

```bash
go test -count=1 ./...
```

4. AdaptiveBDP stress:

```bash
go test -count=100 ./internal/congestion -run AdaptiveBDP
```

5. Deterministic network scenarios from F14.
6. A long-running soak test with repeated capacity, RTT, loss, idle, and
   migration transitions.

### Soak invariants

Fail the soak test if any of these occur:

- zero pacing rate;
- cwnd outside configured bounds;
- packet-history limit inversion;
- NaN or infinite gain/rate calculation;
- ProbeRTT retry loop;
- persistent stale bandwidth after the filter window;
- more than one round-gated action per round;
- unbounded queue growth;
- goroutine or memory growth across repeated connections.

### Release acceptance criteria

Production is **GO** only when:

- F11–F14 acceptance criteria pass;
- the full suite is green on Windows and Linux;
- race tests are green;
- network results satisfy the documented numeric thresholds;
- no Critical or High audit issue remains;
- the worktree changes have been reviewed and committed;
- the final report includes commands, versions, scenario data, and known
  limitations.

### F15 implementation status (2026-07-28)

**PASS / GO.** The dedicated release workflow enforces Linux/CGO core race,
full static analysis, Windows and Linux full-repository tests, 100-repeat
AdaptiveBDP stress, the repeated F14 deterministic matrix, and a 25-cycle
capacity/RTT/loss/idle/migration soak. Runtime diagnostics make packet-history
limit inversion directly testable. Windows and local Linux/amd64 verification,
including the final shuffled integration suite, passed. Commands, versions,
scenario counts, F13 before/after evidence, and known limitations are recorded
in `ADAPTIVE_BDP_F15_RELEASE_REPORT.md`; no release blocker remains.

---

## Per-Package Session Handoff

At the end of every package, report:

1. package ID and result;
2. root cause;
3. files changed;
4. tests added;
5. exact commands and results;
6. remaining risks;
7. whether the next package is unblocked.

Do not mark a package complete if a mandatory command failed. A failure that
appears unrelated must be documented and assigned to F13, not silently
ignored.
