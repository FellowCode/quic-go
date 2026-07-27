# AdaptiveBDP Repair Instructions for a Coding Agent

## Purpose

This document is the normative implementation plan for repairing AdaptiveBDP.
It is written for an autonomous coding model. Follow it literally unless the
repository code proves that a stated assumption is false.

Read these files completely before changing code:

1. `ADAPTIVE_BDP_ARCHITECTURE.md`
2. `internal/congestion/adaptive_bdp_sender.go`
3. `internal/congestion/adaptive_bdp_sender_test.go`
4. `internal/ackhandler/sent_packet_handler.go`
5. `internal/ackhandler/sent_packet_handler_test.go`
6. `ADAPTIVE_BDP_VALIDATION_PLAN.md`

The objective is not to redesign AdaptiveBDP. The objective is to make its
existing model correct, predictable, and safe for VPN traffic on wired,
wireless, mobile, high-RTT, lossy, and changing-capacity paths.

## Agent Contract

Apply the following rules to every work package:

- Make one logically independent fix per commit or reviewable patch.
- Write a failing regression test before, or in the same patch as, the fix.
- Prefer deterministic state-machine tests. Do not use real-time sleeps.
- Do not change default gains, thresholds, or public configuration in a
  correctness patch unless the work package explicitly requires it.
- Do not combine a correctness fix with broad renaming or formatting.
- Preserve Reno and Cubic behavior.
- Preserve explicit path-migration behavior: migration creates a fresh
  congestion controller.
- Use `monotime.Time` and the event time passed by the ACK path. Do not mix
  wall-clock time into controller decisions.
- Run `gofmt` on changed Go files.
- Run the test commands listed in the final section after every work package.
- If a requested implementation conflicts with a proven repository invariant,
  stop and document the conflict instead of silently choosing new behavior.

## Required Behavioral Invariants

All fixes must preserve these invariants:

1. An app-limited or under-filled flow cannot prove that path capacity fell.
2. One low delivery-rate sample cannot collapse `shortBw` without congestion
   evidence.
3. Random wireless loss without queue growth causes a smaller reaction than
   loss accompanied by queue growth or ECN.
4. Tiny absolute loss cannot trigger an emergency cutback.
5. Persistent congestion collapses cwnd to the minimum and invalidates the old
   bandwidth model.
6. A named `...Rounds` counter advances at most once per AdaptiveBDP round.
7. A temporary pacing reduction survives ordinary ACK processing until it
   expires or is replaced.
8. A recovery probe advances at most once per round.
9. An expired min-RTT sample is not replaced with a queue-inflated RTT without
   first obtaining a drained-path sample.
10. No configured floor may override explicit persistent congestion.

## Patch Order

Implement work packages in this order:

| Order | ID | Work package | Priority |
|---:|---|---|---|
| 1 | F01 | Fix upload-warmup send-path semantics | Critical |
| 2 | F02 | Make loss accounting round-based | Critical |
| 3 | F03 | Gate loss-recovery probes to one step per round | Critical |
| 4 | F04 | Fully reset the model after persistent congestion | Critical |
| 5 | F05 | Make temporary pacing reductions persistent | High |
| 6 | F06 | Make the bandwidth max filter actually age | High |
| 7 | F07 | Make queue, downshift, and ECN reactions round-gated | High |
| 8 | F08 | Repair min-RTT sampling and implement ProbeRTT | High |
| 9 | F09 | Repair app-limited Startup and idle restart | Medium |
| 10 | F10 | Clarify and validate public configuration semantics | Medium |

Do not start F08, F09, or F10 until F01-F07 pass all tests. Those later work
packages change policy; the earlier packages repair correctness.

---

## F01: Fix Upload-Warmup Send-Path Semantics

### Defect

`adaptiveBDPSender.maybeStartUploadWarmup` starts only when
`bytesInFlight == 0`. The real ACK handler adds the current packet to
`bytesInFlight` before calling `OnPacketSent`. Therefore the first packet of a
new upload phase is passed as approximately:

```text
bytesInFlight == current packet size
```

The direct controller tests incorrectly pass zero and do not reproduce the
real call contract.

### Required implementation

Keep the shared `SendAlgorithm.OnPacketSent` contract unchanged.

Change AdaptiveBDP so that it can identify an empty pipe before the current
packet was added. The preferred implementation is:

1. Pass the current packet size into the warmup helper.
2. Treat the send as an empty-pipe send when
   `bytesInFlight <= currentPacketBytes`.
3. Keep the idle-time test based on `lastRetransmittableSentTime`.
4. Do not start warmup for non-retransmittable/non-ack-eliciting sends.
5. Do not restart warmup for every packet in a burst.

Do not change the ACK handler to pass pre-send bytes-in-flight unless all
congestion controllers and tests are deliberately migrated to that new
contract.

### Required tests

Add or update tests in `internal/congestion/adaptive_bdp_sender_test.go`:

- `TestAdaptiveBDPUploadWarmupStartsWithPostSendBytesInFlight`
  - call `OnPacketSent` with `bytesInFlight == packetBytes`;
  - require warmup to be active.
- A second packet with `bytesInFlight > packetBytes` must not restart warmup.
- A first send after the configured idle interval must restart warmup.
- A non-retransmittable send must not start warmup.
- Preserve the existing tests that block a false upload downshift.

Add at least one integration-level assertion through `sentPacketHandler` if it
can be expressed without exporting new production API. If not, document why
the post-send unit test is the regression boundary.

### Acceptance criteria

- The first real ack-eliciting packet after idle starts warmup.
- Warmup starts once, not once per packet.
- Existing pacing and bytes-in-flight accounting are unchanged.

---

## F02: Make Loss Accounting Round-Based

### Defect

The ACK handler calls `OnCongestionEvent` once per lost packet. The current
implementation updates loss EWMA and `mildLossRounds` on each call. This means
that multiple packet losses in one RTT can be interpreted as multiple
persistent-loss rounds.

The current EWMA also ignores zero-loss rounds, so an old high loss value never
decays during clean traffic.

### Required design

Separate loss collection from loss-round finalization.

1. `OnCongestionEvent` must accumulate lost bytes for the current round.
2. Normal proportional loss policy must be evaluated at most once per round.
3. `lossRatioEWMA` must be updated exactly once for each completed round that
   has a sufficiently large observation sample.
4. The EWMA input for a sufficiently sampled clean round is `0`.
5. `mildLossRounds` must advance at most once per completed round.
6. A sufficiently sampled clean round resets `mildLossRounds`.
7. Emergency loss may react before round completion, but at most once per
   round and only when the current round itself satisfies both:
   - the emergency ratio threshold;
   - the emergency absolute-byte threshold.
8. A stale EWMA must never turn a non-emergency current round into an emergency
   cutback.
9. Finalize the previous round before clearing its `ackedBytesThisRound` and
   `lostBytesThisRound`.
10. Do not update the EWMA both in `OnCongestionEvent` and at the round
    boundary.

Create a helper with a single responsibility, for example:

```text
finalizeLossRound(eventTime, priorInFlight)
```

The exact name is optional. The single-finalization behavior is mandatory.

### Required tests

Add deterministic tests for:

- Ten `OnCongestionEvent` calls in one round increase
  `mildLossRounds` by no more than one.
- The same loss ratio in two distinct completed rounds increases it to two.
- A sufficiently sampled zero-loss round resets `mildLossRounds`.
- With alpha `0.25`, an EWMA of `0.05` becomes `0.0375` after one sufficiently
  sampled zero-loss round.
- An undersized clean sample does not falsely declare a loss-free round.
- An old emergency-level EWMA plus a current below-emergency loss ratio does
  not trigger emergency handling.
- Emergency cutback remains limited to once per round.
- Existing absolute-byte and minimum-sample protections remain valid.

### Acceptance criteria

- Every value named in rounds has round semantics.
- Loss-free traffic demonstrably decays loss memory.
- Burst loss is one bad round, not many bad rounds.

---

## F03: Gate Loss-Recovery Probes to One Step per Round

### Defect

`maybeStartLossRecoveryProbe` runs on every ACK. It records
`lastLossRecoveryProbeRound`, but it does not use that field as a guard. A
probe can therefore grow by its configured gain repeatedly inside one round,
extend its own expiration repeatedly, and jump rapidly to a stale `maxBw`.

### Required implementation

1. Return without changing recovery state if a recovery step was already
   started in `roundCount`.
2. Do not extend `lossRecoveryProbeUntilRound` on later ACKs in the same round.
3. Permit the next step only in a later round and only if all existing queue,
   loss, ECN, and app-limited guards still pass.
4. A new material-loss round immediately cancels the active recovery probe.
5. Reaching the recovery goal stops further steps.
6. Keep the configured `LossRecoveryProbeGain` as a per-round gain.

### Required tests

- Multiple ACKs in one round produce exactly one gain step.
- `lossRecoveryProbeUntilRound` is unchanged by later ACKs in that round.
- The next loss-free round can produce one additional step.
- Queue pressure, ECN, material loss, app limitation, and goal completion each
  block a new step.
- A probe cannot clear `shortBw` solely because it was repeatedly called in
  one round.

### Acceptance criteria

Recovery is gradual in RTT units and cannot race to `maxBw` in milliseconds.

---

## F04: Fully Reset the Model After Persistent Congestion

### Defect

`OnPersistentCongestion` clears `bw` and `shortBw`, but leaves the lifetime
high-water mark, filter, full-bandwidth state, loss memory, and probe state.
The next ACK can restore the stale bandwidth and immediately exit Startup.

### Required implementation

Create a dedicated model-reset helper. On persistent congestion:

Reset:

- `bw`, `maxBw`, `shortBw`;
- all `bwFilter` samples;
- `fullBw`, `fullBwCount`, `fullBwReached`;
- `nextRoundDelivered` and round-local accounting;
- `queueHighRounds`, `downshiftRounds`, `noQueueLow`;
- ProbeUp and suppression state;
- loss EWMA, mild-loss, loss-free, material-loss, recovery-probe, ECN, and
  loss-cutback tracking;
- cached bandwidth-competition sample/growth state.

Preserve:

- configuration;
- min/max/initial cwnd limits;
- pacer object;
- the current path's min RTT, unless a separate path migration reset occurs;
- debug reasons needed to report `persistent_congestion`.

Then:

1. Set cwnd to `minCongestionWindow`.
2. Enter Startup with reason `persistent_congestion`.
3. Bootstrap only from the minimum cwnd and current RTT.
4. Do not allow the old `maxBw` to reappear.

### Required tests

- Seed every resettable state field, call `OnPersistentCongestion`, and verify
  it is cleared.
- Verify min RTT and configuration are preserved.
- Process the first valid ACK after reset and verify:
  - old `maxBw` is not restored;
  - `fullBwReached` remains false;
  - the controller does not immediately enter Drain because of old state.
- Verify the no-congestion startup floor does not raise cwnd above the
  persistent-congestion minimum before new evidence exists.

### Acceptance criteria

Persistent congestion starts a genuinely new capacity model on the same path.

---

## F05: Make Temporary Pacing Reductions Persistent

### Defect

`applyTemporaryPacingMultiplier` ignores its event time and duration. The next
ordinary ACK recomputes pacing and erases the reduction.

### Required implementation

Add explicit temporary pacing state, for example:

```text
pacingCutMultiplier
pacingCutUntil
```

Required behavior:

1. Compute the normal rate, configured floor, and max-probe limit first.
2. If a pacing cut is active, multiply that resulting rate by the active cut.
3. Apply `minimumPacingRate` last.
4. A stronger cut replaces a weaker active cut.
5. A later expiration may extend the active cut, but repeated packet-loss
   callbacks in one round must not compound it.
6. Clear expired state deterministically using the supplied event/clock time.
7. Persistent congestion and path reset clear temporary pacing state.

Do not encode the pacing cut by permanently reducing `maxBw`.

### Required tests

- A no-queue loss cut survives the ACK path's next `updatePacingRate`.
- It remains active immediately before expiration.
- It disappears at or after expiration.
- Repeated same-round loss events do not multiply the multiplier.
- Minimum pacing rate still prevents divide-by-zero and deadlock.

### Acceptance criteria

The duration parameter has observable behavior and ordinary ACK processing
does not cancel it early.

---

## F06: Make the Bandwidth Max Filter Actually Age

### Defect

Lower non-app-limited samples are not inserted into `bwFilter`, and `maxBw`
uses a monotonic `max` assignment. The configured filter window therefore
cannot expire an old high sample.

### Required implementation

1. Insert every valid non-app-limited delivery-rate sample into the round
   filter.
2. Recompute `maxBw` from the current filter window.
3. Allow `maxBw` to fall when all old high samples have aged out.
4. An app-limited sample may raise `maxBw` if it is higher, but an app-limited
   low sample must not reduce it or advance false downshift evidence.
5. Keep `shortBw` as the fast negative adaptation layer.
6. Audit recovery-goal code so an expired lifetime high does not remain a
   permanent target.
7. Bound filter storage when multiple ACK samples occur in one round.

### Required tests

- A high sample remains the max while it is inside the configured window.
- After enough lower non-app-limited rounds, the old sample expires and
  `maxBw` falls.
- Low app-limited samples do not lower `maxBw`.
- A higher app-limited sample may raise it.
- A same-address capacity transition, such as 100 to 10 Mbit/s, eventually
  removes 100 Mbit/s as the recovery goal.

### Acceptance criteria

`BandwidthFilterRounds` controls a real sliding round window, not a lifetime
maximum.

---

## F07: Round-Gate Queue, Downshift, and ECN Reactions

### Defect

`queueHighRounds` and congestion `downshiftRounds` can advance once per ACK,
not once per round. Repeated ECN callbacks can also compound a bandwidth
reduction inside one round.

### Required implementation

Track the last round counted for each persistent signal.

Required behavior:

- `queueHighRounds` increases at most once in a round.
- Congestion-confirmation `downshiftRounds` increases at most once in a round.
- Repeated samples in the same round may update the worst observed value, but
  not the round count.
- Negative evidence resets a streak only according to an explicit rule. Do not
  let ACK ordering create multiple reset/increment cycles in one round.
- ECN may enter ProbeDown promptly, but its bandwidth/cwnd reduction is applied
  at most once per round.
- Do not change the already round-gated `noQueueLow.rounds` behavior.

As a first safe patch, retain binary recent-ECN evidence. A later isolated
patch may add CE-fraction/alpha behavior if the ACK handler exposes the needed
validated counters.

### Required tests

- Many high-queue ACKs in one round count as one queue-high round.
- A later high-queue round advances the streak to two.
- The same assertions apply to congestion downshift confirmation.
- Repeated ECN events in one round do not produce `probeDownGain^N`.
- A later ECN-marked round may produce one additional response.

### Acceptance criteria

Configuration fields ending in `Rounds` are independent of ACK frequency.

---

## F08: Repair Min-RTT Sampling and Implement ProbeRTT

### Defect

The rate sample currently passes `SmoothedRTT` to AdaptiveBDP. AdaptiveBDP
therefore tracks the minimum of a smoothed value instead of a raw recent RTT.
When the min-RTT window expires, the current value may be accepted even while
a standing queue exists. The `ProbeRTT` state is declared but unused.

### Required implementation

Implement this work in two reviewable patches.

### F08a: Correct the RTT sample

1. Populate `RateSample.RTT` from `RTTStats.LatestRTT()` when available.
2. Fall back to smoothed/min RTT only when no latest sample exists.
3. Continue using smoothed RTT for the queue-delay current value if desired,
   but use raw samples for the min filter.
4. Add tests proving that min RTT follows the minimum raw sample rather than
   the minimum SRTT.

### F08b: Use the existing ProbeRTT state

When the min-RTT sample is stale:

1. Do not replace it with a queue-inflated RTT while queue state is building
   or persistent.
2. Schedule ProbeRTT.
3. During ProbeRTT, cap inflight to a small safe window, no smaller than
   `minCongestionWindow`.
4. Stay in ProbeRTT for at least one completed round and a bounded duration.
   Use a default duration of 200 ms unless tests justify another value.
5. Record fresh raw RTT samples during the drain.
6. Exit to ProbeBW after the duration and round requirements are satisfied.
7. Restore cwnd gradually toward the current BDP target; do not restore an
   obsolete absolute cwnd snapshot.
8. Persistent congestion and path migration take precedence over ProbeRTT.
9. Add debug fields/reasons for ProbeRTT scheduling, entry, and exit if needed
   to make tests and production diagnosis possible.

### Required tests

- An expired min RTT plus a standing queue enters ProbeRTT instead of silently
  rebasing min RTT.
- ProbeRTT cannot exit before both its time and round conditions pass.
- A drained raw RTT refreshes min RTT.
- ProbeRTT exits to ProbeBW and does not remain stuck.
- App-limited/idle traffic cannot cause a zero-rate deadlock.

### Acceptance criteria

A standing queue cannot hide itself merely by surviving the filter window.

---

## F09: Repair App-Limited Startup and Idle Restart

### Defect

Startup full-bandwidth detection advances even for app-limited samples. Light
interactive VPN traffic can therefore exit Startup before a bulk transfer
begins. After idle, the existing warmup protects against downshift but does not
provide a controlled fast capacity rediscovery.

### Required implementation

1. Do not advance `fullBwCount` from app-limited rounds.
2. A higher app-limited sample may still raise the bandwidth estimate.
3. Detect idle restart using the same idle boundary used by upload warmup.
4. On idle restart:
   - preserve the last safe bandwidth estimate;
   - cancel stale ProbeUp state;
   - do not release a burst larger than the pacer's existing burst limit;
   - allow controlled per-round probing once non-app-limited traffic resumes.
5. Stop the restart probe when bandwidth no longer grows, queue builds, loss
   becomes material, or ECN is observed.
6. Do not re-enter unrestricted Startup with a stale high target on a known
   degraded path.

### Required tests

- Three app-limited rounds do not set `fullBwReached`.
- Three sufficiently sampled non-app-limited plateau rounds still do.
- Idle followed by bulk traffic starts controlled rediscovery.
- Idle restart does not exceed one probe gain per round.
- Queue/loss/ECN stops idle-restart probing.

### Acceptance criteria

Interactive traffic does not poison later bulk-transfer startup, and idle
restart is fast without a large burst.

---

## F10: Clarify and Validate Public Configuration Semantics

Implement this only after the controller behavior is stable.

### Required changes

1. Decide and document whether `MaxWindowPackets` is:
   - an exact configured maximum; or
   - only permission to raise the library maximum.

   The current field name implies an exact maximum. Prefer exact semantics for
   AdaptiveBDP. Add tests for values below and above 10,000 packets.

2. Validate:
   - `MinWindowPackets <= InitialWindowPackets <= MaxWindowPackets` when all
     are explicitly configured;
   - all ratios are in `[0, 1]`;
   - `lossGrace <= lossSoft <= lossSevere <= emergency`;
   - ProbeDown gain is not greater than 1;
   - ProbeUp and Startup gains are not less than 1;
   - durations are non-negative.

3. Allow an explicit zero no-congestion floor when a startup target exists.
   Because zero currently means "use default 0.5", add an explicit boolean
   such as `DisableNoCongestionRateFloor` or another unambiguous API. Preserve
   the old default for callers that do not set the new field.

4. Add Go documentation to every AdaptiveBDP-specific public field:
   - units;
   - zero/default behavior;
   - valid range;
   - whether it is a hard limit, soft target, or probe hint.

5. Document the default cwnd ceiling. At 1,280-byte MSS, 10,000 packets are
   about 12.8 MB; that can limit very high-bandwidth, high-RTT paths.

### Acceptance criteria

No valid-looking public setting is silently interpreted as a materially
different policy.

---

## Cross-Cutting Observability Requirements

Keep `AdaptiveBDPDebugInfo` useful throughout the repair:

- Do not remove existing fields.
- Add state only when it is required to diagnose a new time- or round-gated
  decision.
- Prefer explicit reasons such as:
  - `upload_warmup_started_after_idle`;
  - `loss_round_finalized`;
  - `loss_recovery_probe_already_started_this_round`;
  - `persistent_congestion_model_reset`;
  - `temporary_pacing_cut_active`;
  - `max_bw_aged_out`;
  - `probe_rtt_min_rtt_expired`.
- Do not overwrite the last meaningful reason with a generic reason during
  the same ACK event.

If public debug API fields are added, update all conversion and propagation
tests in:

- `interface.go`
- `internal/congestion/interface.go`
- `connection.go`
- `connection_test.go`

## Mandatory Test Commands

Run after each work package:

```powershell
go test ./internal/congestion
go test ./internal/ackhandler
go test .
go vet ./internal/congestion ./internal/ackhandler
```

Run before final handoff:

```powershell
go test ./...
```

If the environment supports the race detector, also run:

```powershell
go test -race ./internal/congestion ./internal/ackhandler
```

Report:

- changed files;
- tests added;
- tests run and their results;
- remaining work-package IDs;
- any measured behavior change from `ADAPTIVE_BDP_VALIDATION_PLAN.md`.

Do not claim that AdaptiveBDP is production-ready for arbitrary VPN networks
until the critical and high-priority work packages pass the validation plan.
