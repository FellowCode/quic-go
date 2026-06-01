# AdaptiveBDP Architecture

This document describes the current AdaptiveBDP congestion controller architecture in this fork of `quic-go`.
It is intended as a maintenance guide for the implementation in `internal/congestion/adaptive_bdp_sender.go` and the public tuning/debug API in `interface.go`.

## Goals

AdaptiveBDP is a BDP-oriented congestion controller for high-throughput paths where the sender often has an expected or recently observed capacity. It tries to:

- grow toward the measured or configured path capacity;
- keep pacing and cwnd above a soft no-congestion floor in known-capacity/test modes;
- avoid one-sample bandwidth collapse when there is no congestion evidence;
- react proportionally to packet loss instead of treating all loss as catastrophic;
- separate queue/ECN/loss-confirmed congestion from no-queue bandwidth adaptation;
- expose enough debug state to explain every adaptive decision.

## Main Components

The controller state lives in `adaptiveBDPSender`.

Important state groups:

- Window state: `congestionWindow`, `minCongestionWindow`, `maxCongestionWindow`, `initialWindow`.
- Bandwidth state: `bw`, `maxBw`, `shortBw`, `bwFilter`.
- RTT and queue state: `minRTT`, `minRTTTimestamp`, `queueHighRounds`.
- Round accounting: `roundCount`, `roundStart`, `lastRoundStartTime`, `ackedBytesThisRound`, `lostBytesThisRound`.
- Probe state: `probeUpActive`, `probeUpRoundStart`, `suppressProbeUpUntilRound`, `suppressProbeUpReason`.
- Downshift state: `downshiftRounds`, `noQueueLow`.
- Upload warmup state: `lastRetransmittableSentTime`, `uploadWarmupStartTime`, `uploadWarmupAcked`.
- Loss reaction state: `lossRatioEWMA`, `mildLossRounds`, `lastLossCutbackTime`, `lastLossActionReason`, loss multipliers.
- Debug reasons: `lastStateChangeReason`, `lastCwndChangeReason`, `lastBWChangeReason`.

## Configuration Surface

Public configuration is exposed via `quic.CwndTuning` and converted to `congestion.CwndTuningConfig` in `toCongestionCwndTuningConfig`.

The most important AdaptiveBDP knobs are:

- `StartupTargetRateBps`: known or desired startup/test capacity.
- `MinProbeRateBps`: explicit no-congestion pacing/cwnd floor.
- `NoCongestionRateFloorFraction`: fallback floor fraction of `StartupTargetRateBps`; default behavior is `0.5` when a startup target exists.
- `NoCongestionDownshiftRounds`: low-sample confirmation rounds without congestion evidence.
- `NoCongestionDownshiftFactor`: maximum no-congestion bandwidth reduction per confirmed step.
- `UploadWarmupDuration` and `UploadWarmupBytes`: grace period after a new outbound data phase starts.
- `MinDownshiftSampleBytes`: optional minimum ACKed bytes required before a low-rate sample can participate in downshift.
- `CongestionDownshiftRounds`: optional confirmation rounds when congestion evidence exists.
- Loss knobs: `LossGraceRatio`, `LossSevereThreshold`, `EmergencyLossThreshold`, `LossMinBytes`, `EmergencyLossMinBytes`, `MinLossSampleBytes`, proportional cut limits, EWMA alpha, cooldown.

Public debug state is exposed through `Conn.AdaptiveBDPDebugInfo()`.

## ACK Path

ACK processing enters through `OnPacketAckedWithRateSample`.

The high-level order is:

1. Store the latest rate sample and prior in-flight value.
2. Accumulate ACKed bytes for the current round and upload warmup.
3. Update min RTT and round state.
4. Update bandwidth state from the rate sample.
5. Evaluate ProbeDown conditions.
6. Handle Startup/Drain/ProbeBW transitions.
7. Update pacing rate and cwnd from the current target.
8. Refresh the debug snapshot.

Rate samples without a valid delivery rate are only used for bootstrap and debug refresh. Valid, non-app-limited samples can grow `maxBw`. Low samples are filtered through downshift eligibility before they are allowed to reduce `shortBw`.

## Congestion Evidence

`hasCongestionEvidence()` is the gate between hard congestion response and conservative adaptation.

Current congestion evidence sources:

- persistent queue pressure;
- recent ECN CE;
- loss above the configured/derived loss target and absolute byte threshold.

Without congestion evidence, the controller should not perform hard bandwidth or cwnd collapse from a single low delivery-rate sample.

## Bandwidth Model

AdaptiveBDP tracks three bandwidth values:

- `maxBw`: recent best non-app-limited bandwidth estimate.
- `shortBw`: temporary lower cap used after confirmed downshift.
- `bw`: active bandwidth used for pacing and BDP calculations, normally `min(maxBw, shortBw)` when `shortBw` is set.

`maxBw` represents the path's recent high-water mark. `shortBw` represents a confirmed lower operating estimate. No-queue logic is intentionally conservative about setting `shortBw`.

## No-Congestion Floor

`noCongestionRateFloorBytesPerSecond()` returns a soft floor only when there is no congestion evidence.

Floor source priority:

1. `MinProbeRateBps / 8`, if configured.
2. `StartupTargetRateBps / 8 * NoCongestionRateFloorFraction`, if startup target is configured.
3. Disabled floor if neither is configured.

This floor is applied to:

- pacing rate;
- target cwnd BDP calculation;
- no-queue cwnd cutback floor;
- gradual no-queue short bandwidth downshift.

The floor prevents the log failure mode where pacing and target cwnd collapse to a tiny one-sample delivery rate while queue and fresh loss are absent.

## Pipe-Filled Checks

Low-rate samples are only useful for downshift when the pipe was sufficiently filled against the previous/expected pipe estimate.

`pipeForDownshift()` uses the strongest relevant estimate:

- `maxBw` or `bw`;
- `StartupTargetRateBps`, when configured;
- no-congestion rate floor.

`pipeFillThreshold()` requires a stronger fill ratio without congestion evidence than with congestion evidence. This prevents a partially filled upload ramp from being misclassified as a proven bottleneck.

## Upload Warmup

Upload warmup protects the download-to-upload transition.

`maybeStartUploadWarmup()` starts a warmup when a retransmittable packet is sent with zero bytes in flight, or after enough idle time since the last retransmittable send.

`inUploadWarmup()` remains true until both conditions are satisfied:

- elapsed time is at least `UploadWarmupDuration`, or default `max(3*minRTT, 1s)`;
- ACKed bytes since warmup start are at least `UploadWarmupBytes`, or default `max(2*BDP, 256KB)`.

During warmup, low delivery-rate samples are not allowed to trigger hard downshift. This protects the first upload seconds while cwnd and in-flight data are still ramping.

## Downshift Logic

Downshift is split into two paths.

### Congestion-confirmed downshift

When congestion evidence exists, low delivery-rate samples can reduce `shortBw` after `CongestionDownshiftRounds` confirmation, defaulting to the current immediate behavior. The new bandwidth is interpolated using `negativeBandwidthConfidence()`, which combines queue, loss, and ECN pressure.

This path may enter `ProbeDown`.

Main reasons:

- `congestion_downshift_waiting_rounds`
- `short_bw_downshift_with_congestion_evidence`

### No-congestion downshift

When queue, ECN, and fresh loss do not indicate congestion, low samples are treated as candidates. They must persist for multiple rounds, enough time, and enough ACKed bytes.

After confirmation, `applyGradualNoQueueDownshift()` reduces from the current active estimate gradually:

- not below `NoCongestionDownshiftFactor * activeBW`;
- not below the no-congestion rate floor;
- not directly to one tiny sample unless repeated confirmed steps justify it.

This path stays in ProbeBW/Cruise rather than entering ProbeDown.

Main reasons:

- `low_sample_no_queue_candidate_started`
- `low_sample_no_queue_candidate_waiting_rounds`
- `low_sample_no_queue_candidate_waiting_duration`
- `low_sample_no_queue_candidate_waiting_bytes`
- `short_bw_gradual_no_queue_downshift`

## Loss Reaction

Loss handling is centralized in `handleLossReaction()`.

The flow is:

1. Update loss EWMA.
2. Use the max of round loss ratio and EWMA.
3. Check absolute byte and sample-size eligibility.
4. Apply emergency reaction only when ratio and absolute bytes are both sufficient.
5. For mild no-queue loss, suppress ProbeUp rather than collapsing cwnd.
6. Apply cooldown to avoid repeated cutbacks in the same round or RTT window.
7. Apply proportional cwnd and pacing multipliers.
8. Enter ProbeDown only for loss with queue/ECN evidence.

Tiny loss below `LossMinBytes` is ignored for cutback and recorded as `loss_below_absolute_threshold`.

Emergency loss uses a proportional beta, normally `0.70`, and only strengthens to `0.50` for very high loss with strong queue pressure.

## ProbeUp Suppression

Mild loss and no-queue proportional loss suppress ProbeUp for a round instead of forcing immediate throughput collapse.

`suppressProbeUpForOneRound()` records:

- `suppressProbeUpUntilRound`;
- `suppressProbeUpReason`.

`pacingGain()` returns cruise gain while `canProbeUp()` is false.

## Cwnd Model

`targetCwnd()` is based on effective BDP:

- active `bw`;
- no-congestion floor, when available;
- `cwndGain`;
- `windowGain`.

`reduceCwndTowardTarget()` has separate modes:

- loss-based: allowed to cut to target;
- congestion evidence: hard target cutback with reason `congestion_target_cutback`;
- no congestion: capped gradual cutback with reason `gradual_no_congestion_target_cutback_capped`.

No-congestion cutback cannot reduce by more than 25% in one step and cannot go below the no-congestion floor target. It also never raises cwnd; it only reduces or leaves the window unchanged.

## Pacing

Pacing rate is calculated from:

- active bandwidth;
- state-dependent pacing gain;
- pacing margin;
- optional max probe rate;
- no-congestion floor;
- minimum pacing rate.

`pacingGain()` depends on state:

- Startup: startup pacing gain.
- Drain: drain gain.
- ProbeDown: probe-down gain.
- ProbeBW: probe-up gain only when ProbeUp is active and not suppressed; otherwise cruise gain.

## State Machine

Main states:

- `Startup`: grows toward startup target or full bandwidth.
- `Drain`: drains queue after startup.
- `ProbeBW`: normal cruise/probe state.
- `ProbeDown`: drain/recovery after congestion evidence.
- `ProbeRTT`: reserved state.

No-congestion gradual downshift should stay in `ProbeBW`. ProbeDown is reserved for queue, ECN, loss, or confirmed congestion downshift.

## Debug API

`AdaptiveBDPDebugInfo` exposes:

- state, cwnd, BDP, bandwidth, pacing;
- latest rate sample;
- RTT and queue state;
- congestion evidence and pipe thresholds;
- no-congestion floor and no-queue low-sample state;
- loss ratios, thresholds, multipliers, cooldown/cutback state;
- ProbeUp suppression;
- last state/cwnd/bandwidth reasons.

Use this API to explain observed behavior before changing heuristics.

## Important Invariants

- Tiny absolute loss must not trigger emergency cutback.
- A single low delivery-rate sample must not collapse `shortBw` without congestion evidence.
- No-queue downshift is gradual and requires persistence.
- Startup target can act as a no-congestion floor in known-capacity/test mode.
- `245KB` in flight is not pipe-filled for `30Mbps` and `150ms` RTT.
- Upload warmup blocks immediate hard downshift after an outbound data phase starts.
- No-congestion cwnd cutback is capped and floor-aware.
- ProbeDown is for congestion evidence, not for no-queue gradual adaptation.

## Regression Coverage

The AdaptiveBDP tests in `internal/congestion/adaptive_bdp_sender_test.go` cover:

- false no-queue/no-loss short bandwidth collapse;
- pipe-filled threshold using startup target or previous estimate;
- gradual no-congestion downshift;
- congestion-confirmed hard downshift;
- tiny absolute loss not causing emergency;
- download-to-upload warmup;
- eventual gradual adaptation to a real no-queue bandwidth drop;
- proportional loss reaction;
- public debug info propagation.

