package congestion

import (
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

// telemetrySnapshot returns an independent, chronological view. Recording into
// the bounded ring is O(1); only an explicit debug read copies the history.
func (s *adaptiveBDPSender) telemetrySnapshot() []AdaptiveBDPTelemetrySample {
	if len(s.telemetry) == 0 {
		return nil
	}
	snapshot := make([]AdaptiveBDPTelemetrySample, len(s.telemetry))
	n := copy(snapshot, s.telemetry[s.telemetryHead:])
	copy(snapshot[n:], s.telemetry[:s.telemetryHead])
	return snapshot
}

func (s *adaptiveBDPSender) recordTelemetry(event string, now monotime.Time, priorInFlight protocol.ByteCount) {
	if !s.cfg.EnableAdaptiveBDPTelemetry {
		return
	}
	pacingCutRemaining := time.Duration(0)
	if !s.pacingCutUntil.IsZero() && now.Before(s.pacingCutUntil) {
		pacingCutRemaining = s.pacingCutUntil.Sub(now)
	}
	sample := AdaptiveBDPTelemetrySample{
		Event:                           event,
		Elapsed:                         now.Sub(s.startedAt),
		RoundCount:                      s.roundCount,
		State:                           s.state.String(),
		TransitionReason:                s.lastStateChangeReason,
		CongestionWindow:                s.congestionWindow,
		TargetCwnd:                      s.targetCwnd(),
		BytesInFlight:                   priorInFlight,
		BDP:                             s.bdp(),
		BandwidthBytesPerSecond:         s.bw,
		MaxBandwidthBytesPerSecond:      s.maxBw,
		ShortBandwidthBytesPerSecond:    s.shortBw,
		RecoveryBandwidthBytesPerSecond: s.lossRecoveryProbeBW,
		PacingRateBytesPerSecond:        s.pacingRateBytesPerSecond,
		PacingGain:                      s.pacingGain(),
		CwndGain:                        s.cwndGain(),
		LatestRTT:                       s.rttStats.LatestRTT(),
		SmoothedRTT:                     s.rttStats.SmoothedRTT(),
		MinRTT:                          s.minRTT,
		QueueDelay:                      s.queueDelay(),
		SharedQueueDelay:                s.sharedQueueDelay,
		ControlQueueDelay:               s.controlQueueDelay(),
		QueueTarget:                     s.queueTarget(),
		QueueState:                      s.queueState().String(),
		LossRatioRound:                  s.roundLossRatio(),
		LossRatioEWMA:                   s.lossRatioEWMA,
		LostBytesThisRound:              s.lostBytesThisRound,
		AckedBytesThisRound:             s.ackedBytesThisRound,
		HasRecentECNCE:                  s.hasRecentECNCE(),
		LastLossActionReason:            s.lastLossActionReason,
		LastLossCwndMultiplier:          s.lastLossCwndMultiplier,
		LastLossPacingMultiplier:        s.lastLossPacingMultiplier,
		PacingCutMultiplier:             s.pacingCutMultiplier,
		PacingCutRemaining:              pacingCutRemaining,
		UploadWarmupActive:              !s.uploadWarmupStartTime.IsZero() && s.inUploadWarmup(now),
		IdleRestartActive:               s.idleRestartActive,
		ProbeUpActive:                   s.probeUpActive,
		ProbeDownActive:                 s.state == adaptiveBDPProbeDown,
		ProbeRTTActive:                  s.state == adaptiveBDPProbeRTT,
		FullBwReached:                   s.fullBwReached,
	}
	if len(s.telemetry) == adaptiveBDPTelemetryLimit {
		s.telemetry[s.telemetryHead] = sample
		s.telemetryHead = (s.telemetryHead + 1) % adaptiveBDPTelemetryLimit
		return
	}
	if s.telemetry == nil {
		s.telemetry = make([]AdaptiveBDPTelemetrySample, 0, adaptiveBDPTelemetryLimit)
	}
	s.telemetry = append(s.telemetry, sample)
}
