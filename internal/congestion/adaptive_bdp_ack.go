package congestion

import (
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

func (s *adaptiveBDPSender) OnPacketAcked(
	number protocol.PacketNumber,
	ackedBytes protocol.ByteCount,
	priorInFlight protocol.ByteCount,
	eventTime monotime.Time,
) {
	rtt := s.rttStats.LatestRTT()
	if rtt <= 0 {
		rtt = s.rttStats.SmoothedRTT()
	}
	if rtt <= 0 {
		rtt = s.rttStats.MinRTT()
	}
	s.OnPacketAckedWithRateSample(number, ackedBytes, priorInFlight, eventTime, RateSample{
		AckedBytes:    ackedBytes,
		PriorInFlight: priorInFlight,
		RTT:           rtt,
	})
}

func (s *adaptiveBDPSender) OnPacketAckedWithRateSample(
	_ protocol.PacketNumber,
	ackedBytes protocol.ByteCount,
	priorInFlight protocol.ByteCount,
	eventTime monotime.Time,
	sample RateSample,
) {
	s.observeACK(ackedBytes, priorInFlight, eventTime, sample)
	if s.state == adaptiveBDPProbeRTT {
		s.maybeExitProbeRTT(eventTime)
		s.updatePacingRate()
		if s.state != adaptiveBDPProbeRTT && !sample.AppLimited {
			s.setCwndFromTarget(ackedBytes, priorInFlight)
		}
		s.updateDebugSnapshot(priorInFlight)
		return
	}
	s.maybeRunIdleRestartProbe(sample, eventTime)
	s.updateSharedQueue(sample, priorInFlight, eventTime)
	s.maybeStartLossRecoveryProbe(eventTime, sample, priorInFlight)

	queueGrowthDownshift := s.applyACKStateTransitions(sample, priorInFlight, eventTime)

	s.updatePacingRate()
	if !sample.AppLimited {
		s.setCwndFromTarget(ackedBytes, priorInFlight)
	}
	s.updateDebugSnapshot(priorInFlight)
	if queueGrowthDownshift {
		// A further capacity reduction while already in ProbeDown has no
		// state transition. Record its result after updating pacing and cwnd.
		s.recordTelemetry("bandwidth_downshift", eventTime, priorInFlight)
	}
}

// Commit observations in their causal order: RTT, completed-round loss reaction,
// then bandwidth. Round completion can change state before bandwidth filtering
// (in particular, ProbeRTT samples must remain capacity-limited).
func (s *adaptiveBDPSender) observeACK(ackedBytes, priorInFlight protocol.ByteCount, eventTime monotime.Time, sample RateSample) {
	s.lastRateSample = sample
	s.lastPriorInFlight = priorInFlight
	s.ackedBytesThisRound += ackedBytes
	if !s.uploadWarmupStartTime.IsZero() {
		s.uploadWarmupAcked += ackedBytes
	}
	s.updateMinRTT(sample.RTT, priorInFlight, eventTime)
	s.updateRound(sample, priorInFlight, eventTime)

	s.updateBandwidthAt(sample, priorInFlight, eventTime)
}

func (s *adaptiveBDPSender) applyACKStateTransitions(sample RateSample, priorInFlight protocol.ByteCount, eventTime monotime.Time) bool {
	enterProbeDown := s.shouldEnterProbeDown(sample, priorInFlight, eventTime)
	queueGrowthDownshift := enterProbeDown && s.lastStateChangeReason == "queue_growth_capacity_downshift"
	if enterProbeDown {
		reason := s.lastStateChangeReason
		if reason == "" {
			reason = "probe_down_decision"
		}
		s.enterStateWithReason(adaptiveBDPProbeDown, eventTime, reason)
	}
	if s.state == adaptiveBDPStartup && s.fullBwReached {
		s.enterStateWithReason(adaptiveBDPDrain, eventTime, "startup_full_bw_reached")
	}
	if s.state == adaptiveBDPDrain {
		if priorInFlight <= s.bdp() || s.queueDelay() <= s.queueTarget()/2 {
			s.enterStateWithReason(adaptiveBDPProbeBW, eventTime, "drain_complete")
		}
	}
	if s.state == adaptiveBDPProbeBW {
		queueState := s.queueState()
		if queueState == adaptiveQueueBuilding {
			s.probeUpActive = false
		}
		if queueState == adaptiveQueueEmpty && !s.hasCongestionEvidence() && s.canProbeUp() &&
			s.probeInterval() > 0 && (s.lastProbeTime.IsZero() || eventTime.Sub(s.lastProbeTime) >= s.probeInterval()) && s.roundStart {
			s.probeUpActive = true
			s.probeUpRoundStart = s.roundCount
			s.lastProbeTime = eventTime
		}
		if s.probeUpActive && s.roundStart && s.roundCount > s.probeUpRoundStart {
			s.probeUpActive = false
		}
	}
	if s.state == adaptiveBDPProbeDown {
		minDrain := s.minRTT
		if minDrain <= 0 {
			minDrain = 50 * time.Millisecond
		}
		drained := priorInFlight <= s.bdp() && s.controlQueueDelay() <= s.queueTarget()
		spentMinDrain := eventTime.Sub(s.lastStateChange) >= minDrain
		if spentMinDrain && drained {
			s.enterStateWithReason(adaptiveBDPProbeBW, eventTime, "probe_down_drained")
		}
	}

	return queueGrowthDownshift
}

// RTT observation requests a probe; only the ACK application phase enters it.
func (s *adaptiveBDPSender) updateMinRTT(rtt time.Duration, priorInFlight protocol.ByteCount, now monotime.Time) {
	if s.observeMinRTT(rtt, priorInFlight, now) {
		s.enterProbeRTT(now)
	}
}

// Loss reaction and telemetry consume a completed round before its byte counters
// are cleared. Measurement alone cannot reduce cwnd or change controller state.
func (s *adaptiveBDPSender) updateRound(sample RateSample, priorInFlight protocol.ByteCount, now monotime.Time) {
	if !s.observeRound(sample, priorInFlight, now) {
		return
	}
	s.finalizeLossRound(now, priorInFlight)
	s.recordTelemetry("round", now, priorInFlight)
	if sample.DeliveredBytes > 0 {
		s.nextRoundDelivered = sample.DeliveredBytes
	}
	s.ackedBytesThisRound = 0
	s.lostBytesThisRound = 0
}
