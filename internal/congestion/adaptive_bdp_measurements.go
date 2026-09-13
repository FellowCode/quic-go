package congestion

import (
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

func (s *adaptiveBDPSender) observeMinRTT(rtt time.Duration, priorInFlight protocol.ByteCount, now monotime.Time) bool {
	hasRawRTTSample := rtt > 0
	if rtt <= 0 {
		rtt = s.rttStats.MinRTT()
	}
	if rtt <= 0 {
		return false
	}
	if s.minRTT == 0 || rtt < s.minRTT {
		s.minRTT = rtt
		s.minRTTTimestamp = now
		if s.state == adaptiveBDPProbeRTT && hasRawRTTSample {
			s.probeRTTHasFreshSample = true
		}
		return false
	}
	if s.state == adaptiveBDPProbeRTT {
		if hasRawRTTSample {
			s.recordProbeRTTObservation(rtt, priorInFlight, now)
		}
		if hasRawRTTSample && s.isProbeRTTSampleDrained(rtt) {
			s.minRTT = rtt
			s.minRTTTimestamp = now
			s.probeRTTHasFreshSample = true
		}
		return false
	}
	if !s.minRTTTimestamp.IsZero() && now.Sub(s.minRTTTimestamp) >= s.minRTTFilterWindow() {
		// A shared-queue allowance is a control policy, not propagation RTT.
		// Refreshing minRTT requires the physical queue to be drained.
		if s.queueDelay() > s.queueTarget()/2 {
			if !s.probeRTTRetryNotBefore.IsZero() && now.Before(s.probeRTTRetryNotBefore) {
				return false
			}
			return true
		}
		s.minRTT = rtt
		s.minRTTTimestamp = now
	}
	return false
}

func (s *adaptiveBDPSender) isProbeRTTSampleDrained(rtt time.Duration) bool {
	if rtt <= 0 || s.minRTT <= 0 {
		return false
	}
	target := s.queueTarget()
	return target > 0 && rtt <= s.minRTT+target/2
}

func (s *adaptiveBDPSender) observeRound(sample RateSample, priorInFlight protocol.ByteCount, now monotime.Time) bool {
	s.roundStart = false
	if s.minRTT <= 0 || priorInFlight == 0 {
		return false
	}
	if sample.DeliveredBytes > 0 {
		// Wait for an ACK of a packet sent after the previous round began.
		// The current delivered total can advance many times within one flight.
		// Delivery metadata remains usable even when the rate sample is invalid.
		s.roundStart = sample.PriorDelivered >= s.nextRoundDelivered
	} else {
		// Legacy callers without delivery metadata use a time-based round.
		if s.lastRoundStartTime.IsZero() {
			s.roundStart = true
		} else if now.Sub(s.lastRoundStartTime) >= s.minRTT {
			s.roundStart = true
		}
	}
	if !s.roundStart {
		return false
	}
	s.roundCount++
	if s.state == adaptiveBDPProbeRTT {
		s.probeRTTBandwidthRounds++
	}
	s.lastRoundStartTime = now
	if !sample.AppLimited && sample.IsValid && sample.DeliveryRate > 0 {
		if s.maxBw >= uint64(float64(max(1, s.fullBw))*1.25) {
			s.fullBw = s.maxBw
			s.fullBwCount = 0
		} else {
			s.fullBwCount++
		}
		if s.fullBwCount >= 3 {
			s.fullBwReached = true
		}
	}
	return true
}

// Reset a streak only after a full round without a positive observation, so
// ACK ordering within a round does not change signal persistence.
func (s *adaptiveBDPSender) prepareRoundGatedSignals() {
	if s.hasLastQueueHighRound && s.lastQueueHighRound+1 < s.roundCount {
		s.queueHighRounds = 0
		s.hasLastQueueHighRound = false
	}
	if s.hasLastDownshiftRound && s.lastDownshiftRound+1 < s.roundCount {
		s.downshiftRounds = 0
		s.hasLastDownshiftRound = false
	}
}
