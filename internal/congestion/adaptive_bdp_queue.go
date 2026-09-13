package congestion

import (
	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

func (s *adaptiveBDPSender) shouldEnterProbeDown(sample RateSample, priorInFlight protocol.ByteCount, eventTime monotime.Time) bool {
	s.prepareRoundGatedSignals()
	if s.hasProbeDownQueuePressure() && s.isPipeFilledForDownshift(priorInFlight) {
		if !s.hasLastQueueHighRound || s.lastQueueHighRound != s.roundCount {
			s.queueHighRounds++
			s.lastQueueHighRound = s.roundCount
			s.hasLastQueueHighRound = true
		}
	}
	queueGrowthDownshift := s.maybeDownshiftForGrowingQueue(eventTime, priorInFlight)
	const probeDrainGraceRounds = uint64(4)
	recentProbe := s.probeUpActive || (s.probeUpRoundStart > 0 &&
		s.roundCount >= s.probeUpRoundStart && s.roundCount <= s.probeUpRoundStart+probeDrainGraceRounds)
	reason := decideAdaptiveBDPProbeDown(adaptiveBDPProbeDownInput{
		persistentQueue:      s.hasPersistentQueuePressure(),
		recentProbe:          recentProbe,
		queueGrowthDownshift: queueGrowthDownshift,
		lowerBandwidth: !s.inUploadWarmup(eventTime) && s.canUseSampleForDownshift(sample, priorInFlight) &&
			s.bw > 0 && float64(sample.DeliveryRate) < float64(s.bw)*s.downshiftRatio() &&
			s.downshiftRounds >= s.downshiftRoundsTarget(),
	})
	if reason == "" {
		return false
	}
	if reason == "probe_up_drain" {
		s.probeUpActive = false
	}
	s.lastStateChangeReason = reason
	return true
}

func (s *adaptiveBDPSender) maybeDownshiftForGrowingQueue(eventTime monotime.Time, priorInFlight protocol.ByteCount) bool {
	if !s.roundStart {
		return false
	}
	queueDelay := s.queueDelay()
	previousTime := s.lastQueueGrowthTime
	previousDelay := s.lastQueueGrowthDelay
	hadPrevious := s.hasQueueGrowthSample
	s.lastQueueGrowthTime = eventTime
	s.lastQueueGrowthDelay = queueDelay
	s.hasQueueGrowthSample = true

	if !hadPrevious || previousTime.IsZero() || queueDelay <= previousDelay ||
		!s.hasPersistentQueuePressure() || !s.isPipeFilledForDownshift(priorInFlight) {
		return false
	}
	estimate := estimateAdaptiveBDPQueueCapacity(adaptiveBDPQueueGrowthInput{
		previousDelay: previousDelay, delay: queueDelay, elapsed: eventTime.Sub(previousTime),
		pacingRate: s.pacingRateBytesPerSecond, minimumRate: s.minimumObservableBandwidth(),
		activeRate: s.activeBandwidthBeforeDownshift(), downshiftRatio: s.downshiftRatio(),
	})
	if estimate == 0 {
		return false
	}
	s.applyQueueCapacityEstimate(estimate)
	return true
}

func (s *adaptiveBDPSender) applyQueueCapacityEstimate(estimate uint64) {
	if s.shortBw == 0 {
		s.shortBw = estimate
	} else {
		s.shortBw = min(s.shortBw, estimate)
	}
	s.shortBwCongestionConfirmed = true
	s.bw = minNonZero(s.bw, s.shortBw)
	s.lastBWChangeReason = "queue_growth_capacity_downshift"
}
