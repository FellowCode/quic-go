package congestion

import (
	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

func (s *adaptiveBDPSender) updateBandwidth(sample RateSample, priorInFlight protocol.ByteCount) {
	s.updateBandwidthAt(sample, priorInFlight, s.clock.Now())
}

func (s *adaptiveBDPSender) updateBandwidthAt(sample RateSample, priorInFlight protocol.ByteCount, eventTime monotime.Time) {
	s.lastRateSample = sample
	s.lastPriorInFlight = priorInFlight
	// ProbeRTT deliberately caps inflight. Its low delivery rate is not
	// evidence of lower path capacity; a higher sample is still useful.
	if s.state == adaptiveBDPProbeRTT {
		sample.AppLimited = true
	}
	filterRound := s.roundCount - s.probeRTTBandwidthRounds
	s.prepareRoundGatedSignals()
	if !sample.IsValid || sample.DeliveryRate == 0 {
		if s.bw == 0 {
			s.bootstrapBandwidth()
			s.lastBWChangeReason = "bootstrap_invalid_sample"
		}
		s.updateDebugSnapshot(priorInFlight)
		return
	}

	sampleBW := uint64(sample.DeliveryRate)
	if !sample.AppLimited && s.cfg.StartupTargetRateBps > 0 && sampleBW >= s.configuredStartupTargetRateFloor() {
		s.startupTargetRateValidated = true
	}
	prevMaxBw := s.maxBw
	prevShortBw := s.shortBw
	prevBw := s.bw
	if len(s.bwFilter.samples) == 0 && prevMaxBw > 0 {
		s.bwFilter.Update(filterRound, prevMaxBw)
	}
	s.updateBandwidthCompetition(sample, sampleBW, prevMaxBw)
	if sample.AppLimited {
		if sampleBW > s.maxBw {
			s.bwFilter.Update(filterRound, sampleBW)
			s.maxBw = s.bwFilter.Max(filterRound)
			if s.maxBw != prevMaxBw {
				s.lastBWChangeReason = "app_limited_higher_sample"
			}
		}
	} else {
		s.bwFilter.Update(filterRound, sampleBW)
		s.maxBw = s.bwFilter.Max(filterRound)
		if s.maxBw > prevMaxBw {
			s.lastBWChangeReason = "max_bw_increased_by_delivery_sample"
		} else if s.maxBw < prevMaxBw {
			s.lastBWChangeReason = "max_bw_aged_out"
		}
	}

	activeBW := s.activeBandwidthBeforeDownshift()
	if activeBW == 0 {
		activeBW = sampleBW
	}

	if !sample.AppLimited && activeBW > 0 && float64(sampleBW) < float64(activeBW)*s.downshiftRatio() {
		if s.inUploadWarmup(eventTime) {
			s.noQueueLow = noQueueLowSampleState{}
			s.lastBWChangeReason = "upload_warmup_low_sample_not_capacity_proof"
		} else if !s.canUseSampleForDownshift(sample, priorInFlight) {
			s.noQueueLow = noQueueLowSampleState{}
			if s.queueState() == adaptiveQueueEmpty {
				s.lastBWChangeReason = "queue_empty_low_sample_not_capacity_proof"
			} else if !s.isPipeFilledForDownshift(priorInFlight) {
				s.lastBWChangeReason = "pipe_not_filled_for_downshift"
			} else {
				s.lastBWChangeReason = "low_sample_no_queue_rejected"
			}
		} else if s.hasCongestionEvidence() {
			s.noQueueLow = noQueueLowSampleState{}
			if !s.hasLastDownshiftRound || s.lastDownshiftRound != s.roundCount {
				s.downshiftRounds++
				s.lastDownshiftRound = s.roundCount
				s.hasLastDownshiftRound = true
				if s.downshiftRounds < s.congestionDownshiftRoundsTarget() {
					s.lastBWChangeReason = "congestion_downshift_waiting_rounds"
				} else {
					s.confirmedCongestionDownshift(sampleBW, eventTime)
				}
			}
		} else {
			s.noQueueLowSampleCandidate(sampleBW, sample, priorInFlight, eventTime)
		}
	} else if !sample.AppLimited {
		s.noQueueLow = noQueueLowSampleState{}
		if s.shortBw > 0 && sampleBW > s.shortBw {
			s.shortBw = min(sampleBW, max(s.maxBw, sampleBW))
			s.lastBWChangeReason = "short_bw_recovery"
		}
		if s.shortBw > 0 && s.maxBw > 0 && float64(s.shortBw) >= float64(s.maxBw)*0.95 {
			s.shortBw = 0
			s.lastBWChangeReason = "short_bw_cleared_recovered"
		}
	}

	activeBW = s.maxBw
	if activeBW == 0 {
		activeBW = sampleBW
	}
	if s.shortBw > 0 {
		activeBW = min(activeBW, s.shortBw)
	}
	if s.lossRecoveryProbeActive {
		if s.roundCount > s.lossRecoveryProbeUntilRound || s.hasFreshMaterialLoss() || s.queueState() == adaptiveQueuePersistent {
			s.lossRecoveryProbeActive = false
			s.lossRecoveryProbeBW = 0
		} else if s.lossRecoveryProbeBW > activeBW {
			activeBW = s.lossRecoveryProbeBW
			s.lastBWChangeReason = "loss_recovery_probe_bw_floor"
		}
	}
	s.bw = max(1, activeBW)
	if s.bw != prevBw && s.lastBWChangeReason == "" {
		s.lastBWChangeReason = "bandwidth_estimate_changed"
	}
	if s.shortBw != prevShortBw && s.lastBWChangeReason == "" {
		s.lastBWChangeReason = "short_bw_changed"
	}
}
