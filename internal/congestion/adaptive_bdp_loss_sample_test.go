package congestion

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

func smallWindowLossSender() *adaptiveBDPSender {
	s := newAdaptiveBDPLossRegressionSender(100*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{LossEWMAAlpha: 0.25})
	s.bw = 100_000
	s.maxBw = s.bw
	s.congestionWindow = 20 * 1280
	return s
}

func completeSmallLossRound(s *adaptiveBDPSender, acked, lost protocol.ByteCount) {
	s.ackedBytesThisRound = acked
	if lost > 0 {
		s.OnCongestionEvent(1, lost, s.congestionWindow)
	}
	s.clock.(*mockClock).Advance(100 * time.Millisecond)
	s.updateRound(RateSample{}, s.congestionWindow, s.clock.Now())
}

func TestAdaptiveBDPSmallWindowPersistentLossAccumulates(t *testing.T) {
	s := smallWindowLossSender()
	oldCwnd := s.congestionWindow
	for round := 1; round <= 6; round++ {
		completeSmallLossRound(s, 18*1280, 2*1280)
		require.Equal(t, uint64(round), s.roundCount)
		require.Zero(t, s.ackedBytesThisRound)
		require.Zero(t, s.lostBytesThisRound)
		if round < 3 {
			require.Zero(t, s.lossRatioEWMA)
			require.False(t, s.hasMaterialLossRound)
		} else {
			require.InDelta(t, 0.1, s.lossRatioEWMA, 1e-9)
			require.True(t, s.hasMaterialLossRound)
		}
	}
	require.Equal(t, uint32(2), s.mildLossRounds)
	require.True(t, s.hasLastLossCutbackRound)
	require.Less(t, s.congestionWindow, oldCwnd)
	require.Zero(t, s.pendingLossSampleAcked)
	require.Zero(t, s.pendingLossSampleLost)

	// Clean small rounds must also accumulate, consume the previous losses only
	// once, and permit recovery even though no single round reaches 64 KiB.
	for range 6 {
		completeSmallLossRound(s, 20*1280, 0)
	}
	require.Equal(t, uint32(2), s.lossFreeRounds)
	require.Zero(t, s.mildLossRounds)
	require.InDelta(t, 0.1*0.75*0.75, s.lossRatioEWMA, 1e-9)
}

func TestAdaptiveBDPSmallLossSampleUsesCombinedRatioForEmergency(t *testing.T) {
	s := smallWindowLossSender()
	s.cfg.MinLossSampleBytes = 100_000
	completeSmallLossRound(s, 60_000, 0)
	completeSmallLossRound(s, 35_000, 0)
	// The current round alone is 100% loss; the whole sample is below 10%.
	s.OnCongestionEvent(1, 8*1280, s.congestionWindow)
	require.True(t, s.hasEnoughLossSample())
	require.False(t, s.hasLastEmergencyCutback)
	require.Equal(t, 1.0, s.roundLossRatio())
	require.Less(t, s.lossSampleRatio(), s.emergencyLossThreshold())
}

func TestAdaptiveBDPSmallLossSampleEmergencyIsAppliedOnce(t *testing.T) {
	s := smallWindowLossSender()
	for range 2 {
		completeSmallLossRound(s, 16*1280, 4*1280)
	}
	s.ackedBytesThisRound = 16 * 1280
	s.OnCongestionEvent(1, 4*1280, s.congestionWindow)
	require.True(t, s.hasLastEmergencyCutback)
	cutCwnd := s.congestionWindow
	s.clock.(*mockClock).Advance(100 * time.Millisecond)
	s.updateRound(RateSample{}, s.congestionWindow, s.clock.Now())
	require.Equal(t, cutCwnd, s.congestionWindow, "finalizing an already handled sample must not cut twice")
	require.InDelta(t, 0.2, s.lossRatioEWMA, 1e-9)
	require.Zero(t, s.pendingLossSampleLost)
	require.False(t, s.emergencyLossCutbackThisRound)
}

func TestAdaptiveBDPSmallLossSampleReset(t *testing.T) {
	for _, idle := range []bool{false, true} {
		s := smallWindowLossSender()
		completeSmallLossRound(s, 18*1280, 2*1280)
		require.Positive(t, s.pendingLossSampleLost)
		if idle {
			s.startIdleRestart(s.clock.Now())
		} else {
			s.resetCapacityModelAfterPersistentCongestion(s.clock.Now())
		}
		require.Zero(t, s.pendingLossSampleAcked)
		require.Zero(t, s.pendingLossSampleLost)
	}
}

func TestAdaptiveBDPSmallWindowLossRecoveryWithPacingLimitedSample(t *testing.T) {
	for _, test := range []struct {
		name      string
		change    func(*adaptiveBDPSender, *RateSample, *protocol.ByteCount)
		wantProbe bool
	}{
		{name: "paced pipe is full", wantProbe: true},
		{name: "invalid sample", change: func(_ *adaptiveBDPSender, sample *RateSample, _ *protocol.ByteCount) { sample.IsValid = false }},
		{name: "low delivery rate", change: func(_ *adaptiveBDPSender, sample *RateSample, _ *protocol.ByteCount) { sample.DeliveryRate /= 2 }},
		{name: "underfilled pipe", change: func(_ *adaptiveBDPSender, _ *RateSample, inflight *protocol.ByteCount) { *inflight = 1280 }},
		{name: "no preceding loss", change: func(s *adaptiveBDPSender, _ *RateSample, _ *protocol.ByteCount) { s.hasMaterialLossRound = false }},
		{name: "fresh loss", change: func(s *adaptiveBDPSender, _ *RateSample, _ *protocol.ByteCount) {
			s.lastMaterialLossRound = s.roundCount
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := smallWindowLossSender()
			s.shortBw = 70_000
			s.bw = s.shortBw
			s.pacingRateBytesPerSecond = 90_000
			s.hasMaterialLossRound = true
			s.roundCount = 10
			s.lastMaterialLossRound = 1
			s.lossFreeRounds = 2
			sample := RateSample{IsValid: true, AppLimited: true, DeliveryRate: 89_900}
			inflight := s.bdpForBandwidth(s.pacingRateBytesPerSecond)
			if test.change != nil {
				test.change(s, &sample, &inflight)
			}
			s.maybeStartLossRecoveryProbe(s.clock.Now(), sample, inflight)
			require.Equal(t, test.wantProbe, s.lossRecoveryProbeActive)
			if test.wantProbe {
				require.Greater(t, s.lossRecoveryProbeBW, uint64(70_000))
			}
		})
	}
}
