package congestion

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPQueueBudgetDoesNotScaleWithPropagationRTT(t *testing.T) {
	for _, rtt := range []time.Duration{20 * time.Millisecond, 200 * time.Millisecond} {
		s := smallWindowLossSender()
		s.minRTT = rtt
		s.bw = 1_000_000
		require.Equal(t, 10*time.Millisecond, s.queueTarget())
		require.GreaterOrEqual(t, s.targetCwnd(), s.bdp())
		require.LessOrEqual(t, s.targetCwnd()-s.bdp(), protocol.ByteCount(20_000)+s.maxDatagramSize)
	}
}

func sharedQueueTestSender() *adaptiveBDPSender {
	s := smallWindowLossSender()
	s.minRTT = 20 * time.Millisecond // smoothed RTT is 100 ms
	s.state = adaptiveBDPProbeDown
	s.roundStart = true
	s.hasQueueGrowthSample = true
	s.lastQueueGrowthDelay = s.queueDelay()
	s.sharedQueueDrainStart = s.clock.Now().Add(-time.Second)
	return s
}

func TestAdaptiveBDPSharedQueueRequiresCompletedStableDrain(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*adaptiveBDPSender, *RateSample)
		want   bool
	}{
		{name: "stable residual", want: true},
		{name: "still draining", change: func(s *adaptiveBDPSender, _ *RateSample) { s.sharedQueueDrainStart = s.clock.Now() }},
		{name: "queue growing", change: func(s *adaptiveBDPSender, _ *RateSample) { s.lastQueueGrowthDelay = 0 }},
		{name: "app limited", change: func(_ *adaptiveBDPSender, sample *RateSample) { sample.AppLimited = true }},
		{name: "invalid sample", change: func(_ *adaptiveBDPSender, sample *RateSample) { sample.IsValid = false }},
		{name: "inside round", change: func(s *adaptiveBDPSender, _ *RateSample) { s.roundStart = false }},
		{name: "ECN", change: func(s *adaptiveBDPSender, _ *RateSample) { s.hasLastECNCE = true; s.lastECNCERound = s.roundCount }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := sharedQueueTestSender()
			beforeRTT, beforeQueue := s.minRTT, s.queueDelay()
			sample := RateSample{IsValid: true, DeliveryRate: protocol.ByteCount(s.bw)}
			if test.change != nil {
				test.change(s, &sample)
			}
			s.updateSharedQueue(sample, s.congestionWindow, s.clock.Now())
			require.Equal(t, test.want, s.sharedQueueDelay > 0)
			require.Equal(t, beforeRTT, s.minRTT)
			require.Equal(t, beforeQueue, s.queueDelay(), "physical queue remains observable")
			if test.want {
				require.Equal(t, adaptiveBDPProbeBW, s.state)
				require.Zero(t, s.controlQueueDelay())
			}
		})
	}
}

func TestAdaptiveBDPSharedQueueResetAndBound(t *testing.T) {
	for _, reset := range []string{"loss", "ECN", "idle", "persistent"} {
		t.Run(reset, func(t *testing.T) {
			s := sharedQueueTestSender()
			s.minRTT = time.Millisecond
			s.rttStats.UpdateRTT(time.Second, 0)
			s.lastQueueGrowthDelay = s.queueDelay()
			s.sharedQueueDrainStart = s.clock.Now().Add(-10 * time.Second)
			s.updateSharedQueue(RateSample{IsValid: true}, s.congestionWindow, s.clock.Now())
			require.Positive(t, s.sharedQueueDelay)
			require.LessOrEqual(t, s.sharedQueueDelay, 150*time.Millisecond)
			switch reset {
			case "loss":
				s.ackedBytesThisRound = 70_000
				s.OnCongestionEvent(1, 12_800, s.congestionWindow)
			case "ECN":
				s.OnECNCongestionEvent(s.congestionWindow, s.clock.Now())
			case "idle":
				s.startIdleRestart(s.clock.Now())
			case "persistent":
				s.resetCapacityModelAfterPersistentCongestion(s.clock.Now())
			}
			require.Zero(t, s.sharedQueueDelay)
			require.True(t, s.sharedQueueDrainStart.IsZero())
		})
	}
}

func TestAdaptiveBDPCleanRoundsDoNotInventLossRecovery(t *testing.T) {
	s := smallWindowLossSender()
	s.lossFreeRounds = 10
	s.shortBw = s.bw / 2
	sample := RateSample{IsValid: true, DeliveryRate: protocol.ByteCount(s.bw)}
	s.maybeStartLossRecoveryProbe(s.clock.Now(), sample, s.congestionWindow)
	require.False(t, s.lossRecoveryProbeActive)
	s.hasLastLossCutbackRound = true
	s.maybeStartLossRecoveryProbe(s.clock.Now(), sample, s.congestionWindow)
	require.True(t, s.lossRecoveryProbeActive)
	s.shortBw = 0
	s.lossRecoveryProbeActive = false
	s.roundCount++
	s.maybeStartLossRecoveryProbe(s.clock.Now(), sample, s.congestionWindow)
	require.False(t, s.lossRecoveryProbeActive, "recovered capacity must return to ordinary ProbeBW")
}

func TestAdaptiveBDPSharedQueueDoesNotBecomePropagationRTT(t *testing.T) {
	s := sharedQueueTestSender()
	s.state = adaptiveBDPProbeBW
	s.sharedQueueDelay = s.queueDelay()
	s.minRTTTimestamp = s.clock.Now()
	minRTT := s.minRTT
	require.Equal(t, adaptiveQueueEmpty, s.queueState())
	require.True(t, s.observeMinRTT(s.rttStats.SmoothedRTT(), s.congestionWindow, s.clock.Now().Add(s.minRTTFilterWindow()+time.Second)))
	require.Equal(t, minRTT, s.minRTT, "physical queue still requires ProbeRTT")
}
