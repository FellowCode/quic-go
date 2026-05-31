package congestion

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
	"github.com/stretchr/testify/require"
)

func mbitToBytesPerSecond(mbit float64) uint64 {
	return uint64(mbit * 1_000_000 / 8)
}

func TestAdaptiveBDPBDPCalculation(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:         true,
			CruiseCwndGain: 2.0,
			WindowGain:     1.0,
		},
	)
	s.bw = mbitToBytesPerSecond(100)
	s.minRTT = 300 * time.Millisecond
	s.state = adaptiveBDPProbeBW

	target := s.targetCwnd()
	// ~7.5 MB for 100 Mbit/s and 300 ms at cwnd_gain=2.0
	require.InDelta(t, 7_500_000, float64(target), 200_000)
}

func TestStartupRequiredGainForFiveSeconds(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			InitialWindowPackets:  32,
			StartupTargetRateBps:  100_000_000,
			StartupTargetDuration: 5 * time.Second,
			StartupPacingGain:     2.0,
		},
	)
	s.minRTT = 300 * time.Millisecond
	gain := s.startupRequiredGainPerRTT()
	require.InDelta(t, 1.31, gain, 0.05)
	require.GreaterOrEqual(t, s.startupPacingGain(), gain)
}

func TestStartupReaches100MbitWithin5sModel(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(300*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			InitialWindowPackets:  32,
			StartupTargetRateBps:  100_000_000,
			StartupTargetDuration: 5 * time.Second,
			StartupPacingGain:     2.0,
			StartupCwndGain:       2.0,
		},
	)
	s.minRTT = 300 * time.Millisecond

	const bottleneck = 100_000_000.0 / 8.0
	now := monotime.Now()
	for i := 0; i < 100; i++ {
		rate := min(float64(s.pacingRateBytesPerSecond), bottleneck)
		s.OnPacketAckedWithRateSample(
			protocol.PacketNumber(i+1),
			1280,
			64*1280,
			now,
			RateSample{
				DeliveryRate:  protocol.ByteCount(rate),
				AckedBytes:    1280,
				PriorInFlight: 64 * 1280,
				Interval:      50 * time.Millisecond,
				RTT:           300 * time.Millisecond,
				IsValid:       true,
			},
		)
		now = now.Add(50 * time.Millisecond)
	}

	require.GreaterOrEqual(t, float64(s.pacingRateBytesPerSecond)*8, 95_000_000.0)
	require.GreaterOrEqual(t, float64(s.bdp()), bottleneck*0.95*0.3)
}

func TestDownshiftOnBandwidthDrop(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRounds: 1,
			DownshiftRatio:  0.85,
		},
	)
	s.minRTT = 200 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 128 * 1280
	s.updatePacingRate()

	now := monotime.Now()
	for i := 0; i < 1; i++ {
		s.OnPacketAckedWithRateSample(
			protocol.PacketNumber(i+1),
			1280,
			64*1280,
			now,
			RateSample{
				DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(30)),
				AckedBytes:    1280,
				PriorInFlight: 64 * 1280,
				Interval:      100 * time.Millisecond,
				RTT:           250 * time.Millisecond,
				IsValid:       true,
			},
		)
		now = now.Add(200 * time.Millisecond)
	}

	require.Greater(t, s.shortBw, uint64(0))
	require.LessOrEqual(t, s.bw, mbitToBytesPerSecond(100))
}

func TestQueueDelayTriggersProbeDown(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			QueueTarget:           25 * time.Millisecond,
			QueuePersistentRounds: 2,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond

	sample := RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(50)),
		RTT:          250 * time.Millisecond,
		IsValid:      true,
	}
	s.congestionWindow = 128 * 1280
	require.False(t, s.shouldEnterProbeDown(sample, 64*1280))
	require.True(t, s.shouldEnterProbeDown(sample, 64*1280))
}

func TestLossTriggersProbeDown(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true, LossTarget: 0.005},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 128 * 1280
	oldCwnd := s.congestionWindow
	s.ackedBytesThisRound = 100_000
	s.OnCongestionEvent(1, 2_000, 64*1280)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.LessOrEqual(t, s.congestionWindow, oldCwnd)
}

func TestLossCutbackLimitedToOncePerRound(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			LossTarget:             0.005,
			EmergencyLossThreshold: 1.0,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 4000 * 1280
	s.ackedBytesThisRound = 100_000

	s.OnCongestionEvent(1, 2_000, 64*1280)
	cwndAfterFirstLoss := s.congestionWindow
	shortBwAfterFirstLoss := s.shortBw
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.True(t, s.hasLastLossCutbackRound)

	s.OnCongestionEvent(2, 50_000, 64*1280)
	require.Equal(t, cwndAfterFirstLoss, s.congestionWindow)
	require.Equal(t, shortBwAfterFirstLoss, s.shortBw)
}

func TestEmergencyLoss(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true, EmergencyLossThreshold: 0.02},
	)
	s.minRTT = 200 * time.Millisecond
	s.congestionWindow = 128 * 1280
	s.ackedBytesThisRound = 10_000
	oldCwnd := s.congestionWindow
	s.OnCongestionEvent(1, 20_000, 64*1280)
	require.Less(t, s.congestionWindow, oldCwnd)
	require.GreaterOrEqual(t, s.congestionWindow, s.minCongestionWindow)
}

func TestEmergencyLossCutbackLimitedToOncePerRound(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			LossTarget:             1.0,
			EmergencyLossThreshold: 0.02,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 128 * 1280
	s.ackedBytesThisRound = 10_000

	s.OnCongestionEvent(1, 20_000, 64*1280)
	cwndAfterFirstLoss := s.congestionWindow
	bwAfterFirstLoss := s.bw
	require.True(t, s.hasLastEmergencyCutback)

	s.OnCongestionEvent(2, 20_000, 64*1280)
	require.Equal(t, cwndAfterFirstLoss, s.congestionWindow)
	require.Equal(t, bwAfterFirstLoss, s.bw)
}

func TestLowDeliveryWithoutQueueDoesNotReduceWindow(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRounds: 1,
			DownshiftRatio:  0.85,
		},
	)
	s.minRTT = 200 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 128 * 1280
	oldCwnd := s.congestionWindow
	oldBw := s.bw

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		64*1280,
		monotime.Now(),
		RateSample{
			DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(30)),
			AckedBytes:    1280,
			PriorInFlight: 64 * 1280,
			Interval:      100 * time.Millisecond,
			RTT:           200 * time.Millisecond,
			IsValid:       true,
		},
	)
	require.GreaterOrEqual(t, s.congestionWindow, oldCwnd)
	require.Equal(t, oldBw, s.bw)
	require.Zero(t, s.shortBw)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
}

func TestRetransmissionTimeoutDoesNotCollapseCwnd(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.updatePacingRate()

	s.OnRetransmissionTimeout(true)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Greater(t, s.congestionWindow, s.minCongestionWindow)
}

func TestPersistentCongestionCollapsesCwnd(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.congestionWindow = 400 * 1280
	s.bw = mbitToBytesPerSecond(100)
	s.shortBw = mbitToBytesPerSecond(50)
	s.OnPersistentCongestion(monotime.Now())
	require.Equal(t, s.minCongestionWindow, s.congestionWindow)
	require.Equal(t, adaptiveBDPStartup, s.state)
}

func TestECNTriggersProbeDown(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.updatePacingRate()
	oldRate := s.pacingRateBytesPerSecond

	s.OnECNCongestionEvent(200*1280, monotime.Now())
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Less(t, s.pacingRateBytesPerSecond, oldRate)
	require.Greater(t, s.shortBw, uint64(0))
}

func TestAppLimitedSamplesIgnored(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.maxBw = mbitToBytesPerSecond(100)
	s.bw = s.maxBw
	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		IsValid:      true,
		AppLimited:   true,
	}, 0)
	require.Equal(t, mbitToBytesPerSecond(100), s.maxBw)

	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(120)),
		IsValid:      true,
		AppLimited:   true,
	}, 0)
	require.Equal(t, mbitToBytesPerSecond(120), s.maxBw)
}

func TestProbeUpFindsHigherBandwidth(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:        true,
			ProbeInterval: 200 * time.Millisecond,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 100 * time.Millisecond
	s.maxBw = mbitToBytesPerSecond(50)
	s.bw = s.maxBw
	s.updatePacingRate()
	startRate := s.pacingRateBytesPerSecond

	now := monotime.Now().Add(250 * time.Millisecond)
	s.OnPacketAckedWithRateSample(
		1,
		1280,
		64*1280,
		now,
		RateSample{
			DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(100)),
			AckedBytes:    1280,
			PriorInFlight: 64 * 1280,
			Interval:      100 * time.Millisecond,
			RTT:           100 * time.Millisecond,
			IsValid:       true,
		},
	)
	require.Greater(t, s.maxBw, mbitToBytesPerSecond(50))
	require.Greater(t, s.pacingRateBytesPerSecond, startRate)
}
