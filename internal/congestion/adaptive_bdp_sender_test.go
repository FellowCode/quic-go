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

func TestAdaptiveBDPDebugInfo(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.minRTT = 150 * time.Millisecond
	s.OnPacketAckedWithRateSample(
		1,
		1280,
		64*1280,
		monotime.Now(),
		RateSample{
			DeliveryRate:   protocol.ByteCount(mbitToBytesPerSecond(30)),
			AckedBytes:     1280,
			DeliveredBytes: 1280,
			DeliveredDelta: 1280,
			PriorInFlight:  64 * 1280,
			Interval:       150 * time.Millisecond,
			AckElapsed:     150 * time.Millisecond,
			SendElapsed:    10 * time.Millisecond,
			RTT:            150 * time.Millisecond,
			IsValid:        true,
		},
	)

	info := s.AdaptiveBDPDebugInfo()
	require.Equal(t, "Startup", info.State)
	require.Equal(t, protocol.ByteCount(64*1280), info.PriorInFlight)
	require.Equal(t, protocol.ByteCount(mbitToBytesPerSecond(30)), info.LastDeliveryRateBytesPerSecond)
	require.Equal(t, protocol.ByteCount(1280), info.LastDeliveredDelta)
	require.Equal(t, 150*time.Millisecond, info.LastSampleAckElapsed)
	require.Equal(t, 10*time.Millisecond, info.LastSampleSendElapsed)
	require.Greater(t, info.TargetCwnd, protocol.ByteCount(0))
	require.Greater(t, info.BDP, protocol.ByteCount(0))
	require.Greater(t, info.PacingGain, 0.0)
	require.Greater(t, info.CwndGain, 0.0)
	require.NotEmpty(t, info.LastCwndChangeReason)
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

func TestAdaptiveBDPThirtyMbps150msNoLossModel(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	const baseRTT = 150 * time.Millisecond
	rttStats.UpdateRTT(baseRTT, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			Algorithm:             CongestionControlAdaptiveBDP,
			InitialWindowPackets:  256,
			MinWindowPackets:      32,
			MaxWindowPackets:      2000,
			StartupTargetRateBps:  30_000_000,
			StartupTargetDuration: 5 * time.Second,
			StartupPacingGain:     2.0,
			StartupCwndGain:       2.0,
			CruisePacingGain:      1.0,
			CruiseCwndGain:        1.5,
			ProbeUpGain:           1.15,
			ProbeDownGain:         0.90,
			QueueTarget:           30 * time.Millisecond,
			QueuePersistentRounds: 3,
			DownshiftRatio:        0.75,
			DownshiftRounds:       3,
			PacingMargin:          0.01,
		},
	)
	s.minRTT = baseRTT

	capacityRate := mbitToBytesPerSecond(30)
	capacityPerRTT := protocol.ByteCount(float64(capacityRate) * baseRTT.Seconds())
	cruiseTarget := protocol.ByteCount(float64(capacityPerRTT) * 1.5)
	minHealthyPacing := uint64(float64(capacityRate) * 0.5)
	minHealthyCwnd := protocol.ByteCount(float64(cruiseTarget) * 0.8)

	var deliveredTotal protocol.ByteCount
	var checkedFiveSeconds bool
	minCwndAfterFiveSeconds := s.maxCongestionWindow
	minPacingAfterFiveSeconds := uint64(^uint64(0))
	minFillAfterFiveSeconds := 1.0

	for step, elapsed := 0, time.Duration(0); elapsed <= 20*time.Second; step, elapsed = step+1, elapsed+baseRTT {
		now := start.Add(elapsed)
		clock = mockClock(now)
		priorInFlight := min(s.congestionWindow, capacityPerRTT)
		require.Greater(t, priorInFlight, protocol.ByteCount(0))
		deliveredTotal += priorInFlight
		s.OnPacketAckedWithRateSample(
			protocol.PacketNumber(step+1),
			priorInFlight,
			priorInFlight,
			now,
			RateSample{
				DeliveryRate:   protocol.ByteCount(uint64(float64(priorInFlight) / baseRTT.Seconds())),
				AckedBytes:     priorInFlight,
				DeliveredBytes: deliveredTotal,
				DeliveredDelta: priorInFlight,
				PriorInFlight:  priorInFlight,
				Interval:       baseRTT,
				AckElapsed:     baseRTT,
				SendElapsed:    baseRTT,
				RTT:            baseRTT,
				IsValid:        true,
			},
		)

		if elapsed >= 5*time.Second {
			if !checkedFiveSeconds {
				require.GreaterOrEqual(t, s.pacingRateBytesPerSecond, uint64(float64(capacityRate)*0.8))
				require.GreaterOrEqual(t, s.congestionWindow, minHealthyCwnd)
				require.GreaterOrEqual(t, s.targetCwnd(), minHealthyCwnd)
				checkedFiveSeconds = true
			}
			minCwndAfterFiveSeconds = min(minCwndAfterFiveSeconds, s.congestionWindow)
			minPacingAfterFiveSeconds = min(minPacingAfterFiveSeconds, s.pacingRateBytesPerSecond)
			fill := float64(priorInFlight) / float64(max(s.congestionWindow, 1))
			minFillAfterFiveSeconds = min(minFillAfterFiveSeconds, fill)
		}
	}

	require.True(t, checkedFiveSeconds)
	require.Greater(t, minCwndAfterFiveSeconds, s.minCongestionWindow)
	require.GreaterOrEqual(t, minPacingAfterFiveSeconds, minHealthyPacing)
	require.NotEqual(t, adaptiveBDPProbeDown, s.state)
	require.GreaterOrEqual(t, minFillAfterFiveSeconds, 0.10)
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
	s.congestionWindow = 2500 * 1280
	s.updatePacingRate()

	now := monotime.Now()
	for i := 0; i < 1; i++ {
		s.OnPacketAckedWithRateSample(
			protocol.PacketNumber(i+1),
			1280,
			2000*1280,
			now,
			RateSample{
				DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(30)),
				AckedBytes:    1280,
				PriorInFlight: 2000 * 1280,
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

func TestAdaptiveBDPIgnoresSelfLimitedLowSampleForDownshift(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRounds: 1,
			DownshiftRatio:  0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280

	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(2)),
		IsValid:      true,
	}, 4*1280)

	require.Equal(t, mbitToBytesPerSecond(30), s.bw)
	require.Zero(t, s.shortBw)
	require.Zero(t, s.downshiftRounds)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
}

func TestAdaptiveBDPDownshiftsOnPipeFilledBandwidthDrop(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRounds: 2,
			DownshiftRatio:  0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	priorInFlight := protocol.ByteCount(360 * 1280)

	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		IsValid:      true,
	}, priorInFlight)
	require.Zero(t, s.shortBw)
	require.Equal(t, uint32(1), s.downshiftRounds)

	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		IsValid:      true,
	}, priorInFlight)
	require.InDelta(t, float64(mbitToBytesPerSecond(10)), float64(s.shortBw), float64(mbitToBytesPerSecond(1)))
	require.Equal(t, min(s.maxBw, s.shortBw), s.bw)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Equal(t, "short_bw_downshift_pipe_filled", s.lastBWChangeReason)
}

func TestAdaptiveBDPDownshiftDropsPacingWithinThreeRTT(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRounds: 3,
			DownshiftRatio:  0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.updatePacingRate()
	oldRate := s.pacingRateBytesPerSecond
	oldCwnd := s.congestionWindow
	priorInFlight := protocol.ByteCount(360 * 1280)
	now := monotime.Now()

	for i := range 3 {
		clock = mockClock(now)
		s.OnPacketAckedWithRateSample(
			protocol.PacketNumber(i+1),
			1280,
			priorInFlight,
			now,
			RateSample{
				DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
				AckedBytes:   1280,
				Interval:     150 * time.Millisecond,
				RTT:          150 * time.Millisecond,
				IsValid:      true,
			},
		)
		now = now.Add(150 * time.Millisecond)
	}

	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Greater(t, s.shortBw, uint64(0))
	require.Less(t, s.pacingRateBytesPerSecond, oldRate)
	require.GreaterOrEqual(t, s.congestionWindow, protocol.ByteCount(float64(oldCwnd)*0.85))
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

func TestAdaptiveBDPOnAckEvaluatesProbeDownOnce(t *testing.T) {
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
	s.minRTT = 200 * time.Millisecond
	s.congestionWindow = 2000 * 1280

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		1000*1280,
		monotime.Now(),
		RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(50)),
			AckedBytes:   1280,
			Interval:     100 * time.Millisecond,
			RTT:          250 * time.Millisecond,
			IsValid:      true,
		},
	)

	require.Equal(t, adaptiveBDPStartup, s.state)
	require.Equal(t, uint32(1), s.queueHighRounds)
}

func TestAdaptiveBDPOnAckKeepsSpecificProbeDownReason(t *testing.T) {
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
			QueuePersistentRounds: 1,
		},
	)
	s.minRTT = 200 * time.Millisecond
	s.congestionWindow = 2000 * 1280

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		1000*1280,
		monotime.Now(),
		RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(50)),
			AckedBytes:   1280,
			Interval:     100 * time.Millisecond,
			RTT:          250 * time.Millisecond,
			IsValid:      true,
		},
	)

	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Equal(t, "queue_delay_persistent", s.lastStateChangeReason)
}

func TestAdaptiveBDPShouldEnterProbeDownDoesNotOwnDownshiftRounds(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRounds: 2,
			DownshiftRatio:  0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.downshiftRounds = 1
	priorInFlight := protocol.ByteCount(360 * 1280)

	enter := s.shouldEnterProbeDown(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		IsValid:      true,
	}, priorInFlight)

	require.False(t, enter)
	require.Equal(t, uint32(1), s.downshiftRounds)

	s.downshiftRounds = 2
	enter = s.shouldEnterProbeDown(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		IsValid:      true,
	}, priorInFlight)

	require.True(t, enter)
	require.Equal(t, uint32(2), s.downshiftRounds)
	require.Equal(t, "bandwidth_downshift", s.lastStateChangeReason)
}

func TestAdaptiveBDPProbeDownRequiresMinDrainAndDrained(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	base := monotime.Now()
	s.state = adaptiveBDPProbeDown
	s.lastStateChange = base
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		64*1280,
		base.Add(100*time.Millisecond),
		RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(100)),
			AckedBytes:   1280,
			Interval:     100 * time.Millisecond,
			RTT:          200 * time.Millisecond,
			IsValid:      true,
		},
	)
	require.Equal(t, adaptiveBDPProbeDown, s.state)

	s.OnPacketAckedWithRateSample(
		2,
		1280,
		64*1280,
		base.Add(250*time.Millisecond),
		RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(100)),
			AckedBytes:   1280,
			Interval:     100 * time.Millisecond,
			RTT:          200 * time.Millisecond,
			IsValid:      true,
		},
	)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, "probe_down_drained", s.lastStateChangeReason)
}

func TestAdaptiveBDPRoundTimerDoesNotUseStateTimer(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	base := monotime.Now()
	s.minRTT = 200 * time.Millisecond
	s.lastStateChange = base.Add(-time.Second)
	s.lastRoundStartTime = base

	s.updateRound(RateSample{AckedBytes: 1280}, 64*1280, base.Add(50*time.Millisecond))
	require.False(t, s.roundStart)
	require.Zero(t, s.roundCount)

	s.updateRound(RateSample{AckedBytes: 1280}, 64*1280, base.Add(250*time.Millisecond))
	require.True(t, s.roundStart)
	require.Equal(t, uint64(1), s.roundCount)
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

func TestHealthyBandwidthSuppressesLossCutback(t *testing.T) {
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
	s.congestionWindow = 128 * 1280
	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(100)),
		IsValid:      true,
	}, 64*1280)

	cwndBeforeLoss := s.congestionWindow
	s.ackedBytesThisRound = 100_000
	s.OnCongestionEvent(1, 2_000, 64*1280)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, cwndBeforeLoss, s.congestionWindow)
	require.Zero(t, s.shortBw)
	require.False(t, s.hasLastLossCutbackRound)
	require.False(t, s.hasLastEmergencyCutback)

	s.OnPacketAckedWithRateSample(
		2,
		1280,
		64*1280,
		monotime.Now(),
		RateSample{
			DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(100)),
			AckedBytes:    1280,
			PriorInFlight: 64 * 1280,
			Interval:      100 * time.Millisecond,
			RTT:           250 * time.Millisecond,
			IsValid:       true,
		},
	)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Greater(t, s.congestionWindow, cwndBeforeLoss)
}

func TestLossCutbackWinsWhenBandwidthDrops(t *testing.T) {
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
	s.congestionWindow = 128 * 1280
	s.ackedBytesThisRound = 100_000
	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(70)),
		IsValid:      true,
	}, 64*1280)

	s.OnCongestionEvent(1, 2_000, 64*1280)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.True(t, s.hasLastLossCutbackRound)
	require.Greater(t, s.shortBw, uint64(0))
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

func TestAdaptiveBDPLossCutbackDoesNotDependOnQueueGate(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
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
	s.congestionWindow = 128 * 1280
	s.ackedBytesThisRound = 100_000
	s.updatePacingRate()
	oldRate := s.pacingRateBytesPerSecond
	require.False(t, s.canReduceWindow(64*1280))

	s.OnCongestionEvent(1, 2_000, 64*1280)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.True(t, s.hasLastLossCutbackRound)
	require.Greater(t, s.shortBw, uint64(0))
	require.Less(t, s.pacingRateBytesPerSecond, oldRate)
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

func TestAdaptiveBDPNoLossReductionIsGradualWithQueuePressure(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(260*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.minRTT = 200 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(10)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	oldCwnd := s.congestionWindow
	require.True(t, s.hasQueuePressure())

	s.setCwndFromTarget(1280, oldCwnd)
	require.Equal(t, protocol.ByteCount(float64(oldCwnd)*0.85), s.congestionWindow)
	require.Equal(t, "gradual_no_loss_target_cutback", s.lastCwndChangeReason)
}

func TestAdaptiveBDPNoLossReductionIsGentlerWithoutQueuePressure(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.minRTT = 200 * time.Millisecond
	s.state = adaptiveBDPProbeDown
	s.bw = mbitToBytesPerSecond(10)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	oldCwnd := s.congestionWindow
	require.False(t, s.hasQueuePressure())

	s.setCwndFromTarget(1280, oldCwnd)
	require.Equal(t, protocol.ByteCount(float64(oldCwnd)*0.95), s.congestionWindow)
	require.Equal(t, "gradual_no_loss_target_cutback", s.lastCwndChangeReason)
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

func TestAdaptiveBDPECNDoesNotDependOnQueueGate(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
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
	require.False(t, s.canReduceWindow(200*1280))

	s.OnECNCongestionEvent(200*1280, monotime.Now())
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Less(t, s.pacingRateBytesPerSecond, oldRate)
	require.Greater(t, s.shortBw, uint64(0))
}

func TestCruisePacingUsesHeadroom(t *testing.T) {
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
	s.bw = mbitToBytesPerSecond(100)
	s.updatePacingRate()

	require.Greater(t, s.pacingRateBytesPerSecond, s.bw)
	require.InDelta(t, float64(mbitToBytesPerSecond(103.95)), float64(s.pacingRateBytesPerSecond), 1)
}

func TestAdaptiveBDPPacingFloorTracksMinWindowOverRTT(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:           true,
			MinWindowPackets: 32,
		},
	)
	s.minRTT = 200 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 1

	s.updatePacingRate()
	require.Equal(t, uint64(204800), s.pacingRateBytesPerSecond)
	require.Equal(t, s.minimumPacingRate(), s.pacingRateBytesPerSecond)
}

func TestAdaptiveBDPPacingFloorUsesDefaultRTT(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:           true,
			MinWindowPackets: 32,
		},
	)
	s.minRTT = 0
	s.bw = 1

	s.updatePacingRate()
	require.Equal(t, uint64(409600), s.pacingRateBytesPerSecond)
}

func TestAdaptiveBDPPacingFloorWinsOverLowMaxProbeRate(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:           true,
			MinWindowPackets: 32,
			MaxProbeRateBps:  8_000,
		},
	)
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)

	s.updatePacingRate()
	require.Equal(t, s.minimumPacingRate(), s.pacingRateBytesPerSecond)
}

func TestAdaptiveBDPProtectedStartupPacingFloorUsesInitialWindow(t *testing.T) {
	clock := mockClock(monotime.Now())
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			InitialWindowPackets:  256,
			MinWindowPackets:      32,
			StartupTargetRateBps:  30_000_000,
			StartupTargetDuration: 5 * time.Second,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.bw = 1

	s.updatePacingRate()
	require.Equal(t, s.startupProtectedPacingRate(), s.pacingRateBytesPerSecond)
	require.Greater(t, s.pacingRateBytesPerSecond, s.minimumPacingRate())
}

func TestAdaptiveBDPProtectedStartupPacingFloorExpires(t *testing.T) {
	clock := mockClock(monotime.Now())
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			InitialWindowPackets:  256,
			MinWindowPackets:      32,
			StartupTargetDuration: 5 * time.Second,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.bw = 1
	clock.Advance(6 * time.Second)

	s.updatePacingRate()
	require.Equal(t, s.minimumPacingRate(), s.pacingRateBytesPerSecond)
	require.Less(t, s.pacingRateBytesPerSecond, s.startupProtectedPacingRate())
}

func TestAdaptiveBDPProtectedStartupDoesNotMaskPersistentCongestion(t *testing.T) {
	clock := mockClock(monotime.Now())
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			InitialWindowPackets:  256,
			MinWindowPackets:      32,
			StartupTargetDuration: 5 * time.Second,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.congestionWindow = 400 * 1280

	s.OnPersistentCongestion(monotime.Time(clock))
	require.Equal(t, s.minCongestionWindow, s.congestionWindow)
	require.False(t, s.inProtectedStartup(monotime.Time(clock)))
	require.Less(t, s.pacingRateBytesPerSecond, s.startupProtectedPacingRate())
}

func TestAdaptiveBDPProtectedStartupKeepsNoLossCwndAtInitialWindow(t *testing.T) {
	clock := mockClock(monotime.Now())
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			InitialWindowPackets:  256,
			MinWindowPackets:      32,
			StartupTargetDuration: 5 * time.Second,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.bw = 1
	s.congestionWindow = 300 * 1280
	require.True(t, s.hasQueuePressure())

	s.reduceCwndTowardTarget(s.congestionWindow, false)
	require.Equal(t, s.initialWindow, s.congestionWindow)
}

func TestAdaptiveBDPProtectedStartupAllowsLossBasedCwndCutback(t *testing.T) {
	clock := mockClock(monotime.Now())
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                true,
			InitialWindowPackets:  256,
			MinWindowPackets:      32,
			StartupTargetDuration: 5 * time.Second,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.bw = 1
	s.congestionWindow = 300 * 1280

	s.reduceCwndTowardTarget(s.congestionWindow, true)
	require.Less(t, s.congestionWindow, s.initialWindow)
	require.Equal(t, s.minCongestionWindow, s.congestionWindow)
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
