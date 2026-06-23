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
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.minRTT = 150 * time.Millisecond
	s.lossFreeRounds = 3
	s.lastMaterialLossRound = 7
	s.lossRecoveryProbeActive = true
	s.lossRecoveryProbeBW = 1_750_000
	s.lossRecoveryProbeUntilRound = 9
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
	require.NotEmpty(t, info.QueueState)
	require.NotEmpty(t, info.LastCwndChangeReason)
	require.Equal(t, s.roundLossRatio(), info.LossRatioRound)
	require.Equal(t, s.lossRatioEWMA, info.LossRatioEWMA)
	require.Equal(t, s.lostBytesThisRound, info.LostBytesThisRound)
	require.Equal(t, s.ackedBytesThisRound, info.AckedBytesThisRound)
	require.Equal(t, s.lossMinBytes(), info.LossMinBytes)
	require.Equal(t, s.emergencyLossMinBytes(), info.EmergencyLossMinBytes)
	require.Equal(t, s.minLossSampleBytes(), info.MinLossSampleBytes)
	require.Equal(t, s.lossGraceRatio(), info.LossGraceRatio)
	require.Equal(t, s.lossSevereThreshold(), info.LossSevereThreshold)
	require.Equal(t, s.emergencyLossThreshold(), info.EmergencyLossThreshold)
	require.Equal(t, s.queuePressure(), info.QueuePressure)
	require.Equal(t, s.mildLossRounds, info.MildLossRounds)
	require.Equal(t, s.lastLossActionReason, info.LastLossActionReason)
	require.Equal(t, s.lastLossCwndMultiplier, info.LastLossCwndMultiplier)
	require.Equal(t, s.lastLossPacingMultiplier, info.LastLossPacingMultiplier)
	require.Equal(t, s.lastLossCutbackRound, info.LastLossCutbackRound)
	require.Equal(t, s.suppressProbeUpUntilRound, info.SuppressProbeUpUntilRound)
	require.Equal(t, s.suppressProbeUpReason, info.SuppressProbeUpReason)
	require.Equal(t, s.lossFreeRounds, info.LossFreeRounds)
	require.Equal(t, s.lastMaterialLossRound, info.LastMaterialLossRound)
	require.Equal(t, s.lossRecoveryProbeActive, info.LossRecoveryProbeActive)
	require.Equal(t, s.lossRecoveryProbeBW, info.LossRecoveryProbeBW)
	require.Equal(t, s.lossRecoveryProbeUntilRound, info.LossRecoveryProbeUntilRound)
}

func TestAdaptiveBDPQueueState(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)

	require.Equal(t, adaptiveQueueUnknown, s.queueState())

	s.minRTT = 100 * time.Millisecond
	rttStats.UpdateRTT(104*time.Millisecond, 0)
	require.Equal(t, adaptiveQueueEmpty, s.queueState())

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(110*time.Millisecond, 0)
	s.rttStats = rttStats
	require.Equal(t, adaptiveQueueBuilding, s.queueState())

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(130*time.Millisecond, 0)
	s.rttStats = rttStats
	s.queueHighRounds = s.queuePersistentRounds() - 1
	require.Equal(t, adaptiveQueueBuilding, s.queueState())

	s.queueHighRounds = s.queuePersistentRounds()
	require.Equal(t, adaptiveQueuePersistent, s.queueState())

	info := s.AdaptiveBDPDebugInfo()
	require.Equal(t, "persistent", info.QueueState)
	require.Equal(t, s.queueDelay(), info.QueueDelay)
	require.Equal(t, s.queueTarget(), info.QueueTarget)
	require.Equal(t, s.queueHighRounds, info.QueueHighRounds)
}

func TestAdaptiveBDPCongestionEvidence(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:     true,
			LossTarget: 0.005,
		},
	)
	s.minRTT = 100 * time.Millisecond
	require.False(t, s.hasCongestionEvidence())

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(130*time.Millisecond, 0)
	s.rttStats = rttStats
	s.queueHighRounds = s.queuePersistentRounds()
	require.True(t, s.hasCongestionEvidence())

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s.rttStats = rttStats
	s.queueHighRounds = 0
	s.roundCount = 7
	s.lastECNCERound = 7
	s.hasLastECNCE = true
	require.True(t, s.hasCongestionEvidence())

	s.roundCount = 9
	require.False(t, s.hasCongestionEvidence())

	s.hasLastECNCE = false
	s.ackedBytesThisRound = 10_000
	s.lostBytesThisRound = 214
	require.Greater(t, s.lossRateThisRound(), s.lossTarget())
	require.False(t, s.hasCongestionEvidence())

	s.lostBytesThisRound = 3 * 1280
	require.True(t, s.hasCongestionEvidence())
}

func TestAdaptiveBDPNegativeBandwidthConfidence(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			QueueTarget:            20 * time.Millisecond,
			LossTarget:             0.01,
			EmergencyLossThreshold: 0.05,
		},
	)
	s.minRTT = 100 * time.Millisecond
	require.Zero(t, s.negativeBandwidthConfidence())

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(120*time.Millisecond, 0)
	s.rttStats = rttStats
	require.InDelta(t, 0.5, s.negativeBandwidthConfidence(), 0.001)

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s.rttStats = rttStats
	s.ackedBytesThisRound = 57_000
	s.lostBytesThisRound = 3_000
	require.Less(t, s.lossMinBytes(), s.lostBytesThisRound)
	require.InDelta(t, 1.0, s.negativeBandwidthConfidence(), 0.001)

	s.ackedBytesThisRound = 10_000
	s.lostBytesThisRound = 214
	require.Zero(t, s.negativeBandwidthConfidence())

	s.roundCount = 12
	s.lastECNCERound = 11
	s.hasLastECNCE = true
	require.Equal(t, 1.0, s.negativeBandwidthConfidence())
}

func TestAdaptiveBDPLossReactionPointOneHelpers(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.minRTT = 100 * time.Millisecond
	s.bw = mbitToBytesPerSecond(80)
	s.maxBw = s.bw
	s.ackedBytesThisRound = 196_000
	s.lostBytesThisRound = 4_000

	require.InDelta(t, 0.02, s.roundLossRatio(), 0.0001)
	require.Equal(t, s.roundLossRatio(), s.lossRate())
	require.Equal(t, 2*protocol.ByteCount(1280), s.lossMinBytes())
	require.Equal(t, 8*protocol.ByteCount(1280), s.emergencyLossMinBytes())
	require.Equal(t, max(protocol.ByteCount(64*1024), s.bdp()/8), s.minLossSampleBytes())
	require.True(t, s.hasEnoughLossSample())

	s.ackedBytesThisRound = 1_000
	s.lostBytesThisRound = 4_000
	require.False(t, s.hasEnoughLossSample())
}

func TestAdaptiveBDPQueuePressure(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:      true,
			QueueTarget: 20 * time.Millisecond,
		},
	)
	s.minRTT = 100 * time.Millisecond
	require.Zero(t, s.queuePressure())

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(120*time.Millisecond, 0)
	s.rttStats = rttStats
	require.Zero(t, s.queuePressure())

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(140*time.Millisecond, 0)
	s.rttStats = rttStats
	require.InDelta(t, 0.5, s.queuePressure(), 0.001)

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(160*time.Millisecond, 0)
	s.rttStats = rttStats
	require.Equal(t, 1.0, s.queuePressure())
}

func TestAdaptiveBDPTinyLossIgnoredBeforeCutback(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			LossTarget:             0.001,
			EmergencyLossThreshold: 0.01,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.ackedBytesThisRound = 150_000
	oldCwnd := s.congestionWindow

	s.OnCongestionEvent(1, 214, 64*1280)
	require.Equal(t, oldCwnd, s.congestionWindow)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.False(t, s.hasLastLossCutbackRound)
	require.False(t, s.hasLastEmergencyCutback)
	require.Equal(t, "loss_below_absolute_threshold", s.lastStateChangeReason)
	require.Equal(t, "loss_below_absolute_threshold", s.lastLossActionReason)
}

func TestAdaptiveBDPSmallLossSampleIgnoredBeforeCutback(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			LossTarget:             0.001,
			EmergencyLossThreshold: 0.01,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.ackedBytesThisRound = 10_000
	oldCwnd := s.congestionWindow

	s.OnCongestionEvent(1, 3*1280, 64*1280)
	require.Equal(t, oldCwnd, s.congestionWindow)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.False(t, s.hasLastLossCutbackRound)
	require.False(t, s.hasLastEmergencyCutback)
	require.Equal(t, "loss_sample_too_small", s.lastStateChangeReason)
	require.Equal(t, "loss_sample_too_small", s.lastLossActionReason)
}

func TestAdaptiveBDPLossReactionDefaults(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)

	require.Equal(t, 0.01, s.lossGraceRatio())
	require.Equal(t, s.lossGraceRatio(), s.lossSoftThreshold())
	require.Equal(t, 0.05, s.lossSevereThreshold())
	require.Equal(t, 0.10, s.emergencyLossThreshold())
	require.Equal(t, 0.25, s.lossEWMAAlpha())
	require.Equal(t, 0.15, s.maxLossCwndCutNoQueue())
	require.Equal(t, 0.30, s.maxLossCwndCutWithQueue())
	require.Equal(t, 0.01, s.minLossCwndCut())
	require.Equal(t, 0.10, s.maxLossPacingCutNoQueue())
	require.Equal(t, 0.25, s.maxLossPacingCutWithQueue())
	require.Equal(t, uint32(2), s.lossRecoveryProbeRounds())
	require.Equal(t, 1.25, s.lossRecoveryProbeGain())
	require.Equal(t, uint64(1), s.lossRecoveryProbeDurationRounds())
	require.Equal(t, 0.95, s.lossRecoveryClearShortBwFraction())
}

func TestAdaptiveBDPLossReactionDefaultOverrides(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                           true,
			LossGraceRatio:                   0.02,
			LossSoftThreshold:                0.03,
			LossSevereThreshold:              0.04,
			EmergencyLossThreshold:           0.03,
			LossEWMAAlpha:                    2,
			MaxLossCwndCutNoQueue:            0.60,
			MaxLossCwndCutWithQueue:          0.40,
			MinLossCwndCut:                   0.20,
			MaxLossPacingCutNoQueue:          0.60,
			MaxLossPacingCutWithQueue:        0.40,
			LossRecoveryProbeRounds:          4,
			LossRecoveryProbeGain:            3.0,
			LossRecoveryProbeDurationRounds:  3,
			LossRecoveryClearShortBwFraction: 0.40,
		},
	)

	require.Equal(t, 0.02, s.lossGraceRatio())
	require.Equal(t, 0.03, s.lossSoftThreshold())
	require.Equal(t, 0.04, s.lossSevereThreshold())
	require.Equal(t, 0.04, s.emergencyLossThreshold())
	require.Equal(t, 1.0, s.lossEWMAAlpha())
	require.Equal(t, 0.50, s.maxLossCwndCutNoQueue())
	require.Equal(t, 0.40, s.maxLossCwndCutWithQueue())
	require.Equal(t, 0.10, s.minLossCwndCut())
	require.Equal(t, 0.50, s.maxLossPacingCutNoQueue())
	require.Equal(t, 0.40, s.maxLossPacingCutWithQueue())
	require.Equal(t, uint32(4), s.lossRecoveryProbeRounds())
	require.Equal(t, 2.0, s.lossRecoveryProbeGain())
	require.Equal(t, uint64(3), s.lossRecoveryProbeDurationRounds())
	require.Equal(t, 0.50, s.lossRecoveryClearShortBwFraction())
}

func TestAdaptiveBDPLossPressure(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:              true,
			LossSoftThreshold:   0.01,
			LossSevereThreshold: 0.05,
		},
	)

	require.Zero(t, s.lossPressure(0.01))
	require.InDelta(t, 0.25, s.lossPressure(0.02), 0.001)
	require.InDelta(t, 0.0625, s.squaredLossPressure(0.02), 0.001)
	require.Equal(t, 1.0, s.lossPressure(0.05))
	require.Equal(t, 1.0, s.squaredLossPressure(0.10))
}

func TestAdaptiveBDPLossCwndMultiplierNoQueue(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.minRTT = 100 * time.Millisecond

	require.Equal(t, 1.0, s.lossCwndMultiplier(0.01))
	require.InDelta(t, 0.98125, s.lossCwndMultiplier(0.02), 0.0001)
	require.InDelta(t, 0.85, s.lossCwndMultiplier(0.05), 0.0001)
}

func TestAdaptiveBDPLossCwndMultiplierWithQueueAndECN(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(140*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:      true,
			QueueTarget: 20 * time.Millisecond,
		},
	)
	s.minRTT = 100 * time.Millisecond
	require.InDelta(t, 0.9628125, s.lossCwndMultiplier(0.02), 0.0001)

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s.rttStats = rttStats
	s.roundCount = 10
	s.lastECNCERound = 10
	s.hasLastECNCE = true
	require.InDelta(t, 0.971875, s.lossCwndMultiplier(0.02), 0.0001)
}

func TestAdaptiveBDPLossPacingMultiplierNoQueue(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)
	s.minRTT = 100 * time.Millisecond

	require.Equal(t, 1.0, s.lossPacingMultiplier(0.01))
	require.InDelta(t, 0.9890625, s.lossPacingMultiplier(0.02), 0.0001)
	require.InDelta(t, 0.90, s.lossPacingMultiplier(0.05), 0.0001)
}

func TestAdaptiveBDPLossPacingMultiplierWithQueueAndECN(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(140*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:      true,
			QueueTarget: 20 * time.Millisecond,
		},
	)
	s.minRTT = 100 * time.Millisecond
	require.InDelta(t, 0.97203125, s.lossPacingMultiplier(0.02), 0.0001)

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s.rttStats = rttStats
	s.roundCount = 10
	s.lastECNCERound = 10
	s.hasLastECNCE = true
	require.InDelta(t, 0.9796875, s.lossPacingMultiplier(0.02), 0.0001)
}

func TestAdaptiveBDPLossEligibility(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{Enable: true},
	)

	s.ackedBytesThisRound = 100_000
	s.lostBytesThisRound = 214
	require.False(t, s.canReactToLoss())
	require.Equal(t, "loss_below_absolute_threshold", s.lastLossActionReason)

	s.lostBytesThisRound = 3 * 1280
	s.ackedBytesThisRound = 10_000
	require.False(t, s.canReactToLoss())
	require.Equal(t, "loss_sample_too_small", s.lastLossActionReason)

	s.ackedBytesThisRound = 100_000
	require.True(t, s.canReactToLoss())
}

func TestAdaptiveBDPMaterialLossRoundAccountingHelpers(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:             true,
			LossGraceRatio:     0.01,
			MinLossSampleBytes: 64 * 1024,
		},
	)

	s.ackedBytesThisRound = 100_000
	s.lostBytesThisRound = 214
	require.False(t, s.roundHasMaterialLoss())

	s.lostBytesThisRound = 3 * 1280
	s.ackedBytesThisRound = 10_000
	require.False(t, s.roundHasMaterialLoss())

	s.lostBytesThisRound = 1_000
	s.ackedBytesThisRound = 99_000
	require.False(t, s.roundHasMaterialLoss())

	s.lostBytesThisRound = 3_000
	s.ackedBytesThisRound = 97_000
	require.True(t, s.roundHasMaterialLoss())

	s.lossFreeRounds = 3
	s.lossRecoveryProbeActive = true
	s.lossRecoveryProbeBW = 1_750_000
	s.roundCount = 12
	s.noteMaterialLossRound()
	require.Zero(t, s.lossFreeRounds)
	require.Equal(t, uint64(12), s.lastMaterialLossRound)
	require.True(t, s.hasMaterialLossRound)
	require.False(t, s.lossRecoveryProbeActive)
	require.Zero(t, s.lossRecoveryProbeBW)

	s.ackedBytesThisRound = 10_000
	s.noteLossFreeRound()
	require.Zero(t, s.lossFreeRounds)

	s.ackedBytesThisRound = 100_000
	s.noteLossFreeRound()
	require.Equal(t, uint32(1), s.lossFreeRounds)
}

func TestAdaptiveBDPMildLossWaitsForPersistenceWithoutQueue(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                   true,
			MinLossSampleBytes:       64 * 1024,
			MildLossPersistentRounds: 2,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.probeUpActive = true
	s.ackedBytesThisRound = 100_000
	oldCwnd := s.congestionWindow

	s.OnCongestionEvent(1, 3_000, 64*1280)
	require.Equal(t, oldCwnd, s.congestionWindow)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.False(t, s.probeUpActive)
	require.Equal(t, uint32(1), s.mildLossRounds)
	require.False(t, s.hasLastLossCutbackRound)
	require.Equal(t, "mild_loss_waiting_persistence", s.lastLossActionReason)
	require.Equal(t, "mild_loss_waiting_persistence", s.lastStateChangeReason)
}

func TestAdaptiveBDPLossEWMA(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:        true,
			LossEWMAAlpha: 0.25,
		},
	)
	s.ackedBytesThisRound = 95_000
	s.lostBytesThisRound = 5_000
	s.updateLossEWMA()
	require.InDelta(t, 0.05, s.lossRatioEWMA, 0.001)

	s.ackedBytesThisRound = 98_000
	s.lostBytesThisRound = 2_000
	s.updateLossEWMA()
	require.InDelta(t, 0.0425, s.lossRatioEWMA, 0.001)
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
	s.queueHighRounds = s.queuePersistentRounds()
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

func TestAdaptiveBDPUploadWarmupSuppressesHardDownshift(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
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
			DownshiftRatio:  0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.queueHighRounds = s.queuePersistentRounds()

	s.OnPacketSent(start, 0, 1, 1280, true)
	require.True(t, s.inUploadWarmup(start))

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		600*1280,
		start.Add(100*time.Millisecond),
		RateSample{
			DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(5)),
			AckedBytes:    1280,
			PriorInFlight: 600 * 1280,
			Interval:      100 * time.Millisecond,
			RTT:           200 * time.Millisecond,
			IsValid:       true,
		},
	)

	require.Zero(t, s.shortBw)
	require.Equal(t, uint32(0), s.downshiftRounds)
	require.Equal(t, "upload_warmup_low_sample_not_capacity_proof", s.lastBWChangeReason)
}

func TestAdaptiveBDPConfirmedDownshiftAfterUploadWarmup(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
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
			DownshiftRatio:  0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.queueHighRounds = s.queuePersistentRounds()
	s.uploadWarmupStartTime = start.Add(-2 * time.Second)
	s.uploadWarmupAcked = s.uploadWarmupBytes()
	require.False(t, s.inUploadWarmup(start))

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		600*1280,
		start,
		RateSample{
			DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(5)),
			AckedBytes:    1280,
			PriorInFlight: 600 * 1280,
			Interval:      100 * time.Millisecond,
			RTT:           200 * time.Millisecond,
			IsValid:       true,
		},
	)

	require.Greater(t, s.shortBw, uint64(0))
	require.Equal(t, min(s.maxBw, s.shortBw), s.bw)
	require.Equal(t, "short_bw_downshift_with_congestion_evidence", s.lastBWChangeReason)
}

func TestAdaptiveBDPDownshiftConfigKnobs(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                    true,
			DownshiftRatio:            0.75,
			UploadWarmupDuration:      250 * time.Millisecond,
			UploadWarmupBytes:         12 * 1280,
			MinDownshiftSampleBytes:   4 * 1280,
			CongestionDownshiftRounds: 2,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.queueHighRounds = s.queuePersistentRounds()

	s.OnPacketSent(start, 0, 1, 1280, true)
	clock.Advance(300 * time.Millisecond)
	s.uploadWarmupAcked = 12 * 1280
	require.False(t, s.inUploadWarmup(start.Add(300*time.Millisecond)))

	lowSample := RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(5)),
		AckedBytes:   2 * 1280,
		RTT:          200 * time.Millisecond,
		IsValid:      true,
	}
	s.updateBandwidthAt(lowSample, 600*1280, start.Add(300*time.Millisecond))
	require.Zero(t, s.shortBw)
	require.Equal(t, "low_sample_no_queue_rejected", s.lastBWChangeReason)

	lowSample.AckedBytes = 4 * 1280
	s.updateBandwidthAt(lowSample, 600*1280, start.Add(400*time.Millisecond))
	require.Zero(t, s.shortBw)
	require.Equal(t, uint32(1), s.downshiftRounds)
	require.Equal(t, "congestion_downshift_waiting_rounds", s.lastBWChangeReason)

	s.updateBandwidthAt(lowSample, 600*1280, start.Add(500*time.Millisecond))
	require.Greater(t, s.shortBw, uint64(0))
	require.Equal(t, uint32(2), s.downshiftRounds)
	require.Equal(t, "short_bw_downshift_with_congestion_evidence", s.lastBWChangeReason)
}

func TestAdaptiveBDPIgnoresSelfLimitedLowSampleForDownshift(t *testing.T) {
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

func TestAdaptiveBDPLowSampleWithoutQueueDoesNotDownshift(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:               true,
			DownshiftRounds:      1,
			DownshiftRatio:       0.75,
			StartupTargetRateBps: 30_000_000,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.updatePacingRate()

	oldBW := s.bw
	oldRate := s.pacingRateBytesPerSecond
	s.OnPacketAckedWithRateSample(
		1,
		64*1280,
		245_048,
		start,
		RateSample{
			DeliveryRate:  293_940,
			AckedBytes:    64 * 1280,
			PriorInFlight: 245_048,
			Interval:      150 * time.Millisecond,
			RTT:           150 * time.Millisecond,
			IsValid:       true,
		},
	)

	require.Equal(t, oldBW, s.bw)
	require.Zero(t, s.shortBw)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, "queue_empty_low_sample_not_capacity_proof", s.lastBWChangeReason)
	require.GreaterOrEqual(t, s.pacingRateBytesPerSecond, oldRate/2)
	require.False(t, s.hasCongestionEvidence())
	require.Equal(t, adaptiveQueueEmpty, s.queueState())
}

func TestAdaptiveBDPPipeFilledForDownshiftUsesKnownTarget(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:               true,
			StartupTargetRateBps: 30_000_000,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 293_940
	s.maxBw = 293_940
	s.shortBw = 293_940
	s.congestionWindow = 64 * 1280

	expectedPipe := roundUpToMSS(protocol.ByteCount(float64(30_000_000/8)*0.150), 1280)
	require.Equal(t, expectedPipe, s.pipeForDownshift())
	require.Equal(t, protocol.ByteCount(float64(expectedPipe)*0.90), s.pipeFillThreshold())
	require.False(t, s.isPipeFilledForDownshift(245_048))

	priorNearNoQueueThreshold := protocol.ByteCount(float64(expectedPipe)*0.90) - 2*s.maxDatagramSize
	require.True(t, s.isPipeFilledForDownshift(priorNearNoQueueThreshold))

	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s.rttStats = rttStats
	s.minRTT = 150 * time.Millisecond
	s.queueHighRounds = s.queuePersistentRounds()
	require.True(t, s.hasCongestionEvidence())
	require.Equal(t, protocol.ByteCount(float64(expectedPipe)*0.75), s.pipeFillThreshold())
}

func TestAdaptiveBDPHighSampleWithoutQueueRaisesMaxBW(t *testing.T) {
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
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw

	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(40)),
		AckedBytes:   64 * 1280,
		RTT:          150 * time.Millisecond,
		IsValid:      true,
	}, 64*1280)

	require.Equal(t, mbitToBytesPerSecond(40), s.maxBw)
	require.Equal(t, mbitToBytesPerSecond(40), s.bw)
	require.Equal(t, "max_bw_increased_by_delivery_sample", s.lastBWChangeReason)
	require.False(t, s.hasCongestionEvidence())
}

func TestAdaptiveBDPLowSampleWithoutEvidenceStartsCandidate(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRatio:  0.75,
			DownshiftRounds: 1,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280

	s.updateBandwidthAt(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		AckedBytes:   64 * 1280,
		RTT:          150 * time.Millisecond,
		IsValid:      true,
	}, 430*1280, start)

	require.Equal(t, adaptiveQueueEmpty, s.queueState())
	require.False(t, s.hasCongestionEvidence())
	require.Zero(t, s.shortBw)
	require.Equal(t, mbitToBytesPerSecond(30), s.bw)
	require.True(t, s.noQueueLow.active)
	require.Equal(t, "low_sample_no_queue_candidate_started", s.lastBWChangeReason)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
}

func TestAdaptiveBDPLowSampleBuildingQueueRejected(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(115*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRatio:  0.75,
			DownshiftRounds: 1,
			QueueTarget:     20 * time.Millisecond,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280

	s.updateBandwidthAt(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		AckedBytes:   64 * 1280,
		RTT:          115 * time.Millisecond,
		IsValid:      true,
	}, 430*1280, start)

	require.Equal(t, adaptiveQueueBuilding, s.queueState())
	require.False(t, s.hasCongestionEvidence())
	require.Zero(t, s.shortBw)
	require.Equal(t, "low_sample_no_queue_rejected", s.lastBWChangeReason)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
}

func TestAdaptiveBDPLowSamplePipeNotFilledReason(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(115*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:          true,
			DownshiftRatio:  0.75,
			DownshiftRounds: 1,
			QueueTarget:     20 * time.Millisecond,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280

	s.updateBandwidthAt(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		AckedBytes:   64 * 1280,
		RTT:          115 * time.Millisecond,
		IsValid:      true,
	}, 64*1280, start)

	require.Equal(t, adaptiveQueueBuilding, s.queueState())
	require.False(t, s.isPipeFilledForDownshift(64*1280))
	require.Zero(t, s.shortBw)
	require.Equal(t, "pipe_not_filled_for_downshift", s.lastBWChangeReason)
}

func TestAdaptiveBDPNoQueueLowCandidateWaitsForConfirmation(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                      true,
			DownshiftRatio:              0.75,
			NoCongestionDownshiftRounds: 4,
			NoCongestionDownshiftFactor: 0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	priorInFlight := protocol.ByteCount(430 * 1280)

	for i := 0; i < 2; i++ {
		s.roundCount = uint64(i)
		s.updateBandwidthAt(RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
			AckedBytes:   100 * 1280,
			RTT:          150 * time.Millisecond,
			IsValid:      true,
		}, priorInFlight, start.Add(time.Duration(i)*150*time.Millisecond))
	}
	require.Zero(t, s.shortBw)
	require.Equal(t, uint32(2), s.noQueueLow.rounds)
	require.Equal(t, "low_sample_no_queue_candidate_waiting_rounds", s.lastBWChangeReason)

	for i := 2; i < 4; i++ {
		s.roundCount = uint64(i)
		s.updateBandwidthAt(RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
			AckedBytes:   100 * 1280,
			RTT:          150 * time.Millisecond,
			IsValid:      true,
		}, priorInFlight, start.Add(time.Duration(i)*150*time.Millisecond))
	}
	require.Zero(t, s.shortBw)
	require.Equal(t, uint32(4), s.noQueueLow.rounds)
	require.Equal(t, "low_sample_no_queue_candidate_waiting_duration", s.lastBWChangeReason)

	s2 := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                      true,
			DownshiftRatio:              0.75,
			NoCongestionDownshiftRounds: 4,
			NoCongestionDownshiftFactor: 0.75,
		},
	)
	s2.minRTT = 150 * time.Millisecond
	s2.state = adaptiveBDPProbeBW
	s2.bw = mbitToBytesPerSecond(30)
	s2.maxBw = s2.bw
	s2.congestionWindow = 800 * 1280
	for i := 0; i < 4; i++ {
		s2.roundCount = uint64(i)
		s2.updateBandwidthAt(RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
			AckedBytes:   1280,
			RTT:          150 * time.Millisecond,
			IsValid:      true,
		}, priorInFlight, start.Add(time.Duration(i)*400*time.Millisecond))
	}
	require.Zero(t, s2.shortBw)
	require.Equal(t, uint32(4), s2.noQueueLow.rounds)
	require.Equal(t, "low_sample_no_queue_candidate_waiting_bytes", s2.lastBWChangeReason)
}

func TestAdaptiveBDPDownshiftsOnPipeFilledBandwidthDrop(t *testing.T) {
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
			DownshiftRounds: 2,
			DownshiftRatio:  0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.queueHighRounds = s.queuePersistentRounds()
	priorInFlight := protocol.ByteCount(360 * 1280)

	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		IsValid:      true,
	}, priorInFlight)
	require.Greater(t, s.shortBw, uint64(0))
	require.GreaterOrEqual(t, s.shortBw, uint64(mbitToBytesPerSecond(10)))
	require.Less(t, s.shortBw, uint64(mbitToBytesPerSecond(30)))
	require.Equal(t, min(s.maxBw, s.shortBw), s.bw)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Equal(t, "short_bw_downshift_with_congestion_evidence", s.lastBWChangeReason)
}

func TestAdaptiveBDPQueuePersistentConfirmsDownshift(t *testing.T) {
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
			DownshiftRatio:  0.75,
			DownshiftRounds: 1,
			QueueTarget:     20 * time.Millisecond,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.queueHighRounds = s.queuePersistentRounds()

	s.updateBandwidth(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		AckedBytes:   64 * 1280,
		RTT:          200 * time.Millisecond,
		IsValid:      true,
	}, 360*1280)

	require.True(t, s.hasCongestionEvidence())
	require.Equal(t, adaptiveQueuePersistent, s.queueState())
	require.Greater(t, s.shortBw, uint64(0))
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Equal(t, "short_bw_downshift_with_congestion_evidence", s.lastBWChangeReason)
}

func TestAdaptiveBDPCongestionDownshiftInterpolatesByConfidence(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(120*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			DownshiftRatio:         0.75,
			DownshiftRounds:        1,
			LossTarget:             0.01,
			EmergencyLossThreshold: 0.05,
		},
	)
	s.minRTT = 100 * time.Millisecond
	rttStats = utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s.rttStats = rttStats
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 2000 * 1280
	s.ackedBytesThisRound = 97_000
	s.lostBytesThisRound = 3_000

	sampleBW := mbitToBytesPerSecond(20)
	confidence := s.negativeBandwidthConfidence()
	require.InDelta(t, 0.5, confidence, 0.001)
	expected := uint64(float64(s.maxBw)*(1-confidence) + float64(sampleBW)*confidence)
	expected = max(expected, s.minimumObservableBandwidth())

	s.updateBandwidthAt(RateSample{
		DeliveryRate: protocol.ByteCount(sampleBW),
		AckedBytes:   100 * 1280,
		RTT:          120 * time.Millisecond,
		IsValid:      true,
	}, 1200*1280, start)

	require.Equal(t, expected, s.shortBw)
	require.Equal(t, expected, s.bw)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Equal(t, "short_bw_downshift_with_congestion_evidence", s.lastStateChangeReason)
	require.Equal(t, "short_bw_downshift_with_congestion_evidence", s.lastBWChangeReason)
}

func TestAdaptiveBDPNoQueueLowSamplesDownshiftGradually(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                        true,
			DownshiftRatio:                0.75,
			NoCongestionDownshiftRounds:   4,
			NoCongestionDownshiftFactor:   0.75,
			StartupTargetRateBps:          30_000_000,
			NoCongestionRateFloorFraction: 0.5,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	priorInFlight := protocol.ByteCount(430 * 1280)

	for i := 0; i < 4; i++ {
		s.roundCount = uint64(i)
		now := start.Add(time.Duration(i) * 400 * time.Millisecond)
		clock = mockClock(now)
		s.updateBandwidthAt(RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
			AckedBytes:   100 * 1280,
			RTT:          150 * time.Millisecond,
			IsValid:      true,
		}, priorInFlight, now)
	}

	require.Equal(t, adaptiveQueueEmpty, s.queueState())
	require.False(t, s.hasCongestionEvidence())
	require.Greater(t, s.shortBw, uint64(0))
	require.GreaterOrEqual(t, s.shortBw, uint64(float64(mbitToBytesPerSecond(30))*0.75))
	require.Equal(t, "short_bw_gradual_no_queue_downshift", s.lastBWChangeReason)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
}

func TestAdaptiveBDPDoesNotCollapseOnNoQueueNoLossShortBWDownshift(t *testing.T) {
	start := monotime.Now()
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(151*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                        true,
			InitialWindowPackets:          256,
			MinWindowPackets:              32,
			StartupTargetRateBps:          30_000_000,
			CruiseCwndGain:                1.5,
			NoCongestionRateFloorFraction: 0.5,
			NoCongestionDownshiftFactor:   0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 30_000_000 / 8
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.updatePacingRate()
	oldCwnd := s.congestionWindow
	floorRate := s.noCongestionRateFloorBytesPerSecond()
	require.Equal(t, uint64(15_000_000/8), floorRate)

	s.OnPacketAckedWithRateSample(
		1,
		64*1280,
		245_048,
		start,
		RateSample{
			IsValid:        true,
			DeliveryRate:   293_940,
			DeliveredDelta: 64 * 1024,
			AckedBytes:     64 * 1024,
			PriorInFlight:  245_048,
			Interval:       150 * time.Millisecond,
			RTT:            151 * time.Millisecond,
		},
	)

	require.False(t, s.hasCongestionEvidence())
	require.False(t, s.isPipeFilledForDownshift(245_048))
	require.NotEqual(t, uint64(293_940), s.shortBw)
	require.Zero(t, s.shortBw)
	require.GreaterOrEqual(t, s.pacingRateBytesPerSecond, floorRate)
	require.Greater(t, s.congestionWindow, protocol.ByteCount(53*1024))
	require.GreaterOrEqual(t, s.congestionWindow, oldCwnd)
	require.NotEqual(t, "short_bw_downshift_pipe_filled", s.lastBWChangeReason)
}

func TestAdaptiveBDPPipeFilledUsesStartupTargetOrPreviousEstimate(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:               true,
			StartupTargetRateBps: 30_000_000,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 293_940
	s.maxBw = 293_940
	s.shortBw = 293_940

	pipe := s.pipeForDownshift()
	require.InDelta(t, 562_500, float64(pipe), 1280)
	require.InDelta(t, 506_250, float64(s.pipeFillThreshold()), 1280)
	require.False(t, s.hasCongestionEvidence())
	require.False(t, s.isPipeFilledForDownshift(245_048))
}

func TestAdaptiveBDPNoCongestionDownshiftIsGradual(t *testing.T) {
	start := monotime.Now()
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                      true,
			DownshiftRatio:              0.75,
			NoCongestionDownshiftRounds: 4,
			NoCongestionDownshiftFactor: 0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 30_000_000 / 8
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280

	for i := 0; i < 4; i++ {
		s.roundCount = uint64(i)
		s.updateBandwidthAt(RateSample{
			IsValid:      true,
			DeliveryRate: 293_940,
			AckedBytes:   100 * 1280,
			RTT:          150 * time.Millisecond,
		}, 600*1280, start.Add(time.Duration(i)*400*time.Millisecond))
	}
	s.updatePacingRate()

	minFirstDownshift := uint64(float64(30_000_000/8) * 0.75)
	require.False(t, s.hasCongestionEvidence())
	require.GreaterOrEqual(t, s.shortBw, minFirstDownshift)
	require.GreaterOrEqual(t, s.pacingRateBytesPerSecond, uint64(float64(minFirstDownshift)*0.90))
	require.Greater(t, s.targetCwnd(), protocol.ByteCount(500*1024))
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, "short_bw_gradual_no_queue_downshift", s.lastBWChangeReason)
}

func TestAdaptiveBDPCongestionConfirmedDownshiftCanBeHard(t *testing.T) {
	start := monotime.Now()
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
			DownshiftRatio:  0.75,
			DownshiftRounds: 1,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 30_000_000 / 8
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.hasLastECNCE = true
	s.lastECNCERound = s.roundCount
	s.updatePacingRate()
	oldPacing := s.pacingRateBytesPerSecond

	s.updateBandwidthAt(RateSample{
		IsValid:      true,
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(5)),
		AckedBytes:   100 * 1280,
		RTT:          150 * time.Millisecond,
	}, 600*1280, start)
	s.updatePacingRate()
	s.reduceCwndTowardTarget(600*1280, false)

	require.True(t, s.hasCongestionEvidence())
	require.Equal(t, mbitToBytesPerSecond(5), s.shortBw)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.Less(t, s.pacingRateBytesPerSecond, oldPacing)
	require.Less(t, s.congestionWindow, protocol.ByteCount(800*1280))
	require.Equal(t, "short_bw_downshift_with_congestion_evidence", s.lastBWChangeReason)
}

func TestAdaptiveBDPTinyAbsoluteLossDoesNotEmergencyCutback(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			EmergencyLossThreshold: 0.001,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.ackedBytesThisRound = 150_000
	oldCwnd := s.congestionWindow

	s.OnCongestionEvent(1, 214, 64*1280)

	require.False(t, s.shouldEmergencyCutback(s.roundLossRatio()))
	require.Equal(t, oldCwnd, s.congestionWindow)
	require.False(t, s.hasLastEmergencyCutback)
	require.Equal(t, "loss_below_absolute_threshold", s.lastLossActionReason)
}

func TestAdaptiveBDPDownloadToUploadWarmupPreventsImmediateHardDownshift(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                        true,
			InitialWindowPackets:          256,
			MinWindowPackets:              32,
			StartupTargetRateBps:          30_000_000,
			CruiseCwndGain:                1.5,
			NoCongestionRateFloorFraction: 0.5,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 30_000_000 / 8
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	floorRate := s.noCongestionRateFloorBytesPerSecond()
	floorCwnd := s.noCongestionCwndFloor()

	s.OnPacketSent(start, 0, 1, 1280, true)
	require.True(t, s.inUploadWarmup(start))
	for i, rateMbit := range []float64{5, 10} {
		now := start.Add(time.Duration(i+1) * 200 * time.Millisecond)
		clock = mockClock(now)
		s.OnPacketAckedWithRateSample(
			protocol.PacketNumber(i+1),
			64*1280,
			245_048,
			now,
			RateSample{
				IsValid:      true,
				DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(rateMbit)),
				AckedBytes:   64 * 1280,
				RTT:          150 * time.Millisecond,
			},
		)
	}

	require.True(t, s.inUploadWarmup(start.Add(400*time.Millisecond)))
	require.Zero(t, s.shortBw)
	require.GreaterOrEqual(t, s.pacingRateBytesPerSecond, floorRate)
	require.GreaterOrEqual(t, s.congestionWindow, floorCwnd)
	require.Equal(t, "upload_warmup_low_sample_not_capacity_proof", s.lastBWChangeReason)
}

func TestAdaptiveBDPRealNoQueueBandwidthDropEventuallyAdaptsGradually(t *testing.T) {
	start := monotime.Now()
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                      true,
			DownshiftRatio:              0.75,
			NoCongestionDownshiftRounds: 4,
			NoCongestionDownshiftFactor: 0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 30_000_000 / 8
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	priorInFlight := protocol.ByteCount(600 * 1280)
	sampleBW := mbitToBytesPerSecond(10)

	var observed []uint64
	for cycle := 0; cycle < 4; cycle++ {
		for i := 0; i < 4; i++ {
			round := cycle*4 + i
			s.roundCount = uint64(round)
			s.updateBandwidthAt(RateSample{
				IsValid:      true,
				DeliveryRate: protocol.ByteCount(sampleBW),
				AckedBytes:   100 * 1280,
				RTT:          150 * time.Millisecond,
			}, priorInFlight, start.Add(time.Duration(round)*400*time.Millisecond))
		}
		observed = append(observed, s.shortBw)
		require.False(t, s.hasCongestionEvidence())
		require.Equal(t, adaptiveBDPProbeBW, s.state)
	}

	require.GreaterOrEqual(t, observed[0], uint64(float64(30_000_000/8)*0.75))
	require.Less(t, observed[1], observed[0])
	require.Less(t, observed[2], observed[1])
	require.Equal(t, sampleBW, observed[3])
	require.NotEqual(t, uint64(293_940), observed[0])
}

func TestAdaptiveBDPStartupTargetFloorDisabledOnCongestion(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                        true,
			StartupTargetRateBps:          30_000_000,
			NoCongestionRateFloorFraction: 0.5,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 1

	floor := s.noCongestionRateFloorBytesPerSecond()
	require.Equal(t, uint64(30_000_000/8/2), floor)
	s.updatePacingRate()
	require.GreaterOrEqual(t, s.pacingRateBytesPerSecond, floor)

	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s.minRTT = 100 * time.Millisecond
	s.queueHighRounds = s.queuePersistentRounds()
	require.True(t, s.hasCongestionEvidence())
	require.Zero(t, s.noCongestionRateFloorBytesPerSecond())
	s.updatePacingRate()
	require.Less(t, s.pacingRateBytesPerSecond, floor)
}

func TestAdaptiveBDPNoCongestionFloorRaisesTargetCwnd(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                        true,
			StartupTargetRateBps:          30_000_000,
			NoCongestionRateFloorFraction: 0.5,
			CruiseCwndGain:                1.5,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 1

	floor := s.noCongestionRateFloorBytesPerSecond()
	require.Equal(t, uint64(30_000_000/8/2), floor)
	floorBDP := s.bdpForBandwidth(floor)
	expected := roundUpToMSS(protocol.ByteCount(float64(floorBDP)*s.cruiseCwndGain()), s.maxDatagramSize)
	require.Equal(t, expected, s.targetCwnd())

	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s.minRTT = 100 * time.Millisecond
	s.queueHighRounds = s.queuePersistentRounds()
	require.True(t, s.hasCongestionEvidence())
	require.Less(t, s.targetCwnd(), expected)
}

func TestAdaptiveBDPMinProbeRateOverridesStartupFloor(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                        true,
			StartupTargetRateBps:          30_000_000,
			MinProbeRateBps:               20_000_000,
			NoCongestionRateFloorFraction: 0.5,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 1

	require.Equal(t, uint64(20_000_000/8), s.noCongestionRateFloorBytesPerSecond())
	s.updatePacingRate()
	require.GreaterOrEqual(t, s.pacingRateBytesPerSecond, uint64(20_000_000/8))
}

func TestAdaptiveBDPDownshiftDropsPacingWithinThreeRTT(t *testing.T) {
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
			DownshiftRounds: 3,
			DownshiftRatio:  0.75,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = mbitToBytesPerSecond(30)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	s.queueHighRounds = s.queuePersistentRounds()
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
	require.Less(t, s.congestionWindow, oldCwnd)
	require.GreaterOrEqual(t, s.congestionWindow, s.minCongestionWindow)
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
	now := monotime.Now()
	require.False(t, s.shouldEnterProbeDown(sample, 64*1280, now))
	require.True(t, s.shouldEnterProbeDown(sample, 64*1280, now))
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
	priorInFlight := protocol.ByteCount(430 * 1280)
	now := monotime.Now()

	enter := s.shouldEnterProbeDown(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		IsValid:      true,
	}, priorInFlight, now)

	require.False(t, enter)
	require.Equal(t, uint32(1), s.downshiftRounds)

	s.downshiftRounds = 2
	enter = s.shouldEnterProbeDown(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(10)),
		IsValid:      true,
	}, priorInFlight, now)

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

func TestAdaptiveBDPRoundAccountingTracksMaterialAndLossFreeRounds(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:             true,
			LossGraceRatio:     0.01,
			MinLossSampleBytes: 64 * 1024,
		},
	)
	base := monotime.Now()
	s.minRTT = 100 * time.Millisecond
	s.lastRoundStartTime = base
	s.lossFreeRounds = 2
	s.lossRecoveryProbeActive = true
	s.lossRecoveryProbeBW = 1_750_000

	s.ackedBytesThisRound = 97_000
	s.lostBytesThisRound = 3_000
	s.updateRound(RateSample{AckedBytes: 1280}, 64*1280, base.Add(150*time.Millisecond))
	require.True(t, s.roundStart)
	require.Equal(t, uint64(1), s.roundCount)
	require.Zero(t, s.lossFreeRounds)
	require.True(t, s.hasMaterialLossRound)
	require.Equal(t, uint64(1), s.lastMaterialLossRound)
	require.False(t, s.lossRecoveryProbeActive)
	require.Zero(t, s.lossRecoveryProbeBW)
	require.Zero(t, s.ackedBytesThisRound)
	require.Zero(t, s.lostBytesThisRound)

	s.ackedBytesThisRound = 100_000
	s.lostBytesThisRound = 0
	s.updateRound(RateSample{AckedBytes: 1280}, 64*1280, base.Add(300*time.Millisecond))
	require.Equal(t, uint64(2), s.roundCount)
	require.Equal(t, uint32(1), s.lossFreeRounds)
	require.Zero(t, s.ackedBytesThisRound)
	require.Zero(t, s.lostBytesThisRound)

	s.ackedBytesThisRound = 10_000
	s.lostBytesThisRound = 0
	s.updateRound(RateSample{AckedBytes: 1280}, 64*1280, base.Add(450*time.Millisecond))
	require.Equal(t, uint64(3), s.roundCount)
	require.Equal(t, uint32(1), s.lossFreeRounds)
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
		CwndTuningConfig{Enable: true, LossTarget: 0.005, MinLossSampleBytes: 64 * 1024},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 128 * 1280
	oldCwnd := s.congestionWindow
	s.ackedBytesThisRound = 100_000
	s.OnCongestionEvent(1, 3_000, 64*1280)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.LessOrEqual(t, s.congestionWindow, oldCwnd)
}

func TestHealthyBandwidthGetsProportionalLossCutback(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                   true,
			LossTarget:               0.005,
			EmergencyLossThreshold:   1.0,
			MinLossSampleBytes:       64 * 1024,
			MildLossPersistentRounds: 1,
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
	s.OnCongestionEvent(1, 3_000, 64*1280)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Less(t, s.congestionWindow, cwndBeforeLoss)
	require.Zero(t, s.shortBw)
	require.True(t, s.hasLastLossCutbackRound)
	require.False(t, s.hasLastEmergencyCutback)
	require.Equal(t, "proportional_loss_cutback", s.lastLossActionReason)

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
}

func TestLossCutbackWinsWhenBandwidthDrops(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                   true,
			LossTarget:               0.005,
			EmergencyLossThreshold:   1.0,
			MinLossSampleBytes:       64 * 1024,
			MildLossPersistentRounds: 1,
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

	s.OnCongestionEvent(1, 3_000, 64*1280)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.True(t, s.hasLastLossCutbackRound)
	require.Zero(t, s.shortBw)
	require.Equal(t, "proportional_loss_cutback", s.lastLossActionReason)
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
			MinLossSampleBytes:     64 * 1024,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 4000 * 1280
	s.ackedBytesThisRound = 100_000

	s.OnCongestionEvent(1, 3_000, 64*1280)
	cwndAfterFirstLoss := s.congestionWindow
	shortBwAfterFirstLoss := s.shortBw
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.True(t, s.hasLastLossCutbackRound)

	s.OnCongestionEvent(2, 50_000, 64*1280)
	require.Equal(t, cwndAfterFirstLoss, s.congestionWindow)
	require.Equal(t, shortBwAfterFirstLoss, s.shortBw)
	require.Equal(t, "loss_cutback_cooldown", s.lastLossActionReason)
	require.Equal(t, "loss_cutback_cooldown", s.lastStateChangeReason)
}

func TestAdaptiveBDPLossCutbackCooldown(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                   true,
			LossTarget:               0.005,
			EmergencyLossThreshold:   1.0,
			MinLossSampleBytes:       64 * 1024,
			MildLossPersistentRounds: 1,
			LossCutbackCooldown:      200 * time.Millisecond,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.ackedBytesThisRound = 100_000

	s.OnCongestionEvent(1, 3_000, 64*1280)
	cwndAfterFirstLoss := s.congestionWindow
	shortBwAfterFirstLoss := s.shortBw
	require.True(t, s.hasLastLossCutbackRound)
	require.Equal(t, start, s.lastLossCutbackTime)

	s.roundCount++
	clock.Advance(100 * time.Millisecond)
	s.OnCongestionEvent(2, 3_000, 64*1280)
	require.Equal(t, cwndAfterFirstLoss, s.congestionWindow)
	require.Equal(t, shortBwAfterFirstLoss, s.shortBw)
	require.Equal(t, "loss_cutback_cooldown", s.lastLossActionReason)

	s.roundCount++
	clock.Advance(100 * time.Millisecond)
	s.OnCongestionEvent(3, 3_000, 64*1280)
	require.True(t, s.lastLossCutbackTime.After(start))
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
		CwndTuningConfig{Enable: true, EmergencyLossThreshold: 0.02, MinLossSampleBytes: 64 * 1024},
	)
	s.minRTT = 200 * time.Millisecond
	s.congestionWindow = 128 * 1280
	s.ackedBytesThisRound = 100_000
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
			MinLossSampleBytes:     64 * 1024,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 128 * 1280
	s.ackedBytesThisRound = 100_000

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
			Enable:                   true,
			LossTarget:               0.005,
			EmergencyLossThreshold:   1.0,
			MinLossSampleBytes:       64 * 1024,
			MildLossPersistentRounds: 1,
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

	s.OnCongestionEvent(1, 3_000, 64*1280)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.True(t, s.hasLastLossCutbackRound)
	require.Zero(t, s.shortBw)
	require.Less(t, s.pacingRateBytesPerSecond, oldRate)
	require.Equal(t, "proportional_loss_cutback", s.lastLossActionReason)
	require.Equal(t, "proportional_loss_no_queue", s.lastStateChangeReason)
}

func TestAdaptiveBDPProportionalLossNoQueue(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(200*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                   true,
			EmergencyLossThreshold:   1.0,
			MinLossSampleBytes:       64 * 1024,
			MildLossPersistentRounds: 1,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.ackedBytesThisRound = 100_000
	s.updatePacingRate()

	oldCwnd := s.congestionWindow
	oldPacing := s.pacingRateBytesPerSecond
	s.OnCongestionEvent(1, 3_000, 64*1280)

	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Less(t, s.congestionWindow, oldCwnd)
	require.Less(t, s.pacingRateBytesPerSecond, oldPacing)
	require.Zero(t, s.shortBw)
	require.True(t, s.hasLastLossCutbackRound)
	require.Equal(t, start, s.lastLossCutbackTime)
	require.Equal(t, "proportional_loss_cutback", s.lastLossActionReason)
	require.Equal(t, "proportional_loss_no_queue", s.lastStateChangeReason)
	require.InDelta(t, s.lossCwndMultiplier(max(s.roundLossRatio(), s.lossRatioEWMA)), s.lastLossCwndMultiplier, 0.000001)
	require.InDelta(t, s.lossPacingMultiplier(max(s.roundLossRatio(), s.lossRatioEWMA)), s.lastLossPacingMultiplier, 0.000001)
}

func TestAdaptiveBDPProportionalLossWithQueue(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(250*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			EmergencyLossThreshold: 1.0,
			MinLossSampleBytes:     64 * 1024,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	oldBw := s.bw
	oldMaxBw := s.maxBw
	s.congestionWindow = 400 * 1280
	s.ackedBytesThisRound = 100_000

	s.OnCongestionEvent(1, 3_000, 64*1280)

	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.True(t, s.hasLastLossCutbackRound)
	require.Greater(t, s.shortBw, uint64(0))
	require.Less(t, s.shortBw, oldBw)
	require.Equal(t, oldBw, s.bw)
	require.Equal(t, oldMaxBw, s.maxBw)
	require.Equal(t, "proportional_loss_cutback", s.lastLossActionReason)
	require.Equal(t, "proportional_loss_with_queue", s.lastStateChangeReason)
	require.Equal(t, "short_bw_proportional_loss_with_queue", s.lastBWChangeReason)
}

func TestAdaptiveBDPEmergencyLossProportional(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(300*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                 true,
			EmergencyLossThreshold: 0.10,
			MinLossSampleBytes:     64 * 1024,
			EmergencyLossMinBytes:  8 * 1280,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 200 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 400 * 1280
	s.ackedBytesThisRound = 100_000

	oldCwnd := s.congestionWindow
	oldBw := s.bw
	oldMaxBw := s.maxBw
	s.OnCongestionEvent(1, 40_000, 64*1280)

	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.True(t, s.hasLastLossCutbackRound)
	require.True(t, s.hasLastEmergencyCutback)
	require.Equal(t, protocol.ByteCount(float64(oldCwnd)*0.50), s.congestionWindow)
	require.Equal(t, uint64(float64(oldBw)*0.50), s.shortBw)
	require.Equal(t, s.shortBw, s.bw)
	require.Equal(t, oldMaxBw, s.maxBw)
	require.Equal(t, "emergency_loss_proportional", s.lastLossActionReason)
	require.Equal(t, "emergency_loss_proportional", s.lastStateChangeReason)
	require.InDelta(t, 0.50, s.lastLossCwndMultiplier, 0.000001)
	require.InDelta(t, 0.50, s.lastLossPacingMultiplier, 0.000001)
}

func newAdaptiveBDPLossRegressionSender(srtt, minRTT time.Duration, cfg CwndTuningConfig) *adaptiveBDPSender {
	if !cfg.Enable {
		cfg.Enable = true
	}
	clock := mockClock(monotime.Now())
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(srtt, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		cfg,
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = minRTT
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw
	s.congestionWindow = 800 * 1280
	return s
}

func TestAdaptiveBDPOnePercentLossNoQueueDoesNotCutCwnd(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(100*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                 true,
		LossGraceRatio:         0.01,
		EmergencyLossThreshold: 1.0,
		MinLossSampleBytes:     64 * 1024,
	})
	oldCwnd := s.congestionWindow
	oldShortBw := s.shortBw
	s.ackedBytesThisRound = 990 * 1280

	s.OnCongestionEvent(1, 10*1280, 64*1280)

	require.InDelta(t, 0.01, s.roundLossRatio(), 0.000001)
	require.Equal(t, oldCwnd, s.congestionWindow)
	require.Equal(t, "mild_loss_below_grace_no_cwnd_cut", s.lastLossActionReason)
	require.Equal(t, uint64(1), s.suppressProbeUpUntilRound)
	require.Equal(t, "mild_loss_below_grace_no_cwnd_cut", s.suppressProbeUpReason)
	require.Equal(t, oldShortBw, s.shortBw)
	require.False(t, s.hasLastLossCutbackRound)
}

func TestAdaptiveBDPTwoPercentLossNoQueueSmallCutback(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(100*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                   true,
		LossGraceRatio:           0.01,
		LossSevereThreshold:      0.05,
		MaxLossCwndCutNoQueue:    0.15,
		EmergencyLossThreshold:   1.0,
		MinLossSampleBytes:       64 * 1024,
		MildLossPersistentRounds: 1,
	})
	oldCwnd := s.congestionWindow
	oldShortBw := s.shortBw
	s.ackedBytesThisRound = 980 * 1280

	s.OnCongestionEvent(1, 20*1280, 64*1280)

	cut := float64(oldCwnd-s.congestionWindow) / float64(oldCwnd)
	require.Greater(t, cut, 0.0)
	require.Less(t, cut, 0.05)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, oldShortBw, s.shortBw)
	require.Equal(t, "proportional_loss_cutback", s.lastLossActionReason)
	require.Equal(t, "proportional_loss_no_queue", s.lastStateChangeReason)
}

func TestAdaptiveBDPOnePercentLossWithQueueModerateCutback(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(140*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                 true,
		LossGraceRatio:         0.005,
		QueueTarget:            20 * time.Millisecond,
		EmergencyLossThreshold: 1.0,
		MinLossSampleBytes:     64 * 1024,
	})
	oldCwnd := s.congestionWindow
	s.ackedBytesThisRound = 990 * 1280

	s.OnCongestionEvent(1, 10*1280, 64*1280)

	cut := float64(oldCwnd-s.congestionWindow) / float64(oldCwnd)
	require.GreaterOrEqual(t, cut, 0.01)
	require.LessOrEqual(t, cut, 0.10)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.False(t, s.hasLastEmergencyCutback)
	require.Equal(t, "proportional_loss_cutback", s.lastLossActionReason)
}

func TestAdaptiveBDPFivePercentLossNoQueueCappedAtMaxNoQueueCut(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(100*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                   true,
		LossGraceRatio:           0.01,
		LossSevereThreshold:      0.05,
		MaxLossCwndCutNoQueue:    0.15,
		MinLossSampleBytes:       64 * 1024,
		MildLossPersistentRounds: 1,
	})
	oldCwnd := s.congestionWindow
	s.ackedBytesThisRound = 950 * 1280

	s.OnCongestionEvent(1, 50*1280, 64*1280)

	require.GreaterOrEqual(t, float64(s.congestionWindow)/float64(oldCwnd), 0.85)
	require.Greater(t, s.congestionWindow, s.minCongestionWindow)
	require.False(t, s.hasLastEmergencyCutback)
	require.InDelta(t, 0.85, s.lastLossCwndMultiplier, 0.000001)
}

func TestAdaptiveBDPTinyLossDoesNotEmergencyCutback(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(100*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                 true,
		EmergencyLossThreshold: 0.01,
	})
	oldCwnd := s.congestionWindow
	s.ackedBytesThisRound = 150_000

	s.OnCongestionEvent(1, 214, 64*1280)

	require.Equal(t, oldCwnd, s.congestionWindow)
	require.False(t, s.hasLastEmergencyCutback)
	require.False(t, s.hasLastLossCutbackRound)
	require.Equal(t, "loss_below_absolute_threshold", s.lastLossActionReason)
}

func TestAdaptiveBDPEmergencyRequiresAbsoluteBytes(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(100*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                 true,
		EmergencyLossThreshold: 0.10,
	})
	oldCwnd := s.congestionWindow
	s.ackedBytesThisRound = 16 * 1280

	s.OnCongestionEvent(1, 4*1280, 64*1280)

	require.InDelta(t, 0.20, s.roundLossRatio(), 0.000001)
	require.Equal(t, oldCwnd, s.congestionWindow)
	require.False(t, s.hasLastEmergencyCutback)
	require.NotEqual(t, "emergency_loss_proportional", s.lastLossActionReason)
	require.Equal(t, "loss_sample_too_small", s.lastLossActionReason)
}

func TestAdaptiveBDPEmergencyLossForSevereCongestion(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(130*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                 true,
		EmergencyLossThreshold: 0.10,
		EmergencyLossMinBytes:  8 * 1280,
		MinLossSampleBytes:     64 * 1024,
		QueueTarget:            20 * time.Millisecond,
	})
	oldCwnd := s.congestionWindow
	oldBW := s.bw
	s.ackedBytesThisRound = 170 * 1280

	s.OnCongestionEvent(1, 30*1280, 64*1280)

	require.Equal(t, adaptiveBDPProbeDown, s.state)
	require.True(t, s.hasLastEmergencyCutback)
	require.InDelta(t, 0.70, float64(s.congestionWindow)/float64(oldCwnd), 0.000001)
	require.Equal(t, uint64(float64(oldBW)*0.70), s.shortBw)
	require.Equal(t, s.shortBw, s.bw)
	require.Equal(t, "emergency_loss_proportional", s.lastLossActionReason)
}

func TestAdaptiveBDPPersistentCongestionStillMinCwnd(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(130*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{Enable: true})
	s.congestionWindow = 800 * 1280

	s.OnPersistentCongestion(monotime.Now())

	require.Equal(t, s.minCongestionWindow, s.congestionWindow)
	require.Equal(t, adaptiveBDPStartup, s.state)
	require.Equal(t, "persistent_congestion", s.lastStateChangeReason)
}

func TestAdaptiveBDPLossCutbackAtMostOncePerRound(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(140*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                 true,
		EmergencyLossThreshold: 1.0,
		MinLossSampleBytes:     64 * 1024,
	})
	s.ackedBytesThisRound = 100_000

	s.OnCongestionEvent(1, 3_000, 64*1280)
	cwndAfterFirstLoss := s.congestionWindow
	shortBwAfterFirstLoss := s.shortBw

	s.OnCongestionEvent(2, 50_000, 64*1280)

	require.Equal(t, cwndAfterFirstLoss, s.congestionWindow)
	require.Equal(t, shortBwAfterFirstLoss, s.shortBw)
	require.Equal(t, "loss_cutback_cooldown", s.lastLossActionReason)
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
	target := s.targetCwnd()
	require.True(t, s.hasQueuePressure())
	s.queueHighRounds = s.queuePersistentRounds()

	s.setCwndFromTarget(1280, oldCwnd)
	require.Equal(t, max(target, s.minCongestionWindow), s.congestionWindow)
	require.Equal(t, "congestion_target_cutback", s.lastCwndChangeReason)
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
	require.Equal(t, protocol.ByteCount(float64(oldCwnd)*0.75), s.congestionWindow)
	require.Equal(t, "gradual_no_congestion_target_cutback_capped", s.lastCwndChangeReason)
}

func TestAdaptiveBDPNoQueueCwndCutbackHonorsRateFloor(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                        true,
			StartupTargetRateBps:          30_000_000,
			NoCongestionRateFloorFraction: 0.5,
			CruiseCwndGain:                1.5,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 1
	s.maxBw = 1
	s.shortBw = 1
	s.congestionWindow = 400 * 1280
	oldCwnd := s.congestionWindow
	require.False(t, s.hasCongestionEvidence())

	rateFloor := s.noCongestionRateFloorBytesPerSecond()
	floorBDP := s.bdpForBandwidth(rateFloor)
	expectedFloor := roundUpToMSS(protocol.ByteCount(float64(floorBDP)*s.cruiseCwndGain()), s.maxDatagramSize)
	require.Greater(t, expectedFloor, protocol.ByteCount(float64(oldCwnd)*0.75))

	s.reduceCwndTowardTarget(s.congestionWindow, false)
	require.Equal(t, expectedFloor, s.congestionWindow)
	require.Equal(t, "gradual_no_congestion_target_cutback_capped", s.lastCwndChangeReason)
}

func TestAdaptiveBDPNoQueueCwndCutbackNeverRaisesCwnd(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                        true,
			StartupTargetRateBps:          30_000_000,
			NoCongestionRateFloorFraction: 0.5,
			CruiseCwndGain:                1.5,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.state = adaptiveBDPProbeBW
	s.bw = 1
	s.maxBw = 1
	s.shortBw = 1
	s.congestionWindow = 300 * 1280
	oldCwnd := s.congestionWindow
	require.Greater(t, s.noCongestionCwndFloor(), oldCwnd)

	s.reduceCwndTowardTarget(s.congestionWindow, false)
	require.Equal(t, oldCwnd, s.congestionWindow)
	require.Empty(t, s.lastCwndChangeReason)
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

func TestAdaptiveBDPProbePolicyStartsWhenEmptyStopsWhenBuilding(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:           true,
			ProbeInterval:    100 * time.Millisecond,
			ProbeUpGain:      1.25,
			CruisePacingGain: 1.0,
			QueueTarget:      20 * time.Millisecond,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 100 * time.Millisecond
	s.maxBw = mbitToBytesPerSecond(50)
	s.bw = s.maxBw
	s.lastProbeTime = start.Add(-200 * time.Millisecond)
	s.lastRoundStartTime = start.Add(-200 * time.Millisecond)

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		64*1280,
		start,
		RateSample{
			DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(50)),
			AckedBytes:    1280,
			PriorInFlight: 64 * 1280,
			Interval:      100 * time.Millisecond,
			RTT:           100 * time.Millisecond,
			IsValid:       true,
		},
	)
	require.Equal(t, adaptiveQueueEmpty, s.queueState())
	require.True(t, s.probeUpActive)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, s.probeUpGain(), s.pacingGain())

	buildingRTT := utils.NewRTTStats()
	buildingRTT.UpdateRTT(115*time.Millisecond, 0)
	s.rttStats = buildingRTT
	s.lastProbeTime = start.Add(-200 * time.Millisecond)
	s.lastRoundStartTime = start.Add(-200 * time.Millisecond)

	s.OnPacketAckedWithRateSample(
		2,
		1280,
		64*1280,
		start.Add(200*time.Millisecond),
		RateSample{
			DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(50)),
			AckedBytes:    1280,
			PriorInFlight: 64 * 1280,
			Interval:      100 * time.Millisecond,
			RTT:           115 * time.Millisecond,
			IsValid:       true,
		},
	)
	require.Equal(t, adaptiveQueueBuilding, s.queueState())
	require.False(t, s.probeUpActive)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, s.cruisePacingGain(), s.pacingGain())
}

func TestAdaptiveBDPProbeUpSuppressionOverridesProbeGain(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:           true,
			ProbeUpGain:      1.25,
			CruisePacingGain: 1.0,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.roundCount = 10
	s.probeUpActive = true
	require.True(t, s.canProbeUp())
	require.Equal(t, s.probeUpGain(), s.pacingGain())

	s.suppressProbeUpForOneRound("proportional_loss_no_queue")
	require.False(t, s.canProbeUp())
	require.False(t, s.probeUpActive)
	require.Equal(t, uint64(11), s.suppressProbeUpUntilRound)
	require.Equal(t, "proportional_loss_no_queue", s.suppressProbeUpReason)

	s.probeUpActive = true
	require.Equal(t, s.cruisePacingGain(), s.pacingGain())

	s.roundCount = 11
	require.False(t, s.canProbeUp())
	require.Equal(t, s.cruisePacingGain(), s.pacingGain())

	s.roundCount = 12
	require.True(t, s.canProbeUp())
	require.Equal(t, s.probeUpGain(), s.pacingGain())
}

func TestAdaptiveBDPCanProbeUpClearsSuppressionAfterLossFreeRounds(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                  true,
			QueueTarget:             20 * time.Millisecond,
			LossRecoveryProbeRounds: 2,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.roundCount = 10
	s.suppressProbeUpUntilRound = 20
	s.suppressProbeUpReason = "proportional_loss_no_queue"
	require.False(t, s.canProbeUp())
	require.Equal(t, "proportional_loss_no_queue", s.suppressProbeUpReason)

	s.lossFreeRounds = 2
	require.True(t, s.canProbeUp())
	require.Zero(t, s.suppressProbeUpUntilRound)
	require.Equal(t, "cleared_after_loss_free_rounds", s.suppressProbeUpReason)
}

func TestAdaptiveBDPCanProbeUpBlocksFreshCongestionSignals(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:      true,
			QueueTarget: 20 * time.Millisecond,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.roundCount = 10
	require.True(t, s.canProbeUp())

	s.lastECNCERound = 10
	s.hasLastECNCE = true
	require.False(t, s.canProbeUp())

	s.lastECNCERound = 8
	s.lastMaterialLossRound = 9
	s.hasMaterialLossRound = true
	require.False(t, s.canProbeUp())

	s.lastMaterialLossRound = 8
	persistentRTT := utils.NewRTTStats()
	persistentRTT.UpdateRTT(140*time.Millisecond, 0)
	s.rttStats = persistentRTT
	s.queueHighRounds = s.queuePersistentRounds()
	require.False(t, s.canProbeUp())
}

func TestAdaptiveBDPMaybeStartLossRecoveryProbeLiftsShortBandwidth(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                           true,
			QueueTarget:                      20 * time.Millisecond,
			LossRecoveryProbeRounds:          2,
			LossRecoveryProbeGain:            1.25,
			LossRecoveryProbeDurationRounds:  2,
			LossRecoveryClearShortBwFraction: 0.95,
			MaxProbeRateBps:                  30_000_000,
		},
	)
	s.state = adaptiveBDPProbeDown
	s.minRTT = 100 * time.Millisecond
	s.roundCount = 10
	s.lossFreeRounds = 2
	s.maxBw = mbitToBytesPerSecond(30)
	s.bw = mbitToBytesPerSecond(4)
	s.shortBw = mbitToBytesPerSecond(4)
	s.suppressProbeUpUntilRound = 20
	s.suppressProbeUpReason = "proportional_loss_no_queue"

	s.maybeStartLossRecoveryProbe(
		start,
		RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(4)),
			IsValid:      true,
		},
		64*1280,
	)

	require.True(t, s.lossRecoveryProbeActive)
	require.Equal(t, mbitToBytesPerSecond(5), s.lossRecoveryProbeBW)
	require.Equal(t, uint64(12), s.lossRecoveryProbeUntilRound)
	require.Equal(t, uint64(10), s.lastLossRecoveryProbeRound)
	require.True(t, s.hasLastLossRecoveryProbe)
	require.Equal(t, s.lossRecoveryProbeBW, s.shortBw)
	require.True(t, s.probeUpActive)
	require.Equal(t, uint64(10), s.probeUpRoundStart)
	require.Equal(t, start, s.lastProbeTime)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, "loss_free_recovery_probe", s.lastStateChangeReason)
	require.Equal(t, "short_bw_lifted_after_loss_free_rounds", s.lastBWChangeReason)
	require.Zero(t, s.suppressProbeUpUntilRound)
	require.Equal(t, "cleared_after_loss_free_rounds", s.suppressProbeUpReason)
}

func TestAdaptiveBDPMaybeStartLossRecoveryProbeHonorsGoalAndGuards(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                           true,
			QueueTarget:                      20 * time.Millisecond,
			LossRecoveryProbeRounds:          2,
			LossRecoveryProbeGain:            1.25,
			LossRecoveryClearShortBwFraction: 0.95,
			MaxProbeRateBps:                  20_000_000,
			StartupTargetRateBps:             30_000_000,
		},
	)
	s.state = adaptiveBDPProbeDown
	s.minRTT = 100 * time.Millisecond
	s.roundCount = 10
	s.lossFreeRounds = 2
	s.maxBw = mbitToBytesPerSecond(20)
	s.bw = mbitToBytesPerSecond(19)
	s.shortBw = mbitToBytesPerSecond(19)

	require.Equal(t, mbitToBytesPerSecond(20), s.lossRecoveryGoalBandwidth())
	s.maybeStartLossRecoveryProbe(start, RateSample{IsValid: true}, 64*1280)
	require.True(t, s.lossRecoveryProbeActive)
	require.Equal(t, mbitToBytesPerSecond(20), s.lossRecoveryProbeBW)
	require.Zero(t, s.shortBw)
	require.Equal(t, "short_bw_cleared_after_loss_recovery", s.lastBWChangeReason)

	s.lossRecoveryProbeActive = false
	s.lossRecoveryProbeBW = 0
	s.shortBw = mbitToBytesPerSecond(4)
	s.lossFreeRounds = 1
	s.maybeStartLossRecoveryProbe(start, RateSample{IsValid: true}, 64*1280)
	require.False(t, s.lossRecoveryProbeActive)

	s.lossFreeRounds = 2
	s.maybeStartLossRecoveryProbe(start, RateSample{IsValid: true, AppLimited: true}, 64*1280)
	require.False(t, s.lossRecoveryProbeActive)

	buildingRTT := utils.NewRTTStats()
	buildingRTT.UpdateRTT(115*time.Millisecond, 0)
	s.rttStats = buildingRTT
	s.maybeStartLossRecoveryProbe(start, RateSample{IsValid: true}, 64*1280)
	require.False(t, s.lossRecoveryProbeActive)

	s.rttStats = rttStats
	s.lastECNCERound = s.roundCount
	s.hasLastECNCE = true
	s.maybeStartLossRecoveryProbe(start, RateSample{IsValid: true}, 64*1280)
	require.False(t, s.lossRecoveryProbeActive)

	s.hasLastECNCE = false
	s.lastMaterialLossRound = s.roundCount
	s.hasMaterialLossRound = true
	s.maybeStartLossRecoveryProbe(start, RateSample{IsValid: true}, 64*1280)
	require.False(t, s.lossRecoveryProbeActive)
}

func TestAdaptiveBDPLossRecoveryProbeBandwidthFloor(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:      true,
			QueueTarget: 20 * time.Millisecond,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.roundCount = 10
	s.maxBw = mbitToBytesPerSecond(30)
	s.bw = mbitToBytesPerSecond(4)
	s.shortBw = mbitToBytesPerSecond(4)
	s.lossRecoveryProbeActive = true
	s.lossRecoveryProbeBW = mbitToBytesPerSecond(8)
	s.lossRecoveryProbeUntilRound = 11

	s.updateBandwidthAt(
		RateSample{
			DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(4)),
			AckedBytes:   1280,
			Interval:     100 * time.Millisecond,
			RTT:          100 * time.Millisecond,
			IsValid:      true,
		},
		64*1280,
		start,
	)

	require.True(t, s.lossRecoveryProbeActive)
	require.Equal(t, mbitToBytesPerSecond(8), s.bw)
	require.Equal(t, "loss_recovery_probe_bw_floor", s.lastBWChangeReason)
}

func TestAdaptiveBDPLossRecoveryProbeBandwidthFloorStopsOnExpiryAndCongestion(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	newSender := func() *adaptiveBDPSender {
		s := NewAdaptiveBDPSender(
			&clock,
			rttStats,
			&utils.ConnectionStats{},
			1280,
			CwndTuningConfig{
				Enable:      true,
				QueueTarget: 20 * time.Millisecond,
			},
		)
		s.minRTT = 100 * time.Millisecond
		s.roundCount = 10
		s.maxBw = mbitToBytesPerSecond(30)
		s.bw = mbitToBytesPerSecond(4)
		s.shortBw = mbitToBytesPerSecond(4)
		s.lossRecoveryProbeActive = true
		s.lossRecoveryProbeBW = mbitToBytesPerSecond(8)
		s.lossRecoveryProbeUntilRound = 9
		return s
	}
	s := newSender()
	s.updateBandwidthAt(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(4)),
		Interval:     100 * time.Millisecond,
		RTT:          100 * time.Millisecond,
		IsValid:      true,
	}, 64*1280, start)
	require.False(t, s.lossRecoveryProbeActive)
	require.Zero(t, s.lossRecoveryProbeBW)
	require.Equal(t, mbitToBytesPerSecond(4), s.bw)

	s = newSender()
	s.lossRecoveryProbeUntilRound = 11
	s.lastMaterialLossRound = 10
	s.hasMaterialLossRound = true
	s.updateBandwidthAt(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(4)),
		Interval:     100 * time.Millisecond,
		RTT:          100 * time.Millisecond,
		IsValid:      true,
	}, 64*1280, start)
	require.False(t, s.lossRecoveryProbeActive)
	require.Zero(t, s.lossRecoveryProbeBW)
	require.Equal(t, mbitToBytesPerSecond(4), s.bw)

	s = newSender()
	s.lossRecoveryProbeUntilRound = 11
	persistentRTT := utils.NewRTTStats()
	persistentRTT.UpdateRTT(140*time.Millisecond, 0)
	s.rttStats = persistentRTT
	s.queueHighRounds = s.queuePersistentRounds()
	s.updateBandwidthAt(RateSample{
		DeliveryRate: protocol.ByteCount(mbitToBytesPerSecond(4)),
		Interval:     100 * time.Millisecond,
		RTT:          140 * time.Millisecond,
		IsValid:      true,
	}, 64*1280, start)
	require.False(t, s.lossRecoveryProbeActive)
	require.Zero(t, s.lossRecoveryProbeBW)
	require.Equal(t, mbitToBytesPerSecond(4), s.bw)
}

func TestAdaptiveBDPACKPathStartsLossRecoveryProbe(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                           true,
			QueueTarget:                      20 * time.Millisecond,
			LossRecoveryProbeRounds:          2,
			LossRecoveryProbeGain:            1.25,
			LossRecoveryProbeDurationRounds:  1,
			LossRecoveryClearShortBwFraction: 0.95,
			MaxProbeRateBps:                  30_000_000,
		},
	)
	s.state = adaptiveBDPProbeDown
	s.minRTT = 100 * time.Millisecond
	s.lastRoundStartTime = start.Add(-200 * time.Millisecond)
	s.lossFreeRounds = 2
	s.maxBw = mbitToBytesPerSecond(30)
	s.bw = mbitToBytesPerSecond(4)
	s.shortBw = mbitToBytesPerSecond(4)
	s.congestionWindow = 64 * 1280

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		64*1280,
		start,
		RateSample{
			DeliveryRate:   protocol.ByteCount(mbitToBytesPerSecond(4)),
			AckedBytes:     1280,
			DeliveredBytes: 1280,
			PriorInFlight:  64 * 1280,
			Interval:       100 * time.Millisecond,
			RTT:            100 * time.Millisecond,
			IsValid:        true,
		},
	)

	require.True(t, s.lossRecoveryProbeActive)
	require.Equal(t, mbitToBytesPerSecond(5), s.lossRecoveryProbeBW)
	require.Equal(t, s.roundCount+s.lossRecoveryProbeDurationRounds(), s.lossRecoveryProbeUntilRound)
	require.Equal(t, s.lossRecoveryProbeBW, s.shortBw)
	require.True(t, s.probeUpActive)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, "loss_free_recovery_probe", s.lastStateChangeReason)
}

func TestLossCutbackRecoversAfterLossFreeRounds(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                           true,
			StartupTargetRateBps:             30_000_000,
			MaxProbeRateBps:                  30_000_000,
			InitialWindowPackets:             256,
			MinWindowPackets:                 32,
			QueueTarget:                      20 * time.Millisecond,
			LossRecoveryProbeRounds:          2,
			LossRecoveryProbeGain:            1.25,
			LossRecoveryProbeDurationRounds:  1,
			LossRecoveryClearShortBwFraction: 0.95,
		},
	)
	s.state = adaptiveBDPProbeDown
	s.minRTT = 150 * time.Millisecond
	s.lastRoundStartTime = start.Add(-300 * time.Millisecond)
	s.maxBw = 30_000_000 / 8
	s.shortBw = 300 * 1024
	s.bw = s.shortBw
	s.lossFreeRounds = 2
	s.congestionWindow = 256 * 1280
	s.updatePacingRate()
	oldPacing := s.pacingRateBytesPerSecond

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		256*1280,
		start,
		RateSample{
			DeliveryRate:   protocol.ByteCount(300 * 1024),
			AckedBytes:     1280,
			DeliveredBytes: 1280,
			PriorInFlight:  256 * 1280,
			Interval:       150 * time.Millisecond,
			RTT:            150 * time.Millisecond,
			IsValid:        true,
		},
	)

	require.True(t, s.lossRecoveryProbeActive)
	require.Greater(t, s.lossRecoveryProbeBW, uint64(300*1024))
	require.Greater(t, s.shortBw, uint64(300*1024))
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, "loss_free_recovery_probe", s.lastStateChangeReason)

	s.OnPacketAckedWithRateSample(
		2,
		1280,
		256*1280,
		start.Add(150*time.Millisecond),
		RateSample{
			DeliveryRate:   protocol.ByteCount(300 * 1024),
			AckedBytes:     1280,
			DeliveredBytes: 2560,
			PriorInFlight:  256 * 1280,
			Interval:       150 * time.Millisecond,
			RTT:            150 * time.Millisecond,
			IsValid:        true,
		},
	)
	require.Greater(t, s.bw, uint64(300*1024))
	require.Greater(t, s.pacingRateBytesPerSecond, oldPacing)
}

func TestLossRecoveryProbeDoesNotRunWithQueue(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(130*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                  true,
			QueueTarget:             20 * time.Millisecond,
			LossRecoveryProbeRounds: 2,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.roundCount = 10
	s.lossFreeRounds = 2
	s.maxBw = mbitToBytesPerSecond(30)
	s.bw = mbitToBytesPerSecond(4)
	s.shortBw = mbitToBytesPerSecond(4)
	oldShortBw := s.shortBw

	s.maybeStartLossRecoveryProbe(start, RateSample{IsValid: true}, 64*1280)
	require.False(t, s.lossRecoveryProbeActive)
	require.Equal(t, oldShortBw, s.shortBw)
}

func TestLossRecoveryProbeDoesNotRunWithFreshLoss(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                  true,
			QueueTarget:             20 * time.Millisecond,
			LossRecoveryProbeRounds: 2,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.roundCount = 10
	s.lossFreeRounds = 2
	s.lastMaterialLossRound = 10
	s.hasMaterialLossRound = true
	s.maxBw = mbitToBytesPerSecond(30)
	s.bw = mbitToBytesPerSecond(4)
	s.shortBw = mbitToBytesPerSecond(4)

	s.maybeStartLossRecoveryProbe(start, RateSample{IsValid: true}, 64*1280)
	require.False(t, s.lossRecoveryProbeActive)
	require.Equal(t, mbitToBytesPerSecond(4), s.shortBw)
}

func TestLossRecoveryClearsProbeSuppress(t *testing.T) {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                  true,
			QueueTarget:             20 * time.Millisecond,
			LossRecoveryProbeRounds: 2,
		},
	)
	s.minRTT = 100 * time.Millisecond
	s.roundCount = 10
	s.suppressProbeUpUntilRound = s.roundCount + 100
	s.suppressProbeUpReason = "proportional_loss_no_queue"
	s.lossFreeRounds = 2

	require.True(t, s.canProbeUp())
	require.Zero(t, s.suppressProbeUpUntilRound)
	require.Equal(t, "cleared_after_loss_free_rounds", s.suppressProbeUpReason)
}

func TestLossRecoveryDoesNotRequireHighDeliverySample(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(150*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:                           true,
			QueueTarget:                      20 * time.Millisecond,
			LossRecoveryProbeRounds:          2,
			LossRecoveryProbeGain:            1.25,
			LossRecoveryProbeDurationRounds:  1,
			LossRecoveryClearShortBwFraction: 0.95,
		},
	)
	s.minRTT = 150 * time.Millisecond
	s.roundCount = 10
	s.lossFreeRounds = 2
	s.maxBw = 3_750_000
	s.bw = 294 * 1024
	s.shortBw = 294 * 1024

	s.maybeStartLossRecoveryProbe(
		start,
		RateSample{
			DeliveryRate: protocol.ByteCount(294 * 1024),
			IsValid:      true,
		},
		64*1280,
	)
	require.True(t, s.lossRecoveryProbeActive)
	require.Greater(t, s.lossRecoveryProbeBW, uint64(294*1024))

	s.updateBandwidthAt(
		RateSample{
			DeliveryRate: protocol.ByteCount(294 * 1024),
			Interval:     150 * time.Millisecond,
			RTT:          150 * time.Millisecond,
			IsValid:      true,
		},
		64*1280,
		start,
	)
	require.Greater(t, s.bw, uint64(294*1024))
}

func TestAdaptiveBDPProbeUpSuppressionBlocksNewProbe(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(
		&clock,
		rttStats,
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable:           true,
			ProbeInterval:    100 * time.Millisecond,
			ProbeUpGain:      1.25,
			CruisePacingGain: 1.0,
		},
	)
	s.state = adaptiveBDPProbeBW
	s.minRTT = 100 * time.Millisecond
	s.bw = mbitToBytesPerSecond(50)
	s.maxBw = s.bw
	s.lastProbeTime = start.Add(-200 * time.Millisecond)
	s.lastRoundStartTime = start.Add(-200 * time.Millisecond)
	s.suppressProbeUpForOneRound("mild_loss_waiting_persistence")

	s.OnPacketAckedWithRateSample(
		1,
		1280,
		64*1280,
		start,
		RateSample{
			DeliveryRate:  protocol.ByteCount(mbitToBytesPerSecond(50)),
			AckedBytes:    1280,
			PriorInFlight: 64 * 1280,
			Interval:      100 * time.Millisecond,
			RTT:           100 * time.Millisecond,
			IsValid:       true,
		},
	)
	require.False(t, s.probeUpActive)
	require.Equal(t, s.cruisePacingGain(), s.pacingGain())
	require.Equal(t, "mild_loss_waiting_persistence", s.suppressProbeUpReason)
}

func TestAdaptiveBDPLossToleranceBelowGraceRatioWithQueue(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(140*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                 true,
		LossGraceRatio:         0.02,
		QueueTarget:            20 * time.Millisecond,
		EmergencyLossThreshold: 1.0,
		MinLossSampleBytes:     64 * 1024,
	})
	oldCwnd := s.congestionWindow
	oldShortBw := s.shortBw
	// 1.5% loss (below grace ratio of 2%)
	s.ackedBytesThisRound = 985 * 1280
	s.OnCongestionEvent(1, 15*1280, 64*1280)

	require.Equal(t, oldCwnd, s.congestionWindow)
	require.Equal(t, oldShortBw, s.shortBw)
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, "mild_loss_below_grace_no_cwnd_cut", s.lastLossActionReason)
}

func TestAdaptiveBDPLossBelowSoftThresholdDoesNotCut(t *testing.T) {
	s := newAdaptiveBDPLossRegressionSender(140*time.Millisecond, 100*time.Millisecond, CwndTuningConfig{
		Enable:                 true,
		LossGraceRatio:         0.01,
		LossSoftThreshold:      0.05,
		QueueTarget:            20 * time.Millisecond,
		EmergencyLossThreshold: 1.0,
		MinLossSampleBytes:     64 * 1024,
	})
	s.shortBw = s.bw
	oldCwnd := s.congestionWindow
	oldShortBw := s.shortBw
	// 3% loss (above grace ratio of 1% but below soft threshold of 5%)
	s.ackedBytesThisRound = 970 * 1280
	s.OnCongestionEvent(1, 30*1280, 64*1280)

	require.Equal(t, oldCwnd, s.congestionWindow)
	require.Equal(t, oldShortBw, s.shortBw)
	require.Equal(t, adaptiveBDPProbeDown, s.state)
}

func TestAdaptiveBDPMinLossSampleBytesIsAdaptive(t *testing.T) {
	var clock mockClock
	s := NewAdaptiveBDPSender(
		&clock,
		utils.NewRTTStats(),
		&utils.ConnectionStats{},
		1280,
		CwndTuningConfig{
			Enable: true,
		},
	)
	s.congestionWindow = 1000 * 1280
	s.minRTT = 100 * time.Millisecond
	s.bw = mbitToBytesPerSecond(100)
	s.maxBw = s.bw

	expectedMinBytes := max(protocol.ByteCount(64*1024), max(s.bdp()/8, s.congestionWindow/4))
	require.Equal(t, expectedMinBytes, s.minLossSampleBytes())
}
