package congestion

import (
	"math"
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
)

type adaptiveBDPState uint8

const (
	adaptiveBDPStartup adaptiveBDPState = iota
	adaptiveBDPDrain
	adaptiveBDPProbeBW
	adaptiveBDPProbeDown
	adaptiveBDPProbeRTT
)

func (s adaptiveBDPState) String() string {
	switch s {
	case adaptiveBDPStartup:
		return "Startup"
	case adaptiveBDPDrain:
		return "Drain"
	case adaptiveBDPProbeBW:
		return "ProbeBW"
	case adaptiveBDPProbeDown:
		return "ProbeDown"
	case adaptiveBDPProbeRTT:
		return "ProbeRTT"
	default:
		return "Unknown"
	}
}

const (
	adaptiveBDPHealthyBandwidthRatio = 0.98
	adaptiveBDPCruisePacingGain      = 1.05
)

type bandwidthSample struct {
	round uint64
	bw    uint64
}

type bandwidthMaxFilter struct {
	samples []bandwidthSample
	rounds  uint32
}

func (f *bandwidthMaxFilter) Update(round uint64, bw uint64) {
	if bw == 0 {
		return
	}
	keepRounds := f.rounds
	if keepRounds == 0 {
		keepRounds = 6
	}
	keepFrom := uint64(0)
	if round >= uint64(keepRounds) {
		keepFrom = round - uint64(keepRounds) + 1
	}
	n := f.samples[:0]
	for _, s := range f.samples {
		if s.round >= keepFrom {
			n = append(n, s)
		}
	}
	f.samples = append(n, bandwidthSample{round: round, bw: bw})
}

func (f *bandwidthMaxFilter) Max(round uint64) uint64 {
	keepRounds := f.rounds
	if keepRounds == 0 {
		keepRounds = 6
	}
	keepFrom := uint64(0)
	if round >= uint64(keepRounds) {
		keepFrom = round - uint64(keepRounds) + 1
	}
	var maxBW uint64
	for _, s := range f.samples {
		if s.round >= keepFrom && s.bw > maxBW {
			maxBW = s.bw
		}
	}
	return maxBW
}

type adaptiveBDPSender struct {
	rttStats  *utils.RTTStats
	connStats *utils.ConnectionStats
	pacer     *pacer
	clock     Clock

	maxDatagramSize protocol.ByteCount

	congestionWindow    protocol.ByteCount
	minCongestionWindow protocol.ByteCount
	maxCongestionWindow protocol.ByteCount
	initialWindow       protocol.ByteCount

	pacingRateBytesPerSecond uint64

	state adaptiveBDPState

	minRTT          time.Duration
	minRTTTimestamp monotime.Time

	bw      uint64
	maxBw   uint64
	shortBw uint64

	bwFilter bandwidthMaxFilter

	nextRoundDelivered protocol.ByteCount
	roundCount         uint64
	roundStart         bool

	fullBw        uint64
	fullBwCount   uint32
	fullBwReached bool

	queueHighRounds uint32
	downshiftRounds uint32

	lastProbeTime     monotime.Time
	probeUpRoundStart uint64
	probeUpActive     bool

	lostBytesThisRound  protocol.ByteCount
	ackedBytesThisRound protocol.ByteCount

	lastRateSample    RateSample
	lastPriorInFlight protocol.ByteCount
	lastTargetCwnd    protocol.ByteCount
	lastBDP           protocol.ByteCount
	lastQueueDelay    time.Duration
	lastQueueTarget   time.Duration
	lastPacingGain    float64
	lastCwndGain      float64

	lastStateChangeReason string
	lastCwndChangeReason  string
	lastBWChangeReason    string

	lastRoundStartTime monotime.Time

	lastBandwidthSample      uint64
	lastBandwidthSampleRound uint64
	hasLastBandwidthSample   bool
	lastBandwidthGrowthRound uint64
	hasLastBandwidthGrowth   bool

	lastLossCutbackRound      uint64
	hasLastLossCutbackRound   bool
	lastEmergencyCutbackRound uint64
	hasLastEmergencyCutback   bool

	cfg CwndTuningConfig

	lastStateChange monotime.Time
}

var (
	_ SendAlgorithm                         = &adaptiveBDPSender{}
	_ SendAlgorithmWithDebugInfos           = &adaptiveBDPSender{}
	_ SendAlgorithmWithRateSample           = &adaptiveBDPSender{}
	_ SendAlgorithmWithECN                  = &adaptiveBDPSender{}
	_ SendAlgorithmWithPersistentCongestion = &adaptiveBDPSender{}
	_ SendAlgorithmWithAdaptiveBDPDebugInfo = &adaptiveBDPSender{}
)

func NewAdaptiveBDPSender(
	clock Clock,
	rttStats *utils.RTTStats,
	connStats *utils.ConnectionStats,
	initialMaxDatagramSize protocol.ByteCount,
	cfg CwndTuningConfig,
) *adaptiveBDPSender {
	maxPackets := protocol.ByteCount(protocol.MaxCongestionWindowPackets)
	if cfg.MaxWindowPackets > 0 {
		maxPackets = max(maxPackets, protocol.ByteCount(cfg.MaxWindowPackets))
	}
	initialPackets := protocol.ByteCount(initialCongestionWindow)
	if cfg.InitialWindowPackets > 0 {
		initialPackets = protocol.ByteCount(cfg.InitialWindowPackets)
	}
	minPackets := protocol.ByteCount(minCongestionWindowPackets)
	if cfg.MinWindowPackets > 0 {
		minPackets = protocol.ByteCount(cfg.MinWindowPackets)
	}
	now := clock.Now()
	s := &adaptiveBDPSender{
		rttStats:              rttStats,
		connStats:             connStats,
		clock:                 clock,
		maxDatagramSize:       initialMaxDatagramSize,
		minCongestionWindow:   minPackets * initialMaxDatagramSize,
		maxCongestionWindow:   maxPackets * initialMaxDatagramSize,
		initialWindow:         initialPackets * initialMaxDatagramSize,
		state:                 adaptiveBDPStartup,
		cfg:                   cfg,
		lastProbeTime:         now,
		lastStateChange:       now,
		lastRoundStartTime:    now,
		lastStateChangeReason: "startup",
	}
	s.congestionWindow = min(max(s.initialWindow, s.minCongestionWindow), s.maxCongestionWindow)
	s.bwFilter.rounds = cfg.BandwidthFilterRounds
	s.pacer = newPacerWithRate(s.PacingRateBytesPerSecond)
	s.updatePacingRate()
	s.updateDebugSnapshot(0)
	return s
}

func (s *adaptiveBDPSender) TimeUntilSend(_ protocol.ByteCount) monotime.Time {
	return s.pacer.TimeUntilSend()
}

func (s *adaptiveBDPSender) HasPacingBudget(now monotime.Time) bool {
	return s.pacer.Budget(now) >= s.maxDatagramSize
}

func (s *adaptiveBDPSender) OnPacketSent(
	sentTime monotime.Time,
	_ protocol.ByteCount,
	_ protocol.PacketNumber,
	bytes protocol.ByteCount,
	isRetransmittable bool,
) {
	s.pacer.SentPacket(sentTime, bytes)
	if !isRetransmittable {
		return
	}
}

func (s *adaptiveBDPSender) CanSend(bytesInFlight protocol.ByteCount) bool {
	return bytesInFlight < s.congestionWindow
}

func (s *adaptiveBDPSender) InRecovery() bool {
	return s.state == adaptiveBDPDrain || s.state == adaptiveBDPProbeDown
}

func (s *adaptiveBDPSender) InSlowStart() bool {
	return s.state == adaptiveBDPStartup
}

func (s *adaptiveBDPSender) GetCongestionWindow() protocol.ByteCount {
	return s.congestionWindow
}

func (s *adaptiveBDPSender) MaybeExitSlowStart() {}

func (s *adaptiveBDPSender) OnPacketAcked(
	number protocol.PacketNumber,
	ackedBytes protocol.ByteCount,
	priorInFlight protocol.ByteCount,
	eventTime monotime.Time,
) {
	s.OnPacketAckedWithRateSample(number, ackedBytes, priorInFlight, eventTime, RateSample{
		AckedBytes:    ackedBytes,
		PriorInFlight: priorInFlight,
		RTT:           s.rttStats.SmoothedRTT(),
	})
}

func (s *adaptiveBDPSender) OnPacketAckedWithRateSample(
	_ protocol.PacketNumber,
	ackedBytes protocol.ByteCount,
	priorInFlight protocol.ByteCount,
	eventTime monotime.Time,
	sample RateSample,
) {
	s.lastRateSample = sample
	s.lastPriorInFlight = priorInFlight
	s.ackedBytesThisRound += ackedBytes
	s.updateMinRTT(sample.RTT, eventTime)
	s.updateRound(sample, priorInFlight, eventTime)

	s.updateBandwidth(sample, priorInFlight)

	enterProbeDown := s.shouldEnterProbeDown(sample, priorInFlight)
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
		if s.probeInterval() > 0 && (s.lastProbeTime.IsZero() || eventTime.Sub(s.lastProbeTime) >= s.probeInterval()) && s.roundStart {
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
		drained := priorInFlight <= s.bdp() || s.queueDelay() <= s.queueTarget()/2
		spentMinDrain := eventTime.Sub(s.lastStateChange) >= minDrain
		if spentMinDrain && drained {
			s.enterStateWithReason(adaptiveBDPProbeBW, eventTime, "probe_down_drained")
		}
	}

	s.updatePacingRate()
	if !sample.AppLimited {
		s.setCwndFromTarget(ackedBytes, priorInFlight)
	}
	s.updateDebugSnapshot(priorInFlight)
}

func (s *adaptiveBDPSender) OnCongestionEvent(_ protocol.PacketNumber, lostBytes, priorInFlight protocol.ByteCount) {
	s.lastPriorInFlight = priorInFlight
	s.connStats.PacketsLost.Add(1)
	s.connStats.BytesLost.Add(uint64(lostBytes))
	s.lostBytesThisRound += lostBytes

	lossRate := s.lossRate()
	if lossRate <= 0 {
		return
	}
	if !s.canLossCutbackThisRound() {
		return
	}

	emergency := lossRate > s.emergencyLossThreshold()
	if !emergency && lossRate <= s.lossTarget() {
		return
	}
	if !emergency && s.bandwidthOutweighsLoss() {
		return
	}

	now := s.clock.Now()
	s.enterStateWithReason(adaptiveBDPProbeDown, now, "loss_target_exceeded")
	if emergency && s.canEmergencyCutbackThisRound() {
		oldCwnd := s.congestionWindow
		s.congestionWindow = max(s.minCongestionWindow, protocol.ByteCount(float64(s.congestionWindow)*0.7))
		s.noteCwndChange(oldCwnd, "emergency_loss_cutback")
		s.markEmergencyCutbackRound()
	}
	if s.bw > 0 {
		s.shortBw = max(1, uint64(float64(s.bw)*s.probeDownGain()))
		s.bw = min(s.bw, s.shortBw)
		s.lastBWChangeReason = "loss_probe_down_bw_cutback"
	}
	s.updatePacingRate()
	s.reduceCwndTowardTarget(priorInFlight, true)
	s.markLossCutbackRound()
	s.updateDebugSnapshot(priorInFlight)
}

func (s *adaptiveBDPSender) OnECNCongestionEvent(priorInFlight protocol.ByteCount, eventTime monotime.Time) {
	s.lastPriorInFlight = priorInFlight
	s.enterStateWithReason(adaptiveBDPProbeDown, eventTime, "ecn_congestion")
	if s.bw > 0 {
		s.shortBw = max(1, uint64(float64(s.bw)*s.probeDownGain()))
		s.bw = min(s.bw, s.shortBw)
		s.lastBWChangeReason = "ecn_congestion"
	}
	s.updatePacingRate()
	s.reduceCwndTowardTarget(priorInFlight, true)
	s.updateDebugSnapshot(priorInFlight)
}

func (s *adaptiveBDPSender) OnRetransmissionTimeout(packetsRetransmitted bool) {
	if !packetsRetransmitted {
		return
	}
	s.enterStateWithReason(adaptiveBDPProbeDown, s.clock.Now(), "retransmission_timeout")
	if s.bw > 0 {
		s.shortBw = max(1, uint64(float64(s.bw)*s.probeDownGain()))
		s.bw = min(s.bw, s.shortBw)
		s.lastBWChangeReason = "retransmission_timeout"
	}
	s.updatePacingRate()
	s.updateDebugSnapshot(s.lastPriorInFlight)
}

func (s *adaptiveBDPSender) OnPersistentCongestion(eventTime monotime.Time) {
	oldCwnd := s.congestionWindow
	s.congestionWindow = s.minCongestionWindow
	s.shortBw = 0
	s.bw = 0
	s.noteCwndChange(oldCwnd, "persistent_congestion")
	s.lastBWChangeReason = "persistent_congestion"
	s.enterStateWithReason(adaptiveBDPStartup, eventTime, "persistent_congestion")
	s.updatePacingRate()
	s.updateDebugSnapshot(s.lastPriorInFlight)
}

func (s *adaptiveBDPSender) SetMaxDatagramSize(mss protocol.ByteCount) {
	if mss < s.maxDatagramSize {
		return
	}
	s.maxDatagramSize = mss
	s.pacer.SetMaxDatagramSize(mss)
}

func (s *adaptiveBDPSender) PacingRateBytesPerSecond() uint64 {
	return s.pacingRateBytesPerSecond
}

func (s *adaptiveBDPSender) AdaptiveBDPDebugInfo() AdaptiveBDPDebugInfo {
	now := s.clock.Now()
	s.updateDebugSnapshot(s.lastPriorInFlight)
	pacerBudget := s.pacer.Budget(now)
	timeUntilSend := time.Duration(0)
	if sendTime := s.pacer.TimeUntilSend(); !sendTime.IsZero() {
		timeUntilSend = sendTime.Sub(now)
		if timeUntilSend < 0 {
			timeUntilSend = 0
		}
	}
	return AdaptiveBDPDebugInfo{
		State: s.state.String(),

		CongestionWindow: s.congestionWindow,
		TargetCwnd:       s.lastTargetCwnd,
		MinCwnd:          s.minCongestionWindow,
		MaxCwnd:          s.maxCongestionWindow,
		BDP:              s.lastBDP,
		BytesInFlight:    s.lastPriorInFlight,
		PriorInFlight:    s.lastPriorInFlight,

		BandwidthBytesPerSecond:      s.bw,
		MaxBandwidthBytesPerSecond:   s.maxBw,
		ShortBandwidthBytesPerSecond: s.shortBw,
		PacingRateBytesPerSecond:     s.pacingRateBytesPerSecond,

		LastDeliveryRateBytesPerSecond: s.lastRateSample.DeliveryRate,
		LastDeliveredDelta:             s.lastRateSample.DeliveredDelta,
		LastSampleInterval:             s.lastRateSample.Interval,
		LastSampleAckElapsed:           s.lastRateSample.AckElapsed,
		LastSampleSendElapsed:          s.lastRateSample.SendElapsed,
		LastSampleAppLimited:           s.lastRateSample.AppLimited,
		LastSampleValid:                s.lastRateSample.IsValid,

		MinRTT:      s.minRTT,
		SmoothedRTT: s.rttStats.SmoothedRTT(),
		QueueDelay:  s.lastQueueDelay,
		QueueTarget: s.lastQueueTarget,
		PacingGain:  s.lastPacingGain,
		CwndGain:    s.lastCwndGain,

		RoundCount:         s.roundCount,
		RoundStart:         s.roundStart,
		LastRoundStartTime: s.lastRoundStartTime,
		QueueHighRounds:    s.queueHighRounds,
		DownshiftRounds:    s.downshiftRounds,
		FullBwReached:      s.fullBwReached,
		ProbeUpActive:      s.probeUpActive,
		PacerBudget:        pacerBudget,
		TimeUntilSend:      timeUntilSend,
		HasPacingBudget:    pacerBudget >= s.maxDatagramSize,

		LastStateChangeReason: s.lastStateChangeReason,
		LastCwndChangeReason:  s.lastCwndChangeReason,
		LastBWChangeReason:    s.lastBWChangeReason,
	}
}

func (s *adaptiveBDPSender) updateDebugSnapshot(priorInFlight protocol.ByteCount) {
	s.lastPriorInFlight = priorInFlight
	s.lastBDP = s.bdp()
	s.lastTargetCwnd = s.targetCwnd()
	s.lastQueueDelay = s.queueDelay()
	s.lastQueueTarget = s.queueTarget()
	s.lastPacingGain = s.pacingGain()
	s.lastCwndGain = s.cwndGain()
}

func (s *adaptiveBDPSender) enterState(st adaptiveBDPState, now monotime.Time) {
	s.enterStateWithReason(st, now, "state_transition")
}

func (s *adaptiveBDPSender) enterStateWithReason(st adaptiveBDPState, now monotime.Time, reason string) {
	if s.state == st {
		if reason != "" {
			s.lastStateChangeReason = reason
		}
		return
	}
	s.state = st
	s.lastStateChange = now
	s.lastStateChangeReason = reason
	if st == adaptiveBDPProbeBW {
		s.queueHighRounds = 0
		s.downshiftRounds = 0
	}
}

func (s *adaptiveBDPSender) updateMinRTT(rtt time.Duration, now monotime.Time) {
	if rtt <= 0 {
		rtt = s.rttStats.MinRTT()
	}
	if rtt <= 0 {
		return
	}
	window := s.minRTTFilterWindow()
	if s.minRTT == 0 || rtt < s.minRTT || (!s.minRTTTimestamp.IsZero() && now.Sub(s.minRTTTimestamp) > window) {
		s.minRTT = rtt
		s.minRTTTimestamp = now
	}
}

func (s *adaptiveBDPSender) updateRound(sample RateSample, priorInFlight protocol.ByteCount, now monotime.Time) {
	s.roundStart = false
	if s.minRTT <= 0 || priorInFlight == 0 {
		return
	}
	if sample.IsValid && sample.DeliveredBytes > 0 {
		if s.nextRoundDelivered == 0 {
			s.nextRoundDelivered = sample.DeliveredBytes
		}
		if sample.DeliveredBytes >= s.nextRoundDelivered {
			s.roundStart = true
		}
	}
	if !s.roundStart {
		if s.lastRoundStartTime.IsZero() {
			s.roundStart = true
		} else if now.Sub(s.lastRoundStartTime) >= s.minRTT {
			s.roundStart = true
		}
	}
	if !s.roundStart {
		return
	}
	s.roundCount++
	s.lastRoundStartTime = now
	if s.maxBw >= uint64(float64(max(1, s.fullBw))*1.25) {
		s.fullBw = s.maxBw
		s.fullBwCount = 0
	} else {
		s.fullBwCount++
	}
	if s.fullBwCount >= 3 {
		s.fullBwReached = true
	}
	if sample.DeliveredBytes > 0 {
		s.nextRoundDelivered = sample.DeliveredBytes + max(1, sample.AckedBytes)
	} else {
		s.nextRoundDelivered += max(1, sample.AckedBytes)
	}
	s.ackedBytesThisRound = 0
	s.lostBytesThisRound = 0
}

func (s *adaptiveBDPSender) updateBandwidth(sample RateSample, priorInFlight protocol.ByteCount) {
	s.lastRateSample = sample
	s.lastPriorInFlight = priorInFlight
	if !sample.IsValid || sample.DeliveryRate == 0 {
		if s.bw == 0 {
			s.bootstrapBandwidth()
			s.lastBWChangeReason = "bootstrap_invalid_sample"
		}
		s.updateDebugSnapshot(priorInFlight)
		return
	}

	sampleBW := uint64(sample.DeliveryRate)
	prevMaxBw := s.maxBw
	prevShortBw := s.shortBw
	prevBw := s.bw
	s.updateBandwidthCompetition(sample, sampleBW, prevMaxBw)
	if sample.AppLimited {
		if sampleBW > s.maxBw {
			s.bwFilter.Update(s.roundCount, sampleBW)
			s.maxBw = max(s.maxBw, s.bwFilter.Max(s.roundCount))
			s.lastBWChangeReason = "app_limited_higher_sample"
		}
	} else {
		if s.maxBw == 0 || sampleBW >= s.maxBw {
			s.bwFilter.Update(s.roundCount, sampleBW)
			s.maxBw = max(s.maxBw, s.bwFilter.Max(s.roundCount))
			if s.maxBw != prevMaxBw {
				s.lastBWChangeReason = "non_app_limited_bw_growth"
			}
		}
	}

	activeBW := s.maxBw
	if activeBW == 0 {
		activeBW = sampleBW
	}

	if s.canUseSampleForDownshift(sample, priorInFlight) && activeBW > 0 && float64(sampleBW) < float64(activeBW)*s.downshiftRatio() {
		s.downshiftRounds++
		if s.downshiftRounds >= s.downshiftRoundsTarget() {
			newShort := max(sampleBW, s.minimumObservableBandwidth())
			if s.shortBw == 0 {
				s.shortBw = newShort
			} else {
				s.shortBw = min(s.shortBw, newShort)
			}
			s.enterStateWithReason(adaptiveBDPProbeDown, s.clock.Now(), "bandwidth_downshift_pipe_filled")
			s.lastBWChangeReason = "short_bw_downshift_pipe_filled"
		}
	} else if !sample.AppLimited {
		s.downshiftRounds = 0
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
	s.bw = max(1, activeBW)
	if s.bw != prevBw && s.lastBWChangeReason == "" {
		s.lastBWChangeReason = "bandwidth_estimate_changed"
	}
	if s.shortBw != prevShortBw && s.lastBWChangeReason == "" {
		s.lastBWChangeReason = "short_bw_changed"
	}
}

func (s *adaptiveBDPSender) isPipeFilled(priorInFlight protocol.ByteCount) bool {
	if priorInFlight == 0 {
		return false
	}
	pipe := s.bdp()
	if pipe <= 0 {
		pipe = min(s.congestionWindow, s.targetCwnd())
	}
	if pipe <= 0 {
		return false
	}
	threshold := protocol.ByteCount(float64(pipe) * 0.75)
	threshold = max(threshold, 4*s.maxDatagramSize)
	return priorInFlight+2*s.maxDatagramSize >= threshold
}

func (s *adaptiveBDPSender) canUseSampleForDownshift(sample RateSample, priorInFlight protocol.ByteCount) bool {
	if !sample.IsValid || sample.DeliveryRate == 0 {
		return false
	}
	if sample.AppLimited {
		return false
	}
	return s.isPipeFilled(priorInFlight)
}

func (s *adaptiveBDPSender) minimumObservableBandwidth() uint64 {
	srtt := s.rttStats.SmoothedRTT()
	if srtt <= 0 {
		srtt = s.minRTT
	}
	if srtt <= 0 {
		srtt = 100 * time.Millisecond
	}
	return max(uint64(1), uint64(float64(s.minCongestionWindow)/srtt.Seconds()))
}

func (s *adaptiveBDPSender) updateBandwidthCompetition(sample RateSample, sampleBW, prevMaxBw uint64) {
	if sample.AppLimited {
		return
	}
	s.lastBandwidthSample = sampleBW
	s.lastBandwidthSampleRound = s.roundCount
	s.hasLastBandwidthSample = true
	if prevMaxBw == 0 || sampleBW >= prevMaxBw {
		s.lastBandwidthGrowthRound = s.roundCount
		s.hasLastBandwidthGrowth = true
	}
}

func (s *adaptiveBDPSender) bootstrapBandwidth() {
	srtt := s.rttStats.SmoothedRTT()
	if srtt <= 0 {
		srtt = 100 * time.Millisecond
	}
	prevBw := s.bw
	prevMaxBw := s.maxBw
	s.bw = uint64(float64(max(s.congestionWindow, s.maxDatagramSize)) / srtt.Seconds())
	if s.maxBw < s.bw {
		s.maxBw = s.bw
	}
	if s.bw != prevBw || s.maxBw != prevMaxBw {
		s.lastBWChangeReason = "bootstrap_bandwidth"
	}
}

func (s *adaptiveBDPSender) updatePacingRate() {
	if s.bw == 0 {
		s.bootstrapBandwidth()
	}
	rate := float64(s.bw) * s.pacingGain() * (1 - s.pacingMargin())
	maxProbe := s.cfg.MaxProbeRateBps
	if maxProbe > 0 {
		rate = min(rate, float64(maxProbe)/8.0)
	}
	minRate := s.minimumPacingRate()
	if s.inProtectedStartup(s.clock.Now()) {
		minRate = max(minRate, s.startupProtectedPacingRate())
	}
	s.pacingRateBytesPerSecond = max(uint64(rate), minRate)
}

func (s *adaptiveBDPSender) minimumPacingRate() uint64 {
	srtt := s.rttStats.SmoothedRTT()
	if srtt <= 0 {
		srtt = s.minRTT
	}
	if srtt <= 0 {
		srtt = 100 * time.Millisecond
	}
	floor := uint64(float64(s.minCongestionWindow) / srtt.Seconds())
	return max(uint64(1024), floor)
}

func (s *adaptiveBDPSender) startupProtectedPacingRate() uint64 {
	srtt := s.rttStats.SmoothedRTT()
	if srtt <= 0 {
		srtt = s.minRTT
	}
	if srtt <= 0 {
		srtt = 100 * time.Millisecond
	}
	return max(uint64(1024), uint64(float64(s.initialWindow)/srtt.Seconds()))
}

func (s *adaptiveBDPSender) bdp() protocol.ByteCount {
	if s.bw == 0 || s.minRTT <= 0 {
		return s.initialWindow
	}
	bytes := float64(s.bw) * s.minRTT.Seconds()
	return roundUpToMSS(protocol.ByteCount(bytes), s.maxDatagramSize)
}

func (s *adaptiveBDPSender) targetCwnd() protocol.ByteCount {
	windowGain := s.windowGain()
	base := float64(s.bdp()) * s.cwndGain() * windowGain
	return clampCwnd(
		roundUpToMSS(protocol.ByteCount(base), s.maxDatagramSize),
		s.minCongestionWindow,
		s.maxCongestionWindow,
	)
}

func (s *adaptiveBDPSender) setCwndFromTarget(ackedBytes, priorInFlight protocol.ByteCount) {
	target := s.targetCwnd()
	oldCwnd := s.congestionWindow
	s.updateDebugSnapshot(priorInFlight)
	if s.state == adaptiveBDPStartup {
		s.congestionWindow += ackedBytes
		if s.congestionWindow < target {
			maxStep := max(ackedBytes, 2*s.maxDatagramSize)
			s.congestionWindow = min(target, s.congestionWindow+maxStep)
		}
		s.noteCwndChange(oldCwnd, "startup_ack_growth")
	} else if s.congestionWindow < target {
		s.congestionWindow += min(ackedBytes, target-s.congestionWindow)
		s.noteCwndChange(oldCwnd, "ack_growth_to_target")
	} else if s.shouldReduceCwndTowardTarget(priorInFlight, target) {
		s.reduceCwndTowardTarget(priorInFlight, false)
	}
	preClampCwnd := s.congestionWindow
	s.congestionWindow = clampCwnd(s.congestionWindow, s.minCongestionWindow, s.maxCongestionWindow)
	s.noteCwndChange(preClampCwnd, "clamp_to_limits")
	if s.congestionWindow < oldCwnd && s.lastCwndChangeReason == "" {
		s.lastCwndChangeReason = "cwnd_reduced"
	}
}

func (s *adaptiveBDPSender) reduceCwndTowardTarget(priorInFlight protocol.ByteCount, lossBased bool) {
	target := s.targetCwnd()
	oldCwnd := s.congestionWindow
	s.updateDebugSnapshot(priorInFlight)

	if oldCwnd <= target {
		return
	}

	if lossBased {
		s.congestionWindow = max(target, s.minCongestionWindow)
		s.noteCwndChange(oldCwnd, "loss_based_target_cutback")
		return
	}

	maxNoLossCut := protocol.ByteCount(float64(oldCwnd) * 0.85)
	floor := max(target, maxNoLossCut)
	if !s.hasQueuePressure() {
		floor = max(floor, protocol.ByteCount(float64(oldCwnd)*0.95))
	}
	if s.inProtectedStartup(s.clock.Now()) {
		floor = max(floor, s.initialWindow)
	}

	s.congestionWindow = clampCwnd(floor, s.minCongestionWindow, s.maxCongestionWindow)
	s.noteCwndChange(oldCwnd, "gradual_no_loss_target_cutback")
}

func (s *adaptiveBDPSender) noteCwndChange(oldCwnd protocol.ByteCount, reason string) {
	if s.congestionWindow != oldCwnd && reason != "" {
		s.lastCwndChangeReason = reason
	}
}

func (s *adaptiveBDPSender) queueDelay() time.Duration {
	srtt := s.rttStats.SmoothedRTT()
	if srtt <= 0 || s.minRTT <= 0 || srtt <= s.minRTT {
		return 0
	}
	return srtt - s.minRTT
}

func (s *adaptiveBDPSender) canReduceWindow(priorInFlight protocol.ByteCount) bool {
	return s.shouldReduceCwndForQueue(priorInFlight)
}

func (s *adaptiveBDPSender) hasQueuePressure() bool {
	return s.queueDelay() > s.queueTarget()
}

func (s *adaptiveBDPSender) hasPersistentQueuePressure() bool {
	return s.queueHighRounds >= s.queuePersistentRounds()
}

func (s *adaptiveBDPSender) shouldReduceCwndForQueue(priorInFlight protocol.ByteCount) bool {
	return s.hasQueuePressure() && s.isPipeFilled(priorInFlight)
}

func (s *adaptiveBDPSender) shouldReduceCwndTowardTarget(priorInFlight protocol.ByteCount, target protocol.ByteCount) bool {
	if s.state == adaptiveBDPProbeDown {
		return true
	}
	return priorInFlight > target && s.hasQueuePressure()
}

func (s *adaptiveBDPSender) inProtectedStartup(now monotime.Time) bool {
	if s.state != adaptiveBDPStartup {
		return false
	}
	if s.lastStateChangeReason == "persistent_congestion" {
		return false
	}
	if s.hasPersistentQueuePressure() || s.lossRate() > s.lossTarget() {
		return false
	}
	if s.lastRateSample.AppLimited {
		return false
	}
	d := s.cfg.StartupTargetDuration
	if d <= 0 {
		d = 5 * time.Second
	}
	if s.lastStateChange.IsZero() {
		return true
	}
	return now.Sub(s.lastStateChange) <= d
}

func (s *adaptiveBDPSender) bandwidthOutweighsLoss() bool {
	if !s.hasLastBandwidthSample || !s.isFreshBandwidthRound(s.lastBandwidthSampleRound) || s.maxBw == 0 {
		return false
	}
	if s.hasLastBandwidthGrowth && s.isFreshBandwidthRound(s.lastBandwidthGrowthRound) {
		return true
	}
	return float64(s.lastBandwidthSample) >= float64(s.maxBw)*adaptiveBDPHealthyBandwidthRatio
}

func (s *adaptiveBDPSender) isFreshBandwidthRound(round uint64) bool {
	return round == s.roundCount || round+1 == s.roundCount
}

func (s *adaptiveBDPSender) canLossCutbackThisRound() bool {
	return !s.hasLastLossCutbackRound || s.lastLossCutbackRound != s.roundCount
}

func (s *adaptiveBDPSender) markLossCutbackRound() {
	s.lastLossCutbackRound = s.roundCount
	s.hasLastLossCutbackRound = true
}

func (s *adaptiveBDPSender) canEmergencyCutbackThisRound() bool {
	return !s.hasLastEmergencyCutback || s.lastEmergencyCutbackRound != s.roundCount
}

func (s *adaptiveBDPSender) markEmergencyCutbackRound() {
	s.lastEmergencyCutbackRound = s.roundCount
	s.hasLastEmergencyCutback = true
}

func (s *adaptiveBDPSender) shouldEnterProbeDown(sample RateSample, priorInFlight protocol.ByteCount) bool {
	if s.hasQueuePressure() && s.isPipeFilled(priorInFlight) {
		s.queueHighRounds++
	} else {
		s.queueHighRounds = 0
	}
	if s.hasPersistentQueuePressure() {
		s.lastStateChangeReason = "queue_delay_persistent"
		return true
	}
	if s.lossRate() > s.lossTarget() && !s.bandwidthOutweighsLoss() {
		s.lastStateChangeReason = "loss_target_exceeded"
		return true
	}
	if s.canUseSampleForDownshift(sample, priorInFlight) && s.bw > 0 && float64(sample.DeliveryRate) < float64(s.bw)*s.downshiftRatio() {
		if s.downshiftRounds >= s.downshiftRoundsTarget() {
			s.lastStateChangeReason = "bandwidth_downshift"
			return true
		}
	}
	return false
}

func (s *adaptiveBDPSender) lossRate() float64 {
	total := s.lostBytesThisRound + s.ackedBytesThisRound
	if total == 0 {
		return 0
	}
	return float64(s.lostBytesThisRound) / float64(total)
}

func (s *adaptiveBDPSender) cwndGain() float64 {
	switch s.state {
	case adaptiveBDPStartup:
		return s.startupCwndGain()
	case adaptiveBDPDrain:
		return s.startupCwndGain()
	case adaptiveBDPProbeDown:
		return min(s.cruiseCwndGain(), 1.25)
	default:
		return s.cruiseCwndGain()
	}
}

func (s *adaptiveBDPSender) startupRequiredGainPerRTT() float64 {
	targetRateBps := s.cfg.StartupTargetRateBps
	if targetRateBps == 0 {
		targetRateBps = s.cfg.MaxProbeRateBps
	}
	if targetRateBps == 0 || s.minRTT <= 0 {
		return 0
	}
	targetBDP := (float64(targetRateBps) / 8.0) * s.minRTT.Seconds()
	if targetBDP <= 0 || s.initialWindow <= 0 {
		return 0
	}
	ratio := targetBDP / float64(s.initialWindow)
	if ratio <= 1 {
		return 1
	}
	d := s.cfg.StartupTargetDuration
	if d <= 0 {
		d = 5 * time.Second
	}
	p := s.minRTT.Seconds() / d.Seconds()
	if p <= 0 {
		return 0
	}
	return math.Pow(ratio, p)
}

func (s *adaptiveBDPSender) startupPacingGain() float64 {
	gain := s.cfg.StartupPacingGain
	if gain <= 0 {
		gain = 2.0
	}
	required := s.startupRequiredGainPerRTT()
	if required > 0 {
		gain = max(gain, required)
	}
	return min(2.77, max(1.25, gain))
}

func (s *adaptiveBDPSender) pacingGain() float64 {
	switch s.state {
	case adaptiveBDPStartup:
		return s.startupPacingGain()
	case adaptiveBDPDrain:
		return 0.5
	case adaptiveBDPProbeDown:
		return s.probeDownGain()
	case adaptiveBDPProbeBW:
		if s.probeUpActive {
			return s.probeUpGain()
		}
		return s.cruisePacingGain()
	default:
		return 1.0
	}
}

func (s *adaptiveBDPSender) windowGain() float64 {
	if s.cfg.WindowGain <= 0 {
		return 1.0
	}
	return s.cfg.WindowGain
}

func (s *adaptiveBDPSender) startupCwndGain() float64 {
	if s.cfg.StartupCwndGain <= 0 {
		return 2.0
	}
	return s.cfg.StartupCwndGain
}

func (s *adaptiveBDPSender) probeUpGain() float64 {
	if s.cfg.ProbeUpGain <= 0 {
		return 1.25
	}
	return s.cfg.ProbeUpGain
}

func (s *adaptiveBDPSender) probeDownGain() float64 {
	if s.cfg.ProbeDownGain <= 0 {
		return 0.90
	}
	return s.cfg.ProbeDownGain
}

func (s *adaptiveBDPSender) cruisePacingGain() float64 {
	if s.cfg.CruisePacingGain <= 0 {
		return adaptiveBDPCruisePacingGain
	}
	return s.cfg.CruisePacingGain
}

func (s *adaptiveBDPSender) cruiseCwndGain() float64 {
	if s.cfg.CruiseCwndGain <= 0 {
		return 1.5
	}
	return s.cfg.CruiseCwndGain
}

func (s *adaptiveBDPSender) queueTarget() time.Duration {
	if s.cfg.QueueTarget > 0 {
		return s.cfg.QueueTarget
	}
	base := s.minRTT / 8
	return max(5*time.Millisecond, min(25*time.Millisecond, base))
}

func (s *adaptiveBDPSender) queuePersistentRounds() uint32 {
	if s.cfg.QueuePersistentRounds == 0 {
		return 2
	}
	return s.cfg.QueuePersistentRounds
}

func (s *adaptiveBDPSender) lossTarget() float64 {
	if s.cfg.LossTarget <= 0 {
		return 0.005
	}
	return s.cfg.LossTarget
}

func (s *adaptiveBDPSender) emergencyLossThreshold() float64 {
	if s.cfg.EmergencyLossThreshold <= 0 {
		return 0.02
	}
	return s.cfg.EmergencyLossThreshold
}

func (s *adaptiveBDPSender) downshiftRatio() float64 {
	if s.cfg.DownshiftRatio <= 0 {
		return 0.85
	}
	return s.cfg.DownshiftRatio
}

func (s *adaptiveBDPSender) downshiftRoundsTarget() uint32 {
	if s.cfg.DownshiftRounds == 0 {
		return 2
	}
	return s.cfg.DownshiftRounds
}

func (s *adaptiveBDPSender) minRTTFilterWindow() time.Duration {
	if s.cfg.MinRTTFilterWindow <= 0 {
		return 10 * time.Second
	}
	return s.cfg.MinRTTFilterWindow
}

func (s *adaptiveBDPSender) probeInterval() time.Duration {
	if s.cfg.ProbeInterval <= 0 {
		return 5 * time.Second
	}
	return s.cfg.ProbeInterval
}

func (s *adaptiveBDPSender) pacingMargin() float64 {
	if s.cfg.PacingMargin <= 0 {
		return 0.01
	}
	return min(0.99, s.cfg.PacingMargin)
}

func roundUpToMSS(v, mss protocol.ByteCount) protocol.ByteCount {
	if mss <= 0 {
		return v
	}
	if v <= 0 {
		return mss
	}
	r := v % mss
	if r == 0 {
		return v
	}
	return v + (mss - r)
}

func clampCwnd(v, minCwnd, maxCwnd protocol.ByteCount) protocol.ByteCount {
	if v < minCwnd {
		return minCwnd
	}
	if v > maxCwnd {
		return maxCwnd
	}
	return v
}
