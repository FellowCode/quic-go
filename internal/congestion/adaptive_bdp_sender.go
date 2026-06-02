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

type adaptiveQueueState uint8

const (
	adaptiveQueueUnknown adaptiveQueueState = iota
	adaptiveQueueEmpty
	adaptiveQueueBuilding
	adaptiveQueuePersistent
)

func (s adaptiveQueueState) String() string {
	switch s {
	case adaptiveQueueEmpty:
		return "empty"
	case adaptiveQueueBuilding:
		return "building"
	case adaptiveQueuePersistent:
		return "persistent"
	default:
		return "unknown"
	}
}

const (
	adaptiveBDPHealthyBandwidthRatio = 0.98
	adaptiveBDPCruisePacingGain      = 1.05
)

type noQueueLowSampleState struct {
	active      bool
	startTime   monotime.Time
	lastRound   uint64
	rounds      uint32
	acked       protocol.ByteCount
	minSampleBW uint64
}

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
	noQueueLow      noQueueLowSampleState

	lastRetransmittableSentTime monotime.Time
	uploadWarmupStartTime       monotime.Time
	uploadWarmupAcked           protocol.ByteCount

	lastProbeTime             monotime.Time
	probeUpRoundStart         uint64
	probeUpActive             bool
	suppressProbeUpUntilRound uint64
	suppressProbeUpReason     string

	lostBytesThisRound          protocol.ByteCount
	ackedBytesThisRound         protocol.ByteCount
	lossRatioEWMA               float64
	mildLossRounds              uint32
	lossFreeRounds              uint32
	lastMaterialLossRound       uint64
	hasMaterialLossRound        bool
	lossRecoveryProbeBW         uint64
	lossRecoveryProbeUntilRound uint64
	lossRecoveryProbeActive     bool
	lastLossRecoveryProbeRound  uint64
	hasLastLossRecoveryProbe    bool

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
	lastLossCutbackTime       monotime.Time
	lastLossActionReason      string
	lastLossCwndMultiplier    float64
	lastLossPacingMultiplier  float64
	lastEmergencyCutbackRound uint64
	hasLastEmergencyCutback   bool
	lastECNCERound            uint64
	hasLastECNCE              bool

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
	bytesInFlight protocol.ByteCount,
	_ protocol.PacketNumber,
	bytes protocol.ByteCount,
	isRetransmittable bool,
) {
	s.pacer.SentPacket(sentTime, bytes)
	if !isRetransmittable {
		return
	}
	s.maybeStartUploadWarmup(sentTime, bytesInFlight)
	s.lastRetransmittableSentTime = sentTime
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
	if !s.uploadWarmupStartTime.IsZero() {
		s.uploadWarmupAcked += ackedBytes
	}
	s.updateMinRTT(sample.RTT, eventTime)
	s.updateRound(sample, priorInFlight, eventTime)

	s.updateBandwidthAt(sample, priorInFlight, eventTime)
	s.maybeStartLossRecoveryProbe(eventTime, sample, priorInFlight)

	enterProbeDown := s.shouldEnterProbeDown(sample, priorInFlight, eventTime)
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

	s.handleLossReaction(s.clock.Now(), priorInFlight)
	s.updateDebugSnapshot(priorInFlight)
}

func (s *adaptiveBDPSender) OnECNCongestionEvent(priorInFlight protocol.ByteCount, eventTime monotime.Time) {
	s.lastPriorInFlight = priorInFlight
	s.lastECNCERound = s.roundCount
	s.hasLastECNCE = true
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
		QueueState:  s.queueState().String(),
		PacingGain:  s.lastPacingGain,
		CwndGain:    s.lastCwndGain,

		NegativeBandwidthConfidence:    s.negativeBandwidthConfidence(),
		HasCongestionEvidence:          s.hasCongestionEvidence(),
		PipeForDownshift:               s.pipeForDownshift(),
		PipeFillThreshold:              s.pipeFillThreshold(),
		ActiveBandwidthBeforeDownshift: s.activeBandwidthBeforeDownshift(),
		NoCongestionRateFloor:          s.noCongestionRateFloorBytesPerSecond(),
		NoQueueLowRounds:               s.noQueueLow.rounds,
		NoQueueLowAcked:                s.noQueueLow.acked,

		LossRatioRound:              s.roundLossRatio(),
		LossRatioEWMA:               s.lossRatioEWMA,
		LostBytesThisRound:          s.lostBytesThisRound,
		AckedBytesThisRound:         s.ackedBytesThisRound,
		LossMinBytes:                s.lossMinBytes(),
		EmergencyLossMinBytes:       s.emergencyLossMinBytes(),
		MinLossSampleBytes:          s.minLossSampleBytes(),
		LossGraceRatio:              s.lossGraceRatio(),
		LossSevereThreshold:         s.lossSevereThreshold(),
		EmergencyLossThreshold:      s.emergencyLossThreshold(),
		QueuePressure:               s.queuePressure(),
		MildLossRounds:              s.mildLossRounds,
		LastLossActionReason:        s.lastLossActionReason,
		LastLossCwndMultiplier:      s.lastLossCwndMultiplier,
		LastLossPacingMultiplier:    s.lastLossPacingMultiplier,
		LastLossCutbackRound:        s.lastLossCutbackRound,
		SuppressProbeUpUntilRound:   s.suppressProbeUpUntilRound,
		SuppressProbeUpReason:       s.suppressProbeUpReason,
		LossFreeRounds:              s.lossFreeRounds,
		LastMaterialLossRound:       s.lastMaterialLossRound,
		LossRecoveryProbeActive:     s.lossRecoveryProbeActive,
		LossRecoveryProbeBW:         s.lossRecoveryProbeBW,
		LossRecoveryProbeUntilRound: s.lossRecoveryProbeUntilRound,

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
	s.updateLossEWMA()
	if s.roundHasMaterialLoss() {
		s.noteMaterialLossRound()
	} else {
		s.noteLossFreeRound()
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
	s.updateBandwidthAt(sample, priorInFlight, s.clock.Now())
}

func (s *adaptiveBDPSender) updateBandwidthAt(sample RateSample, priorInFlight protocol.ByteCount, eventTime monotime.Time) {
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
		if s.maxBw == 0 || sampleBW > s.maxBw {
			s.bwFilter.Update(s.roundCount, sampleBW)
			s.maxBw = max(sampleBW, s.bwFilter.Max(s.roundCount))
			if s.maxBw != prevMaxBw {
				s.lastBWChangeReason = "max_bw_increased_by_delivery_sample"
			}
		}
	}

	activeBW := s.activeBandwidthBeforeDownshift()
	if activeBW == 0 {
		activeBW = sampleBW
	}

	if !sample.AppLimited && activeBW > 0 && float64(sampleBW) < float64(activeBW)*s.downshiftRatio() {
		if s.inUploadWarmup(eventTime) {
			s.downshiftRounds = 0
			s.noQueueLow = noQueueLowSampleState{}
			s.lastBWChangeReason = "upload_warmup_low_sample_not_capacity_proof"
		} else if !s.canUseSampleForDownshift(sample, priorInFlight) {
			s.downshiftRounds = 0
			s.noQueueLow = noQueueLowSampleState{}
			if s.queueState() == adaptiveQueueEmpty {
				s.lastBWChangeReason = "queue_empty_low_sample_not_capacity_proof"
			} else if !s.isPipeFilledForDownshift(priorInFlight) {
				s.lastBWChangeReason = "pipe_not_filled_for_downshift"
			} else {
				s.lastBWChangeReason = "low_sample_no_queue_rejected"
			}
		} else if s.hasCongestionEvidence() {
			s.downshiftRounds++
			s.noQueueLow = noQueueLowSampleState{}
			if s.downshiftRounds < s.congestionDownshiftRoundsTarget() {
				s.lastBWChangeReason = "congestion_downshift_waiting_rounds"
			} else {
				s.confirmedCongestionDownshift(sampleBW, eventTime)
			}
		} else {
			s.downshiftRounds = 0
			s.noQueueLowSampleCandidate(sampleBW, sample, priorInFlight, eventTime)
		}
	} else if !sample.AppLimited {
		s.downshiftRounds = 0
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

func (s *adaptiveBDPSender) maybeStartUploadWarmup(sentTime monotime.Time, bytesInFlight protocol.ByteCount) {
	if bytesInFlight != 0 {
		return
	}
	if s.lastRetransmittableSentTime.IsZero() || sentTime.Sub(s.lastRetransmittableSentTime) >= s.uploadWarmupDuration() {
		s.uploadWarmupStartTime = sentTime
		s.uploadWarmupAcked = 0
	}
}

func (s *adaptiveBDPSender) uploadWarmupDuration() time.Duration {
	if s.cfg.UploadWarmupDuration > 0 {
		return s.cfg.UploadWarmupDuration
	}
	if s.minRTT > 0 {
		return maxDuration(3*s.minRTT, time.Second)
	}
	return time.Second
}

func (s *adaptiveBDPSender) uploadWarmupBytes() protocol.ByteCount {
	if s.cfg.UploadWarmupBytes > 0 {
		return protocol.ByteCount(s.cfg.UploadWarmupBytes)
	}
	bdp := s.bdp()
	if bdp > 0 {
		return max(2*bdp, 256*1024)
	}
	return 256 * 1024
}

func (s *adaptiveBDPSender) inUploadWarmup(now monotime.Time) bool {
	if s.uploadWarmupStartTime.IsZero() {
		return false
	}
	return now.Sub(s.uploadWarmupStartTime) < s.uploadWarmupDuration() || s.uploadWarmupAcked < s.uploadWarmupBytes()
}

func (s *adaptiveBDPSender) canUseSampleForDownshift(sample RateSample, priorInFlight protocol.ByteCount) bool {
	if !sample.IsValid || sample.DeliveryRate == 0 {
		return false
	}
	if sample.AppLimited {
		return false
	}
	if s.cfg.MinDownshiftSampleBytes > 0 && sample.AckedBytes < s.minDownshiftSampleBytes() {
		return false
	}
	return s.isPipeFilledForDownshift(priorInFlight)
}

func (s *adaptiveBDPSender) canUseSampleForNoQueueDownshift(sample RateSample, priorInFlight protocol.ByteCount, _ monotime.Time) bool {
	if !s.canUseSampleForDownshift(sample, priorInFlight) {
		return false
	}
	return s.queueState() == adaptiveQueueEmpty
}

func (s *adaptiveBDPSender) noQueueLowSampleCandidate(sampleBW uint64, sample RateSample, priorInFlight protocol.ByteCount, eventTime monotime.Time) {
	if !s.canUseSampleForNoQueueDownshift(sample, priorInFlight, eventTime) {
		s.noQueueLow = noQueueLowSampleState{}
		if s.queueState() == adaptiveQueueEmpty {
			s.lastBWChangeReason = "queue_empty_low_sample_not_capacity_proof"
		} else {
			s.lastBWChangeReason = "low_sample_no_queue_rejected"
		}
		return
	}

	if !s.noQueueLow.active {
		s.noQueueLow = noQueueLowSampleState{
			active:      true,
			startTime:   eventTime,
			lastRound:   s.roundCount,
			rounds:      1,
			acked:       sample.AckedBytes,
			minSampleBW: sampleBW,
		}
		s.lastBWChangeReason = "low_sample_no_queue_candidate_started"
		return
	}

	if s.noQueueLow.lastRound != s.roundCount {
		s.noQueueLow.rounds++
		s.noQueueLow.lastRound = s.roundCount
	}
	s.noQueueLow.acked += sample.AckedBytes
	s.noQueueLow.minSampleBW = min(s.noQueueLow.minSampleBW, sampleBW)
	if s.noQueueLowConfirmed(eventTime) {
		s.applyGradualNoQueueDownshift(eventTime)
	}
}

func (s *adaptiveBDPSender) noQueueLowConfirmed(eventTime monotime.Time) bool {
	rounds := s.cfg.NoCongestionDownshiftRounds
	if rounds == 0 {
		rounds = 4
	}
	if s.noQueueLow.rounds < rounds {
		s.lastBWChangeReason = "low_sample_no_queue_candidate_waiting_rounds"
		return false
	}

	minRTT := s.minRTT
	if minRTT <= 0 {
		minRTT = 100 * time.Millisecond
	}
	minDuration := maxDuration(3*minRTT, time.Second)
	if eventTime.Sub(s.noQueueLow.startTime) < minDuration {
		s.lastBWChangeReason = "low_sample_no_queue_candidate_waiting_duration"
		return false
	}

	if s.noQueueLow.acked < s.minDownshiftSampleBytes() {
		s.lastBWChangeReason = "low_sample_no_queue_candidate_waiting_bytes"
		return false
	}
	return true
}

func (s *adaptiveBDPSender) applyGradualNoQueueDownshift(_ monotime.Time) {
	active := s.activeBandwidthBeforeDownshift()
	if active == 0 {
		return
	}
	if s.shortBw > 0 {
		active = min(active, s.shortBw)
	}
	candidate := s.noQueueLow.minSampleBW
	factor := s.cfg.NoCongestionDownshiftFactor
	if factor <= 0 {
		factor = 0.75
	}
	if factor > 1 {
		factor = 1
	}
	newShort := max(candidate, uint64(float64(active)*factor))
	if floor := s.noCongestionRateFloorBytesPerSecond(); floor > 0 {
		newShort = max(newShort, floor)
	}
	if s.shortBw == 0 {
		s.shortBw = newShort
	} else {
		s.shortBw = min(s.shortBw, newShort)
	}
	s.noQueueLow = noQueueLowSampleState{}
	s.lastBWChangeReason = "short_bw_gradual_no_queue_downshift"
}

func (s *adaptiveBDPSender) confirmedCongestionDownshift(sampleBW uint64, eventTime monotime.Time) {
	confidence := s.negativeBandwidthConfidence()
	if confidence <= 0 {
		return
	}
	active := s.activeBandwidthBeforeDownshift()
	if active == 0 {
		active = s.bw
	}
	newBW := uint64(float64(active)*(1-confidence) + float64(sampleBW)*confidence)
	newBW = max(newBW, s.minimumObservableBandwidth())
	if s.shortBw == 0 {
		s.shortBw = newBW
	} else {
		s.shortBw = min(s.shortBw, newBW)
	}
	s.enterStateWithReason(adaptiveBDPProbeDown, eventTime, "short_bw_downshift_with_congestion_evidence")
	s.lastBWChangeReason = "short_bw_downshift_with_congestion_evidence"
}

func (s *adaptiveBDPSender) activeBandwidthBeforeDownshift() uint64 {
	active := s.maxBw
	if active == 0 {
		active = s.bw
	}
	if active == 0 && s.lastRateSample.DeliveryRate > 0 {
		active = uint64(s.lastRateSample.DeliveryRate)
	}
	return active
}

func (s *adaptiveBDPSender) lossRecoveryGoalBandwidth() uint64 {
	goal := s.maxBw
	if s.cfg.StartupTargetRateBps > 0 {
		goal = max(goal, s.cfg.StartupTargetRateBps/8)
	}
	if floor := s.noCongestionRateFloorBytesPerSecond(); floor > 0 {
		goal = max(goal, floor)
	}
	if s.cfg.MaxProbeRateBps > 0 {
		goal = min(goal, s.cfg.MaxProbeRateBps/8)
	}
	return goal
}

func (s *adaptiveBDPSender) currentRecoveryBaseBandwidth() uint64 {
	cur := s.bw
	if cur == 0 {
		cur = s.minimumObservableBandwidth()
	}
	if s.shortBw > 0 {
		cur = min(cur, s.shortBw)
	}
	if cur == 0 && s.lastRateSample.DeliveryRate > 0 {
		cur = uint64(s.lastRateSample.DeliveryRate)
	}
	return max(cur, 1)
}

func (s *adaptiveBDPSender) clearProbeSuppressAfterLossRecovery() {
	if s.lossFreeRounds >= s.lossRecoveryProbeRounds() && s.queueState() == adaptiveQueueEmpty && !s.hasRecentECNCE() {
		if s.suppressProbeUpUntilRound > s.roundCount {
			s.suppressProbeUpUntilRound = 0
			s.suppressProbeUpReason = "cleared_after_loss_free_rounds"
		}
	}
}

func (s *adaptiveBDPSender) maybeStartLossRecoveryProbe(eventTime monotime.Time, sample RateSample, _ protocol.ByteCount) {
	if s.lossFreeRounds < s.lossRecoveryProbeRounds() {
		return
	}
	if s.queueState() != adaptiveQueueEmpty {
		return
	}
	if s.hasRecentECNCE() || s.hasFreshMaterialLoss() {
		return
	}
	if sample.AppLimited {
		return
	}

	goal := s.lossRecoveryGoalBandwidth()
	if goal == 0 {
		return
	}

	cur := s.currentRecoveryBaseBandwidth()
	if cur >= goal {
		return
	}

	next := uint64(float64(cur) * s.lossRecoveryProbeGain())
	if floor := s.noCongestionRateFloorBytesPerSecond(); floor > 0 {
		next = max(next, min(goal, floor))
	}
	next = min(goal, max(next, cur+uint64(s.maxDatagramSize)))

	s.clearProbeSuppressAfterLossRecovery()

	s.lossRecoveryProbeBW = next
	s.lossRecoveryProbeUntilRound = s.roundCount + s.lossRecoveryProbeDurationRounds()
	s.lossRecoveryProbeActive = true
	s.lastLossRecoveryProbeRound = s.roundCount
	s.hasLastLossRecoveryProbe = true

	if s.shortBw > 0 && s.shortBw < next {
		s.shortBw = next
		s.lastBWChangeReason = "short_bw_lifted_after_loss_free_rounds"
	}
	if s.maxBw > 0 && s.shortBw > 0 && float64(s.shortBw) >= float64(s.maxBw)*s.lossRecoveryClearShortBwFraction() {
		s.shortBw = 0
		s.lastBWChangeReason = "short_bw_cleared_after_loss_recovery"
	}

	s.probeUpActive = true
	s.probeUpRoundStart = s.roundCount
	s.lastProbeTime = eventTime
	s.enterStateWithReason(adaptiveBDPProbeBW, eventTime, "loss_free_recovery_probe")
}

func (s *adaptiveBDPSender) pipeForDownshift() protocol.ByteCount {
	bw := s.maxBw
	if bw == 0 {
		bw = s.bw
	}
	if s.cfg.StartupTargetRateBps > 0 {
		bw = max(bw, s.cfg.StartupTargetRateBps/8)
	}
	if floor := s.noCongestionRateFloorBytesPerSecond(); floor > 0 {
		bw = max(bw, floor)
	}
	if bw == 0 || s.minRTT <= 0 {
		return 0
	}
	return s.bdpForBandwidth(bw)
}

func (s *adaptiveBDPSender) pipeFillThreshold() protocol.ByteCount {
	pipe := s.pipeForDownshift()
	if pipe == 0 {
		return 0
	}
	fill := 0.75
	if !s.hasCongestionEvidence() {
		fill = 0.90
	}
	threshold := protocol.ByteCount(float64(pipe) * fill)
	return max(threshold, 4*s.maxDatagramSize)
}

func (s *adaptiveBDPSender) isPipeFilledForDownshift(priorInFlight protocol.ByteCount) bool {
	threshold := s.pipeFillThreshold()
	if threshold == 0 {
		return false
	}
	return priorInFlight+2*s.maxDatagramSize >= threshold
}

func (s *adaptiveBDPSender) queueState() adaptiveQueueState {
	if s.minRTT <= 0 {
		return adaptiveQueueUnknown
	}
	q := s.queueDelay()
	target := s.queueTarget()
	if target <= 0 {
		return adaptiveQueueUnknown
	}
	if q <= target/2 {
		return adaptiveQueueEmpty
	}
	if q <= target {
		return adaptiveQueueBuilding
	}
	if s.hasPersistentQueuePressure() {
		return adaptiveQueuePersistent
	}
	return adaptiveQueueBuilding
}

func (s *adaptiveBDPSender) hasCongestionEvidence() bool {
	if s.queueState() == adaptiveQueuePersistent {
		return true
	}
	if s.hasRecentECNCE() {
		return true
	}
	return s.lostBytesThisRound >= s.lossMinBytes() && s.lossRateThisRound() > s.lossTarget()
}

func (s *adaptiveBDPSender) hasRecentECNCE() bool {
	if !s.hasLastECNCE {
		return false
	}
	return s.lastECNCERound == s.roundCount || s.lastECNCERound+1 == s.roundCount
}

func (s *adaptiveBDPSender) hasFreshMaterialLoss() bool {
	if !s.hasMaterialLossRound {
		return false
	}
	return s.lastMaterialLossRound == s.roundCount || s.lastMaterialLossRound+1 == s.roundCount
}

func (s *adaptiveBDPSender) negativeBandwidthConfidence() float64 {
	q := 0.0
	if target := s.queueTarget(); target > 0 {
		delay := s.queueDelay()
		if delay > target/2 {
			q = clampFloat(float64(delay-target/2)/float64(target), 0, 1)
		}
	}

	loss := 0.0
	lr := s.lossRateThisRound()
	if s.lostBytesThisRound >= s.lossMinBytes() && lr > s.lossGraceRatio() {
		denom := s.lossSevereThreshold() - s.lossGraceRatio()
		if denom <= 0 {
			denom = 0.01
		}
		loss = clampFloat((lr-s.lossGraceRatio())/denom, 0, 1)
	}

	ecn := 0.0
	if s.hasRecentECNCE() {
		ecn = 1.0
	}

	return max(q, max(loss, ecn))
}

func (s *adaptiveBDPSender) lossRateThisRound() float64 {
	return s.roundLossRatio()
}

func (s *adaptiveBDPSender) lossGraceRatio() float64 {
	if s.cfg.LossGraceRatio > 0 {
		return s.cfg.LossGraceRatio
	}
	return 0.01
}

func (s *adaptiveBDPSender) lossSoftThreshold() float64 {
	if s.cfg.LossSoftThreshold > 0 {
		return s.cfg.LossSoftThreshold
	}
	return s.lossGraceRatio()
}

func (s *adaptiveBDPSender) lossMinBytes() protocol.ByteCount {
	if s.cfg.LossMinBytes > 0 {
		return protocol.ByteCount(s.cfg.LossMinBytes)
	}
	return 2 * s.maxDatagramSize
}

func (s *adaptiveBDPSender) emergencyLossMinBytes() protocol.ByteCount {
	if s.cfg.EmergencyLossMinBytes > 0 {
		return protocol.ByteCount(s.cfg.EmergencyLossMinBytes)
	}
	return 8 * s.maxDatagramSize
}

func (s *adaptiveBDPSender) lossSevereThreshold() float64 {
	if s.cfg.LossSevereThreshold > 0 {
		return max(s.cfg.LossSevereThreshold, s.lossSoftThreshold())
	}
	return 0.05
}

func (s *adaptiveBDPSender) lossEWMAAlpha() float64 {
	if s.cfg.LossEWMAAlpha > 0 {
		return clampFloat(s.cfg.LossEWMAAlpha, 0.01, 1)
	}
	return 0.25
}

func (s *adaptiveBDPSender) maxLossCwndCutNoQueue() float64 {
	if s.cfg.MaxLossCwndCutNoQueue > 0 {
		return clampFloat(s.cfg.MaxLossCwndCutNoQueue, 0, 0.50)
	}
	return 0.15
}

func (s *adaptiveBDPSender) maxLossCwndCutWithQueue() float64 {
	if s.cfg.MaxLossCwndCutWithQueue > 0 {
		return clampFloat(s.cfg.MaxLossCwndCutWithQueue, 0, 0.50)
	}
	return 0.30
}

func (s *adaptiveBDPSender) minLossCwndCut() float64 {
	if s.cfg.MinLossCwndCut > 0 {
		return clampFloat(s.cfg.MinLossCwndCut, 0, 0.10)
	}
	return 0.01
}

func (s *adaptiveBDPSender) maxLossPacingCutNoQueue() float64 {
	if s.cfg.MaxLossPacingCutNoQueue > 0 {
		return clampFloat(s.cfg.MaxLossPacingCutNoQueue, 0, 0.50)
	}
	return 0.10
}

func (s *adaptiveBDPSender) maxLossPacingCutWithQueue() float64 {
	if s.cfg.MaxLossPacingCutWithQueue > 0 {
		return clampFloat(s.cfg.MaxLossPacingCutWithQueue, 0, 0.50)
	}
	return 0.25
}

func (s *adaptiveBDPSender) noCongestionRateFloorBytesPerSecond() uint64 {
	if s.hasCongestionEvidence() {
		return 0
	}
	if s.cfg.MinProbeRateBps > 0 {
		return s.cfg.MinProbeRateBps / 8
	}
	if s.cfg.StartupTargetRateBps == 0 {
		return 0
	}
	fraction := s.cfg.NoCongestionRateFloorFraction
	if fraction <= 0 {
		fraction = 0.5
	}
	return uint64((float64(s.cfg.StartupTargetRateBps) / 8.0) * clampFloat(fraction, 0, 1))
}

func (s *adaptiveBDPSender) noCongestionCwndFloor() protocol.ByteCount {
	rateFloor := uint64(0)
	if s.cfg.MinProbeRateBps > 0 {
		rateFloor = s.cfg.MinProbeRateBps / 8
	} else if s.cfg.StartupTargetRateBps > 0 {
		fraction := s.cfg.NoCongestionRateFloorFraction
		if fraction <= 0 {
			fraction = 0.5
		}
		rateFloor = uint64((float64(s.cfg.StartupTargetRateBps) / 8.0) * clampFloat(fraction, 0, 1))
	}
	if rateFloor == 0 {
		return 0
	}
	floor := protocol.ByteCount(float64(s.bdpForBandwidth(rateFloor)) * s.cruiseCwndGain())
	if floor == 0 {
		return 0
	}
	return max(s.minCongestionWindow, roundUpToMSS(floor, s.maxDatagramSize))
}

func (s *adaptiveBDPSender) bdpForBandwidth(bw uint64) protocol.ByteCount {
	if bw == 0 || s.minRTT <= 0 {
		return 0
	}
	return roundUpToMSS(protocol.ByteCount(float64(bw)*s.minRTT.Seconds()), s.maxDatagramSize)
}

func (s *adaptiveBDPSender) minDownshiftSampleBytes() protocol.ByteCount {
	if s.cfg.MinDownshiftSampleBytes > 0 {
		return protocol.ByteCount(s.cfg.MinDownshiftSampleBytes)
	}
	pipe := s.pipeForDownshift()
	if pipe > 0 {
		return max(16*s.maxDatagramSize, min(pipe/4, 128*s.maxDatagramSize))
	}
	return 16 * s.maxDatagramSize
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
	if floor := s.noCongestionRateFloorBytesPerSecond(); floor > 0 {
		rate = max(rate, float64(floor))
	}
	minRate := s.minimumPacingRate()
	if s.inProtectedStartup(s.clock.Now()) {
		minRate = max(minRate, s.startupProtectedPacingRate())
	}
	s.pacingRateBytesPerSecond = max(uint64(rate), minRate)
}

func (s *adaptiveBDPSender) applyTemporaryPacingMultiplier(multiplier float64, _ monotime.Time, _ time.Duration) {
	if multiplier <= 0 {
		return
	}
	if s.pacingRateBytesPerSecond == 0 {
		s.updatePacingRate()
	}
	rate := uint64(float64(s.pacingRateBytesPerSecond) * multiplier)
	s.pacingRateBytesPerSecond = max(rate, s.minimumPacingRate())
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
	return s.bdpForBandwidth(s.bw)
}

func (s *adaptiveBDPSender) targetCwnd() protocol.ByteCount {
	windowGain := s.windowGain()
	bdp := s.bdp()
	if floor := s.noCongestionRateFloorBytesPerSecond(); floor > 0 {
		bdp = max(bdp, s.bdpForBandwidth(floor))
	}
	base := float64(bdp) * s.cwndGain() * windowGain
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

	if s.hasCongestionEvidence() {
		s.congestionWindow = max(target, s.minCongestionWindow)
		s.noteCwndChange(oldCwnd, "congestion_target_cutback")
		return
	}

	floor := max(target, protocol.ByteCount(float64(oldCwnd)*s.noCongestionCwndCutbackFactor()))
	if rateFloor := s.noCongestionRateFloorBytesPerSecond(); rateFloor > 0 {
		floorBDP := s.bdpForBandwidth(rateFloor)
		if floorBDP > 0 {
			floor = max(floor, roundUpToMSS(protocol.ByteCount(float64(floorBDP)*s.cruiseCwndGain()), s.maxDatagramSize))
		}
	}
	if s.inProtectedStartup(s.clock.Now()) {
		floor = max(floor, s.initialWindow)
	}

	newCwnd := min(oldCwnd, floor)
	s.congestionWindow = clampCwnd(newCwnd, s.minCongestionWindow, s.maxCongestionWindow)
	s.noteCwndChange(oldCwnd, "gradual_no_congestion_target_cutback_capped")
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
	return s.hasQueuePressure() && s.isPipeFilledForDownshift(priorInFlight)
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

func (s *adaptiveBDPSender) canReactToLoss() bool {
	if s.lostBytesThisRound < s.lossMinBytes() {
		s.lastLossActionReason = "loss_below_absolute_threshold"
		return false
	}
	if !s.hasEnoughLossSample() {
		s.lastLossActionReason = "loss_sample_too_small"
		return false
	}
	return true
}

func (s *adaptiveBDPSender) roundHasMaterialLoss() bool {
	if s.lostBytesThisRound < s.lossMinBytes() {
		return false
	}
	if s.ackedBytesThisRound+s.lostBytesThisRound < s.minLossSampleBytes() {
		return false
	}
	return s.lossRateThisRound() > s.lossGraceRatio()
}

func (s *adaptiveBDPSender) noteMaterialLossRound() {
	s.lossFreeRounds = 0
	s.lastMaterialLossRound = s.roundCount
	s.hasMaterialLossRound = true
	s.lossRecoveryProbeActive = false
	s.lossRecoveryProbeBW = 0
}

func (s *adaptiveBDPSender) noteLossFreeRound() {
	if s.ackedBytesThisRound < s.minLossSampleBytes() {
		return
	}
	s.lossFreeRounds++
}

func (s *adaptiveBDPSender) updateMildLossRounds(lossRatio float64) {
	if lossRatio > s.lossGraceRatio() {
		s.mildLossRounds++
	} else {
		s.mildLossRounds = 0
	}
}

func (s *adaptiveBDPSender) mildLossPersistentRoundsTarget() uint32 {
	if s.cfg.MildLossPersistentRounds > 0 {
		return s.cfg.MildLossPersistentRounds
	}
	return 2
}

func (s *adaptiveBDPSender) lossRecoveryProbeRounds() uint32 {
	if s.cfg.LossRecoveryProbeRounds == 0 {
		return 2
	}
	return s.cfg.LossRecoveryProbeRounds
}

func (s *adaptiveBDPSender) lossRecoveryProbeGain() float64 {
	if s.cfg.LossRecoveryProbeGain <= 0 {
		return 1.25
	}
	return clampFloat(s.cfg.LossRecoveryProbeGain, 1.01, 2.0)
}

func (s *adaptiveBDPSender) lossRecoveryProbeDurationRounds() uint64 {
	if s.cfg.LossRecoveryProbeDurationRounds == 0 {
		return 1
	}
	return uint64(s.cfg.LossRecoveryProbeDurationRounds)
}

func (s *adaptiveBDPSender) lossRecoveryClearShortBwFraction() float64 {
	if s.cfg.LossRecoveryClearShortBwFraction <= 0 {
		return 0.95
	}
	return clampFloat(s.cfg.LossRecoveryClearShortBwFraction, 0.50, 1.0)
}

func (s *adaptiveBDPSender) suppressProbeUpForOneRound(reason string) {
	s.suppressProbeUpUntilRound = max(s.suppressProbeUpUntilRound, s.roundCount+1)
	s.suppressProbeUpReason = reason
	s.probeUpActive = false
	s.probeUpRoundStart = s.roundCount
	s.lastLossActionReason = reason
	s.lastStateChangeReason = reason
}

func (s *adaptiveBDPSender) canProbeUp() bool {
	queueState := s.queueState()
	if queueState == adaptiveQueuePersistent {
		return false
	}
	if s.hasRecentECNCE() {
		return false
	}
	if s.hasFreshMaterialLoss() {
		return false
	}
	if s.suppressProbeUpUntilRound == 0 || s.roundCount > s.suppressProbeUpUntilRound {
		return true
	}
	if s.lossFreeRounds >= s.lossRecoveryProbeRounds() && queueState == adaptiveQueueEmpty {
		s.suppressProbeUpUntilRound = 0
		s.suppressProbeUpReason = "cleared_after_loss_free_rounds"
		return true
	}
	return false
}

func (s *adaptiveBDPSender) isFreshBandwidthRound(round uint64) bool {
	return round == s.roundCount || round+1 == s.roundCount
}

func (s *adaptiveBDPSender) lossCutbackCooldown() time.Duration {
	if s.cfg.LossCutbackCooldown > 0 {
		return s.cfg.LossCutbackCooldown
	}
	if s.minRTT > 0 {
		return s.minRTT
	}
	return 100 * time.Millisecond
}

func (s *adaptiveBDPSender) canCutbackForLoss(now monotime.Time) bool {
	if s.hasLastLossCutbackRound && s.lastLossCutbackRound == s.roundCount {
		return false
	}
	if !s.lastLossCutbackTime.IsZero() && now.Sub(s.lastLossCutbackTime) < s.lossCutbackCooldown() {
		return false
	}
	return true
}

func (s *adaptiveBDPSender) markLossCutback(now monotime.Time) {
	s.lastLossCutbackRound = s.roundCount
	s.hasLastLossCutbackRound = true
	s.lastLossCutbackTime = now
}

func (s *adaptiveBDPSender) canEmergencyCutbackThisRound() bool {
	return !s.hasLastEmergencyCutback || s.lastEmergencyCutbackRound != s.roundCount
}

func (s *adaptiveBDPSender) markEmergencyCutbackRound() {
	s.lastEmergencyCutbackRound = s.roundCount
	s.hasLastEmergencyCutback = true
}

func (s *adaptiveBDPSender) handleLossReaction(eventTime monotime.Time, priorInFlight protocol.ByteCount) {
	s.updateLossEWMA()
	lossRatio := max(s.roundLossRatio(), s.lossRatioEWMA)
	s.updateMildLossRounds(lossRatio)
	if lossRatio <= 0 {
		return
	}
	if !s.canReactToLoss() {
		s.lastStateChangeReason = s.lastLossActionReason
		return
	}
	if s.shouldEmergencyCutback(lossRatio) {
		s.applyEmergencyLossCutback(eventTime, lossRatio)
		return
	}

	hasQueue := s.queuePressure() > 0 || s.hasRecentECNCE()
	if !hasQueue && lossRatio <= s.lossGraceRatio() {
		s.suppressProbeUpForOneRound("mild_loss_below_grace_no_cwnd_cut")
		return
	}
	if !hasQueue && s.mildLossRounds < s.mildLossPersistentRoundsTarget() {
		s.suppressProbeUpForOneRound("mild_loss_waiting_persistence")
		return
	}
	if !s.canCutbackForLoss(eventTime) {
		s.lastLossActionReason = "loss_cutback_cooldown"
		s.lastStateChangeReason = "loss_cutback_cooldown"
		return
	}

	cwndMul := s.lossCwndMultiplier(lossRatio)
	pacingMul := s.lossPacingMultiplier(lossRatio)

	oldCwnd := s.congestionWindow
	newCwnd := max(s.minCongestionWindow, protocol.ByteCount(float64(oldCwnd)*cwndMul))
	if !hasQueue {
		if floor := s.noCongestionCwndFloor(); floor > 0 {
			newCwnd = max(newCwnd, floor)
		}
	}
	s.congestionWindow = min(oldCwnd, newCwnd)
	s.noteCwndChange(oldCwnd, "proportional_loss_cutback")

	s.applyTemporaryPacingMultiplier(pacingMul, eventTime, s.lossCutbackCooldown())
	if hasQueue {
		s.maybeReduceShortBwForLoss(lossRatio)
		s.enterStateWithReason(adaptiveBDPProbeDown, eventTime, "proportional_loss_with_queue")
	} else {
		s.suppressProbeUpForOneRound("proportional_loss_no_queue")
	}

	s.markLossCutback(eventTime)
	s.lastLossCwndMultiplier = cwndMul
	s.lastLossPacingMultiplier = pacingMul
	s.lastLossActionReason = "proportional_loss_cutback"
	s.lastCwndChangeReason = "proportional_loss_cutback"
	_ = priorInFlight
}

func (s *adaptiveBDPSender) shouldEmergencyCutback(lossRatio float64) bool {
	return lossRatio >= s.emergencyLossThreshold() && s.lostBytesThisRound >= s.emergencyLossMinBytes()
}

func (s *adaptiveBDPSender) maybeReduceShortBwForLoss(lossRatio float64) {
	if s.queuePressure() == 0 && !s.hasRecentECNCE() {
		return
	}
	bw := s.bw
	if bw == 0 {
		bw = s.activeBandwidthBeforeDownshift()
	}
	if bw == 0 {
		return
	}

	candidate := max(uint64(1), uint64(float64(bw)*s.lossPacingMultiplier(lossRatio)))
	if s.shortBw == 0 {
		s.shortBw = candidate
	} else {
		s.shortBw = min(s.shortBw, candidate)
	}
	s.lastBWChangeReason = "short_bw_proportional_loss_with_queue"
}

func (s *adaptiveBDPSender) applyEmergencyLossCutback(eventTime monotime.Time, lossRatio float64) {
	if !s.canCutbackForLoss(eventTime) {
		s.lastLossActionReason = "loss_cutback_cooldown"
		s.lastStateChangeReason = "loss_cutback_cooldown"
		return
	}

	beta := 0.70
	if lossRatio >= 0.25 && s.queuePressure() > 0.5 {
		beta = 0.50
	}

	oldCwnd := s.congestionWindow
	s.congestionWindow = max(s.minCongestionWindow, protocol.ByteCount(float64(s.congestionWindow)*beta))
	s.noteCwndChange(oldCwnd, "emergency_loss_proportional")
	if s.bw > 0 {
		s.shortBw = max(uint64(1), uint64(float64(s.bw)*beta))
		s.bw = minNonZero(s.bw, s.shortBw)
		s.lastBWChangeReason = "emergency_loss_proportional"
	}
	s.updatePacingRate()
	s.enterStateWithReason(adaptiveBDPProbeDown, eventTime, "emergency_loss_proportional")

	s.markLossCutback(eventTime)
	s.markEmergencyCutbackRound()
	s.lastLossCwndMultiplier = beta
	s.lastLossPacingMultiplier = beta
	s.lastLossActionReason = "emergency_loss_proportional"
	s.lastCwndChangeReason = "emergency_loss_proportional"
}

func (s *adaptiveBDPSender) shouldEnterProbeDown(sample RateSample, priorInFlight protocol.ByteCount, eventTime monotime.Time) bool {
	if s.hasQueuePressure() && s.isPipeFilledForDownshift(priorInFlight) {
		s.queueHighRounds++
	} else {
		s.queueHighRounds = 0
	}
	if s.hasPersistentQueuePressure() {
		s.lastStateChangeReason = "queue_delay_persistent"
		return true
	}
	if !s.inUploadWarmup(eventTime) && s.canUseSampleForDownshift(sample, priorInFlight) && s.bw > 0 && float64(sample.DeliveryRate) < float64(s.bw)*s.downshiftRatio() {
		if s.downshiftRounds >= s.downshiftRoundsTarget() {
			s.lastStateChangeReason = "bandwidth_downshift"
			return true
		}
	}
	return false
}

func (s *adaptiveBDPSender) roundLossRatio() float64 {
	total := s.lostBytesThisRound + s.ackedBytesThisRound
	if total == 0 {
		return 0
	}
	return float64(s.lostBytesThisRound) / float64(total)
}

func (s *adaptiveBDPSender) lossRate() float64 {
	return s.roundLossRatio()
}

func (s *adaptiveBDPSender) updateLossEWMA() {
	ratio := s.roundLossRatio()
	if ratio <= 0 {
		return
	}
	alpha := s.lossEWMAAlpha()
	if s.lossRatioEWMA == 0 {
		s.lossRatioEWMA = ratio
		return
	}
	s.lossRatioEWMA = (1-alpha)*s.lossRatioEWMA + alpha*ratio
}

func (s *adaptiveBDPSender) lossPressure(lossRatio float64) float64 {
	soft := s.lossSoftThreshold()
	severe := s.lossSevereThreshold()
	if severe <= soft {
		severe = soft + 0.01
	}
	if lossRatio <= soft {
		return 0
	}
	return clampFloat((lossRatio-soft)/(severe-soft), 0, 1)
}

func (s *adaptiveBDPSender) squaredLossPressure(lossRatio float64) float64 {
	pressure := s.lossPressure(lossRatio)
	return pressure * pressure
}

func (s *adaptiveBDPSender) lossCwndMultiplier(lossRatio float64) float64 {
	q := s.queuePressure()
	hasQueue := q > 0 || s.hasRecentECNCE()
	if !hasQueue && lossRatio <= s.lossGraceRatio() {
		return 1.0
	}

	pressure := s.squaredLossPressure(lossRatio)
	minCut := s.minLossCwndCut()
	maxCut := s.maxLossCwndCutNoQueue()
	if hasQueue {
		maxCut = s.maxLossCwndCutWithQueue()
		pressure = clampFloat(pressure*(1+q), 0, 1)
	}

	cut := minCut + pressure*(maxCut-minCut)
	cut = clampFloat(cut, 0, maxCut)
	return 1 - cut
}

func (s *adaptiveBDPSender) lossPacingMultiplier(lossRatio float64) float64 {
	q := s.queuePressure()
	hasQueue := q > 0 || s.hasRecentECNCE()
	if !hasQueue && lossRatio <= s.lossGraceRatio() {
		return 1.0
	}

	pressure := s.squaredLossPressure(lossRatio)
	maxCut := s.maxLossPacingCutNoQueue()
	if hasQueue {
		maxCut = s.maxLossPacingCutWithQueue()
		pressure = clampFloat(pressure*(1+q), 0, 1)
	}

	minCut := 0.005
	cut := minCut + pressure*(maxCut-minCut)
	cut = clampFloat(cut, 0, maxCut)
	return 1 - cut
}

func (s *adaptiveBDPSender) minLossSampleBytes() protocol.ByteCount {
	if s.cfg.MinLossSampleBytes > 0 {
		return protocol.ByteCount(s.cfg.MinLossSampleBytes)
	}
	bdp := s.bdp()
	if bdp > 0 {
		return max(64*1024, bdp/8)
	}
	return 64 * 1024
}

func (s *adaptiveBDPSender) hasEnoughLossSample() bool {
	return s.lostBytesThisRound+s.ackedBytesThisRound >= s.minLossSampleBytes()
}

func (s *adaptiveBDPSender) queuePressure() float64 {
	target := s.queueTarget()
	if target <= 0 {
		return 0
	}
	q := s.queueDelay()
	if q <= target {
		return 0
	}
	return clampFloat(float64(q-target)/float64(2*target), 0, 1)
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
		if !s.canProbeUp() {
			return s.cruisePacingGain()
		}
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
	if s.cfg.EmergencyLossThreshold > 0 {
		return max(s.cfg.EmergencyLossThreshold, s.lossSevereThreshold())
	}
	return 0.10
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

func (s *adaptiveBDPSender) noCongestionCwndCutbackFactor() float64 {
	return 0.75
}

func (s *adaptiveBDPSender) congestionDownshiftRoundsTarget() uint32 {
	if s.cfg.CongestionDownshiftRounds > 0 {
		return s.cfg.CongestionDownshiftRounds
	}
	return 1
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

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func minNonZero(a, b uint64) uint64 {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	return min(a, b)
}

func clampFloat(v, minV, maxV float64) float64 {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}
