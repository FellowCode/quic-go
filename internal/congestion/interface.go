package congestion

import (
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

// RenoRTTScalingConfig configures RTT-dependent Reno aggressiveness.
// Aggression controls how quickly the aggressiveness factor grows with RTT:
// factor = 1 + Aggression * (RTT / 1s).
// MaxFactor caps this factor. If MaxFactor is 0, a default cap is applied.
type RenoRTTScalingConfig struct {
	Aggression float64
	MaxFactor  float64
}

type CongestionControlAlgorithm int

const (
	CongestionControlReno CongestionControlAlgorithm = iota
	CongestionControlCubic
	CongestionControlAdaptiveBDP
)

type CwndTuningConfig struct {
	Enable bool

	Algorithm CongestionControlAlgorithm

	InitialWindowPackets uint32
	MinWindowPackets     uint32
	MaxWindowPackets     uint32

	WindowGain float64

	MaxProbeRateBps      uint64
	StartupTargetRateBps uint64

	StartupTargetDuration time.Duration
	StartupPacingGain     float64
	StartupCwndGain       float64

	ProbeUpGain      float64
	ProbeDownGain    float64
	CruisePacingGain float64
	CruiseCwndGain   float64

	QueueTarget           time.Duration
	QueuePersistentRounds uint32

	LossTarget             float64
	EmergencyLossThreshold float64

	BandwidthFilterRounds uint32
	DownshiftRounds       uint32
	DownshiftRatio        float64

	MinRTTFilterWindow time.Duration
	ProbeInterval      time.Duration

	PacingMargin float64
}

type RateSample struct {
	DeliveryRate   protocol.ByteCount // bytes/sec
	AckedBytes     protocol.ByteCount
	LostBytes      protocol.ByteCount
	DeliveredBytes protocol.ByteCount
	DeliveredDelta protocol.ByteCount
	PriorInFlight  protocol.ByteCount
	Interval       time.Duration
	AckElapsed     time.Duration
	SendElapsed    time.Duration
	RTT            time.Duration
	AppLimited     bool
	IsValid        bool
}

type AdaptiveBDPDebugInfo struct {
	State string

	CongestionWindow protocol.ByteCount
	TargetCwnd       protocol.ByteCount
	MinCwnd          protocol.ByteCount
	MaxCwnd          protocol.ByteCount
	BDP              protocol.ByteCount
	BytesInFlight    protocol.ByteCount
	PriorInFlight    protocol.ByteCount

	BandwidthBytesPerSecond      uint64
	MaxBandwidthBytesPerSecond   uint64
	ShortBandwidthBytesPerSecond uint64
	PacingRateBytesPerSecond     uint64

	LastDeliveryRateBytesPerSecond protocol.ByteCount
	LastDeliveredDelta             protocol.ByteCount
	LastSampleInterval             time.Duration
	LastSampleAckElapsed           time.Duration
	LastSampleSendElapsed          time.Duration
	LastSampleAppLimited           bool
	LastSampleValid                bool

	MinRTT      time.Duration
	SmoothedRTT time.Duration
	QueueDelay  time.Duration
	QueueTarget time.Duration
	PacingGain  float64
	CwndGain    float64

	RoundCount         uint64
	RoundStart         bool
	LastRoundStartTime monotime.Time
	QueueHighRounds    uint32
	DownshiftRounds    uint32
	FullBwReached      bool
	ProbeUpActive      bool
	PacerBudget        protocol.ByteCount
	TimeUntilSend      time.Duration
	HasPacingBudget    bool

	LastStateChangeReason string
	LastCwndChangeReason  string
	LastBWChangeReason    string
}

// A SendAlgorithm performs congestion control
type SendAlgorithm interface {
	TimeUntilSend(bytesInFlight protocol.ByteCount) monotime.Time
	HasPacingBudget(now monotime.Time) bool
	OnPacketSent(sentTime monotime.Time, bytesInFlight protocol.ByteCount, packetNumber protocol.PacketNumber, bytes protocol.ByteCount, isRetransmittable bool)
	CanSend(bytesInFlight protocol.ByteCount) bool
	MaybeExitSlowStart()
	OnPacketAcked(number protocol.PacketNumber, ackedBytes protocol.ByteCount, priorInFlight protocol.ByteCount, eventTime monotime.Time)
	OnCongestionEvent(number protocol.PacketNumber, lostBytes protocol.ByteCount, priorInFlight protocol.ByteCount)
	OnRetransmissionTimeout(packetsRetransmitted bool)
	SetMaxDatagramSize(protocol.ByteCount)
}

// A SendAlgorithmWithDebugInfos is a SendAlgorithm that exposes some debug infos
type SendAlgorithmWithDebugInfos interface {
	SendAlgorithm
	InSlowStart() bool
	InRecovery() bool
	GetCongestionWindow() protocol.ByteCount
}

type SendAlgorithmWithAdaptiveBDPDebugInfo interface {
	SendAlgorithm
	AdaptiveBDPDebugInfo() AdaptiveBDPDebugInfo
}

// SendAlgorithmWithRateSample receives delivery-rate measurements from the ACK handler.
type SendAlgorithmWithRateSample interface {
	SendAlgorithm
	OnPacketAckedWithRateSample(
		number protocol.PacketNumber,
		ackedBytes protocol.ByteCount,
		priorInFlight protocol.ByteCount,
		eventTime monotime.Time,
		sample RateSample,
	)
}

// SendAlgorithmWithECN receives validated ECN-CE congestion signals.
type SendAlgorithmWithECN interface {
	SendAlgorithm
	OnECNCongestionEvent(priorInFlight protocol.ByteCount, eventTime monotime.Time)
}

// SendAlgorithmWithPersistentCongestion receives explicit persistent congestion signals.
type SendAlgorithmWithPersistentCongestion interface {
	SendAlgorithm
	OnPersistentCongestion(eventTime monotime.Time)
}
