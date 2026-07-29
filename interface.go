package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"slices"
	"time"

	"github.com/quic-go/quic-go/internal/handshake"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/qlogwriter"
)

// The StreamID is the ID of a QUIC stream.
type StreamID = protocol.StreamID

// A Version is a QUIC version number.
type Version = protocol.Version

const (
	// Version1 is RFC 9000
	Version1 = protocol.Version1
	// Version2 is RFC 9369
	Version2 = protocol.Version2
)

type CongestionControlAlgorithm int

const (
	CongestionControlReno CongestionControlAlgorithm = iota
	CongestionControlCubic
	CongestionControlAdaptiveBDP
)

type CwndTuning struct {
	// Enable enables CwndTuning. AdaptiveBDP settings apply only with Algorithm set to CongestionControlAdaptiveBDP.
	Enable bool
	// EnableAdaptiveBDPTelemetry retains a bounded history of completed
	// AdaptiveBDP rounds and state transitions in AdaptiveBDPDebugInfo.
	// It is intended for deterministic validation and diagnostics.
	EnableAdaptiveBDPTelemetry bool

	// Algorithm selects Reno, Cubic, or AdaptiveBDP. The zero value selects Reno.
	Algorithm CongestionControlAlgorithm

	// InitialWindowPackets is the starting cwnd in packets. Zero uses 32 packets; the effective value must be between MinWindowPackets and MaxWindowPackets.
	InitialWindowPackets uint32
	// MinWindowPackets is the hard cwnd floor in packets. Zero uses 2 packets; the effective value must not exceed InitialWindowPackets.
	MinWindowPackets uint32
	// MaxWindowPackets is the exact hard AdaptiveBDP cwnd ceiling in packets. Zero uses 10,000 packets (about 12.8 MB at a 1,280-byte MSS). Accepted values are at most 100,000 packets. The maximum permits 200,000 outstanding and 250,000 tracked packet records, budgeting about 30.5 MiB of packet-history bookkeeping per connection before frame-data overhead; the effective value must not be below InitialWindowPackets.
	MaxWindowPackets uint32

	// WindowGain is a positive soft multiplier for the BDP-derived cwnd target. Zero uses 1; negative values are invalid.
	WindowGain float64

	// MaxProbeRateBps is a probe-rate ceiling hint in bits per second. Zero leaves probing uncapped; when both rate bounds are set it must not be below MinProbeRateBps.
	MaxProbeRateBps uint64
	// MinProbeRateBps is a soft no-congestion pacing floor in bits per second. Zero uses the StartupTargetRateBps-derived floor; it must not exceed an explicit MaxProbeRateBps.
	MinProbeRateBps uint64
	// StartupTargetRateBps is a soft known-capacity startup target in bits per second. Zero disables the target; it is not a guaranteed or hard minimum rate.
	StartupTargetRateBps uint64

	// StartupTargetDuration is the non-negative time hint for growth toward StartupTargetRateBps. Zero uses 5 seconds.
	StartupTargetDuration time.Duration
	// StartupPacingGain is a soft Startup pacing multiplier. Zero uses 2; an explicit value must be in [1, 2.77] and may be raised to pursue StartupTargetRateBps.
	StartupPacingGain float64
	// StartupCwndGain is the Startup cwnd-target multiplier. Zero uses 2; a non-zero value must be at least 1.
	StartupCwndGain float64

	// ProbeUpGain is the per-round ProbeUp pacing multiplier hint. Zero uses 1.25; a non-zero value must be at least 1.
	ProbeUpGain float64
	// ProbeDownGain is the ProbeDown pacing multiplier in [0, 1]. Zero uses 0.90.
	ProbeDownGain float64
	// CruisePacingGain is a non-negative steady-state pacing multiplier. Zero uses 1.01.
	CruisePacingGain float64
	// CruiseCwndGain is a non-negative steady-state cwnd-target multiplier. Zero uses 1.5.
	CruiseCwndGain float64

	// QueueTarget is a non-negative soft queue-delay target. Zero derives a target from min RTT, bounded to 5–25 ms.
	QueueTarget time.Duration
	// QueuePersistentRounds is the positive confirmation count in AdaptiveBDP rounds for persistent queue evidence. Zero uses 3 rounds.
	QueuePersistentRounds uint32

	// LossTarget is the target loss ratio in [0, 1]. Zero uses the controller default.
	LossTarget float64
	// LossGraceRatio is the no-cwnd-cut loss ratio in [0, 1]. Zero uses the controller default.
	LossGraceRatio float64
	// LossSoftThreshold is the proportional-loss threshold in [0, 1]. Zero uses LossGraceRatio.
	LossSoftThreshold float64
	// LossSevereThreshold is the severe-loss threshold in [0, 1]. Zero uses the controller default.
	LossSevereThreshold float64
	// EmergencyLossThreshold is the emergency-loss threshold in [0, 1]. Zero uses the controller default.
	EmergencyLossThreshold float64
	// LossMinBytes is the minimum lost-byte observation for loss reaction. Zero uses the controller default.
	LossMinBytes uint64
	// EmergencyLossMinBytes is the minimum lost-byte observation for emergency reaction. Zero uses the controller default.
	EmergencyLossMinBytes uint64
	// MinLossSampleBytes is the minimum total ACKed plus lost byte sample. Zero uses the controller default.
	MinLossSampleBytes uint64
	// LossEWMAAlpha is the loss-memory weighting fraction. Zero uses 0.25; an explicit value must be in [0.01, 1].
	LossEWMAAlpha float64
	// MaxLossCwndCutNoQueue is the hard no-queue cwnd cut cap as a fraction in [0, 0.5]. Zero uses 0.15.
	MaxLossCwndCutNoQueue float64
	// MaxLossCwndCutWithQueue is the hard queued-loss cwnd cut cap as a fraction in [0, 0.5]. Zero uses 0.30.
	MaxLossCwndCutWithQueue float64
	// MinLossCwndCut is the proportional cwnd cut floor as a fraction in [0, 0.1]. Zero uses 0.01.
	MinLossCwndCut float64
	// MaxLossPacingCutNoQueue is the hard no-queue pacing cut cap as a fraction in [0, 0.5]. Zero uses 0.10.
	MaxLossPacingCutNoQueue float64
	// MaxLossPacingCutWithQueue is the hard queued-loss pacing cut cap as a fraction in [0, 0.5]. Zero uses 0.25.
	MaxLossPacingCutWithQueue float64
	// LossCutbackCooldown is the minimum time between loss cutbacks. Zero uses the controller default; it must not be negative.
	LossCutbackCooldown time.Duration
	// MildLossPersistentRounds is the number of loss rounds required before a no-queue cutback. Zero uses the controller default.
	MildLossPersistentRounds uint32
	// LossRecoveryProbeRounds is the number of loss-free rounds before recovery probing. Zero uses the controller default.
	LossRecoveryProbeRounds uint32
	// LossRecoveryProbeGain is the per-round recovery probe multiplier hint. Zero uses 1.25; an explicit value must be in [1.01, 2].
	LossRecoveryProbeGain float64
	// LossRecoveryProbeDurationRounds is the maximum recovery-probe lifetime in rounds. Zero uses the controller default.
	LossRecoveryProbeDurationRounds uint32
	// LossRecoveryClearShortBwFraction is the short-bandwidth recovery threshold fraction. Zero uses 0.95; an explicit value must be in [0.5, 1].
	LossRecoveryClearShortBwFraction float64

	// BandwidthFilterRounds is the max-bandwidth filter window in rounds. Zero uses the controller default.
	BandwidthFilterRounds uint32
	// DownshiftRounds is the generic downshift confirmation count in rounds. Zero uses the controller default.
	DownshiftRounds uint32
	// DownshiftRatio is the low-sample ratio in [0, 1]. Zero uses the controller default.
	DownshiftRatio float64

	// NoCongestionRateFloorFraction is the soft StartupTargetRateBps floor fraction in [0, 1]. Zero uses 0.5 unless DisableNoCongestionRateFloor is set.
	NoCongestionRateFloorFraction float64
	// DisableNoCongestionRateFloor explicitly selects a zero no-congestion floor even when StartupTargetRateBps is configured.
	DisableNoCongestionRateFloor bool
	// NoCongestionDownshiftRounds is the low-sample confirmation count in rounds. Zero uses the controller default.
	NoCongestionDownshiftRounds uint32
	// NoCongestionDownshiftFactor is the retained-bandwidth multiplier per no-congestion downshift in [0, 1]. Zero uses 0.75.
	NoCongestionDownshiftFactor float64
	// UploadWarmupDuration is the post-idle downshift grace duration. Zero uses the controller default; it must not be negative.
	UploadWarmupDuration time.Duration
	// UploadWarmupBytes is the post-idle ACKed-byte grace threshold. Zero uses the controller default.
	UploadWarmupBytes uint64
	// MinDownshiftSampleBytes is the minimum ACKed-byte downshift sample. Zero uses the controller default.
	MinDownshiftSampleBytes uint64
	// CongestionDownshiftRounds is the congestion-confirmed downshift count in rounds. Zero uses the controller default.
	CongestionDownshiftRounds uint32

	// MinRTTFilterWindow is the min-RTT filter lifetime. Zero uses the controller default; it must not be negative.
	MinRTTFilterWindow time.Duration
	// ProbeInterval is the minimum interval between ordinary ProbeUp attempts. Zero uses 900 ms; it must not be negative.
	ProbeInterval time.Duration

	// PacingMargin is the pacing safety fraction in [0, 0.99]. Zero uses the controller default.
	PacingMargin float64
}

// AdaptiveBDPTelemetrySample records one completed AdaptiveBDP controller
// round or state transition.
type AdaptiveBDPTelemetrySample struct {
	Event            string
	Elapsed          time.Duration
	RoundCount       uint64
	State            string
	TransitionReason string

	CongestionWindow uint64
	TargetCwnd       uint64
	BytesInFlight    uint64
	BDP              uint64

	BandwidthBytesPerSecond         uint64
	MaxBandwidthBytesPerSecond      uint64
	ShortBandwidthBytesPerSecond    uint64
	RecoveryBandwidthBytesPerSecond uint64
	PacingRateBytesPerSecond        uint64
	PacingGain                      float64
	CwndGain                        float64

	LatestRTT   time.Duration
	SmoothedRTT time.Duration
	MinRTT      time.Duration
	QueueDelay  time.Duration
	QueueTarget time.Duration
	QueueState  string

	LossRatioRound           float64
	LossRatioEWMA            float64
	LostBytesThisRound       uint64
	AckedBytesThisRound      uint64
	HasRecentECNCE           bool
	LastLossActionReason     string
	LastLossCwndMultiplier   float64
	LastLossPacingMultiplier float64

	PacingCutMultiplier float64
	PacingCutRemaining  time.Duration
	UploadWarmupActive  bool
	IdleRestartActive   bool
	ProbeUpActive       bool
	ProbeDownActive     bool
	ProbeRTTActive      bool
	FullBwReached       bool
}

// AdaptiveBDPDebugInfo contains diagnostic state for CongestionControlAdaptiveBDP.
//
// It is intended for observability and debugging. The values are snapshots of
// the congestion controller at the time [Conn.AdaptiveBDPDebugInfo] is called.
type AdaptiveBDPDebugInfo struct {
	State string
	// Telemetry is populated only when
	// CwndTuning.EnableAdaptiveBDPTelemetry is enabled.
	Telemetry []AdaptiveBDPTelemetrySample

	CongestionWindow uint64
	TargetCwnd       uint64
	MinCwnd          uint64
	MaxCwnd          uint64
	BDP              uint64
	BytesInFlight    uint64
	PriorInFlight    uint64

	BandwidthBytesPerSecond      uint64
	MaxBandwidthBytesPerSecond   uint64
	ShortBandwidthBytesPerSecond uint64
	PacingRateBytesPerSecond     uint64

	LastDeliveryRateBytesPerSecond uint64
	LastDeliveredDelta             uint64
	LastSampleInterval             time.Duration
	LastSampleAckElapsed           time.Duration
	LastSampleSendElapsed          time.Duration
	LastSampleAppLimited           bool
	LastSampleValid                bool

	MinRTT      time.Duration
	SmoothedRTT time.Duration
	QueueDelay  time.Duration
	QueueTarget time.Duration
	QueueState  string
	PacingGain  float64
	CwndGain    float64

	NegativeBandwidthConfidence    float64
	HasCongestionEvidence          bool
	PipeForDownshift               uint64
	PipeFillThreshold              uint64
	ActiveBandwidthBeforeDownshift uint64
	NoCongestionRateFloor          uint64
	NoQueueLowRounds               uint32
	NoQueueLowAcked                uint64

	LossRatioRound               float64
	LossRatioEWMA                float64
	LostBytesThisRound           uint64
	AckedBytesThisRound          uint64
	LossMinBytes                 uint64
	EmergencyLossMinBytes        uint64
	MinLossSampleBytes           uint64
	LossGraceRatio               float64
	LossSevereThreshold          float64
	EmergencyLossThreshold       float64
	QueuePressure                float64
	MildLossRounds               uint32
	LastLossActionReason         string
	LastLossCwndMultiplier       float64
	LastLossPacingMultiplier     float64
	LastLossCutbackRound         uint64
	SuppressProbeUpUntilRound    uint64
	SuppressProbeUpReason        string
	LossFreeRounds               uint32
	LastMaterialLossRound        uint64
	LossRecoveryProbeActive      bool
	LossRecoveryProbeBW          uint64
	LossRecoveryProbeUntilRound  uint64
	HasLastECNCE                 bool
	LastECNCERound               uint64
	PersistentCongestionEvents   uint64
	LastPersistentCongestionSpan time.Duration
	LastPersistentCongestionGate time.Duration
	MaxOutstandingSentPackets    uint64
	MaxTrackedSentPackets        uint64
	TrackedSentPackets           uint64

	RoundCount         uint64
	RoundStart         bool
	LastRoundStartTime time.Time
	QueueHighRounds    uint32
	DownshiftRounds    uint32
	FullBwReached      bool
	ProbeUpActive      bool
	PacerBudget        uint64
	TimeUntilSend      time.Duration
	HasPacingBudget    bool

	LastStateChangeReason string
	LastCwndChangeReason  string
	LastBWChangeReason    string
}

// SupportedVersions returns the support versions, sorted in descending order of preference.
func SupportedVersions() []Version {
	// clone the slice to prevent the caller from modifying the slice
	return slices.Clone(protocol.SupportedVersions)
}

// A ClientToken is a token received by the client.
// It can be used to skip address validation on future connection attempts.
type ClientToken struct {
	data []byte
	rtt  time.Duration
}

type TokenStore interface {
	// Pop searches for a ClientToken associated with the given key.
	// Since tokens are not supposed to be reused, it must remove the token from the cache.
	// It returns nil when no token is found.
	Pop(key string) (token *ClientToken)

	// Put adds a token to the cache with the given key. It might get called
	// multiple times in a connection.
	Put(key string, token *ClientToken)
}

// Err0RTTRejected is the returned from:
//   - Open{Uni}Stream{Sync}
//   - Accept{Uni}Stream
//   - Stream.Read and Stream.Write
//
// when the server rejects a 0-RTT connection attempt.
var Err0RTTRejected = errors.New("0-RTT rejected")

// ErrWouldBlock is returned by [SendStream.TryWriteAll] if the entire slice can't be queued immediately.
var ErrWouldBlock = errors.New("operation would block")

// ErrWriteLimitReached is returned by [SendStream.WriteWithLimit] when its limiter prevents accepting the entire slice.
var ErrWriteLimitReached = errors.New("write limit reached")

// QUICVersionContextKey can be used to find out the QUIC version of a TLS handshake from the
// context returned by tls.Config.ClientInfo.Context.
var QUICVersionContextKey = handshake.QUICVersionContextKey

// StatelessResetKey is a key used to derive stateless reset tokens.
type StatelessResetKey [32]byte

// TokenGeneratorKey is a key used to encrypt session resumption tokens.
type TokenGeneratorKey = handshake.TokenProtectorKey

// A ConnectionID is a QUIC Connection ID, as defined in RFC 9000.
// It is not able to handle QUIC Connection IDs longer than 20 bytes,
// as they are allowed by RFC 8999.
type ConnectionID = protocol.ConnectionID

// ConnectionIDFromBytes interprets b as a [ConnectionID]. It panics if b is
// longer than 20 bytes.
func ConnectionIDFromBytes(b []byte) ConnectionID {
	return protocol.ParseConnectionID(b)
}

// A ConnectionIDGenerator allows the application to take control over the generation of Connection IDs.
// Connection IDs generated by an implementation must be of constant length.
type ConnectionIDGenerator interface {
	// GenerateConnectionID generates a new Connection ID.
	// Generated Connection IDs must be unique and observers should not be able to correlate two Connection IDs.
	GenerateConnectionID() (ConnectionID, error)

	// ConnectionIDLen returns the length of Connection IDs generated by this implementation.
	// Implementations must return constant-length Connection IDs with lengths between 0 and 20 bytes.
	// A length of 0 can only be used when an endpoint doesn't need to multiplex connections during migration.
	ConnectionIDLen() int
}

// Config contains all configuration data needed for a QUIC server or client.
type Config struct {
	// GetConfigForClient is called for incoming connections.
	// If the error is not nil, the connection attempt is refused.
	GetConfigForClient func(info *ClientInfo) (*Config, error)
	// The QUIC versions that can be negotiated.
	// If not set, it uses all versions available.
	Versions []Version
	// HandshakeIdleTimeout is the idle timeout before completion of the handshake.
	// If we don't receive any packet from the peer within this time, the connection attempt is aborted.
	// Additionally, if the handshake doesn't complete in twice this time, the connection attempt is also aborted.
	// If this value is zero, the timeout is set to 5 seconds.
	HandshakeIdleTimeout time.Duration
	// MaxIdleTimeout is the maximum duration that may pass without any incoming network activity.
	// The actual value for the idle timeout is the minimum of this value and the peer's.
	// This value only applies after the handshake has completed.
	// If the timeout is exceeded, the connection is closed.
	// If this value is zero, the timeout is set to 30 seconds.
	MaxIdleTimeout time.Duration
	// The TokenStore stores tokens received from the server.
	// Tokens are used to skip address validation on future connection attempts.
	// The key used to store tokens is the ServerName from the tls.Config, if set
	// otherwise the token is associated with the server's IP address.
	TokenStore TokenStore
	// InitialStreamReceiveWindow is the initial size of the stream-level flow control window for receiving data.
	// If the application is consuming data quickly enough, the flow control auto-tuning algorithm
	// will increase the window up to MaxStreamReceiveWindow.
	// If this value is zero, it will default to 512 KB.
	// Values larger than the maximum varint (quicvarint.Max) will be clipped to that value.
	InitialStreamReceiveWindow uint64
	// MaxStreamReceiveWindow is the maximum stream-level flow control window for receiving data.
	// If this value is zero, it will default to 6 MB.
	// Values larger than the maximum varint (quicvarint.Max) will be clipped to that value.
	MaxStreamReceiveWindow uint64
	// InitialConnectionReceiveWindow is the initial size of the stream-level flow control window for receiving data.
	// If the application is consuming data quickly enough, the flow control auto-tuning algorithm
	// will increase the window up to MaxConnectionReceiveWindow.
	// If this value is zero, it will default to 512 KB.
	// Values larger than the maximum varint (quicvarint.Max) will be clipped to that value.
	InitialConnectionReceiveWindow uint64
	// MaxConnectionReceiveWindow is the connection-level flow control window for receiving data.
	// If this value is zero, it will default to 15 MB.
	// Values larger than the maximum varint (quicvarint.Max) will be clipped to that value.
	MaxConnectionReceiveWindow uint64
	// AllowConnectionWindowIncrease is called every time the connection flow controller attempts
	// to increase the connection flow control window.
	// If set, the caller can prevent an increase of the window. Typically, it would do so to
	// limit the memory usage.
	// To avoid deadlocks, it is not valid to call other functions on the connection or on streams
	// in this callback.
	AllowConnectionWindowIncrease func(conn *Conn, delta uint64) bool
	// MaxIncomingStreams is the maximum number of concurrent bidirectional streams that a peer is allowed to open.
	// If not set, it will default to 100.
	// If set to a negative value, it doesn't allow any bidirectional streams.
	// Values larger than 2^60 will be clipped to that value.
	MaxIncomingStreams int64
	// MaxIncomingUniStreams is the maximum number of concurrent unidirectional streams that a peer is allowed to open.
	// If not set, it will default to 100.
	// If set to a negative value, it doesn't allow any unidirectional streams.
	// Values larger than 2^60 will be clipped to that value.
	MaxIncomingUniStreams int64
	// KeepAlivePeriod defines whether this peer will periodically send a packet to keep the connection alive.
	// If set to 0, then no keep alive is sent. Otherwise, the keep alive is sent on that period (or at most
	// every half of MaxIdleTimeout, whichever is smaller).
	KeepAlivePeriod time.Duration
	// InitialPacketSize is the initial size (and the lower limit) for packets sent.
	// Under most circumstances, it is not necessary to manually set this value,
	// since path MTU discovery quickly finds the path's MTU.
	// If set too high, the path might not support packets of that size, leading to a timeout of the QUIC handshake.
	// Values below 1200 are invalid.
	InitialPacketSize uint16
	// DisablePathMTUDiscovery disables Path MTU Discovery (RFC 8899).
	// This allows the sending of QUIC packets that fully utilize the available MTU of the path.
	// Path MTU discovery is only available on systems that allow setting of the Don't Fragment (DF) bit.
	DisablePathMTUDiscovery bool
	// Allow0RTT allows the application to decide if a 0-RTT connection attempt should be accepted.
	// Only valid for the server.
	Allow0RTT bool
	// Enable QUIC datagram support (RFC 9221).
	EnableDatagrams bool
	// Enable QUIC Stream Resets with Partial Delivery.
	// See https://datatracker.ietf.org/doc/html/draft-ietf-quic-reliable-stream-reset-09.
	EnableStreamResetPartialDelivery bool
	// CwndTuning controls congestion control selection and tuning parameters.
	CwndTuning CwndTuning
	// RenoRTTScalingAggression enables RTT-dependent Reno CWND aggressiveness.
	// If set to a positive value, the aggressiveness factor is computed as:
	// factor = 1 + RenoRTTScalingAggression * (RTT / 1s).
	// A value of 0 disables RTT-dependent aggression.
	RenoRTTScalingAggression float64
	// RenoRTTScalingMaxFactor caps the RTT-dependent Reno aggressiveness factor.
	// If set to 0, an internal default cap is used.
	RenoRTTScalingMaxFactor float64

	Tracer func(ctx context.Context, isClient bool, connID ConnectionID) qlogwriter.Trace
}

// ClientInfo contains information about an incoming connection attempt.
type ClientInfo struct {
	// RemoteAddr is the remote address on the Initial packet.
	// Unless AddrVerified is set, the address is not yet verified, and could be a spoofed IP address.
	RemoteAddr net.Addr
	// AddrVerified says if the remote address was verified using QUIC's Retry mechanism.
	// Note that the Retry mechanism costs one network roundtrip,
	// and is not performed unless Transport.MaxUnvalidatedHandshakes is surpassed.
	AddrVerified bool
}

// ConnectionState records basic details about a QUIC connection.
type ConnectionState struct {
	// TLS contains information about the TLS connection state, incl. the tls.ConnectionState.
	TLS tls.ConnectionState
	// SupportsDatagrams indicates support for QUIC datagrams (RFC 9221).
	SupportsDatagrams struct {
		// Remote is true if the peer advertised datagram support.
		// Local is true if datagram support was enabled via Config.EnableDatagrams.
		Remote, Local bool
	}
	// SupportsStreamResetPartialDelivery indicates support for QUIC Stream Resets with Partial Delivery.
	SupportsStreamResetPartialDelivery struct {
		// Remote is true if the peer advertised support.
		// Local is true if support was enabled via Config.EnableStreamResetPartialDelivery.
		Remote, Local bool
	}
	// Used0RTT says if 0-RTT resumption was used.
	Used0RTT bool
	// Version is the QUIC version of the QUIC connection.
	Version Version
	// GSO says if generic segmentation offload is used.
	GSO bool
}
