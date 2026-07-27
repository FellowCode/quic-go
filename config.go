package quic

import (
	"fmt"
	"time"

	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/quicvarint"
)

// Clone clones a Config.
func (c *Config) Clone() *Config {
	copy := *c
	return &copy
}

func (c *Config) handshakeTimeout() time.Duration {
	return 2 * c.HandshakeIdleTimeout
}

func (c *Config) maxRetryTokenAge() time.Duration {
	return c.handshakeTimeout()
}

func validateConfig(config *Config) error {
	if config == nil {
		return nil
	}
	if config.CwndTuning.Algorithm == CongestionControlAdaptiveBDP {
		if err := validateAdaptiveBDPCwndTuning(config.CwndTuning); err != nil {
			return err
		}
	}
	const maxStreams = 1 << 60
	if config.MaxIncomingStreams > maxStreams {
		config.MaxIncomingStreams = maxStreams
	}
	if config.MaxIncomingUniStreams > maxStreams {
		config.MaxIncomingUniStreams = maxStreams
	}
	if config.MaxStreamReceiveWindow > quicvarint.Max {
		config.MaxStreamReceiveWindow = quicvarint.Max
	}
	if config.MaxConnectionReceiveWindow > quicvarint.Max {
		config.MaxConnectionReceiveWindow = quicvarint.Max
	}
	if config.InitialPacketSize > 0 && config.InitialPacketSize < protocol.MinInitialPacketSize {
		config.InitialPacketSize = protocol.MinInitialPacketSize
	}
	if config.InitialPacketSize > protocol.MaxPacketBufferSize {
		config.InitialPacketSize = protocol.MaxPacketBufferSize
	}
	if config.RenoRTTScalingAggression < 0 {
		config.RenoRTTScalingAggression = 0
	}
	if config.RenoRTTScalingMaxFactor < 0 {
		config.RenoRTTScalingMaxFactor = 0
	}
	if config.RenoRTTScalingMaxFactor > 0 && config.RenoRTTScalingMaxFactor < 1 {
		config.RenoRTTScalingMaxFactor = 1
	}
	if config.CwndTuning.WindowGain < 0 {
		config.CwndTuning.WindowGain = 0
	}
	if config.CwndTuning.Algorithm < CongestionControlReno || config.CwndTuning.Algorithm > CongestionControlAdaptiveBDP {
		config.CwndTuning.Algorithm = CongestionControlReno
	}
	if config.CwndTuning.StartupPacingGain < 0 {
		config.CwndTuning.StartupPacingGain = 0
	}
	if config.CwndTuning.StartupCwndGain < 0 {
		config.CwndTuning.StartupCwndGain = 0
	}
	if config.CwndTuning.ProbeUpGain < 0 {
		config.CwndTuning.ProbeUpGain = 0
	}
	if config.CwndTuning.ProbeDownGain < 0 {
		config.CwndTuning.ProbeDownGain = 0
	}
	if config.CwndTuning.CruisePacingGain < 0 {
		config.CwndTuning.CruisePacingGain = 0
	}
	if config.CwndTuning.CruiseCwndGain < 0 {
		config.CwndTuning.CruiseCwndGain = 0
	}
	if config.CwndTuning.LossTarget < 0 {
		config.CwndTuning.LossTarget = 0
	}
	if config.CwndTuning.LossGraceRatio < 0 {
		config.CwndTuning.LossGraceRatio = 0
	}
	if config.CwndTuning.LossSoftThreshold < 0 {
		config.CwndTuning.LossSoftThreshold = 0
	}
	if config.CwndTuning.LossSevereThreshold < 0 {
		config.CwndTuning.LossSevereThreshold = 0
	}
	if config.CwndTuning.EmergencyLossThreshold < 0 {
		config.CwndTuning.EmergencyLossThreshold = 0
	}
	if config.CwndTuning.LossEWMAAlpha < 0 {
		config.CwndTuning.LossEWMAAlpha = 0
	}
	if config.CwndTuning.LossEWMAAlpha > 1 {
		config.CwndTuning.LossEWMAAlpha = 1
	}
	if config.CwndTuning.MaxLossCwndCutNoQueue < 0 {
		config.CwndTuning.MaxLossCwndCutNoQueue = 0
	}
	if config.CwndTuning.MaxLossCwndCutNoQueue > 1 {
		config.CwndTuning.MaxLossCwndCutNoQueue = 1
	}
	if config.CwndTuning.MaxLossCwndCutWithQueue < 0 {
		config.CwndTuning.MaxLossCwndCutWithQueue = 0
	}
	if config.CwndTuning.MaxLossCwndCutWithQueue > 1 {
		config.CwndTuning.MaxLossCwndCutWithQueue = 1
	}
	if config.CwndTuning.MinLossCwndCut < 0 {
		config.CwndTuning.MinLossCwndCut = 0
	}
	if config.CwndTuning.MinLossCwndCut > 1 {
		config.CwndTuning.MinLossCwndCut = 1
	}
	if config.CwndTuning.MaxLossPacingCutNoQueue < 0 {
		config.CwndTuning.MaxLossPacingCutNoQueue = 0
	}
	if config.CwndTuning.MaxLossPacingCutNoQueue > 1 {
		config.CwndTuning.MaxLossPacingCutNoQueue = 1
	}
	if config.CwndTuning.MaxLossPacingCutWithQueue < 0 {
		config.CwndTuning.MaxLossPacingCutWithQueue = 0
	}
	if config.CwndTuning.MaxLossPacingCutWithQueue > 1 {
		config.CwndTuning.MaxLossPacingCutWithQueue = 1
	}
	if config.CwndTuning.LossCutbackCooldown < 0 {
		config.CwndTuning.LossCutbackCooldown = 0
	}
	if config.CwndTuning.DownshiftRatio < 0 {
		config.CwndTuning.DownshiftRatio = 0
	}
	if config.CwndTuning.NoCongestionRateFloorFraction < 0 {
		config.CwndTuning.NoCongestionRateFloorFraction = 0
	}
	if config.CwndTuning.NoCongestionRateFloorFraction > 1 {
		config.CwndTuning.NoCongestionRateFloorFraction = 1
	}
	if config.CwndTuning.NoCongestionDownshiftFactor < 0 {
		config.CwndTuning.NoCongestionDownshiftFactor = 0
	}
	if config.CwndTuning.NoCongestionDownshiftFactor > 1 {
		config.CwndTuning.NoCongestionDownshiftFactor = 1
	}
	if config.CwndTuning.UploadWarmupDuration < 0 {
		config.CwndTuning.UploadWarmupDuration = 0
	}
	if config.CwndTuning.PacingMargin < 0 {
		config.CwndTuning.PacingMargin = 0
	}
	if config.CwndTuning.PacingMargin > 0.99 {
		config.CwndTuning.PacingMargin = 0.99
	}
	// check that all QUIC versions are actually supported
	for _, v := range config.Versions {
		if !protocol.IsValidVersion(v) {
			return fmt.Errorf("invalid QUIC version: %s", v)
		}
	}
	return nil
}

func validateAdaptiveBDPCwndTuning(c CwndTuning) error {
	const (
		defaultMinWindowPackets     = 2
		defaultInitialWindowPackets = 32
	)
	minWindow := c.MinWindowPackets
	if minWindow == 0 {
		minWindow = defaultMinWindowPackets
	}
	initialWindow := c.InitialWindowPackets
	if initialWindow == 0 {
		initialWindow = defaultInitialWindowPackets
	}
	maxWindow := c.MaxWindowPackets
	if maxWindow == 0 {
		maxWindow = protocol.MaxCongestionWindowPackets
	}
	if c.MaxWindowPackets > protocol.MaxAdaptiveBDPWindowPackets {
		return fmt.Errorf(
			"invalid AdaptiveBDP MaxWindowPackets: must not exceed %d (got %d)",
			protocol.MaxAdaptiveBDPWindowPackets,
			c.MaxWindowPackets,
		)
	}
	if minWindow > initialWindow || initialWindow > maxWindow {
		return fmt.Errorf(
			"invalid AdaptiveBDP effective window limits: require MinWindowPackets (%d) <= InitialWindowPackets (%d) <= MaxWindowPackets (%d)",
			minWindow,
			initialWindow,
			maxWindow,
		)
	}

	for name, value := range map[string]float64{
		"LossTarget":                    c.LossTarget,
		"LossGraceRatio":                c.LossGraceRatio,
		"LossSoftThreshold":             c.LossSoftThreshold,
		"LossSevereThreshold":           c.LossSevereThreshold,
		"EmergencyLossThreshold":        c.EmergencyLossThreshold,
		"DownshiftRatio":                c.DownshiftRatio,
		"NoCongestionRateFloorFraction": c.NoCongestionRateFloorFraction,
		"NoCongestionDownshiftFactor":   c.NoCongestionDownshiftFactor,
	} {
		if value < 0 || value > 1 {
			return fmt.Errorf("invalid AdaptiveBDP %s: must be in [0, 1]", name)
		}
	}
	for name, value := range map[string]float64{
		"MaxLossCwndCutNoQueue":     c.MaxLossCwndCutNoQueue,
		"MaxLossCwndCutWithQueue":   c.MaxLossCwndCutWithQueue,
		"MaxLossPacingCutNoQueue":   c.MaxLossPacingCutNoQueue,
		"MaxLossPacingCutWithQueue": c.MaxLossPacingCutWithQueue,
	} {
		if value < 0 || value > 0.5 {
			return fmt.Errorf("invalid AdaptiveBDP %s: must be in [0, 0.5]", name)
		}
	}
	if c.MinLossCwndCut < 0 || c.MinLossCwndCut > 0.1 {
		return fmt.Errorf("invalid AdaptiveBDP MinLossCwndCut: must be in [0, 0.1]")
	}
	if c.LossEWMAAlpha < 0 || (c.LossEWMAAlpha > 0 && c.LossEWMAAlpha < 0.01) || c.LossEWMAAlpha > 1 {
		return fmt.Errorf("invalid AdaptiveBDP LossEWMAAlpha: must be zero or in [0.01, 1]")
	}
	if c.LossRecoveryClearShortBwFraction < 0 ||
		(c.LossRecoveryClearShortBwFraction > 0 && c.LossRecoveryClearShortBwFraction < 0.5) ||
		c.LossRecoveryClearShortBwFraction > 1 {
		return fmt.Errorf("invalid AdaptiveBDP LossRecoveryClearShortBwFraction: must be zero or in [0.5, 1]")
	}
	grace := c.LossGraceRatio
	if grace == 0 {
		grace = 0.01
	}
	soft := c.LossSoftThreshold
	if soft == 0 {
		soft = grace
	}
	severe := c.LossSevereThreshold
	if severe == 0 {
		severe = 0.05
	}
	emergency := c.EmergencyLossThreshold
	if emergency == 0 {
		emergency = 0.10
	}
	if grace > soft || soft > severe || severe > emergency {
		return fmt.Errorf("invalid AdaptiveBDP loss thresholds: require LossGraceRatio <= LossSoftThreshold <= LossSevereThreshold <= EmergencyLossThreshold")
	}
	if c.WindowGain < 0 {
		return fmt.Errorf("invalid AdaptiveBDP WindowGain: must not be negative")
	}
	if c.ProbeDownGain < 0 || c.ProbeDownGain > 1 {
		return fmt.Errorf("invalid AdaptiveBDP ProbeDownGain: must be in [0, 1]")
	}
	if c.ProbeUpGain < 0 || (c.ProbeUpGain > 0 && c.ProbeUpGain < 1) {
		return fmt.Errorf("invalid AdaptiveBDP ProbeUpGain: must be at least 1 when set")
	}
	if c.StartupPacingGain < 0 || (c.StartupPacingGain > 0 && c.StartupPacingGain < 1) || c.StartupPacingGain > 2.77 {
		return fmt.Errorf("invalid AdaptiveBDP StartupPacingGain: must be zero or in [1, 2.77]")
	}
	if c.StartupCwndGain < 0 || (c.StartupCwndGain > 0 && c.StartupCwndGain < 1) {
		return fmt.Errorf("invalid AdaptiveBDP StartupCwndGain: must be at least 1 when set")
	}
	if c.CruisePacingGain < 0 {
		return fmt.Errorf("invalid AdaptiveBDP CruisePacingGain: must not be negative")
	}
	if c.CruiseCwndGain < 0 {
		return fmt.Errorf("invalid AdaptiveBDP CruiseCwndGain: must not be negative")
	}
	if c.LossRecoveryProbeGain < 0 ||
		(c.LossRecoveryProbeGain > 0 && c.LossRecoveryProbeGain < 1.01) ||
		c.LossRecoveryProbeGain > 2 {
		return fmt.Errorf("invalid AdaptiveBDP LossRecoveryProbeGain: must be zero or in [1.01, 2]")
	}
	if c.MinProbeRateBps > 0 && c.MaxProbeRateBps > 0 && c.MinProbeRateBps > c.MaxProbeRateBps {
		return fmt.Errorf("invalid AdaptiveBDP probe rates: MinProbeRateBps must not exceed MaxProbeRateBps")
	}
	if c.PacingMargin < 0 || c.PacingMargin > 0.99 {
		return fmt.Errorf("invalid AdaptiveBDP PacingMargin: must be in [0, 0.99]")
	}
	for name, value := range map[string]time.Duration{
		"StartupTargetDuration": c.StartupTargetDuration,
		"QueueTarget":           c.QueueTarget,
		"LossCutbackCooldown":   c.LossCutbackCooldown,
		"UploadWarmupDuration":  c.UploadWarmupDuration,
		"MinRTTFilterWindow":    c.MinRTTFilterWindow,
		"ProbeInterval":         c.ProbeInterval,
	} {
		if value < 0 {
			return fmt.Errorf("invalid AdaptiveBDP %s: must not be negative", name)
		}
	}
	return nil
}

// populateConfig populates fields in the quic.Config with their default values, if none are set
// it may be called with nil
func populateConfig(config *Config) *Config {
	if config == nil {
		config = &Config{}
	}
	versions := config.Versions
	if len(versions) == 0 {
		versions = protocol.SupportedVersions
	}
	handshakeIdleTimeout := protocol.DefaultHandshakeIdleTimeout
	if config.HandshakeIdleTimeout != 0 {
		handshakeIdleTimeout = config.HandshakeIdleTimeout
	}
	idleTimeout := protocol.DefaultIdleTimeout
	if config.MaxIdleTimeout != 0 {
		idleTimeout = config.MaxIdleTimeout
	}
	initialStreamReceiveWindow := config.InitialStreamReceiveWindow
	if initialStreamReceiveWindow == 0 {
		initialStreamReceiveWindow = protocol.DefaultInitialMaxStreamData
	}
	maxStreamReceiveWindow := config.MaxStreamReceiveWindow
	if maxStreamReceiveWindow == 0 {
		maxStreamReceiveWindow = protocol.DefaultMaxReceiveStreamFlowControlWindow
	}
	initialConnectionReceiveWindow := config.InitialConnectionReceiveWindow
	if initialConnectionReceiveWindow == 0 {
		initialConnectionReceiveWindow = protocol.DefaultInitialMaxData
	}
	maxConnectionReceiveWindow := config.MaxConnectionReceiveWindow
	if maxConnectionReceiveWindow == 0 {
		maxConnectionReceiveWindow = protocol.DefaultMaxReceiveConnectionFlowControlWindow
	}
	maxIncomingStreams := config.MaxIncomingStreams
	if maxIncomingStreams == 0 {
		maxIncomingStreams = protocol.DefaultMaxIncomingStreams
	} else if maxIncomingStreams < 0 {
		maxIncomingStreams = 0
	}
	maxIncomingUniStreams := config.MaxIncomingUniStreams
	if maxIncomingUniStreams == 0 {
		maxIncomingUniStreams = protocol.DefaultMaxIncomingUniStreams
	} else if maxIncomingUniStreams < 0 {
		maxIncomingUniStreams = 0
	}
	initialPacketSize := config.InitialPacketSize
	if initialPacketSize == 0 {
		initialPacketSize = protocol.InitialPacketSize
	}

	return &Config{
		GetConfigForClient:               config.GetConfigForClient,
		Versions:                         versions,
		HandshakeIdleTimeout:             handshakeIdleTimeout,
		MaxIdleTimeout:                   idleTimeout,
		KeepAlivePeriod:                  config.KeepAlivePeriod,
		InitialStreamReceiveWindow:       initialStreamReceiveWindow,
		MaxStreamReceiveWindow:           maxStreamReceiveWindow,
		InitialConnectionReceiveWindow:   initialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:       maxConnectionReceiveWindow,
		AllowConnectionWindowIncrease:    config.AllowConnectionWindowIncrease,
		MaxIncomingStreams:               maxIncomingStreams,
		MaxIncomingUniStreams:            maxIncomingUniStreams,
		TokenStore:                       config.TokenStore,
		EnableDatagrams:                  config.EnableDatagrams,
		InitialPacketSize:                initialPacketSize,
		DisablePathMTUDiscovery:          config.DisablePathMTUDiscovery,
		EnableStreamResetPartialDelivery: config.EnableStreamResetPartialDelivery,
		CwndTuning:                       config.CwndTuning,
		RenoRTTScalingAggression:         config.RenoRTTScalingAggression,
		RenoRTTScalingMaxFactor:          config.RenoRTTScalingMaxFactor,
		Allow0RTT:                        config.Allow0RTT,
		Tracer:                           config.Tracer,
	}
}
