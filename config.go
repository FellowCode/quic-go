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
	if config.CwndTuning.EmergencyLossThreshold < 0 {
		config.CwndTuning.EmergencyLossThreshold = 0
	}
	if config.CwndTuning.DownshiftRatio < 0 {
		config.CwndTuning.DownshiftRatio = 0
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
