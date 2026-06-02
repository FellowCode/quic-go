package quic

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/qlogwriter"
	"github.com/quic-go/quic-go/quicvarint"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidation(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		require.NoError(t, validateConfig(nil))
	})

	t.Run("config with a few values set", func(t *testing.T) {
		conf := populateConfig(&Config{
			MaxIncomingStreams:     5,
			MaxStreamReceiveWindow: 10,
		})
		require.NoError(t, validateConfig(conf))
		require.Equal(t, int64(5), conf.MaxIncomingStreams)
		require.Equal(t, uint64(10), conf.MaxStreamReceiveWindow)
	})

	t.Run("stream limits", func(t *testing.T) {
		conf := &Config{
			MaxIncomingStreams:    1<<60 + 1,
			MaxIncomingUniStreams: 1<<60 + 2,
		}
		require.NoError(t, validateConfig(conf))
		require.Equal(t, int64(1<<60), conf.MaxIncomingStreams)
		require.Equal(t, int64(1<<60), conf.MaxIncomingUniStreams)
	})

	t.Run("flow control windows", func(t *testing.T) {
		conf := &Config{
			MaxStreamReceiveWindow:     quicvarint.Max + 1,
			MaxConnectionReceiveWindow: quicvarint.Max + 2,
		}
		require.NoError(t, validateConfig(conf))
		require.Equal(t, uint64(quicvarint.Max), conf.MaxStreamReceiveWindow)
		require.Equal(t, uint64(quicvarint.Max), conf.MaxConnectionReceiveWindow)
	})

	t.Run("initial packet size", func(t *testing.T) {
		// not set
		conf := &Config{InitialPacketSize: 0}
		require.NoError(t, validateConfig(conf))
		require.Zero(t, conf.InitialPacketSize)

		// too small
		conf = &Config{InitialPacketSize: 10}
		require.NoError(t, validateConfig(conf))
		require.Equal(t, uint16(1200), conf.InitialPacketSize)

		// too large
		conf = &Config{InitialPacketSize: protocol.MaxPacketBufferSize + 1}
		require.NoError(t, validateConfig(conf))
		require.Equal(t, uint16(protocol.MaxPacketBufferSize), conf.InitialPacketSize)
	})

	t.Run("cwnd tuning downshift knobs", func(t *testing.T) {
		conf := &Config{CwndTuning: CwndTuning{
			NoCongestionRateFloorFraction: 2,
			NoCongestionDownshiftFactor:   -1,
			UploadWarmupDuration:          -time.Second,
		}}
		require.NoError(t, validateConfig(conf))
		require.Equal(t, 1.0, conf.CwndTuning.NoCongestionRateFloorFraction)
		require.Zero(t, conf.CwndTuning.NoCongestionDownshiftFactor)
		require.Zero(t, conf.CwndTuning.UploadWarmupDuration)
	})
}

func TestConfigHandshakeIdleTimeout(t *testing.T) {
	c := &Config{HandshakeIdleTimeout: time.Second * 11 / 2}
	require.Equal(t, 11*time.Second, c.handshakeTimeout())
}

func configWithNonZeroNonFunctionFields(t *testing.T) *Config {
	t.Helper()
	c := &Config{}
	v := reflect.ValueOf(c).Elem()

	typ := v.Type()
	for i := 0; i < typ.NumField(); i++ {
		f := v.Field(i)
		if !f.CanSet() {
			// unexported field; not cloned.
			continue
		}

		switch fn := typ.Field(i).Name; fn {
		case "GetConfigForClient", "RequireAddressValidation", "GetLogWriter", "AllowConnectionWindowIncrease", "Tracer":
			// Can't compare functions.
		case "Versions":
			f.Set(reflect.ValueOf([]Version{1, 2, 3}))
		case "ConnectionIDLength":
			f.Set(reflect.ValueOf(8))
		case "ConnectionIDGenerator":
			f.Set(reflect.ValueOf(&protocol.DefaultConnectionIDGenerator{ConnLen: protocol.DefaultConnectionIDLength}))
		case "HandshakeIdleTimeout":
			f.Set(reflect.ValueOf(time.Second))
		case "MaxIdleTimeout":
			f.Set(reflect.ValueOf(time.Hour))
		case "TokenStore":
			f.Set(reflect.ValueOf(NewLRUTokenStore(2, 3)))
		case "InitialStreamReceiveWindow":
			f.Set(reflect.ValueOf(uint64(1234)))
		case "MaxStreamReceiveWindow":
			f.Set(reflect.ValueOf(uint64(9)))
		case "InitialConnectionReceiveWindow":
			f.Set(reflect.ValueOf(uint64(4321)))
		case "MaxConnectionReceiveWindow":
			f.Set(reflect.ValueOf(uint64(10)))
		case "MaxIncomingStreams":
			f.Set(reflect.ValueOf(int64(11)))
		case "MaxIncomingUniStreams":
			f.Set(reflect.ValueOf(int64(12)))
		case "StatelessResetKey":
			f.Set(reflect.ValueOf(&StatelessResetKey{1, 2, 3, 4}))
		case "KeepAlivePeriod":
			f.Set(reflect.ValueOf(time.Second))
		case "EnableDatagrams":
			f.Set(reflect.ValueOf(true))
		case "DisableVersionNegotiationPackets":
			f.Set(reflect.ValueOf(true))
		case "InitialPacketSize":
			f.Set(reflect.ValueOf(uint16(1350)))
		case "DisablePathMTUDiscovery":
			f.Set(reflect.ValueOf(true))
		case "Allow0RTT":
			f.Set(reflect.ValueOf(true))
		case "EnableStreamResetPartialDelivery":
			f.Set(reflect.ValueOf(true))
		case "CwndTuning":
			f.Set(reflect.ValueOf(CwndTuning{
				Enable:                           true,
				Algorithm:                        CongestionControlAdaptiveBDP,
				InitialWindowPackets:             32,
				MinWindowPackets:                 2,
				MaxWindowPackets:                 8000,
				WindowGain:                       1.1,
				MaxProbeRateBps:                  100_000_000,
				MinProbeRateBps:                  15_000_000,
				StartupTargetRateBps:             100_000_000,
				StartupTargetDuration:            5 * time.Second,
				StartupPacingGain:                2.0,
				StartupCwndGain:                  2.0,
				ProbeUpGain:                      1.25,
				ProbeDownGain:                    0.9,
				CruisePacingGain:                 1.0,
				CruiseCwndGain:                   1.5,
				QueueTarget:                      25 * time.Millisecond,
				QueuePersistentRounds:            2,
				LossTarget:                       0.005,
				LossGraceRatio:                   0.01,
				LossSoftThreshold:                0.015,
				LossSevereThreshold:              0.05,
				EmergencyLossThreshold:           0.02,
				LossMinBytes:                     2560,
				EmergencyLossMinBytes:            10240,
				MinLossSampleBytes:               64 * 1024,
				LossEWMAAlpha:                    0.25,
				MaxLossCwndCutNoQueue:            0.15,
				MaxLossCwndCutWithQueue:          0.30,
				MinLossCwndCut:                   0.01,
				MaxLossPacingCutNoQueue:          0.10,
				MaxLossPacingCutWithQueue:        0.25,
				LossCutbackCooldown:              200 * time.Millisecond,
				MildLossPersistentRounds:         2,
				LossRecoveryProbeRounds:          3,
				LossRecoveryProbeGain:            1.35,
				LossRecoveryProbeDurationRounds:  4,
				LossRecoveryClearShortBwFraction: 0.90,
				BandwidthFilterRounds:            6,
				DownshiftRounds:                  2,
				DownshiftRatio:                   0.85,
				NoCongestionRateFloorFraction:    0.5,
				NoCongestionDownshiftRounds:      4,
				NoCongestionDownshiftFactor:      0.75,
				UploadWarmupDuration:             time.Second,
				UploadWarmupBytes:                512 * 1024,
				MinDownshiftSampleBytes:          128 * 1024,
				CongestionDownshiftRounds:        3,
				MinRTTFilterWindow:               10 * time.Second,
				ProbeInterval:                    5 * time.Second,
				PacingMargin:                     0.01,
			}))
		case "RenoRTTScalingAggression":
			f.Set(reflect.ValueOf(2.5))
		case "RenoRTTScalingMaxFactor":
			f.Set(reflect.ValueOf(3.5))
		default:
			t.Fatalf("all fields must be accounted for, but saw unknown field %q", fn)
		}
	}
	return c
}

func TestConfigClone(t *testing.T) {
	t.Run("function fields", func(t *testing.T) {
		var calledAllowConnectionWindowIncrease, calledTracer bool
		c1 := &Config{
			GetConfigForClient:            func(info *ClientInfo) (*Config, error) { return nil, assert.AnError },
			AllowConnectionWindowIncrease: func(*Conn, uint64) bool { calledAllowConnectionWindowIncrease = true; return true },
			Tracer: func(context.Context, bool, ConnectionID) qlogwriter.Trace {
				calledTracer = true
				return nil
			},
		}
		c2 := c1.Clone()
		c2.AllowConnectionWindowIncrease(nil, 1234)
		require.True(t, calledAllowConnectionWindowIncrease)
		_, err := c2.GetConfigForClient(&ClientInfo{})
		require.ErrorIs(t, err, assert.AnError)
		c2.Tracer(context.Background(), true, protocol.ConnectionID{})
		require.True(t, calledTracer)
	})

	t.Run("non-function fields", func(t *testing.T) {
		c := configWithNonZeroNonFunctionFields(t)
		require.Equal(t, c, c.Clone())
	})

	t.Run("returns a copy", func(t *testing.T) {
		c1 := &Config{MaxIncomingStreams: 100}
		c2 := c1.Clone()
		c2.MaxIncomingStreams = 200
		require.EqualValues(t, 100, c1.MaxIncomingStreams)
	})
}

func TestConfigDefaultValues(t *testing.T) {
	// if set, the values should be copied
	c := configWithNonZeroNonFunctionFields(t)
	require.Equal(t, c, populateConfig(c))

	// if not set, some fields use default values
	c = populateConfig(&Config{})
	require.Equal(t, protocol.SupportedVersions, c.Versions)
	require.Equal(t, protocol.DefaultHandshakeIdleTimeout, c.HandshakeIdleTimeout)
	require.Equal(t, protocol.DefaultIdleTimeout, c.MaxIdleTimeout)
	require.EqualValues(t, protocol.DefaultInitialMaxStreamData, c.InitialStreamReceiveWindow)
	require.EqualValues(t, protocol.DefaultMaxReceiveStreamFlowControlWindow, c.MaxStreamReceiveWindow)
	require.EqualValues(t, protocol.DefaultInitialMaxData, c.InitialConnectionReceiveWindow)
	require.EqualValues(t, protocol.DefaultMaxReceiveConnectionFlowControlWindow, c.MaxConnectionReceiveWindow)
	require.EqualValues(t, protocol.DefaultMaxIncomingStreams, c.MaxIncomingStreams)
	require.EqualValues(t, protocol.DefaultMaxIncomingUniStreams, c.MaxIncomingUniStreams)
	require.False(t, c.DisablePathMTUDiscovery)
	require.Nil(t, c.GetConfigForClient)
}

func TestConfigZeroLimits(t *testing.T) {
	config := &Config{
		MaxIncomingStreams:    -1,
		MaxIncomingUniStreams: -1,
	}
	c := populateConfig(config)
	require.Zero(t, c.MaxIncomingStreams)
	require.Zero(t, c.MaxIncomingUniStreams)
}

func TestConfigValidationRenoRTTScaling(t *testing.T) {
	conf := &Config{
		RenoRTTScalingAggression: -1,
		RenoRTTScalingMaxFactor:  0.5,
	}
	require.NoError(t, validateConfig(conf))
	require.Zero(t, conf.RenoRTTScalingAggression)
	require.Equal(t, 1.0, conf.RenoRTTScalingMaxFactor)

	conf = &Config{
		RenoRTTScalingAggression: 2,
		RenoRTTScalingMaxFactor:  -2,
	}
	require.NoError(t, validateConfig(conf))
	require.Equal(t, 2.0, conf.RenoRTTScalingAggression)
	require.Zero(t, conf.RenoRTTScalingMaxFactor)
}
