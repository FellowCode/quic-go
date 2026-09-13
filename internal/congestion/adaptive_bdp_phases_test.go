package congestion

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPRoundObservationDoesNotApplyLoss(t *testing.T) {
	s := smallWindowLossSender()
	s.cfg.EnableAdaptiveBDPTelemetry = true
	s.ackedBytesThisRound = 80_000
	s.lostBytesThisRound = 20_000
	state, cwnd, bw := s.state, s.congestionWindow, s.bw
	now := s.clock.Now().Add(time.Second)
	require.True(t, s.observeRound(RateSample{}, cwnd, now))
	require.Equal(t, state, s.state)
	require.Equal(t, cwnd, s.congestionWindow)
	require.Equal(t, bw, s.bw)
	require.Equal(t, protocol.ByteCount(20_000), s.lostBytesThisRound)
	require.Zero(t, s.lossRatioEWMA)
	require.Empty(t, s.telemetry)
	// The application phase consumes the same observation and may now cut.
	s.finalizeLossRound(now, cwnd)
	require.Less(t, s.congestionWindow, cwnd)
	require.InDelta(t, 0.2, s.lossRatioEWMA, 1e-9)
}

func TestAdaptiveBDPRTTObservationRequestsProbeWithoutEnteringIt(t *testing.T) {
	s := smallWindowLossSender()
	s.minRTT = 10 * time.Millisecond
	s.minRTTTimestamp = s.clock.Now()
	state, cwnd := s.state, s.congestionWindow
	now := s.clock.Now().Add(s.minRTTFilterWindow() + time.Second)
	require.True(t, s.observeMinRTT(100*time.Millisecond, cwnd, now))
	require.Equal(t, state, s.state)
	require.Equal(t, cwnd, s.congestionWindow)
	s.updateMinRTT(100*time.Millisecond, cwnd, now)
	require.Equal(t, adaptiveBDPProbeRTT, s.state)
	require.Equal(t, s.minCongestionWindow, s.congestionWindow)
}
