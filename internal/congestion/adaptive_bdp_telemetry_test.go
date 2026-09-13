package congestion

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPTelemetryRing(t *testing.T) {
	s := smallWindowLossSender()
	require.Nil(t, s.telemetrySnapshot())
	s.recordTelemetry("disabled", s.clock.Now(), 0)
	require.Nil(t, s.telemetry)
	s.cfg.EnableAdaptiveBDPTelemetry = true
	for i := 0; i < 3*adaptiveBDPTelemetryLimit+17; i++ {
		s.roundCount = uint64(i)
		s.recordTelemetry("round", s.clock.Now().Add(time.Duration(i)*time.Millisecond), 0)
		if i == 2 || i == adaptiveBDPTelemetryLimit-1 || i == adaptiveBDPTelemetryLimit || i == 3*adaptiveBDPTelemetryLimit+16 {
			snapshot := s.AdaptiveBDPDebugInfo().Telemetry
			require.Len(t, snapshot, min(i+1, adaptiveBDPTelemetryLimit))
			for j, sample := range snapshot {
				require.Equal(t, uint64(i-len(snapshot)+1+j), sample.RoundCount)
			}
			snapshot[0].Event = "mutated"
			require.Equal(t, "round", s.telemetrySnapshot()[0].Event)
		}
	}
	require.Zero(t, testing.AllocsPerRun(100, func() { s.recordTelemetry("round", s.clock.Now(), 0) }))
}
