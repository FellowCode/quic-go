package congestion

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPRoundsFollowSendTimeDelivered(t *testing.T) {
	for _, test := range []struct {
		name       string
		valid      bool
		appLimited bool
		ackSpacing time.Duration
	}{
		{name: "compressed ACKs", valid: true, ackSpacing: time.Microsecond},
		{name: "ACKs delayed beyond min RTT", valid: true, ackSpacing: 10 * time.Millisecond},
		{name: "invalid rate with delivery metadata", ackSpacing: time.Microsecond},
		{name: "app limited flight", valid: true, appLimited: true, ackSpacing: time.Microsecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			start := monotime.Now()
			clock := mockClock(start)
			s := NewAdaptiveBDPSender(&clock, utils.NewRTTStats(), &utils.ConnectionStats{}, 1280, CwndTuningConfig{Enable: true})
			s.minRTT = 100 * time.Millisecond
			const priorDelivered protocol.ByteCount = 1_000_000
			const flightBytes protocol.ByteCount = 100 * 1280
			// Every packet in this flight was sent before any of its ACKs.
			for n := 1; n <= 100; n++ {
				delta := protocol.ByteCount(n * 1280)
				clock.Advance(test.ackSpacing)
				s.updateRound(RateSample{
					DeliveredBytes: priorDelivered + delta,
					PriorDelivered: priorDelivered,
					DeliveredDelta: delta,
					AckedBytes:     delta,
					DeliveryRate:   delta * 10,
					IsValid:        test.valid,
					AppLimited:     test.appLimited,
				}, flightBytes, clock.Now())
				require.Equal(t, uint64(1), s.roundCount, "ACK %d is still from the same flight", n)
				require.Equal(t, n == 1, s.roundStart)
			}
			require.Equal(t, priorDelivered+1280, s.nextRoundDelivered)
			require.False(t, s.fullBwReached, "one flight must not complete Startup")

			// A packet sent after those ACKs starts the next packet-timed round.
			clock.Advance(100 * time.Millisecond)
			sample := RateSample{
				DeliveredBytes: priorDelivered + flightBytes + 1280,
				PriorDelivered: priorDelivered + flightBytes,
				IsValid:        test.valid,
			}
			s.updateRound(sample, flightBytes, clock.Now())
			require.Equal(t, uint64(2), s.roundCount)
			require.True(t, s.roundStart)
			require.Equal(t, sample.DeliveredBytes, s.nextRoundDelivered)

			// Even a late ACK for the old flight cannot advance that boundary.
			clock.Advance(200 * time.Millisecond)
			sample.PriorDelivered = priorDelivered
			sample.DeliveredBytes += 1280
			s.updateRound(sample, flightBytes, clock.Now())
			require.Equal(t, uint64(2), s.roundCount)
			require.False(t, s.roundStart)
		})
	}
}
