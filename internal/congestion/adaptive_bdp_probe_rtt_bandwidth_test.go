package congestion

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPProbeRTTPreservesCapacityOnShortRTT(t *testing.T) {
	start := monotime.Now()
	clock := mockClock(start)
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(10*time.Millisecond, 0)
	s := NewAdaptiveBDPSender(&clock, rttStats, &utils.ConnectionStats{}, 1280, CwndTuningConfig{Enable: true})
	s.state = adaptiveBDPProbeBW
	s.minRTT = 10 * time.Millisecond
	s.minRTTTimestamp = start
	const capacity uint64 = 12_500_000
	s.bw, s.maxBw = capacity, capacity
	s.bwFilter.Update(0, capacity)
	s.congestionWindow = s.targetCwnd()
	oldCwnd := s.congestionWindow
	s.enterProbeRTT(start)
	var delivered protocol.ByteCount
	ack := func(bytes protocol.ByteCount, rate uint64) {
		clock.Advance(10 * time.Millisecond)
		priorDelivered := delivered
		delivered += bytes
		s.OnPacketAckedWithRateSample(1, bytes, bytes, clock.Now(), RateSample{
			IsValid: true, DeliveryRate: protocol.ByteCount(rate), RTT: 10 * time.Millisecond,
			Interval: 10 * time.Millisecond, DeliveredBytes: delivered, PriorDelivered: priorDelivered,
			DeliveredDelta: bytes, AckedBytes: bytes,
		})
	}
	for range 20 {
		ack(s.minCongestionWindow, uint64(s.minCongestionWindow)*100)
		require.Equal(t, capacity, s.maxBw, "the ProbeRTT cap must not become a capacity estimate")
		require.Equal(t, capacity, s.bw)
		if s.state == adaptiveBDPProbeRTT {
			require.Equal(t, s.minCongestionWindow, s.congestionWindow)
		}
	}
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, oldCwnd, s.congestionWindow, "a successful measurement must restore the working window")

	// The next lower sample must not immediately expire the pre-probe maximum.
	// Normal lower measurements must still age it out after six usable rounds.
	for n := 1; n <= 6; n++ {
		ack(oldCwnd, capacity/2)
		if n < 6 {
			require.Equal(t, capacity, s.maxBw, "ProbeRTT rounds must not age the bandwidth filter")
		} else {
			require.Equal(t, capacity/2, s.maxBw)
		}
	}
}

func TestAdaptiveBDPProbeRTTHigherSampleStillUpdatesCapacity(t *testing.T) {
	start := monotime.Now()
	s, clock := newAdaptiveBDPProbeRTTTestSender(start)
	s.bw, s.maxBw = 1_000_000, 1_000_000
	s.bwFilter.Update(s.roundCount, s.maxBw)
	clock.Advance(100 * time.Millisecond)
	s.OnPacketAckedWithRateSample(1, 1280, s.minCongestionWindow, clock.Now(), RateSample{
		IsValid: true, DeliveryRate: 2_000_000, RTT: 150 * time.Millisecond,
		DeliveredBytes: 1280, AckedBytes: 1280,
	})
	require.Equal(t, uint64(2_000_000), s.maxBw)
	require.Equal(t, uint64(2_000_000), s.bw)
	require.Equal(t, adaptiveBDPProbeRTT, s.state)
	require.Equal(t, s.minCongestionWindow, s.congestionWindow)
}

func TestAdaptiveBDPProbeRTTTimeoutPreservesBandwidth(t *testing.T) {
	start := monotime.Now()
	s, clock := newAdaptiveBDPProbeRTTTestSender(start)
	const capacity uint64 = 12_500_000
	s.bw, s.maxBw = capacity, capacity
	s.bwFilter.Update(s.roundCount, capacity)
	var delivered protocol.ByteCount
	for range 20 {
		clock.Advance(100 * time.Millisecond)
		prior := delivered
		delivered += s.minCongestionWindow
		// No fresh RTT measurement: the probe must time out without
		// learning the deliberately limited delivery rate as capacity.
		s.OnPacketAckedWithRateSample(1, s.minCongestionWindow, s.minCongestionWindow, clock.Now(), RateSample{
			IsValid: true, DeliveryRate: s.minCongestionWindow * 10, Interval: 100 * time.Millisecond,
			DeliveredBytes: delivered, PriorDelivered: prior, AckedBytes: s.minCongestionWindow,
		})
		require.Equal(t, capacity, s.maxBw)
		require.Equal(t, capacity, s.bw)
	}
	require.Equal(t, adaptiveBDPProbeBW, s.state)
	require.Equal(t, "probe_rtt_timeout_insufficient_drain_evidence", s.lastStateChangeReason)
	require.LessOrEqual(t, s.congestionWindow, 2*s.minCongestionWindow, "an inconclusive probe keeps gradual ACK-driven recovery")
}

func TestAdaptiveBDPProbeRTTDoesNotRestoreWindowAfterCongestion(t *testing.T) {
	for _, ecn := range []bool{false, true} {
		t.Run(map[bool]string{false: "material loss", true: "ECN"}[ecn], func(t *testing.T) {
			start := monotime.Now()
			s, _ := newAdaptiveBDPProbeRTTTestSender(start)
			s.roundCount++
			if ecn {
				s.hasLastECNCE, s.lastECNCERound = true, s.roundCount
			} else {
				s.hasMaterialLossRound, s.lastMaterialLossRound = true, s.roundCount
			}
			s.updateMinRTT(30*time.Millisecond, s.minCongestionWindow, start.Add(200*time.Millisecond))
			s.maybeExitProbeRTT(start.Add(200 * time.Millisecond))
			require.Equal(t, adaptiveBDPProbeBW, s.state)
			require.Equal(t, s.minCongestionWindow, s.congestionWindow)
		})
	}
}
