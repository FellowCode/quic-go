package ackhandler

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/congestion"
	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
	"github.com/quic-go/quic-go/internal/wire"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPACKTrainCountsOneRound(t *testing.T) {
	sph := NewSentPacketHandlerWithCongestionConfig(
		0, 1200, utils.NewRTTStats(), &utils.ConnectionStats{}, true, false,
		nil, protocol.PerspectiveClient, nil, utils.DefaultLogger,
		CongestionControlConfig{CwndTuning: congestion.CwndTuningConfig{
			Enable: true, Algorithm: congestion.CongestionControlAdaptiveBDP, InitialWindowPackets: 100,
		}},
	).(*sentPacketHandler)
	start := monotime.Now()
	packets := make([]protocol.PacketNumber, 100)
	for i := range packets {
		packets[i] = sph.PopPacketNumber(protocol.Encryption1RTT)
		sph.SentPacket(start, packets[i], protocol.InvalidPacketNumber, nil,
			[]Frame{{Frame: &wire.PingFrame{}}}, protocol.Encryption1RTT, protocol.ECNNon, 1200, false, false)
	}
	for i, pn := range packets {
		_, err := sph.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pn)}, protocol.Encryption1RTT,
			start.Add(100*time.Millisecond+time.Duration(i)*time.Microsecond))
		require.NoError(t, err)
		info, ok := sph.AdaptiveBDPDebugInfo()
		require.True(t, ok)
		require.Equal(t, uint64(1), info.RoundCount, "ACK %d belongs to the same flight", i)
		require.False(t, info.FullBwReached)
	}
	pn := sph.PopPacketNumber(protocol.Encryption1RTT)
	sph.SentPacket(start.Add(200*time.Millisecond), pn, protocol.InvalidPacketNumber, nil,
		[]Frame{{Frame: &wire.PingFrame{}}}, protocol.Encryption1RTT, protocol.ECNNon, 1200, false, false)
	_, err := sph.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pn)}, protocol.Encryption1RTT, start.Add(300*time.Millisecond))
	require.NoError(t, err)
	info, ok := sph.AdaptiveBDPDebugInfo()
	require.True(t, ok)
	require.Equal(t, uint64(2), info.RoundCount)
}

func TestRateSamplerRoundMetadataCoversEntireACKBatch(t *testing.T) {
	sph := NewSentPacketHandler(
		0, 1200, utils.NewRTTStats(), &utils.ConnectionStats{}, true, false,
		nil, protocol.PerspectiveClient, nil, utils.DefaultLogger,
	).(*sentPacketHandler)
	algo := &captureRateSampleAlgorithm{cwnd: 1200}
	sph.congestion = algo
	start := monotime.Now()
	send := func(at time.Duration) protocol.PacketNumber {
		pn := sph.PopPacketNumber(protocol.Encryption1RTT)
		sph.SentPacket(start.Add(at), pn, protocol.InvalidPacketNumber, nil,
			[]Frame{{Frame: &wire.PingFrame{}}}, protocol.Encryption1RTT, protocol.ECNNon, 1200, false, false)
		return pn
	}
	ack := func(at time.Duration, packets ...protocol.PacketNumber) {
		_, err := sph.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(packets...)}, protocol.Encryption1RTT, start.Add(at))
		require.NoError(t, err)
	}
	first := send(0)
	older := send(time.Millisecond)
	ack(100*time.Millisecond, first)
	// Make the later packet app-limited, so the batch chooses the older
	// non-app-limited packet's rate even though the later packet starts a round.
	algo.cwnd = 100 * 1200
	newer := send(101 * time.Millisecond)
	ack(201*time.Millisecond, older, newer)
	require.Len(t, algo.samples, 2)
	sample := algo.samples[1]
	require.False(t, sample.AppLimited)
	require.Equal(t, protocol.ByteCount(2400), sample.DeliveredDelta, "rate still comes from the older packet")
	require.Equal(t, protocol.ByteCount(3600), sample.DeliveredBytes, "round boundary includes all newly delivered packets")
	require.Equal(t, protocol.ByteCount(1200), sample.PriorDelivered, "send-time snapshot comes from the newer packet")
}
