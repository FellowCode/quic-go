package simnet

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testPacket(data string) Packet { return Packet{Data: []byte(data)} }

func TestDeterministicLinkShapesDirectionsAndAccountsQueue(t *testing.T) {
	link := NewDeterministicLink(DeterministicLinkConfig{
		Forward: DeterministicDirectionConfig{BandwidthBitsPerSecond: 8_000, BaseLatency: 5 * time.Millisecond, QueueLimitBytes: 3},
		Reverse: DeterministicDirectionConfig{BandwidthBitsPerSecond: 16_000, BaseLatency: time.Millisecond},
	})
	require.True(t, sendAccepted(t, link, LinkForward, testPacket("a")))
	require.True(t, sendAccepted(t, link, LinkForward, testPacket("b")))
	require.True(t, sendAccepted(t, link, LinkForward, testPacket("c")))
	require.False(t, sendAccepted(t, link, LinkForward, testPacket("d")), "the finite queue tail-drops the fourth packet")
	require.Equal(t, uint64(3), link.QueueBytes(LinkForward))

	// 1 byte at 8 kbit/s serializes in 1 ms; the first packet reaches the
	// receiver after one serialization interval plus five milliseconds.
	require.Empty(t, link.AdvanceTo(5*time.Millisecond))
	delivered := link.AdvanceTo(6 * time.Millisecond)
	require.Len(t, delivered, 1)
	require.Equal(t, "a", string(delivered[0].Packet.Data))
	require.Equal(t, uint64(2), link.QueueBytes(LinkForward))

	require.True(t, sendAccepted(t, link, LinkReverse, testPacket("r")))
	require.Len(t, link.AdvanceTo(7*time.Millisecond), 1, "the forward queue remains independently shaped")
	reverse := link.AdvanceTo(7500 * time.Microsecond)
	require.Len(t, reverse, 1, "reverse direction has independent latency and shaping")
	require.Equal(t, LinkReverse, reverse[0].Direction)
	forward := link.Counters(LinkForward)
	require.Equal(t, uint64(1), forward.TailDrops)
	require.Equal(t, uint64(2), forward.DeliveredPackets)
}

func TestDeterministicLinkLossECNReorderingDuplicationAndChanges(t *testing.T) {
	config := DeterministicDirectionConfig{
		BandwidthBitsPerSecond: 8_000,
		BaseLatency:            time.Millisecond,
		ECNThresholdBytes:      2,
		LossIntervals:          []LossInterval{{Start: 3 * time.Millisecond, End: 5 * time.Millisecond}},
		ReorderEvery:           2,
		ReorderDelay:           10 * time.Millisecond,
		DuplicateEvery:         3,
	}
	link := NewDeterministicLink(DeterministicLinkConfig{Forward: config, Seed: 42})
	require.True(t, sendAccepted(t, link, LinkForward, testPacket("a")))
	accepted, marked := link.Send(LinkForward, testPacket("b"))
	require.True(t, accepted)
	require.True(t, marked, "the queued second packet is ECN-marked")

	// Change both bandwidth and base RTT at an exact virtual time. A packet
	// sent after the change observes it; packets already in flight do not.
	changed := config
	changed.BandwidthBitsPerSecond = 16_000
	changed.BaseLatency = 4 * time.Millisecond
	changed.LossIntervals = nil
	link.ScheduleChange(LinkForward, 5*time.Millisecond, changed)
	delivered := link.AdvanceTo(3 * time.Millisecond)
	require.False(t, sendAccepted(t, link, LinkForward, testPacket("x")), "the scripted loss interval applies before its end")
	link.AdvanceTo(5 * time.Millisecond)
	require.True(t, sendAccepted(t, link, LinkForward, testPacket("c")))

	delivered = append(delivered, link.AdvanceTo(20*time.Millisecond)...)
	require.Len(t, delivered, 4, "one reordered packet plus one deterministic duplicate are delivered")
	require.Equal(t, "a", string(delivered[0].Packet.Data))
	require.Equal(t, "c", string(delivered[1].Packet.Data), "reorder delay lets the later packet overtake b")
	require.Equal(t, "c", string(delivered[2].Packet.Data))
	require.True(t, delivered[1].ECNMarked)
	require.True(t, delivered[2].Duplicate)
	require.Equal(t, "b", string(delivered[3].Packet.Data))
	counters := link.Counters(LinkForward)
	require.Equal(t, uint64(1), counters.ScriptedLosses)
	require.Equal(t, uint64(2), counters.ECNMarks)
	require.Equal(t, uint64(1), counters.Reordered)
	require.Equal(t, uint64(1), counters.Duplicates)
	require.Zero(t, link.QueueBytes(LinkForward))
}

func TestDeterministicLinkRandomLossUsesFixedSeed(t *testing.T) {
	newLink := func() *DeterministicLink {
		return NewDeterministicLink(DeterministicLinkConfig{
			Forward: DeterministicDirectionConfig{RandomLossProbability: .5},
			Seed:    99,
		})
	}
	first, second := newLink(), newLink()
	for i := 0; i < 32; i++ {
		acceptedFirst, _ := first.Send(LinkForward, testPacket("x"))
		acceptedSecond, _ := second.Send(LinkForward, testPacket("x"))
		require.Equal(t, acceptedFirst, acceptedSecond)
	}
	require.Equal(t, first.Counters(LinkForward), second.Counters(LinkForward))
	require.Greater(t, first.Counters(LinkForward).RandomLosses, uint64(0))
}

func TestDeterministicLinkGilbertElliottLossUsesFixedSeed(t *testing.T) {
	newLink := func() *DeterministicLink {
		return NewDeterministicLink(DeterministicLinkConfig{
			Forward: DeterministicDirectionConfig{GilbertElliottLoss: &GilbertElliottLossConfig{
				GoodToBadProbability: .2,
				BadToGoodProbability: .1,
				BadLossProbability:   .9,
			}},
			Seed: 101,
		})
	}
	first, second := newLink(), newLink()
	for range 128 {
		acceptedFirst, _ := first.Send(LinkForward, testPacket("x"))
		acceptedSecond, _ := second.Send(LinkForward, testPacket("x"))
		require.Equal(t, acceptedFirst, acceptedSecond)
	}
	require.Equal(t, first.Counters(LinkForward), second.Counters(LinkForward))
	require.Greater(t, first.Counters(LinkForward).RandomLosses, uint64(0))
}

func TestDeterministicLinkSchedulesCrossTrafficAtVirtualTime(t *testing.T) {
	link := NewDeterministicLink(DeterministicLinkConfig{
		Forward: DeterministicDirectionConfig{BandwidthBitsPerSecond: 8_000, BaseLatency: time.Millisecond},
	})
	link.SchedulePacket(LinkForward, 5*time.Millisecond, testPacket("x"))
	require.Empty(t, link.AdvanceTo(4*time.Millisecond))
	require.Equal(t, uint64(0), link.Counters(LinkForward).SubmittedPackets)
	require.Len(t, link.AdvanceTo(7*time.Millisecond), 1)
	require.Equal(t, uint64(1), link.Counters(LinkForward).SubmittedPackets)
}

func TestDeterministicLinkSchedulesExactPacketLossBurst(t *testing.T) {
	link := NewDeterministicLink(DeterministicLinkConfig{})
	link.ScheduleLossBurst(LinkForward, 5*time.Millisecond, 3)
	link.AdvanceTo(5 * time.Millisecond)
	for range 3 {
		require.False(t, sendAccepted(t, link, LinkForward, testPacket("x")))
	}
	require.True(t, sendAccepted(t, link, LinkForward, testPacket("x")))
	require.Equal(t, uint64(3), link.Counters(LinkForward).ScriptedLosses)
}

func TestDeterministicLinkRejectsInvalidConfiguration(t *testing.T) {
	require.Panics(t, func() {
		NewDeterministicLink(DeterministicLinkConfig{Forward: DeterministicDirectionConfig{RandomLossProbability: 1.01}})
	})
	require.Panics(t, func() {
		NewDeterministicLink(DeterministicLinkConfig{Forward: DeterministicDirectionConfig{BaseLatency: -time.Millisecond}})
	})
}

func sendAccepted(t *testing.T, link *DeterministicLink, direction LinkDirection, packet Packet) bool {
	t.Helper()
	accepted, _ := link.Send(direction, packet)
	return accepted
}
