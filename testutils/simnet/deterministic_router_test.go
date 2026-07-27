package simnet

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeterministicRouterDeliversOnlyWhenVirtualTimeAdvances(t *testing.T) {
	clientAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1000}
	serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 1000}
	link := NewDeterministicLink(DeterministicLinkConfig{
		Forward: DeterministicDirectionConfig{BandwidthBitsPerSecond: 8_000, BaseLatency: time.Millisecond},
		Reverse: DeterministicDirectionConfig{BandwidthBitsPerSecond: 8_000, BaseLatency: time.Millisecond},
	})
	router := NewDeterministicRouter(link, func(packet Packet) LinkDirection {
		if packet.From.String() == clientAddr.String() {
			return LinkForward
		}
		return LinkReverse
	})
	client := NewSimConn(clientAddr, router)
	server := NewSimConn(serverAddr, router)

	_, err := client.WriteTo([]byte("deterministic"), serverAddr)
	require.NoError(t, err)
	require.Empty(t, router.AdvanceTo(time.Millisecond))
	delivered := router.AdvanceTo(14 * time.Millisecond)
	require.Len(t, delivered, 1)
	buf := make([]byte, 64)
	n, from, err := server.ReadFrom(buf)
	require.NoError(t, err)
	require.Equal(t, []byte("deterministic"), buf[:n])
	require.Equal(t, clientAddr.String(), from.String())
	_ = client.Close()
	_ = server.Close()
}

func TestDeterministicRouterPacketDelaySharesBottleneck(t *testing.T) {
	clientAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1000}
	serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 1000}
	link := NewDeterministicLink(DeterministicLinkConfig{
		Forward: DeterministicDirectionConfig{BandwidthBitsPerSecond: 8_000, BaseLatency: time.Millisecond},
		Reverse: DeterministicDirectionConfig{BandwidthBitsPerSecond: 8_000, BaseLatency: time.Millisecond},
	})
	router := NewDeterministicRouterWithDelay(link, func(packet Packet) LinkDirection {
		if packet.From.String() == clientAddr.String() {
			return LinkForward
		}
		return LinkReverse
	}, func(packet Packet) time.Duration {
		if packet.From.String() == clientAddr.String() {
			return 5 * time.Millisecond
		}
		return 0
	})
	client := NewSimConn(clientAddr, router)
	server := NewSimConn(serverAddr, router)
	defer client.Close()
	defer server.Close()

	_, err := client.WriteTo([]byte("delayed"), serverAddr)
	require.NoError(t, err)
	require.Empty(t, router.AdvanceTo(12*time.Millisecond))
	delivered := router.AdvanceTo(13 * time.Millisecond)
	require.Len(t, delivered, 1)
	require.Equal(t, uint64(7), link.Counters(LinkForward).SubmittedBytes)
}

func TestDeterministicRouterPreservesECNMarkForSimConnAdapter(t *testing.T) {
	clientAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1000}
	serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 1000}
	link := NewDeterministicLink(DeterministicLinkConfig{
		Forward: DeterministicDirectionConfig{BandwidthBitsPerSecond: 8_000_000, BaseLatency: time.Millisecond, ECNThresholdBytes: 1},
		Reverse: DeterministicDirectionConfig{BandwidthBitsPerSecond: 8_000_000, BaseLatency: time.Millisecond},
	})
	router := NewDeterministicRouter(link, func(packet Packet) LinkDirection {
		if packet.From.String() == clientAddr.String() {
			return LinkForward
		}
		return LinkReverse
	})
	client := NewSimConn(clientAddr, router)
	server := NewSimConn(serverAddr, router)
	defer client.Close()
	defer server.Close()

	_, err := client.WriteTo([]byte("ecn"), serverAddr)
	require.NoError(t, err)
	router.AdvanceTo(2 * time.Millisecond)
	buf := make([]byte, 16)
	n, from, marked, err := server.ReadFromWithECN(buf)
	require.NoError(t, err)
	require.Equal(t, []byte("ecn"), buf[:n])
	require.Equal(t, clientAddr.String(), from.String())
	require.True(t, marked)
}
