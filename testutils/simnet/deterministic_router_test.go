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
