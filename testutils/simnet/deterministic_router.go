package simnet

import (
	"errors"
	"net"
	"time"
)

// DirectionSelector maps an outgoing packet to one of a deterministic link's
// independently shaped directions.
type DirectionSelector func(Packet) LinkDirection

// DeterministicRouter adapts a DeterministicLink to the Router interface.
// Call AdvanceTo explicitly to deliver packets; it deliberately owns no
// goroutines and therefore remains compatible with virtual-time tests.
type DeterministicRouter struct {
	link      *DeterministicLink
	direction DirectionSelector
	nodes     addrMap[PacketReceiver]
}

// NewDeterministicRouter creates a Router backed by link. selector must map
// every outgoing packet to its forward or reverse direction.
func NewDeterministicRouter(link *DeterministicLink, selector DirectionSelector) *DeterministicRouter {
	if link == nil || selector == nil {
		panic("simnet: deterministic router requires a link and direction selector")
	}
	return &DeterministicRouter{link: link, direction: selector}
}

// SendPacket queues a packet in virtual time. Packets become visible to their
// recipient only after AdvanceTo reaches their scheduled delivery time.
func (r *DeterministicRouter) SendPacket(packet Packet) error {
	if _, ok := r.nodes.Get(packet.To); !ok {
		return errors.New("unknown destination")
	}
	r.link.Send(r.direction(packet), packet)
	return nil
}

// AddNode implements Router.
func (r *DeterministicRouter) AddNode(addr net.Addr, receiver PacketReceiver) {
	_ = r.nodes.Set(addr, receiver)
}

// AdvanceTo advances the attached link and synchronously delivers every ready
// packet to its registered endpoint. It returns the link events for scenario
// telemetry, including ECN and duplicate annotations.
func (r *DeterministicRouter) AdvanceTo(atDuration time.Duration) []DeliveredPacket {
	delivered := r.link.AdvanceTo(atDuration)
	for _, event := range delivered {
		receiver, ok := r.nodes.Get(event.Packet.To)
		if !ok {
			continue
		}
		receiver.RecvPacket(event.Packet)
	}
	return delivered
}

// Link returns the router's deterministic link for scenario configuration and
// counter collection.
func (r *DeterministicRouter) Link() *DeterministicLink { return r.link }

var _ Router = &DeterministicRouter{}
