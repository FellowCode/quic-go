package simnet

import (
	"errors"
	"net"
	"time"
)

// DirectionSelector maps an outgoing packet to one of a deterministic link's
// independently shaped directions.
type DirectionSelector func(Packet) LinkDirection

// PacketDelaySelector supplies optional per-packet access-path latency before
// the packet reaches the shared deterministic bottleneck. It is useful for
// scenarios with a common bottleneck but unequal end-to-end RTTs.
type PacketDelaySelector func(Packet) time.Duration

// DeterministicRouter adapts a DeterministicLink to the Router interface.
// Call AdvanceTo explicitly to deliver packets; it deliberately owns no
// goroutines and therefore remains compatible with virtual-time tests.
type DeterministicRouter struct {
	link      *DeterministicLink
	direction DirectionSelector
	delay     PacketDelaySelector
	nodes     addrMap[PacketReceiver]
}

// NewDeterministicRouter creates a Router backed by link. selector must map
// every outgoing packet to its forward or reverse direction.
func NewDeterministicRouter(link *DeterministicLink, selector DirectionSelector) *DeterministicRouter {
	return NewDeterministicRouterWithDelay(link, selector, nil)
}

// NewDeterministicRouterWithDelay creates a router with optional packet
// access-path latency before its shared deterministic bottleneck.
func NewDeterministicRouterWithDelay(link *DeterministicLink, selector DirectionSelector, delay PacketDelaySelector) *DeterministicRouter {
	if link == nil || selector == nil {
		panic("simnet: deterministic router requires a link and direction selector")
	}
	return &DeterministicRouter{link: link, direction: selector, delay: delay}
}

// SendPacket queues a packet in virtual time. Packets become visible to their
// recipient only after AdvanceTo reaches their scheduled delivery time.
func (r *DeterministicRouter) SendPacket(packet Packet) error {
	if _, ok := r.nodes.Get(packet.To); !ok {
		return errors.New("unknown destination")
	}
	delay := time.Duration(0)
	if r.delay != nil {
		delay = r.delay(packet)
	}
	if delay < 0 {
		return errors.New("simnet: deterministic router delay must not be negative")
	}
	if delay == 0 {
		r.link.Send(r.direction(packet), packet)
	} else {
		r.link.SchedulePacket(r.direction(packet), r.link.Now()+delay, packet)
	}
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
		packet := event.Packet
		packet.ECNMarked = event.ECNMarked
		receiver.RecvPacket(packet)
	}
	return delivered
}

// Link returns the router's deterministic link for scenario configuration and
// counter collection.
func (r *DeterministicRouter) Link() *DeterministicLink { return r.link }

var _ Router = &DeterministicRouter{}
