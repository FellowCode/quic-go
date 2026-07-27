package simnet

import (
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

var ErrDeadlineExceeded = errors.New("deadline exceeded")

type PacketReceiver interface {
	RecvPacket(p Packet)
}

// Router handles routing of packets between simulated connections.
// Implementations are responsible for delivering packets to their destinations.
type Router interface {
	SendPacket(p Packet) error
	AddNode(addr net.Addr, receiver PacketReceiver)
}

type Packet struct {
	To   net.Addr
	From net.Addr

	Data      []byte
	ECNMarked bool
}

// SimConn is a simulated network connection that implements net.PacketConn.
// It provides packet-based communication through a Router for testing and
// simulation purposes. All send/recv operations are handled through the
// Router's packet delivery mechanism.
type SimConn struct {
	mu              sync.Mutex
	closed          bool
	closedChan      chan struct{}
	deadlineUpdated chan struct{}

	packetsSent atomic.Uint64
	packetsRcvd atomic.Uint64
	bytesSent   atomic.Int64
	bytesRcvd   atomic.Int64

	router Router

	myAddr        *net.UDPAddr
	myLocalAddr   net.Addr
	packetsToRead chan Packet

	// Controls whether to block when receiving packets if our buffer is full.
	// If false, drops packets.
	recvBackPressure bool

	readDeadline  time.Time
	writeDeadline time.Time
}

var _ net.PacketConn = &SimConn{}

// NewSimConn creates a new simulated connection that drops packets if the
// receive buffer is full.
func NewSimConn(addr *net.UDPAddr, rtr Router) *SimConn {
	return newSimConn(addr, rtr, false, 32)
}

// NewBlockingSimConn creates a new simulated connection that blocks if the
// receive buffer is full. Does not drop packets.
func NewBlockingSimConn(addr *net.UDPAddr, rtr Router) *SimConn {
	return newSimConn(addr, rtr, true, 32)
}

// NewBufferedSimConn creates a non-blocking simulated UDP endpoint with an
// explicit receive queue. Deterministic-link tests use this to ensure that
// router delivery batches do not introduce scheduler-dependent local drops.
func NewBufferedSimConn(addr *net.UDPAddr, rtr Router, receiveBuffer int) *SimConn {
	if receiveBuffer <= 0 {
		panic("simnet: receive buffer must be positive")
	}
	return newSimConn(addr, rtr, false, receiveBuffer)
}

func newSimConn(addr *net.UDPAddr, rtr Router, block bool, receiveBuffer int) *SimConn {
	c := &SimConn{
		recvBackPressure: block,
		router:           rtr,
		myAddr:           addr,
		packetsToRead:    make(chan Packet, receiveBuffer),
		closedChan:       make(chan struct{}),
		deadlineUpdated:  make(chan struct{}, 1),
	}
	rtr.AddNode(addr, c)
	return c
}

type ConnStats struct {
	BytesSent   int
	BytesRcvd   int
	PacketsSent int
	PacketsRcvd int
}

func (c *SimConn) Stats() ConnStats {
	return ConnStats{
		BytesSent:   int(c.bytesSent.Load()),
		BytesRcvd:   int(c.bytesRcvd.Load()),
		PacketsSent: int(c.packetsSent.Load()),
		PacketsRcvd: int(c.packetsRcvd.Load()),
	}
}

// SetReadBuffer only exists to quell the warning message from quic-go
func (c *SimConn) SetReadBuffer(n int) error {
	return nil
}

// SetWriteBuffer only exists to quell the warning message from quic-go
func (c *SimConn) SetWriteBuffer(n int) error {
	return nil
}

func (c *SimConn) RecvPacket(p Packet) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	c.packetsRcvd.Add(1)
	c.bytesRcvd.Add(int64(len(p.Data)))

	if c.recvBackPressure {
		select {
		case c.packetsToRead <- p:
		case <-c.closedChan:
			// if the connection is closed, drop the packet
			return
		}
	} else {
		select {
		case c.packetsToRead <- p:
		default:
			// drop the packet if the channel is full
		}
	}
}

func (c *SimConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.closedChan)
	return nil
}

func (c *SimConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, addr, _, err = c.ReadFromWithECN(p)
	return n, addr, err
}

// ReadFromWithECN is ReadFrom plus the deterministic link's ECN-CE delivery
// annotation. It is for virtual network adapters that expose received ECN via
// UDP ancillary data; normal PacketConn users should use ReadFrom.
func (c *SimConn) ReadFromWithECN(p []byte) (n int, addr net.Addr, ecnMarked bool, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, nil, false, net.ErrClosed
	}
	deadline := c.readDeadline
	c.mu.Unlock()

	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return 0, nil, false, ErrDeadlineExceeded
	}

	var pkt Packet
	var deadlineTimer <-chan time.Time
	if !deadline.IsZero() {
		deadlineTimer = time.After(time.Until(deadline))
	}

	select {
	case pkt = <-c.packetsToRead:
	case <-c.closedChan:
		return 0, nil, false, net.ErrClosed
	case <-c.deadlineUpdated:
		return c.ReadFromWithECN(p)
	case <-deadlineTimer:
		return 0, nil, false, ErrDeadlineExceeded
	}

	n = copy(p, pkt.Data)
	// if the provided buffer is not enough to read the whole packet, we drop
	// the rest of the data. this is similar to what `recvfrom` does on Linux
	// and macOS.
	return n, pkt.From, pkt.ECNMarked, nil
}

func (c *SimConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	deadline := c.writeDeadline
	c.mu.Unlock()

	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return 0, ErrDeadlineExceeded
	}

	c.packetsSent.Add(1)
	c.bytesSent.Add(int64(len(p)))

	pkt := Packet{
		From: c.myAddr,
		To:   addr,
		Data: slices.Clone(p),
	}
	return len(p), c.router.SendPacket(pkt)
}

func (c *SimConn) UnicastAddr() net.Addr {
	return c.myAddr
}

func (c *SimConn) LocalAddr() net.Addr {
	if c.myLocalAddr != nil {
		return c.myLocalAddr
	}
	return c.myAddr
}

func (c *SimConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = t
	c.writeDeadline = t
	select {
	case c.deadlineUpdated <- struct{}{}:
	default:
	}
	return nil
}

func (c *SimConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = t
	select {
	case c.deadlineUpdated <- struct{}{}:
	default:
	}
	return nil
}

func (c *SimConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = t
	return nil
}
