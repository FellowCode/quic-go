package simnet

import (
	"cmp"
	"math"
	"math/bits"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// LinkDirection identifies one independently shaped direction of a
// DeterministicLink.
type LinkDirection uint8

const (
	LinkForward LinkDirection = iota
	LinkReverse
)

// LossInterval drops every packet submitted during [Start, End). It is useful
// for deterministic burst-loss and outage scripts.
type LossInterval struct {
	Start time.Duration
	End   time.Duration
}

// GilbertElliottLossConfig models correlated loss. Each submitted packet may
// move between good and bad states; loss probability is selected from the
// resulting state. All probabilities must be in [0, 1].
type GilbertElliottLossConfig struct {
	GoodToBadProbability float64
	BadToGoodProbability float64
	GoodLossProbability  float64
	BadLossProbability   float64
}

// DeterministicDirectionConfig configures a single direction of a
// DeterministicLink. A zero bandwidth is unlimited and a zero queue limit is
// unbounded. RandomLossProbability must be in [0, 1].
type DeterministicDirectionConfig struct {
	BandwidthBitsPerSecond uint64
	BaseLatency            time.Duration
	QueueLimitBytes        uint64
	ECNThresholdBytes      uint64
	RandomLossProbability  float64
	GilbertElliottLoss     *GilbertElliottLossConfig
	LossIntervals          []LossInterval
	ReorderEvery           uint64
	ReorderDelay           time.Duration
	DuplicateEvery         uint64
}

// DeterministicLinkConfig configures both directions and the fixed seed used
// for random loss. The link owns its simulated clock; callers advance it with
// AdvanceTo and never need wall-clock sleeps.
type DeterministicLinkConfig struct {
	Forward DeterministicDirectionConfig
	Reverse DeterministicDirectionConfig
	Seed    uint64
}

// LinkCounters are per-direction simulated-link counters.
type LinkCounters struct {
	SubmittedPackets           uint64
	SubmittedBytes             uint64
	DeliveredPackets           uint64
	DeliveredBytes             uint64
	RandomLosses               uint64
	ScriptedLosses             uint64
	TailDrops                  uint64
	ECNMarks                   uint64
	Duplicates                 uint64
	Reordered                  uint64
	PeakQueueBytes             uint64
	PeakSameTimeSubmittedBytes uint64
}

// DeliveredPacket is emitted by AdvanceTo for every packet that reaches the
// other side of the deterministic link.
type DeliveredPacket struct {
	Direction LinkDirection
	Packet    Packet
	At        time.Duration
	ECNMarked bool
	Duplicate bool
}

type deterministicChange struct {
	at     time.Duration
	config DeterministicDirectionConfig
}

type deterministicDirectionState struct {
	config           DeterministicDirectionConfig
	changes          []deterministicChange
	nextTransmit     time.Duration
	queueBytes       uint64
	packets          uint64
	lossBurstPackets uint64
	gilbertBad       bool
	lastSubmitTime   time.Duration
	sameTimeBytes    uint64
	hasSubmitTime    bool
	counters         LinkCounters
}

type deterministicDelivery struct {
	deliveredAt time.Duration
	departedAt  time.Duration
	order       uint64
	direction   LinkDirection
	packet      Packet
	ecnMarked   bool
	duplicate   bool
	queueBytes  uint64
	dequeued    bool
}

type scheduledPacket struct {
	at        time.Duration
	direction LinkDirection
	packet    Packet
}

type scheduledLossBurst struct {
	at        time.Duration
	direction LinkDirection
	packets   uint64
}

// DeterministicLink is a synchronous, virtual-time bottleneck. It models
// serialization, finite queues, loss, ECN, reordering, duplication and exact
// capacity / latency changes without goroutines or scheduler sleeps.
type DeterministicLink struct {
	mu         sync.Mutex
	now        time.Duration
	directions [2]deterministicDirectionState
	rng        [2]*rand.Rand
	deliveries []deterministicDelivery
	scheduled  []scheduledPacket
	lossBursts []scheduledLossBurst
	nextOrder  uint64
}

// NewDeterministicLink creates a link at simulated time zero.
func NewDeterministicLink(config DeterministicLinkConfig) *DeterministicLink {
	validateDeterministicDirectionConfig(config.Forward)
	validateDeterministicDirectionConfig(config.Reverse)
	return &DeterministicLink{
		directions: [2]deterministicDirectionState{
			{config: cloneDirectionConfig(config.Forward)},
			{config: cloneDirectionConfig(config.Reverse)},
		},
		rng: [2]*rand.Rand{
			rand.New(rand.NewPCG(config.Seed, config.Seed^0x9e3779b97f4a7c15)),
			rand.New(rand.NewPCG(config.Seed^0xd1b54a32d192ed03, config.Seed^0x94d049bb133111eb)),
		},
	}
}

// Now returns the virtual time of the link.
func (l *DeterministicLink) Now() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.now
}

// ScheduleChange changes a direction's shaping configuration at an exact
// virtual time. It panics if asked to rewrite simulated history.
func (l *DeterministicLink) ScheduleChange(direction LinkDirection, at time.Duration, config DeterministicDirectionConfig) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if at < l.now {
		panic("simnet: deterministic link change scheduled in the past")
	}
	validateDeterministicDirectionConfig(config)
	s := l.direction(direction)
	s.changes = append(s.changes, deterministicChange{at: at, config: cloneDirectionConfig(config)})
	slices.SortStableFunc(s.changes, func(a, b deterministicChange) int { return cmp.Compare(a.at, b.at) })
}

// SchedulePacket submits packet at an exact simulated time when AdvanceTo
// reaches it. It is intended for deterministic cross traffic and therefore
// does not require a goroutine or a wall-clock timer.
func (l *DeterministicLink) SchedulePacket(direction LinkDirection, at time.Duration, packet Packet) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if at < l.now {
		panic("simnet: deterministic packet scheduled in the past")
	}
	l.direction(direction) // validate the direction before retaining the packet
	l.scheduled = append(l.scheduled, scheduledPacket{at: at, direction: direction, packet: clonePacket(packet)})
	slices.SortStableFunc(l.scheduled, func(a, b scheduledPacket) int { return cmp.Compare(a.at, b.at) })
}

// ScheduleLossBurst drops exactly packets packets submitted in direction after
// virtual time at. It models a packet-count burst without making the result
// depend on the link-pump tick width.
func (l *DeterministicLink) ScheduleLossBurst(direction LinkDirection, at time.Duration, packets uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if at < l.now {
		panic("simnet: deterministic loss burst scheduled in the past")
	}
	l.direction(direction)
	if packets == 0 {
		return
	}
	l.lossBursts = append(l.lossBursts, scheduledLossBurst{at: at, direction: direction, packets: packets})
	slices.SortStableFunc(l.lossBursts, func(a, b scheduledLossBurst) int { return cmp.Compare(a.at, b.at) })
}

// Send submits a packet at the current virtual time. A false result means the
// packet was dropped by the configured loss script, random loss, or tail drop.
// The second result reports deterministic ECN marking for accepted packets.
func (l *DeterministicLink) Send(direction LinkDirection, packet Packet) (accepted, ecnMarked bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.send(direction, packet)
}

func (l *DeterministicLink) send(direction LinkDirection, packet Packet) (accepted, ecnMarked bool) {
	s := l.direction(direction)
	s.applyChanges(l.now)
	s.counters.SubmittedPackets++
	s.counters.SubmittedBytes += uint64(len(packet.Data))
	if s.hasSubmitTime && s.lastSubmitTime == l.now {
		s.sameTimeBytes += uint64(len(packet.Data))
	} else {
		s.lastSubmitTime = l.now
		s.sameTimeBytes = uint64(len(packet.Data))
		s.hasSubmitTime = true
	}
	s.counters.PeakSameTimeSubmittedBytes = max(s.counters.PeakSameTimeSubmittedBytes, s.sameTimeBytes)
	if s.lossBurstPackets > 0 {
		s.lossBurstPackets--
		s.counters.ScriptedLosses++
		return false, false
	}
	if inLossInterval(s.config.LossIntervals, l.now) {
		s.counters.ScriptedLosses++
		return false, false
	}
	rng := l.rng[direction]
	if s.config.RandomLossProbability > 0 && rng.Float64() < s.config.RandomLossProbability {
		s.counters.RandomLosses++
		return false, false
	}
	if config := s.config.GilbertElliottLoss; config != nil {
		if s.gilbertBad {
			if rng.Float64() < config.BadToGoodProbability {
				s.gilbertBad = false
			}
		} else if rng.Float64() < config.GoodToBadProbability {
			s.gilbertBad = true
		}
		lossProbability := config.GoodLossProbability
		if s.gilbertBad {
			lossProbability = config.BadLossProbability
		}
		if lossProbability > 0 && rng.Float64() < lossProbability {
			s.counters.RandomLosses++
			return false, false
		}
	}

	packetBytes := uint64(len(packet.Data))
	if s.config.QueueLimitBytes > 0 && packetBytes > s.config.QueueLimitBytes-saturatingMin(s.queueBytes, s.config.QueueLimitBytes) {
		s.counters.TailDrops++
		return false, false
	}
	if packetBytes > math.MaxUint64-s.queueBytes {
		s.counters.TailDrops++
		return false, false
	}
	ecnMarked = s.config.ECNThresholdBytes > 0 && packetBytes >= s.config.ECNThresholdBytes-saturatingMin(s.queueBytes, s.config.ECNThresholdBytes)
	if ecnMarked {
		s.counters.ECNMarks++
		packet.ECNBits = 3
	}

	s.packets++
	start := max(l.now, s.nextTransmit)
	finish := saturatingAdd(start, serializationDelay(packetBytes, s.config.BandwidthBitsPerSecond))
	s.nextTransmit = finish
	s.queueBytes += packetBytes
	s.counters.PeakQueueBytes = max(s.counters.PeakQueueBytes, s.queueBytes)
	deliveryAt := saturatingAdd(finish, s.config.BaseLatency)
	reordered := s.config.ReorderEvery > 0 && s.packets%s.config.ReorderEvery == 0
	if reordered {
		deliveryAt = saturatingAdd(deliveryAt, s.config.ReorderDelay)
		s.counters.Reordered++
	}
	l.addDelivery(deterministicDelivery{
		deliveredAt: deliveryAt,
		departedAt:  finish,
		direction:   direction,
		packet:      clonePacket(packet),
		ecnMarked:   ecnMarked,
		queueBytes:  packetBytes,
	})
	if s.config.DuplicateEvery > 0 && s.packets%s.config.DuplicateEvery == 0 {
		s.counters.Duplicates++
		l.addDelivery(deterministicDelivery{
			deliveredAt: deliveryAt,
			departedAt:  finish,
			direction:   direction,
			packet:      clonePacket(packet),
			ecnMarked:   ecnMarked,
			duplicate:   true,
		})
	}
	return true, ecnMarked
}

// AdvanceTo advances virtual time and returns packets delivered during the
// interval. Calling it with an earlier time panics, because a deterministic
// simulation cannot retract already delivered packets.
func (l *DeterministicLink) AdvanceTo(at time.Duration) []DeliveredPacket {
	l.mu.Lock()
	defer l.mu.Unlock()
	if at < l.now {
		panic("simnet: deterministic link advanced backwards")
	}
	for len(l.scheduled) > 0 && l.scheduled[0].at <= at {
		event := l.scheduled[0]
		l.scheduled = l.scheduled[1:]
		l.advanceQueueDepartures(event.at)
		l.now = event.at
		for i := range l.directions {
			l.directions[i].applyChanges(l.now)
		}
		l.send(event.direction, event.packet)
	}
	for len(l.lossBursts) > 0 && l.lossBursts[0].at <= at {
		burst := l.lossBursts[0]
		l.lossBursts = l.lossBursts[1:]
		l.direction(burst.direction).lossBurstPackets += burst.packets
	}
	l.now = at
	for i := range l.directions {
		l.directions[i].applyChanges(l.now)
	}
	l.advanceQueueDepartures(at)
	n := 0
	for n < len(l.deliveries) && l.deliveries[n].deliveredAt <= at {
		n++
	}
	ready := l.deliveries[:n]
	l.deliveries = l.deliveries[n:]
	result := make([]DeliveredPacket, 0, len(ready))
	for _, d := range ready {
		s := l.direction(d.direction)
		s.counters.DeliveredPackets++
		s.counters.DeliveredBytes += uint64(len(d.packet.Data))
		result = append(result, DeliveredPacket{Direction: d.direction, Packet: d.packet, At: d.deliveredAt, ECNMarked: d.ecnMarked, Duplicate: d.duplicate})
	}
	return result
}

func (l *DeterministicLink) advanceQueueDepartures(at time.Duration) {
	for i := range l.deliveries {
		d := &l.deliveries[i]
		if d.dequeued || d.queueBytes == 0 || d.departedAt > at {
			continue
		}
		s := l.direction(d.direction)
		if d.queueBytes > s.queueBytes {
			panic("simnet: deterministic link queue accounting underflow")
		}
		s.queueBytes -= d.queueBytes
		d.dequeued = true
	}
}

// QueueBytes returns the current per-direction occupancy, including packets
// waiting for or currently undergoing bottleneck serialization. Propagating
// packets that already left the bottleneck are not part of queue occupancy.
func (l *DeterministicLink) QueueBytes(direction LinkDirection) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.direction(direction).queueBytes
}

// QueueDelay returns the serialization delay represented by the current
// per-direction queue occupancy at the current shaping rate.
func (l *DeterministicLink) QueueDelay(direction LinkDirection) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.direction(direction)
	return serializationDelay(s.queueBytes, s.config.BandwidthBitsPerSecond)
}

// Counters returns a copy of the current per-direction counters.
func (l *DeterministicLink) Counters(direction LinkDirection) LinkCounters {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.direction(direction).counters
}

// ResetBurstPeak starts a new same-virtual-timestamp burst measurement for a
// direction without changing packet, byte, loss, or queue counters.
func (l *DeterministicLink) ResetBurstPeak(direction LinkDirection) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.direction(direction)
	s.counters.PeakSameTimeSubmittedBytes = 0
	s.sameTimeBytes = 0
	s.hasSubmitTime = false
}

func (l *DeterministicLink) addDelivery(d deterministicDelivery) {
	l.nextOrder++
	d.order = l.nextOrder
	l.deliveries = append(l.deliveries, d)
	slices.SortFunc(l.deliveries, func(a, b deterministicDelivery) int {
		if c := cmp.Compare(a.deliveredAt, b.deliveredAt); c != 0 {
			return c
		}
		return cmp.Compare(a.order, b.order)
	})
}

func (l *DeterministicLink) direction(direction LinkDirection) *deterministicDirectionState {
	if direction > LinkReverse {
		panic("simnet: invalid deterministic link direction")
	}
	return &l.directions[direction]
}

func (s *deterministicDirectionState) applyChanges(at time.Duration) {
	for len(s.changes) > 0 && s.changes[0].at <= at {
		s.config = s.changes[0].config
		s.changes = s.changes[1:]
	}
}

func cloneDirectionConfig(config DeterministicDirectionConfig) DeterministicDirectionConfig {
	config.LossIntervals = slices.Clone(config.LossIntervals)
	if config.GilbertElliottLoss != nil {
		ge := *config.GilbertElliottLoss
		config.GilbertElliottLoss = &ge
	}
	return config
}

func validateDeterministicDirectionConfig(config DeterministicDirectionConfig) {
	if config.BaseLatency < 0 || config.ReorderDelay < 0 {
		panic("simnet: deterministic link latency must not be negative")
	}
	if math.IsNaN(config.RandomLossProbability) || config.RandomLossProbability < 0 || config.RandomLossProbability > 1 {
		panic("simnet: deterministic link random loss probability must be in [0, 1]")
	}
	if ge := config.GilbertElliottLoss; ge != nil {
		for _, probability := range []float64{ge.GoodToBadProbability, ge.BadToGoodProbability, ge.GoodLossProbability, ge.BadLossProbability} {
			if math.IsNaN(probability) || probability < 0 || probability > 1 {
				panic("simnet: Gilbert-Elliott probability must be in [0, 1]")
			}
		}
	}
	for _, interval := range config.LossIntervals {
		if interval.Start < 0 || interval.End < interval.Start {
			panic("simnet: deterministic link loss interval is invalid")
		}
	}
}

func clonePacket(packet Packet) Packet {
	packet.Data = slices.Clone(packet.Data)
	return packet
}

func inLossInterval(intervals []LossInterval, now time.Duration) bool {
	for _, interval := range intervals {
		if interval.Start <= now && now < interval.End {
			return true
		}
	}
	return false
}

func serializationDelay(packetBytes, bandwidthBitsPerSecond uint64) time.Duration {
	if packetBytes == 0 || bandwidthBitsPerSecond == 0 {
		return 0
	}
	if packetBytes > math.MaxUint64/8 {
		return time.Duration(math.MaxInt64)
	}
	packetBits := packetBytes * 8
	seconds := packetBits / bandwidthBitsPerSecond
	if seconds > uint64(math.MaxInt64)/uint64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	remainder := packetBits % bandwidthBitsPerSecond
	hi, lo := bits.Mul64(remainder, uint64(time.Second))
	nanoseconds, rem := bits.Div64(hi, lo, bandwidthBitsPerSecond)
	if rem != 0 {
		nanoseconds++
	}
	base := seconds * uint64(time.Second)
	if nanoseconds > uint64(math.MaxInt64)-base {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(base + nanoseconds)
}

func saturatingAdd(a, b time.Duration) time.Duration {
	if b > 0 && a > time.Duration(math.MaxInt64)-b {
		return time.Duration(math.MaxInt64)
	}
	return a + b
}

func saturatingMin(a, limit uint64) uint64 {
	if a > limit {
		return limit
	}
	return a
}
