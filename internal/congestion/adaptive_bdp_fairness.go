package congestion

import (
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

func (s *adaptiveBDPSender) controlQueueDelay() time.Duration {
	return max(time.Duration(0), s.queueDelay()-s.sharedQueueDelay)
}

func (s *adaptiveBDPSender) resetSharedQueue() {
	s.sharedQueueDelay = 0
	s.sharedQueueDrainStart = 0
}

// A queue that survives several drain RTTs can be maintained by other flows.
// Keep its delay separate from propagation RTT and budget the corresponding
// inflight bytes, instead of repeatedly shrinking the local flow's share.
func (s *adaptiveBDPSender) updateSharedQueue(sample RateSample, priorInFlight protocol.ByteCount, now monotime.Time) {
	queue := s.queueDelay()
	s.sharedQueueDelay = min(s.sharedQueueDelay, queue)
	if s.hasFreshMaterialLoss() || s.hasRecentECNCE() {
		s.resetSharedQueue()
		return
	}
	if queue <= s.queueTarget()/2 {
		s.sharedQueueDrainStart = 0
		return
	}
	if s.state == adaptiveBDPProbeDown && s.sharedQueueDrainStart.IsZero() {
		s.sharedQueueDrainStart = now
	}
	if s.state != adaptiveBDPProbeDown || !s.roundStart || !sample.IsValid || sample.AppLimited ||
		queue <= s.queueTarget() || priorInFlight > s.congestionWindow+2*s.maxDatagramSize {
		return
	}
	if s.sharedQueueDrainStart.IsZero() || now.Sub(s.sharedQueueDrainStart) < max(200*time.Millisecond, 2*s.rttStats.SmoothedRTT()) {
		return
	}
	if !s.hasQueueGrowthSample || queue > s.lastQueueGrowthDelay+s.queueTarget()/8 {
		return
	}
	s.sharedQueueDelay = min(queue, 150*time.Millisecond)
	s.sharedQueueDrainStart = now
	s.enterStateWithReason(adaptiveBDPProbeBW, now, "shared_queue_after_drain")
}
