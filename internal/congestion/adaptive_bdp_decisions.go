package congestion

import "time"

// Decision inputs are value snapshots: evaluating policy cannot modify the
// controller, consume an observation, or emit telemetry.
type adaptiveBDPProbeDownInput struct {
	persistentQueue      bool
	recentProbe          bool
	queueGrowthDownshift bool
	lowerBandwidth       bool
}

func decideAdaptiveBDPProbeDown(in adaptiveBDPProbeDownInput) string {
	if in.persistentQueue {
		if in.recentProbe && !in.queueGrowthDownshift {
			return "probe_up_drain"
		}
		if in.queueGrowthDownshift {
			return "queue_growth_capacity_downshift"
		}
		return "queue_delay_persistent"
	}
	if in.lowerBandwidth {
		return "bandwidth_downshift"
	}
	return ""
}

type adaptiveBDPQueueGrowthInput struct {
	previousDelay, delay, elapsed       time.Duration
	pacingRate, minimumRate, activeRate uint64
	downshiftRatio                      float64
}

// A queue growing by dq in dt drains approximately 1-dq/dt of the paced rate.
// Zero means that the observation does not justify lowering the capacity model.
func estimateAdaptiveBDPQueueCapacity(in adaptiveBDPQueueGrowthInput) uint64 {
	if in.elapsed <= 0 || in.delay <= in.previousDelay {
		return 0
	}
	growth := float64(in.delay-in.previousDelay) / float64(in.elapsed)
	if growth < 0.25 {
		return 0
	}
	estimate := uint64(float64(max(uint64(1), in.pacingRate)) * clampFloat(1-growth, 0.10, 0.90))
	estimate = max(estimate, in.minimumRate)
	if in.activeRate == 0 || estimate >= uint64(float64(in.activeRate)*in.downshiftRatio) {
		return 0
	}
	return estimate
}
