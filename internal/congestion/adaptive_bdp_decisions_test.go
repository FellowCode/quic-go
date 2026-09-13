package congestion

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPProbeDownDecisionPriority(t *testing.T) {
	for _, test := range []struct {
		name   string
		in     adaptiveBDPProbeDownInput
		reason string
	}{
		{"no evidence", adaptiveBDPProbeDownInput{}, ""},
		{"low service rate", adaptiveBDPProbeDownInput{lowerBandwidth: true}, "bandwidth_downshift"},
		{"persistent queue overrides rate", adaptiveBDPProbeDownInput{persistentQueue: true, lowerBandwidth: true}, "queue_delay_persistent"},
		{"probe drain grace", adaptiveBDPProbeDownInput{persistentQueue: true, recentProbe: true}, "probe_up_drain"},
		{"capacity drop overrides probe grace", adaptiveBDPProbeDownInput{persistentQueue: true, recentProbe: true, queueGrowthDownshift: true}, "queue_growth_capacity_downshift"},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.reason, decideAdaptiveBDPProbeDown(test.in))
		})
	}
}

func TestAdaptiveBDPQueueEstimateBounds(t *testing.T) {
	base := adaptiveBDPQueueGrowthInput{previousDelay: 10 * time.Millisecond, delay: 60 * time.Millisecond,
		elapsed: 100 * time.Millisecond, pacingRate: 1_000_000, minimumRate: 10_000, activeRate: 1_000_000, downshiftRatio: 0.8}
	require.Equal(t, uint64(500_000), estimateAdaptiveBDPQueueCapacity(base))
	for _, elapsed := range []time.Duration{0, -time.Second} {
		in := base
		in.elapsed = elapsed
		require.Zero(t, estimateAdaptiveBDPQueueCapacity(in), "non-forward time cannot justify downshift")
	}
	in := base
	in.delay = in.previousDelay
	require.Zero(t, estimateAdaptiveBDPQueueCapacity(in), "standing queue is not a service rate drop")
	in = base
	in.activeRate = 500_000
	require.Zero(t, estimateAdaptiveBDPQueueCapacity(in), "do not lower capacity without a lower estimate")
	in = base
	in.delay = time.Second
	in.minimumRate = 200_000
	require.Equal(t, uint64(200_000), estimateAdaptiveBDPQueueCapacity(in), "preserve observable-rate floor")
}
