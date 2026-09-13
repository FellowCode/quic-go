package self_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPFairnessMeasurementExcludesWarmupAndSoloTail(t *testing.T) {
	// Unequal warmup and solo tails must not bias an equal shared interval.
	first := []adaptiveBDPDeliverySample{{at: time.Second, bytes: 10_000}, {at: 2 * time.Second, bytes: 12_000}, {at: 3 * time.Second, bytes: 14_000}, {at: 4 * time.Second, bytes: 100_000}}
	second := []adaptiveBDPDeliverySample{{at: time.Second, bytes: 100}, {at: 1500 * time.Millisecond, bytes: 1100}, {at: 3 * time.Second, bytes: 4100}, {at: 10 * time.Second, bytes: 100_000}}
	rates := []float64{applicationRateDuring(first, time.Second, 3*time.Second), applicationRateDuring(second, time.Second, 3*time.Second)}
	require.Equal(t, []float64{16_000, 16_000}, rates)
	require.Equal(t, 1.0, jainFairness(rates))
	require.Zero(t, applicationRateDuring(first, time.Second, time.Second))
	require.Zero(t, applicationRateDuring(nil, time.Second, 3*time.Second))
	require.Less(t, jainFairness([]float64{21.55, 7.89}), 0.90, "the reported unfair split must fail the acceptance threshold")
}
