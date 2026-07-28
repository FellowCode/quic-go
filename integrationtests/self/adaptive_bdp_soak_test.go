package self_test

import (
	"math"
	"os"
	"runtime"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/quic-go/quic-go/testutils/simnet"

	"github.com/stretchr/testify/require"
)

const (
	defaultAdaptiveBDPSoakCycles = 2
	maxAdaptiveBDPSoakQueueBytes = 1024 * 1024
)

func TestAdaptiveBDPSoakRepeatedTransitions(t *testing.T) {
	cycles := defaultAdaptiveBDPSoakCycles
	if value := os.Getenv("QUIC_GO_ADAPTIVE_BDP_SOAK_CYCLES"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		require.Positive(t, parsed)
		cycles = parsed
	}

	// Warm caches and pools before taking the resource baseline.
	runAdaptiveBDPTransitionSoakCycle(t, -1)
	synctest.Test(t, runAdaptiveBDPDeterministicMigration)
	before := adaptiveBDPResourceSnapshotAfterGC()

	var last adaptiveBDPLinkScenarioResult
	for cycle := range cycles {
		t.Logf("AdaptiveBDP soak cycle %d/%d", cycle+1, cycles)
		last = runAdaptiveBDPTransitionSoakCycle(t, cycle)
		synctest.Test(t, runAdaptiveBDPDeterministicMigration)
	}

	after := adaptiveBDPResourceSnapshotAfterGC()
	t.Logf(
		"AdaptiveBDP soak resources: cycles=%d goroutines=%d->%d heap_alloc=%d->%d heap_objects=%d->%d",
		cycles,
		before.goroutines,
		after.goroutines,
		before.heapAlloc,
		after.heapAlloc,
		before.heapObjects,
		after.heapObjects,
	)
	t.Logf(
		"AdaptiveBDP soak final network sample: elapsed=%s pacing_Bps=%d bandwidth_Bps=%d max_bandwidth_Bps=%d peak_queue_bytes=%d random_losses=%d scripted_losses=%d",
		last.elapsed,
		last.info.PacingRateBytesPerSecond,
		last.info.BandwidthBytesPerSecond,
		last.info.MaxBandwidthBytesPerSecond,
		last.forward.PeakQueueBytes,
		last.forward.RandomLosses,
		last.forward.ScriptedLosses,
	)

	// These bounds allow normal runtime and crypto/TLS cache noise, while
	// detecting connection-proportional retention across the repeated cycle.
	require.LessOrEqual(t, after.goroutines, before.goroutines+8, "goroutines must not grow with repeated connections")
	require.LessOrEqual(t, after.heapAlloc, before.heapAlloc+32*1024*1024, "live heap must remain bounded across repeated connections")
	require.LessOrEqual(t, after.heapObjects, before.heapObjects+50_000, "live heap objects must remain bounded across repeated connections")
}

func runAdaptiveBDPTransitionSoakCycle(t *testing.T, cycle int) adaptiveBDPLinkScenarioResult {
	t.Helper()
	var result adaptiveBDPLinkScenarioResult
	synctest.Test(t, func(t *testing.T) {
		base := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 30_000_000,
			BaseLatency:            25 * time.Millisecond,
			QueueLimitBytes:        512 * 1024,
		}
		high := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 100_000_000,
			BaseLatency:            10 * time.Millisecond,
			QueueLimitBytes:        maxAdaptiveBDPSoakQueueBytes,
		}
		low := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 10_000_000,
			BaseLatency:            100 * time.Millisecond,
			QueueLimitBytes:        256 * 1024,
		}
		lossyLow := low
		lossyLow.RandomLossProbability = 0.01
		severeLoss := low
		severeLoss.RandomLossProbability = 0.05

		result = runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig: simnet.DeterministicLinkConfig{
				Forward: base,
				Reverse: base,
				Seed:    uint64(15_000 + max(cycle, 0)),
			},
			startupTargetRateBps: 30_000_000,
			payloadBytes:         4 * 1024 * 1024,
			initialWriteBytes:    256 * 1024,
			pacedWriteUntil:      4500 * time.Millisecond,
			pacedWriteInterval:   25 * time.Millisecond,
			pacedWriteBytes:      16 * 1024,
			pacedWritePauses: []adaptiveBDPWritePause{
				{after: 1150 * time.Millisecond, duration: 450 * time.Millisecond},
				{after: 2850 * time.Millisecond, duration: 550 * time.Millisecond},
			},
			minRTTFilterWindow: 500 * time.Millisecond,
			timeout:            15 * time.Second,
			configureLink: func(link *simnet.DeterministicLink) {
				scheduleBothDirections := func(at time.Duration, config simnet.DeterministicDirectionConfig) {
					link.ScheduleChange(simnet.LinkForward, at, config)
					link.ScheduleChange(simnet.LinkReverse, at, config)
				}
				scheduleBothDirections(400*time.Millisecond, high)
				scheduleBothDirections(900*time.Millisecond, lossyLow)
				scheduleBothDirections(1400*time.Millisecond, high)
				scheduleBothDirections(2*time.Second, severeLoss)
				scheduleBothDirections(2300*time.Millisecond, base)
				scheduleBothDirections(3200*time.Millisecond, low)
				scheduleBothDirections(3700*time.Millisecond, high)
				scheduleBothDirections(4100*time.Millisecond, low)
				link.ScheduleLossBurst(simnet.LinkForward, 650*time.Millisecond, 10)
				link.ScheduleLossBurst(simnet.LinkForward, 2650*time.Millisecond, 10)
			},
		})
		assertAdaptiveBDPSoakInvariants(t, result)
	})
	return result
}

func assertAdaptiveBDPSoakInvariants(t *testing.T, result adaptiveBDPLinkScenarioResult) {
	t.Helper()
	info := result.info

	require.Greater(t, info.PacingRateBytesPerSecond, uint64(0))
	require.GreaterOrEqual(t, info.CongestionWindow, info.MinCwnd)
	require.LessOrEqual(t, info.CongestionWindow, info.MaxCwnd)
	require.Greater(t, info.MaxTrackedSentPackets, info.MaxOutstandingSentPackets, "packet-history limits must not invert")
	require.LessOrEqual(t, info.TrackedSentPackets, info.MaxTrackedSentPackets)
	require.LessOrEqual(t, result.forward.PeakQueueBytes, uint64(maxAdaptiveBDPSoakQueueBytes), "queue growth must remain bounded")
	require.Greater(t, result.forward.RandomLosses+result.forward.ScriptedLosses, uint64(0), "soak must exercise loss transitions")
	require.Equal(t, uint64(20), result.forward.ScriptedLosses, "soak must exercise both exact burst-loss transitions")
	require.Less(t, info.PacingRateBytesPerSecond, uint64(30_000_000/8), "final low-capacity epoch must not retain the stale 100 Mbit/s pacing model")
	require.Less(t, info.BandwidthBytesPerSecond, uint64(20_000_000/8), "final low-capacity epoch must age stale bandwidth")

	var probeRTTRetryNotBefore time.Duration
	roundActions := make(map[uint64]map[string]struct{})
	for _, sample := range info.Telemetry {
		require.Greater(t, sample.PacingRateBytesPerSecond, uint64(0))
		require.GreaterOrEqual(t, sample.CongestionWindow, info.MinCwnd)
		require.LessOrEqual(t, sample.CongestionWindow, info.MaxCwnd)
		require.False(t, math.IsNaN(sample.PacingGain) || math.IsInf(sample.PacingGain, 0))
		require.False(t, math.IsNaN(sample.CwndGain) || math.IsInf(sample.CwndGain, 0))

		if sample.Event != "state_transition" {
			continue
		}
		if sample.TransitionReason == "probe_rtt_timeout_insufficient_drain_evidence" {
			probeRTTRetryNotBefore = sample.Elapsed + 2*time.Second
		}
		if sample.State == "ProbeRTT" && probeRTTRetryNotBefore > 0 {
			require.GreaterOrEqual(t, sample.Elapsed, probeRTTRetryNotBefore, "inconclusive ProbeRTT retries must honor backoff")
			probeRTTRetryNotBefore = 0
		}
		if !isAdaptiveBDPRoundGatedTransition(sample.TransitionReason) {
			continue
		}
		if roundActions[sample.RoundCount] == nil {
			roundActions[sample.RoundCount] = make(map[string]struct{})
		}
		_, duplicate := roundActions[sample.RoundCount][sample.TransitionReason]
		require.Falsef(t, duplicate, "round-gated action %q repeated in round %d", sample.TransitionReason, sample.RoundCount)
		roundActions[sample.RoundCount][sample.TransitionReason] = struct{}{}
	}
}

func isAdaptiveBDPRoundGatedTransition(reason string) bool {
	switch reason {
	case "ecn_congestion",
		"proportional_loss_with_queue",
		"emergency_loss_proportional",
		"loss_free_recovery_probe",
		"queue_growth_capacity_downshift",
		"bandwidth_downshift",
		"short_bw_downshift_with_congestion_evidence":
		return true
	default:
		return false
	}
}

type adaptiveBDPResourceSnapshot struct {
	goroutines  int
	heapAlloc   uint64
	heapObjects uint64
}

func adaptiveBDPResourceSnapshotAfterGC() adaptiveBDPResourceSnapshot {
	runtime.GC()
	runtime.Gosched()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return adaptiveBDPResourceSnapshot{
		goroutines:  runtime.NumGoroutine(),
		heapAlloc:   stats.HeapAlloc,
		heapObjects: stats.HeapObjects,
	}
}
