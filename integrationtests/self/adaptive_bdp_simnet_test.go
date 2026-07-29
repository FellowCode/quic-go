package self_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/testutils/simnet"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveBDPDeterministicLinkBulkTransfer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig: simnet.DeterministicLinkConfig{
				Forward: simnet.DeterministicDirectionConfig{
					BandwidthBitsPerSecond: 10_000_000,
					BaseLatency:            5 * time.Millisecond,
					QueueLimitBytes:        128 * 1024,
				},
				Reverse: simnet.DeterministicDirectionConfig{
					BandwidthBitsPerSecond: 10_000_000,
					BaseLatency:            5 * time.Millisecond,
					QueueLimitBytes:        128 * 1024,
				},
			},
			startupTargetRateBps: 10_000_000,
			payloadBytes:         96 * 1024,
		})
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0), "AdaptiveBDP must not enter a zero-rate no-send state")
		require.Greater(t, result.info.CongestionWindow, uint64(0))
		require.Greater(t, result.forward.DeliveredBytes, uint64(0))
	})
}

func TestAdaptiveBDPDeterministicLinkWriteWithLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 10_000_000,
			BaseLatency:            25 * time.Millisecond,
			QueueLimitBytes:        256 * 1024,
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
			startupTargetRateBps: 10_000_000,
			payloadBytes:         2 * 1024 * 1024,
			initialWriteBytes:    256 * 1024,
			pacedWriteUntil:      1200 * time.Millisecond,
			pacedWriteInterval:   25 * time.Millisecond,
			pacedWriteBytes:      16 * 1024,
			pacedWriteWithLimit:  true,
			timeout:              6 * time.Second,
		})
		require.Zero(t, result.forward.TailDrops, "higher-level flow control must not create synthetic congestion")
		require.False(t, result.info.HasCongestionEvidence)
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
		require.GreaterOrEqual(t, result.info.CongestionWindow, result.info.MinCwnd)
		require.LessOrEqual(t, result.info.CongestionWindow, result.info.MaxCwnd)
		require.NotContains(t, result.info.LastBWChangeReason, "downshift")
	})
}

func TestAdaptiveBDPDeterministicLinkCleanPaths(t *testing.T) {
	// C01-C06 are real QUIC upload runs over a finite, one-BDP bottleneck.
	// The invariant is intentionally independent of host scheduling: every
	// completed transfer has non-zero goodput and the controller remains inside
	// its configured cwnd and pacing bounds.
	for _, tc := range []struct {
		id       string
		capacity uint64
		rtt      time.Duration
		queueBDP uint64
		maxCwnd  uint32
		payload  int
		window   time.Duration
		timeout  time.Duration
	}{
		{"C01", 1_000_000, 20 * time.Millisecond, 1, 0, 2 * 1024 * 1024, 500 * time.Millisecond, 30 * time.Second},
		{"C02", 10_000_000, 50 * time.Millisecond, 1, 0, 4 * 1024 * 1024, 200 * time.Millisecond, 6 * time.Second},
		{"C03", 30_000_000, 150 * time.Millisecond, 1, 0, 8 * 1024 * 1024, 300 * time.Millisecond, 6 * time.Second},
		{"C04", 100_000_000, 20 * time.Millisecond, 2, 0, 16 * 1024 * 1024, 100 * time.Millisecond, 6 * time.Second},
		{"C05", 100_000_000, 200 * time.Millisecond, 1, 0, 32 * 1024 * 1024, 300 * time.Millisecond, 8 * time.Second},
		// C06 is also run with the default 10,000-packet cap, as required by
		// the validation plan. The enlarged cap demonstrates the high-BDP path.
		{"C06-default", 1_000_000_000, 100 * time.Millisecond, 1, 0, 64 * 1024 * 1024, 100 * time.Millisecond, 8 * time.Second},
		{"C06-large-cwnd", 1_000_000_000, 100 * time.Millisecond, 1, 100_000, 64 * 1024 * 1024, 100 * time.Millisecond, 8 * time.Second},
	} {
		t.Run(tc.id, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queueBytes := tc.queueBDP * tc.capacity * uint64(tc.rtt) / uint64(time.Second) / 8
				const validationPacketBytes = uint64(1280)
				queueBytes = (queueBytes + validationPacketBytes - 1) / validationPacketBytes * validationPacketBytes
				config := simnet.DeterministicDirectionConfig{
					BandwidthBitsPerSecond: tc.capacity,
					BaseLatency:            tc.rtt / 2,
					QueueLimitBytes:        queueBytes,
				}
				result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
					linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
					startupTargetRateBps: tc.capacity,
					payloadBytes:         tc.payload,
					maxWindowPackets:     tc.maxCwnd,
					timeout:              tc.timeout,
					initialWindowPackets: func() uint32 {
						if tc.id == "C01" {
							return 2
						}
						return 0
					}(),
					minWindowPackets: func() uint32 {
						if tc.id == "C01" {
							return 2
						}
						return 0
					}(),
					queueTarget: func() time.Duration {
						if tc.id == "C01" {
							return 20 * time.Millisecond
						}
						return 0
					}(),
					noCongestionRateFloorFraction: func() float64 {
						if tc.id == "C01" {
							return 0.90
						}
						return 0
					}(),
				})
				require.Greater(t, result.goodputBitsPerSecond(), uint64(0), "clean path must deliver application data")
				require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0), "clean path must not deadlock pacing")
				require.GreaterOrEqual(t, result.info.CongestionWindow, result.info.MinCwnd)
				require.LessOrEqual(t, result.info.CongestionWindow, result.info.MaxCwnd)
				recoveredAt, ok := result.firstApplicationGoodputAtOrAbove(tc.rtt, result.elapsed, tc.window, tc.capacity*90/100)
				require.True(t, ok, "%s must reach 90%% application utilization after convergence", tc.id)
				require.GreaterOrEqual(t, result.medianApplicationGoodput(recoveredAt-tc.window, tc.window), tc.capacity*90/100, "%s median application utilization after convergence must be at least 90%%", tc.id)
				queueLimit := max(2*result.info.QueueTarget, 50*time.Millisecond)
				require.LessOrEqual(t, result.queueDelayPercentileAfter(95, recoveredAt+3*tc.rtt), queueLimit, "%s post-convergence p95 queue delay must remain bounded", tc.id)
				var startupTransitions, probeDownTransitions int
				for _, sample := range result.info.Telemetry {
					if sample.Event != "state_transition" || sample.Elapsed < recoveredAt-tc.window {
						continue
					}
					switch sample.State {
					case "Startup":
						startupTransitions++
					case "ProbeDown":
						if sample.TransitionReason != "probe_up_drain" {
							probeDownTransitions++
						}
					}
				}
				require.Zero(t, startupTransitions, "%s must not re-enter Startup after convergence", tc.id)
				require.LessOrEqual(t, probeDownTransitions, 2, "%s must not exhibit recurring ProbeDown oscillation", tc.id)
			})
		})
	}
}

func TestAdaptiveBDPDeterministicLinkWirelessLoss(t *testing.T) {
	for _, tc := range []struct {
		id       string
		capacity uint64
		rtt      time.Duration
		loss     float64
		seed     uint64
	}{
		{"L01", 30_000_000, 50 * time.Millisecond, .001, 1},
		{"L02", 30_000_000, 100 * time.Millisecond, .01, 12},
		{"L03", 30_000_000, 150 * time.Millisecond, .02, 13},
	} {
		t.Run(tc.id, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config := simnet.DeterministicDirectionConfig{
					BandwidthBitsPerSecond: tc.capacity,
					BaseLatency:            tc.rtt / 2,
					// No finite bottleneck queue: loss is independent wireless loss,
					// not a synthetic congestion signal.
					RandomLossProbability: tc.loss,
				}
				result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
					linkConfig: simnet.DeterministicLinkConfig{
						Forward: config,
						Reverse: config,
						Seed:    tc.seed,
					},
					startupTargetRateBps: tc.capacity,
					payloadBytes:         1024 * 1024,
					timeout:              6 * time.Second,
				})
				require.Greater(t, result.forward.RandomLosses, uint64(0), "fixed seed must exercise the loss path")
				require.Zero(t, result.forward.TailDrops, "wireless-loss scenario must not synthesize queue pressure")
				require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
				require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0), "loss must not deadlock the sender")
			})
		})
	}
}

func TestAdaptiveBDPDeterministicLinkL04BurstLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Drop exactly ten consecutive data packets at a virtual timestamp,
		// without coupling the outcome to the host scheduler or pump tick.
		config := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 100_000_000,
			BaseLatency:            20 * time.Millisecond,
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
			startupTargetRateBps: 100_000_000,
			payloadBytes:         2 * 1024 * 1024,
			timeout:              6 * time.Second,
			configureLink: func(link *simnet.DeterministicLink) {
				link.ScheduleLossBurst(simnet.LinkForward, 200*time.Millisecond, 10)
			},
		})
		require.Equal(t, uint64(10), result.forward.ScriptedLosses, "L04 must exercise an exact ten-packet loss burst")
		require.Zero(t, result.forward.TailDrops, "L04 must remain a no-queue loss scenario")
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0), "burst recovery must not deadlock pacing")
	})
}

func TestAdaptiveBDPDeterministicLinkL05GilbertElliottLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// L05 uses a fixed-seed correlated-loss path rather than a queue or
		// an independent random-loss approximation.
		config := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 10_000_000,
			BaseLatency:            100 * time.Millisecond,
			GilbertElliottLoss: &simnet.GilbertElliottLossConfig{
				GoodToBadProbability: .02,
				BadToGoodProbability: .20,
				GoodLossProbability:  .001,
				BadLossProbability:   .80,
			},
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig: simnet.DeterministicLinkConfig{
				Forward: config,
				Reverse: config,
				Seed:    505,
			},
			startupTargetRateBps: 10_000_000,
			payloadBytes:         1024 * 1024,
			timeout:              6 * time.Second,
		})
		require.Greater(t, result.forward.RandomLosses, uint64(0), "L05 must exercise correlated wireless loss")
		require.Zero(t, result.forward.TailDrops, "L05 must not manufacture queue pressure")
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0), "correlated loss must not deadlock pacing")
	})
}

func TestAdaptiveBDPDeterministicLinkNoQueueLossReactionIsSmallerThanQueuedLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 30_000_000,
			BaseLatency:            25 * time.Millisecond,
		}
		wireless := base
		wireless.RandomLossProbability = 0.01
		noQueue := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: wireless, Reverse: wireless, Seed: 12},
			startupTargetRateBps: 30_000_000,
			payloadBytes:         4 * 1024 * 1024,
			timeout:              8 * time.Second,
		})

		queuedConfig := base
		queuedConfig.QueueLimitBytes = 46_875 // 0.25 BDP
		queued := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: queuedConfig, Reverse: queuedConfig},
			startupTargetRateBps: 100_000_000,
			payloadBytes:         4 * 1024 * 1024,
			timeout:              8 * time.Second,
		})

		noQueueCut, ok := minimumLossCwndMultiplier(noQueue.info.Telemetry)
		require.True(t, ok, "wireless loss must produce a measured controller reaction")
		queuedCut, ok := minimumLossCwndMultiplier(queued.info.Telemetry)
		require.True(t, ok, "queued tail loss must produce a measured controller reaction")
		require.Greater(t, noQueueCut, queuedCut, "random no-queue loss must retain more cwnd than queued loss")
		require.Zero(t, noQueue.forward.TailDrops)
		require.Greater(t, queued.forward.TailDrops, uint64(0))
	})
}

func minimumLossCwndMultiplier(samples []quic.AdaptiveBDPTelemetrySample) (float64, bool) {
	minimum := 1.0
	found := false
	for _, sample := range samples {
		if sample.LastLossCwndMultiplier <= 0 || sample.LastLossCwndMultiplier >= 1 {
			continue
		}
		minimum = min(minimum, sample.LastLossCwndMultiplier)
		found = true
	}
	return minimum, found
}

func TestAdaptiveBDPDeterministicLinkIdleDownloadToUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clientAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 11), Port: 9011}
		serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 12), Port: 9012}
		linkConfig := simnet.DeterministicLinkConfig{
			Forward: simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 10_000_000, BaseLatency: 10 * time.Millisecond, QueueLimitBytes: 128 * 1024},
			Reverse: simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 10_000_000, BaseLatency: 10 * time.Millisecond, QueueLimitBytes: 128 * 1024},
		}
		link := simnet.NewDeterministicLink(linkConfig)
		router := simnet.NewDeterministicRouter(link, func(packet simnet.Packet) simnet.LinkDirection {
			if packet.From.String() == clientAddr.String() {
				return simnet.LinkForward
			}
			return simnet.LinkReverse
		})
		clientPacketConn := simnet.NewBufferedSimConn(clientAddr, router, 4096)
		serverPacketConn := simnet.NewBufferedSimConn(serverAddr, router, 4096)
		defer clientPacketConn.Close()
		defer serverPacketConn.Close()
		stopPump, _ := startDeterministicLinkPump(router)
		pumpStopped := false
		defer func() {
			if !pumpStopped {
				stopPump()
			}
		}()

		config := getQuicConfig(&quic.Config{CwndTuning: quic.CwndTuning{
			Enable:               true,
			Algorithm:            quic.CongestionControlAdaptiveBDP,
			StartupTargetRateBps: 10_000_000,
		}})
		ln, err := quic.Listen(serverPacketConn, getTLSConfig(), config)
		require.NoError(t, err)
		defer ln.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		clientConn, err := quic.Dial(ctx, clientPacketConn, serverAddr, getTLSClientConfig(), config)
		require.NoError(t, err)
		defer clientConn.CloseWithError(0, "")
		serverConn, err := ln.Accept(ctx)
		require.NoError(t, err)
		defer serverConn.CloseWithError(0, "")

		download := bytes.Repeat([]byte("d"), 32*1024)
		downloadDone := make(chan error, 1)
		go func() {
			stream, err := serverConn.AcceptStream(ctx)
			if err == nil {
				_, err = stream.Write(download)
			}
			if err == nil {
				err = stream.Close()
			}
			downloadDone <- err
		}()
		downloadStream, err := clientConn.OpenStreamSync(ctx)
		require.NoError(t, err)
		require.NoError(t, downloadStream.Close())
		received, err := io.ReadAll(downloadStream)
		require.NoError(t, err)
		require.Equal(t, download, received)
		require.NoError(t, <-downloadDone)

		// This is a synctest virtual-time idle period, not a wall-clock sleep.
		<-time.After(500 * time.Millisecond)
		link.ResetBurstPeak(simnet.LinkForward)

		upload := bytes.Repeat([]byte("u"), 32*1024)
		uploadDone := make(chan error, 1)
		go func() {
			stream, err := serverConn.AcceptStream(ctx)
			if err == nil {
				data, readErr := io.ReadAll(stream)
				if readErr == nil && !bytes.Equal(data, upload) {
					readErr = errors.New("unexpected upload payload")
				}
				err = readErr
			}
			uploadDone <- err
		}()
		uploadStream, err := clientConn.OpenStreamSync(ctx)
		require.NoError(t, err)
		_, err = uploadStream.Write(upload)
		require.NoError(t, err)
		require.NoError(t, uploadStream.Close())
		require.NoError(t, <-uploadDone)
		require.LessOrEqual(t, link.Counters(simnet.LinkForward).PeakSameTimeSubmittedBytes, uint64(16*1024), "post-idle upload must preserve the ten-packet pacer burst bound")

		stopPump()
		pumpStopped = true
		synctest.Wait()
		info, ok := clientConn.AdaptiveBDPDebugInfo()
		require.True(t, ok)
		require.Greater(t, info.PacingRateBytesPerSecond, uint64(0), "idle upload restart must not deadlock pacing")
		require.GreaterOrEqual(t, info.CongestionWindow, info.MinCwnd)
	})
}

func TestAdaptiveBDPDeterministicLinkInteractiveDatagramBidirectionalAndBulk(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clientAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 13), Port: 9013}
		serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 14), Port: 9014}
		linkConfig := simnet.DeterministicLinkConfig{
			Forward: simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 20_000_000, BaseLatency: 15 * time.Millisecond, QueueLimitBytes: 256 * 1024},
			Reverse: simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 20_000_000, BaseLatency: 15 * time.Millisecond, QueueLimitBytes: 256 * 1024},
		}
		link := simnet.NewDeterministicLink(linkConfig)
		router := simnet.NewDeterministicRouter(link, func(packet simnet.Packet) simnet.LinkDirection {
			if packet.From.String() == clientAddr.String() {
				return simnet.LinkForward
			}
			return simnet.LinkReverse
		})
		clientPacketConn := simnet.NewBufferedSimConn(clientAddr, router, 4096)
		serverPacketConn := simnet.NewBufferedSimConn(serverAddr, router, 4096)
		defer clientPacketConn.Close()
		defer serverPacketConn.Close()
		stopPump, _ := startDeterministicLinkPump(router)
		pumpStopped := false
		defer func() {
			if !pumpStopped {
				stopPump()
			}
		}()

		config := getQuicConfig(&quic.Config{
			EnableDatagrams: true,
			CwndTuning:      quic.CwndTuning{Enable: true, Algorithm: quic.CongestionControlAdaptiveBDP, StartupTargetRateBps: 20_000_000},
		})
		ln, err := quic.Listen(serverPacketConn, getTLSConfig(), config)
		require.NoError(t, err)
		defer ln.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		clientConn, err := quic.Dial(ctx, clientPacketConn, serverAddr, getTLSClientConfig(), config)
		require.NoError(t, err)
		defer clientConn.CloseWithError(0, "")
		serverConn, err := ln.Accept(ctx)
		require.NoError(t, err)
		defer serverConn.CloseWithError(0, "")
		trafficStart := time.Now()

		const datagrams = 100
		const interval = 100 * time.Millisecond
		sendDatagrams := func(conn *quic.Conn, prefix byte) <-chan error {
			done := make(chan error, 1)
			go func() {
				for range datagrams {
					if err := conn.SendDatagram(bytes.Repeat([]byte{prefix}, 256)); err != nil {
						done <- err
						return
					}
					<-time.After(interval)
				}
				done <- nil
			}()
			return done
		}
		receiveDatagrams := func(conn *quic.Conn, prefix byte) <-chan error {
			done := make(chan error, 1)
			go func() {
				for range datagrams {
					data, err := conn.ReceiveDatagram(ctx)
					if err != nil {
						done <- err
						return
					}
					if !bytes.Equal(data, bytes.Repeat([]byte{prefix}, 256)) {
						done <- errors.New("unexpected interactive datagram")
						return
					}
				}
				done <- nil
			}()
			return done
		}
		clientSends := sendDatagrams(clientConn, 'c')
		serverSends := sendDatagrams(serverConn, 's')
		clientReceives := receiveDatagrams(clientConn, 's')
		serverReceives := receiveDatagrams(serverConn, 'c')
		require.NoError(t, <-clientSends)
		require.NoError(t, <-serverSends)
		require.NoError(t, <-clientReceives)
		require.NoError(t, <-serverReceives)

		clientPayload := bytes.Repeat([]byte("client-bidirectional"), 4096)
		serverPayload := bytes.Repeat([]byte("server-bidirectional"), 4096)
		streamReads := make(chan error, 2)
		go func() {
			stream, err := serverConn.AcceptStream(ctx)
			if err == nil {
				data, readErr := io.ReadAll(stream)
				if readErr == nil && !bytes.Equal(data, clientPayload) {
					readErr = errors.New("unexpected client bidirectional payload")
				}
				err = readErr
			}
			streamReads <- err
		}()
		go func() {
			stream, err := clientConn.AcceptStream(ctx)
			if err == nil {
				data, readErr := io.ReadAll(stream)
				if readErr == nil && !bytes.Equal(data, serverPayload) {
					readErr = errors.New("unexpected server bidirectional payload")
				}
				err = readErr
			}
			streamReads <- err
		}()
		streamWrites := make(chan error, 2)
		for _, flow := range []struct {
			conn    *quic.Conn
			payload []byte
		}{{clientConn, clientPayload}, {serverConn, serverPayload}} {
			go func(conn *quic.Conn, payload []byte) {
				stream, err := conn.OpenStreamSync(ctx)
				if err == nil {
					_, err = stream.Write(payload)
				}
				if err == nil {
					err = stream.Close()
				}
				streamWrites <- err
			}(flow.conn, flow.payload)
		}
		require.NoError(t, <-streamWrites)
		require.NoError(t, <-streamWrites)
		require.NoError(t, <-streamReads)
		require.NoError(t, <-streamReads)

		bulk := bytes.Repeat([]byte("b"), 512*1024)
		bulkRead := make(chan error, 1)
		go func() {
			stream, err := serverConn.AcceptStream(ctx)
			if err == nil {
				data, readErr := io.ReadAll(stream)
				if readErr == nil && !bytes.Equal(data, bulk) {
					readErr = errors.New("unexpected post-interactive bulk payload")
				}
				err = readErr
			}
			bulkRead <- err
		}()
		stream, err := clientConn.OpenStreamSync(ctx)
		require.NoError(t, err)
		_, err = stream.Write(bulk)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		require.NoError(t, <-bulkRead)

		stopPump()
		pumpStopped = true
		synctest.Wait()
		elapsed := time.Since(trafficStart)
		forward := link.Counters(simnet.LinkForward)
		reverse := link.Counters(simnet.LinkReverse)
		require.Greater(t, elapsed, time.Duration(0))
		require.Greater(t, forward.DeliveredBytes, uint64(len(bulk)), "forward interactive and bulk traffic must be delivered")
		require.Greater(t, reverse.DeliveredBytes, uint64(len(serverPayload)), "reverse interactive and bidirectional traffic must be delivered")
		forwardGoodput := uint64(float64((len(bulk)+datagrams*256)*8) / elapsed.Seconds())
		reverseGoodput := uint64(float64((len(serverPayload)+datagrams*256)*8) / elapsed.Seconds())
		t.Logf("interactive DATAGRAM/bidirectional/bulk: elapsed=%s forward_delivered_bytes=%d reverse_delivered_bytes=%d forward_goodput_bps=%d reverse_goodput_bps=%d", elapsed, forward.DeliveredBytes, reverse.DeliveredBytes, forwardGoodput, reverseGoodput)
		clientInfo, ok := clientConn.AdaptiveBDPDebugInfo()
		require.True(t, ok)
		serverInfo, ok := serverConn.AdaptiveBDPDebugInfo()
		require.True(t, ok)
		require.Greater(t, clientInfo.PacingRateBytesPerSecond, uint64(0), "interactive traffic and bulk must not deadlock client pacing")
		require.Greater(t, serverInfo.PacingRateBytesPerSecond, uint64(0), "interactive traffic and bidirectional streams must not deadlock server pacing")
	})
}

func TestAdaptiveBDPDeterministicLinkMigrationUsesNewPath(t *testing.T) {
	synctest.Test(t, runAdaptiveBDPDeterministicMigration)
}

func runAdaptiveBDPDeterministicMigration(t *testing.T) {
	clientAddr1 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 21), Port: 9021}
	clientAddr2 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 22), Port: 9022}
	serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 23), Port: 9023}
	config := simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 10_000_000, BaseLatency: 10 * time.Millisecond, QueueLimitBytes: 2 * 1024 * 1024}
	link := simnet.NewDeterministicLink(simnet.DeterministicLinkConfig{Forward: config, Reverse: config})
	router := simnet.NewDeterministicRouter(link, func(packet simnet.Packet) simnet.LinkDirection {
		if packet.From.String() == clientAddr1.String() || packet.From.String() == clientAddr2.String() {
			return simnet.LinkForward
		}
		return simnet.LinkReverse
	})
	clientConn1 := simnet.NewBufferedSimConn(clientAddr1, router, 4096)
	clientConn2 := simnet.NewBufferedSimConn(clientAddr2, router, 4096)
	serverPacketConn := simnet.NewBufferedSimConn(serverAddr, router, 4096)
	defer clientConn1.Close()
	defer clientConn2.Close()
	defer serverPacketConn.Close()
	stopPump, _ := startDeterministicLinkPump(router)
	pumpStopped := false
	defer func() {
		if !pumpStopped {
			stopPump()
		}
	}()

	quicConfig := getQuicConfig(&quic.Config{CwndTuning: quic.CwndTuning{
		Enable:                     true,
		EnableAdaptiveBDPTelemetry: true,
		Algorithm:                  quic.CongestionControlAdaptiveBDP,
		StartupTargetRateBps:       100_000_000,
	}})
	ln, err := quic.Listen(serverPacketConn, getTLSConfig(), quicConfig)
	require.NoError(t, err)
	defer ln.Close()
	tr1 := &quic.Transport{Conn: clientConn1}
	tr2 := &quic.Transport{Conn: clientConn2}
	defer tr1.Close()
	defer tr2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	clientConn, err := tr1.Dial(ctx, serverAddr, getTLSClientConfig(), quicConfig)
	require.NoError(t, err)
	defer clientConn.CloseWithError(0, "")
	serverConn, err := ln.Accept(ctx)
	require.NoError(t, err)
	defer serverConn.CloseWithError(0, "")

	// Establish a non-zero model on the original path.
	readDone := make(chan error, 1)
	go func() {
		stream, err := serverConn.AcceptStream(ctx)
		if err == nil {
			_, err = io.ReadAll(stream)
		}
		readDone <- err
	}()
	stream, err := clientConn.OpenStreamSync(ctx)
	require.NoError(t, err)
	_, err = stream.Write(bytes.Repeat([]byte("m"), 2*1024*1024))
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.NoError(t, <-readDone)
	synctest.Wait()
	before, ok := clientConn.AdaptiveBDPDebugInfo()
	require.True(t, ok)
	require.Greater(t, before.MaxBandwidthBytesPerSecond, uint64(0))

	path, err := clientConn.AddPath(tr2)
	require.NoError(t, err)
	require.NoError(t, path.Probe(ctx))
	require.NoError(t, path.Switch())
	readDone = make(chan error, 1)
	go func() {
		stream, err := serverConn.AcceptStream(ctx)
		if err == nil {
			_, err = io.ReadAll(stream)
		}
		readDone <- err
	}()
	stream, err = clientConn.OpenStreamSync(ctx)
	require.NoError(t, err)
	postMigrationPayload := bytes.Repeat([]byte("post-migration"), 4096)
	_, err = stream.Write(postMigrationPayload)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.NoError(t, <-readDone)
	synctest.Wait()
	reset, ok := clientConn.AdaptiveBDPDebugInfo()
	require.True(t, ok)
	require.Equal(t, clientAddr2.String(), clientConn.LocalAddr().String())
	require.NotEmpty(t, reset.Telemetry, "confirmed new-path traffic must produce the new controller's telemetry history")
	firstNewPathSample := reset.Telemetry[0]
	require.Equal(t, uint64(1), firstNewPathSample.RoundCount, "new-path telemetry must start at the first controller round")
	require.Equal(t, "Startup", firstNewPathSample.State)
	require.Less(t, firstNewPathSample.MaxBandwidthBytesPerSecond, before.MaxBandwidthBytesPerSecond, "old path max bandwidth must not enter the new controller's first round")
	require.Zero(t, firstNewPathSample.ShortBandwidthBytesPerSecond)
	require.Zero(t, firstNewPathSample.LossRatioEWMA)
	require.Zero(t, firstNewPathSample.RecoveryBandwidthBytesPerSecond)
	require.False(t, firstNewPathSample.FullBwReached)
	require.Zero(t, reset.LossRatioEWMA)
	require.Zero(t, reset.LastMaterialLossRound)
	require.False(t, reset.LossRecoveryProbeActive)
	require.Greater(t, reset.PacingRateBytesPerSecond, uint64(0))
	require.GreaterOrEqual(t, reset.CongestionWindow, reset.MinCwnd)
	stopPump()
	pumpStopped = true
}

func TestAdaptiveBDPDeterministicLinkEqualRTTAdaptiveBDPFairness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := runAdaptiveBDPDeterministicCompetingFlows(t, quic.CongestionControlAdaptiveBDP, quic.CongestionControlAdaptiveBDP)
		jain := jainFairness(result.rates)
		t.Logf("equal-RTT AdaptiveBDP fairness: rates=%0.0f,%0.0f bps jain=%0.4f queue_delay_p95=%s tail_drops=%d", result.rates[0], result.rates[1], jain, result.queueDelayPercentile(95), result.forward.TailDrops)
		require.GreaterOrEqual(t, jain, 0.90)
		require.Zero(t, result.forward.TailDrops)
	})
}

func TestAdaptiveBDPDeterministicLinkCompetesWithCubicAndReno(t *testing.T) {
	for _, test := range []struct {
		name      string
		algorithm quic.CongestionControlAlgorithm
	}{
		{name: "Cubic", algorithm: quic.CongestionControlCubic},
		{name: "Reno", algorithm: quic.CongestionControlReno},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := runAdaptiveBDPDeterministicCompetingFlows(t, quic.CongestionControlAdaptiveBDP, test.algorithm)
				ratio := result.rates[0] / result.rates[1]
				t.Logf("AdaptiveBDP vs %s: rates=%0.0f,%0.0f bps ratio=%0.4f queue_delay_p95=%s tail_drops=%d", test.name, result.rates[0], result.rates[1], ratio, result.queueDelayPercentile(95), result.forward.TailDrops)
				require.GreaterOrEqual(t, ratio, 0.5)
				require.LessOrEqual(t, ratio, 2.0)
				require.Zero(t, result.forward.TailDrops)
			})
		})
	}
}

func TestAdaptiveBDPDeterministicLinkUnequalRTTAdaptiveBDPFairness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client2Addr := "192.0.2.32:9032"
		result := runAdaptiveBDPDeterministicCompetingFlowsWithAccessDelay(t, func(packet simnet.Packet) time.Duration {
			if packet.From.String() == client2Addr || packet.To.String() == client2Addr {
				return 90 * time.Millisecond
			}
			return 0
		}, quic.CongestionControlAdaptiveBDP, quic.CongestionControlAdaptiveBDP)
		jain := jainFairness(result.rates)
		t.Logf("unequal-RTT AdaptiveBDP fairness (20ms,200ms): rates=%0.0f,%0.0f bps jain=%0.4f queue_delay_p95=%s tail_drops=%d", result.rates[0], result.rates[1], jain, result.queueDelayPercentile(95), result.forward.TailDrops)
		require.Greater(t, result.rates[0], float64(0))
		require.Greater(t, result.rates[1], float64(0))
		require.Zero(t, result.forward.TailDrops)
	})
}

func TestAdaptiveBDPDeterministicLinkLateStartAdaptiveBDPFairness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := runAdaptiveBDPDeterministicCompetingFlowsWithSchedule(t, 25*time.Millisecond, nil, []time.Duration{0, 500 * time.Millisecond}, 8*1024*1024, quic.CongestionControlAdaptiveBDP, quic.CongestionControlAdaptiveBDP)
		jain := jainFairness(result.rates)
		t.Logf("late-start AdaptiveBDP fairness: rates=%0.0f,%0.0f bps jain=%0.4f queue_delay_p95=%s tail_drops=%d", result.rates[0], result.rates[1], jain, result.queueDelayPercentile(95), result.forward.TailDrops)
		require.True(t, result.firstFlowActiveAtSecondStart, "the second flow must start while the first transfer is still active")
		require.Greater(t, result.rates[0], float64(0))
		require.Greater(t, result.rates[1], float64(0))
		require.Zero(t, result.forward.TailDrops)
	})
}

func runAdaptiveBDPDeterministicCompetingFlows(t *testing.T, algorithms ...quic.CongestionControlAlgorithm) competingFlowsResult {
	return runAdaptiveBDPDeterministicCompetingFlowsWithSchedule(t, 25*time.Millisecond, nil, nil, 1024*1024, algorithms...)
}

func runAdaptiveBDPDeterministicCompetingFlowsWithAccessDelay(t *testing.T, accessDelay simnet.PacketDelaySelector, algorithms ...quic.CongestionControlAlgorithm) competingFlowsResult {
	return runAdaptiveBDPDeterministicCompetingFlowsWithSchedule(t, 10*time.Millisecond, accessDelay, nil, 1024*1024, algorithms...)
}

type competingFlowsResult struct {
	rates                        []float64
	firstFlowActiveAtSecondStart bool
	forward                      simnet.LinkCounters
	reverse                      simnet.LinkCounters
	queueDelays                  []time.Duration
}

func (r competingFlowsResult) queueDelayPercentile(percentile int) time.Duration {
	if len(r.queueDelays) == 0 {
		return 0
	}
	samples := slices.Clone(r.queueDelays)
	slices.Sort(samples)
	return samples[(len(samples)-1)*percentile/100]
}

func runAdaptiveBDPDeterministicCompetingFlowsWithSchedule(t *testing.T, baseLatency time.Duration, accessDelay simnet.PacketDelaySelector, flowStartDelays []time.Duration, payloadBytes int, algorithms ...quic.CongestionControlAlgorithm) competingFlowsResult {
	t.Helper()
	require.Len(t, algorithms, 2)
	if len(flowStartDelays) == 0 {
		flowStartDelays = make([]time.Duration, len(algorithms))
	}
	require.Len(t, flowStartDelays, len(algorithms))
	require.Greater(t, payloadBytes, 0)
	clientAddrs := []*net.UDPAddr{{IP: net.IPv4(192, 0, 2, 31), Port: 9031}, {IP: net.IPv4(192, 0, 2, 32), Port: 9032}}
	serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 33), Port: 9033}
	// Keep the common bottleneck finite but above the observed competing-flow
	// standing queue, so this fairness scenario measures controller sharing
	// rather than scheduler-sensitive tail-drop recovery.
	linkConfig := simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 30_000_000, BaseLatency: baseLatency, QueueLimitBytes: 1024 * 1024}
	link := simnet.NewDeterministicLink(simnet.DeterministicLinkConfig{Forward: linkConfig, Reverse: linkConfig})
	router := simnet.NewDeterministicRouterWithDelay(link, func(packet simnet.Packet) simnet.LinkDirection {
		for _, addr := range clientAddrs {
			if packet.From.String() == addr.String() {
				return simnet.LinkForward
			}
		}
		return simnet.LinkReverse
	}, accessDelay)
	clientPacketConns := []*simnet.SimConn{simnet.NewBufferedSimConn(clientAddrs[0], router, 4096), simnet.NewBufferedSimConn(clientAddrs[1], router, 4096)}
	serverPacketConn := simnet.NewBufferedSimConn(serverAddr, router, 4096)
	defer clientPacketConns[0].Close()
	defer clientPacketConns[1].Close()
	defer serverPacketConn.Close()
	stopPump, queueDelays := startDeterministicLinkPump(router)
	pumpStopped := false
	defer func() {
		if !pumpStopped {
			stopPump()
		}
	}()
	serverConfig := getQuicConfig(&quic.Config{CwndTuning: quic.CwndTuning{Enable: true, Algorithm: quic.CongestionControlAdaptiveBDP, StartupTargetRateBps: 30_000_000}})
	ln, err := quic.Listen(serverPacketConn, getTLSConfig(), serverConfig)
	require.NoError(t, err)
	defer ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	clients := make([]*quic.Conn, len(clientPacketConns))
	for i, packetConn := range clientPacketConns {
		clientConfig := getQuicConfig(&quic.Config{CwndTuning: quic.CwndTuning{Enable: true, Algorithm: algorithms[i], StartupTargetRateBps: 30_000_000}})
		clients[i], err = quic.Dial(ctx, packetConn, serverAddr, getTLSClientConfig(), clientConfig)
		require.NoError(t, err)
		defer clients[i].CloseWithError(0, "")
	}
	servers := make([]*quic.Conn, len(clients))
	for i := range servers {
		servers[i], err = ln.Accept(ctx)
		require.NoError(t, err)
		defer servers[i].CloseWithError(0, "")
	}
	payload := bytes.Repeat([]byte("f"), payloadBytes)
	type flowResult struct {
		index   int
		elapsed time.Duration
		err     error
	}
	reads := make(chan flowResult, len(servers))
	firstReadDone := make(chan struct{})
	start := time.Now()
	for index, server := range servers {
		go func(index int, server *quic.Conn) {
			stream, err := server.AcceptStream(ctx)
			if err == nil {
				data, readErr := io.ReadAll(stream)
				if readErr == nil && !bytes.Equal(data, payload) {
					readErr = errors.New("unexpected fairness payload")
				}
				err = readErr
			}
			reads <- flowResult{index: index, elapsed: time.Since(start), err: err}
			if index == 0 {
				close(firstReadDone)
			}
		}(index, server)
	}
	type writeResult struct {
		index       int
		started     time.Duration
		firstActive bool
		err         error
	}
	writes := make(chan writeResult, len(clients))
	for index, client := range clients {
		go func(index int, client *quic.Conn) {
			if delay := flowStartDelays[index]; delay > 0 {
				<-time.After(delay)
			}
			firstActive := true
			if index == 1 {
				select {
				case <-firstReadDone:
					firstActive = false
				default:
				}
			}
			started := time.Since(start)
			stream, err := client.OpenStreamSync(ctx)
			if err == nil {
				_, err = stream.Write(payload)
			}
			if err == nil {
				err = stream.Close()
			}
			if !firstActive && err == nil {
				err = errors.New("first flow completed before late-start flow began")
			}
			writes <- writeResult{index: index, started: started, firstActive: firstActive, err: err}
		}(index, client)
	}
	writeStarts := make([]time.Duration, len(clients))
	firstFlowActiveAtSecondStart := true
	for range clients {
		result := <-writes
		require.NoError(t, result.err)
		writeStarts[result.index] = result.started
		if result.index == 1 {
			firstFlowActiveAtSecondStart = result.firstActive
		}
	}
	rates := make([]float64, len(clients))
	for range rates {
		result := <-reads
		require.NoError(t, result.err)
		elapsed := result.elapsed - writeStarts[result.index]
		require.Greater(t, elapsed, time.Duration(0))
		rates[result.index] = float64(len(payload)*8) / elapsed.Seconds()
	}
	stopPump()
	pumpStopped = true
	return competingFlowsResult{
		rates:                        rates,
		firstFlowActiveAtSecondStart: firstFlowActiveAtSecondStart,
		forward:                      link.Counters(simnet.LinkForward),
		reverse:                      link.Counters(simnet.LinkReverse),
		queueDelays:                  queueDelays(),
	}
}

func jainFairness(rates []float64) float64 {
	var sum, sumSquares float64
	for _, rate := range rates {
		sum += rate
		sumSquares += rate * rate
	}
	if sumSquares == 0 {
		return 0
	}
	return sum * sum / (float64(len(rates)) * sumSquares)
}

func TestAdaptiveBDPDeterministicLinkT03RebasesBaseRTT(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 30_000_000,
			BaseLatency:            15 * time.Millisecond,
			QueueLimitBytes:        2 * 1024 * 1024,
		}
		after := before
		after.BaseLatency = 75 * time.Millisecond
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: before, Reverse: before},
			startupTargetRateBps: 30_000_000,
			payloadBytes:         4 * 1024 * 1024,
			minRTTFilterWindow:   10 * time.Millisecond,
			configureLink: func(link *simnet.DeterministicLink) {
				link.ScheduleChange(simnet.LinkForward, 40*time.Millisecond, after)
				link.ScheduleChange(simnet.LinkReverse, 40*time.Millisecond, after)
			},
		})
		require.GreaterOrEqual(t, result.info.MinRTT, 140*time.Millisecond, "T03 must learn the post-change same-5-tuple base RTT")
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
	})
}

func TestAdaptiveBDPDeterministicLinkT05RebasesBaseRTTAfterCapacityDrop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 100_000_000,
			BaseLatency:            10 * time.Millisecond,
			QueueLimitBytes:        2 * 1024 * 1024,
		}
		after := before
		after.BandwidthBitsPerSecond = 2_000_000
		after.BaseLatency = 100 * time.Millisecond
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: before, Reverse: before},
			startupTargetRateBps: 100_000_000,
			payloadBytes:         1024 * 1024,
			minRTTFilterWindow:   10 * time.Millisecond,
			timeout:              6 * time.Second,
			configureLink: func(link *simnet.DeterministicLink) {
				link.ScheduleChange(simnet.LinkForward, 40*time.Millisecond, after)
				link.ScheduleChange(simnet.LinkReverse, 40*time.Millisecond, after)
			},
		})
		require.GreaterOrEqual(t, result.info.MinRTT, 190*time.Millisecond, "T05 must learn the post-change same-5-tuple base RTT")
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
	})
}

func TestAdaptiveBDPDeterministicLinkT04LearnsLowerBaseRTT(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 30_000_000,
			BaseLatency:            75 * time.Millisecond,
			QueueLimitBytes:        2 * 1024 * 1024,
		}
		after := before
		after.BaseLatency = 15 * time.Millisecond
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: before, Reverse: before},
			startupTargetRateBps: 30_000_000,
			payloadBytes:         4 * 1024 * 1024,
			configureLink: func(link *simnet.DeterministicLink) {
				link.ScheduleChange(simnet.LinkForward, 200*time.Millisecond, after)
				link.ScheduleChange(simnet.LinkReverse, 200*time.Millisecond, after)
			},
		})
		require.LessOrEqual(t, result.info.MinRTT, 50*time.Millisecond, "T04 must learn the lower same-5-tuple base RTT")
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
	})
}

func TestAdaptiveBDPDeterministicLinkT06LearnsLowerCapacityAndRTT(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 2_000_000,
			BaseLatency:            100 * time.Millisecond,
			QueueLimitBytes:        128 * 1024,
		}
		after := before
		after.BandwidthBitsPerSecond = 100_000_000
		after.BaseLatency = 10 * time.Millisecond
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: before, Reverse: before},
			startupTargetRateBps: 2_000_000,
			payloadBytes:         1024 * 1024,
			timeout:              6 * time.Second,
			configureLink: func(link *simnet.DeterministicLink) {
				link.ScheduleChange(simnet.LinkForward, 400*time.Millisecond, after)
				link.ScheduleChange(simnet.LinkReverse, 400*time.Millisecond, after)
			},
		})
		require.LessOrEqual(t, result.info.MinRTT, 50*time.Millisecond, "T06 must learn the lower same-5-tuple base RTT")
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
	})
}

func TestAdaptiveBDPDeterministicLinkT01CapacityDownshift(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 100_000_000, BaseLatency: 15 * time.Millisecond, QueueLimitBytes: 37_500}
		after := before
		after.BandwidthBitsPerSecond = 10_000_000
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: before, Reverse: before},
			startupTargetRateBps: 100_000_000,
			payloadBytes:         4 * 1024 * 1024,
			configureLink: func(link *simnet.DeterministicLink) {
				link.ScheduleChange(simnet.LinkForward, 200*time.Millisecond, after)
				link.ScheduleChange(simnet.LinkReverse, 200*time.Millisecond, after)
			},
		})
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
		evidence, ok := firstAdaptiveBDPTelemetry(result.info.Telemetry, func(sample quic.AdaptiveBDPTelemetrySample) bool {
			return sample.TransitionReason == "queue_growth_capacity_downshift"
		})
		require.True(t, ok, "T01 must observe deterministic filled-queue downshift evidence")
		downshift, ok := firstAdaptiveBDPTelemetry(result.info.Telemetry, func(sample quic.AdaptiveBDPTelemetrySample) bool {
			return sample.Elapsed >= evidence.Elapsed && sample.PacingRateBytesPerSecond < uint64(15_000_000/8)
		})
		require.True(t, ok, "T01 pacing must fall below 15 Mbit/s")
		require.LessOrEqual(t, downshift.Elapsed-evidence.Elapsed, 3*30*time.Millisecond, "T01 must downshift within three base RTTs after congestion evidence")
		queueRecoveredAt, ok := result.firstQueueDelayAtOrBelow(evidence.Elapsed, evidence.Elapsed+6*30*time.Millisecond, 2*evidence.QueueTarget)
		require.True(t, ok, "T01 queue delay must return below twice the target within six RTTs")
		require.LessOrEqual(t, queueRecoveredAt-evidence.Elapsed, 6*30*time.Millisecond)
		require.Less(t, result.info.PacingRateBytesPerSecond, uint64(15_000_000/8), "T01 must not remain pinned to the stale 100 Mbit/s startup floor")
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
	})
}

func TestAdaptiveBDPDeterministicLinkT02CapacityUpshift(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 10_000_000, BaseLatency: 15 * time.Millisecond, QueueLimitBytes: 256 * 1024}
		after := before
		after.BandwidthBitsPerSecond = 100_000_000
		after.QueueLimitBytes = 2 * 1024 * 1024
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: before, Reverse: before},
			startupTargetRateBps: 10_000_000,
			payloadBytes:         32 * 1024 * 1024,
			timeout:              8 * time.Second,
			configureLink: func(link *simnet.DeterministicLink) {
				link.ScheduleChange(simnet.LinkForward, 200*time.Millisecond, after)
				link.ScheduleChange(simnet.LinkReverse, 200*time.Millisecond, after)
			},
		})
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
		recovered, ok := firstAdaptiveBDPTelemetry(result.info.Telemetry, func(sample quic.AdaptiveBDPTelemetrySample) bool {
			return sample.Elapsed >= 200*time.Millisecond && sample.PacingRateBytesPerSecond >= uint64(80_000_000/8)
		})
		require.True(t, ok, "T02 must recover at least 80 Mbit/s pacing under saturated demand")
		require.LessOrEqual(t, recovered.Elapsed, 200*time.Millisecond+5*time.Second, "T02 must recover within the documented five-second limit")
		appRecoveredAt, ok := result.firstApplicationGoodputAtOrAbove(200*time.Millisecond, 200*time.Millisecond+5*time.Second, 200*time.Millisecond, 80_000_000)
		maxGoodput, maxGoodputAt := result.maxApplicationGoodput(200*time.Millisecond, 200*time.Millisecond+5*time.Second, 200*time.Millisecond)
		if !ok {
			for _, sample := range result.info.Telemetry {
				if sample.Elapsed >= 4*time.Second {
					t.Logf("T02 late telemetry: elapsed=%s event=%s round=%d state=%s reason=%s pacing=%d bandwidth=%d cwnd=%d",
						sample.Elapsed, sample.Event, sample.RoundCount, sample.State, sample.TransitionReason,
						sample.PacingRateBytesPerSecond, sample.BandwidthBytesPerSecond, sample.CongestionWindow)
				}
			}
		}
		require.Truef(t, ok, "T02 application goodput must reach 80 Mbit/s in a sustained 200 ms window; maximum was %d bit/s at %s; pacing recovered at %s", maxGoodput, maxGoodputAt, recovered.Elapsed)
		require.LessOrEqual(t, appRecoveredAt, 200*time.Millisecond+5*time.Second)
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
	})
}

func TestAdaptiveBDPDeterministicLinkQ02DoesNotRebaseStandingQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 4 BDP at 30 Mbit/s and 50 ms RTT is 750 KiB. A larger queue makes
		// queue persistence possible without forcing tail drops.
		config := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 30_000_000,
			BaseLatency:            25 * time.Millisecond,
			QueueLimitBytes:        1024 * 1024,
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
			startupTargetRateBps: 100_000_000,
			payloadBytes:         4 * 1024 * 1024,
			minRTTFilterWindow:   10 * time.Millisecond,
			configureLink: func(link *simnet.DeterministicLink) {
				// A deterministic cross-traffic burst builds a standing 4-BDP
				// bottleneck queue without a timer or scheduler dependency.
				for range 640 {
					link.SchedulePacket(simnet.LinkForward, 40*time.Millisecond, simnet.Packet{
						To:   &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 9003},
						Data: make([]byte, 1200),
					})
				}
			},
		})
		require.LessOrEqual(t, result.info.MinRTT, 75*time.Millisecond, "Q02 must preserve the 50 ms base RTT instead of accepting a standing queue")
		require.GreaterOrEqual(t, result.forward.PeakQueueBytes, uint64(750*1024), "scenario must create the configured deep queue")
	})
}

func TestAdaptiveBDPDeterministicLinkQ01ShallowTailDropQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 0.25 BDP at 30 Mbit/s / 50 ms. Startup intentionally overshoots
		// this shallow queue, proving that tail-drop delivery reaches QUIC.
		config := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 30_000_000,
			BaseLatency:            25 * time.Millisecond,
			QueueLimitBytes:        46_875,
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
			startupTargetRateBps: 100_000_000,
			payloadBytes:         1024 * 1024,
			timeout:              6 * time.Second,
		})
		require.Greater(t, result.forward.TailDrops, uint64(0), "Q01 must exercise the shallow tail-drop queue")
		require.LessOrEqual(t, result.queueDelayPercentile(99), 12500*time.Microsecond, "Q01 queue delay must remain bounded by the configured 0.25-BDP queue")
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
		require.GreaterOrEqual(t, result.info.CongestionWindow, result.info.MinCwnd)
	})
}

func TestAdaptiveBDPDeterministicLinkQ03ReversePathACKQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		forward := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 100_000_000,
			BaseLatency:            50 * time.Millisecond,
			QueueLimitBytes:        2 * 1024 * 1024,
		}
		reverse := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 1_000_000,
			BaseLatency:            50 * time.Millisecond,
			QueueLimitBytes:        256 * 1024,
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: forward, Reverse: reverse},
			startupTargetRateBps: 100_000_000,
			payloadBytes:         1024 * 1024,
			timeout:              6 * time.Second,
		})
		require.Greater(t, result.reverse.PeakQueueBytes, uint64(0), "Q03 must build reverse-path ACK queue occupancy")
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
	})
}

func TestAdaptiveBDPDeterministicLinkQ04ECNAboveQueueTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// At 30 Mbit/s, the 10 ms AdaptiveBDP queue target is 37,500
		// bytes. Marking above that occupancy exercises the real ECT(0) ->
		// CE -> ACK_ECN -> congestion-controller path without tail drop.
		config := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 30_000_000,
			BaseLatency:            25 * time.Millisecond,
			QueueLimitBytes:        1024 * 1024,
			ECNThresholdBytes:      37_500,
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
			startupTargetRateBps: 100_000_000,
			payloadBytes:         2 * 1024 * 1024,
			timeout:              6 * time.Second,
		})
		require.Greater(t, result.forward.ECNMarks, uint64(0), "Q04 must mark packets above the queue target")
		require.Zero(t, result.forward.TailDrops, "Q04 must signal congestion with ECN, not tail drop")
		require.True(t, result.info.HasLastECNCE, "ACK_ECN feedback must reach AdaptiveBDP")
		require.GreaterOrEqual(t, result.info.LastECNCERound, uint64(1))
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0))
		require.GreaterOrEqual(t, result.info.CongestionWindow, result.info.MinCwnd)
	})
}

func TestAdaptiveBDPDeterministicLinkQ05OutageRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The outage starts after handshake and lasts for more than three PTOs
		// on this 200 ms path, so the real loss detector has persistent-loss
		// evidence before traffic is restored.
		config := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 10_000_000,
			BaseLatency:            100 * time.Millisecond,
			QueueLimitBytes:        256 * 1024,
			LossIntervals:          []simnet.LossInterval{{Start: time.Second, End: 4 * time.Second}},
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
			startupTargetRateBps: 10_000_000,
			payloadBytes:         2 * 1024 * 1024,
			initialWriteBytes:    256 * 1024,
			pacedWriteUntil:      4 * time.Second,
			pacedWriteInterval:   100 * time.Millisecond,
			pacedWriteBytes:      1200,
			timeout:              15 * time.Second,
		})
		require.Greater(t, result.forward.ScriptedLosses, uint64(0), "Q05 must exercise the complete outage")
		var reset quic.AdaptiveBDPTelemetrySample
		resetIndex := -1
		var preOutageMaxBW uint64
		for i, sample := range result.info.Telemetry {
			if sample.Event == "persistent_congestion" {
				reset, resetIndex = sample, i
				break
			}
			preOutageMaxBW = max(preOutageMaxBW, sample.MaxBandwidthBytesPerSecond)
		}
		t.Logf("Q05 persistent-congestion evidence: events=%d last_span=%s gate=%s", result.info.PersistentCongestionEvents, result.info.LastPersistentCongestionSpan, result.info.LastPersistentCongestionGate)
		require.NotEqual(t, -1, resetIndex, "Q05 outage must reach QUIC persistent-congestion handling")
		require.Equal(t, "Startup", reset.State)
		require.Equal(t, result.info.MinCwnd, reset.CongestionWindow)
		require.Equal(t, reset.BandwidthBytesPerSecond, reset.MaxBandwidthBytesPerSecond, "post-reset max bandwidth may contain only the minimum-cwnd bootstrap")
		require.Less(t, reset.MaxBandwidthBytesPerSecond, preOutageMaxBW)
		require.Zero(t, reset.ShortBandwidthBytesPerSecond)
		require.Zero(t, reset.RecoveryBandwidthBytesPerSecond)
		require.Zero(t, reset.LossRatioEWMA)
		require.False(t, reset.FullBwReached)
		var firstPostResetRound quic.AdaptiveBDPTelemetrySample
		for _, sample := range result.info.Telemetry[resetIndex+1:] {
			if sample.Event == "round" {
				firstPostResetRound = sample
				break
			}
		}
		require.NotZero(t, firstPostResetRound.RoundCount)
		require.Less(t, firstPostResetRound.MaxBandwidthBytesPerSecond, preOutageMaxBW, "first ACK round after outage must not restore stale bandwidth")
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0), "traffic must recover after the outage")
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0), "outage recovery must not deadlock pacing")
		require.GreaterOrEqual(t, result.info.CongestionWindow, result.info.MinCwnd)
	})
}

type adaptiveBDPLinkScenario struct {
	linkConfig                    simnet.DeterministicLinkConfig
	startupTargetRateBps          uint64
	initialWindowPackets          uint32
	minWindowPackets              uint32
	payloadBytes                  int
	initialWriteBytes             int
	pacedWriteUntil               time.Duration
	pacedWriteInterval            time.Duration
	pacedWriteBytes               int
	pacedWriteWithLimit           bool
	pacedWritePauses              []adaptiveBDPWritePause
	minRTTFilterWindow            time.Duration
	maxWindowPackets              uint32
	queueTarget                   time.Duration
	noCongestionRateFloorFraction float64
	timeout                       time.Duration
	configureLink                 func(*simnet.DeterministicLink)
}

type adaptiveBDPWritePause struct {
	after    time.Duration
	duration time.Duration
}

func (r adaptiveBDPLinkScenarioResult) goodputBitsPerSecond() uint64 {
	if r.elapsed <= 0 {
		return 0
	}
	return r.payloadBytes * 8 * uint64(time.Second) / uint64(r.elapsed)
}

func (r adaptiveBDPLinkScenarioResult) queueDelayPercentile(percentile int) time.Duration {
	if len(r.queueDelays) == 0 {
		return 0
	}
	samples := slices.Clone(r.queueDelays)
	slices.Sort(samples)
	return samples[(len(samples)-1)*percentile/100]
}

func (r adaptiveBDPLinkScenarioResult) queueDelayPercentileAfter(percentile int, after time.Duration) time.Duration {
	start := int(after / adaptiveBDPVirtualTick)
	if start >= len(r.queueDelays) {
		return 0
	}
	samples := slices.Clone(r.queueDelays[start:])
	slices.Sort(samples)
	return samples[(len(samples)-1)*percentile/100]
}

func (r adaptiveBDPLinkScenarioResult) firstQueueDelayAtOrBelow(after, deadline, limit time.Duration) (time.Duration, bool) {
	start := int(after / adaptiveBDPVirtualTick)
	end := min(len(r.queueDelays), int(deadline/adaptiveBDPVirtualTick)+1)
	for i := max(0, start); i < end; i++ {
		if r.queueDelays[i] <= limit {
			return time.Duration(i+1) * adaptiveBDPVirtualTick, true
		}
	}
	return 0, false
}

type adaptiveBDPLinkScenarioResult struct {
	info            quic.AdaptiveBDPDebugInfo
	forward         simnet.LinkCounters
	reverse         simnet.LinkCounters
	payloadBytes    uint64
	elapsed         time.Duration
	queueDelays     []time.Duration
	deliverySamples []adaptiveBDPDeliverySample
}

type adaptiveBDPDeliverySample struct {
	at    time.Duration
	bytes uint64
}

func (r adaptiveBDPLinkScenarioResult) firstApplicationGoodputAtOrAbove(after, deadline, window time.Duration, targetBitsPerSecond uint64) (time.Duration, bool) {
	for i := range r.deliverySamples {
		start := r.deliverySamples[i]
		if start.at < after {
			continue
		}
		for j := i + 1; j < len(r.deliverySamples); j++ {
			end := r.deliverySamples[j]
			if end.at > deadline {
				break
			}
			if end.at-start.at < window {
				continue
			}
			elapsed := end.at - start.at
			rate := (end.bytes - start.bytes) * 8 * uint64(time.Second) / uint64(elapsed)
			if rate >= targetBitsPerSecond {
				return end.at, true
			}
			break
		}
	}
	return 0, false
}

func (r adaptiveBDPLinkScenarioResult) maxApplicationGoodput(after, deadline, window time.Duration) (uint64, time.Duration) {
	var maxRate uint64
	var maxRateAt time.Duration
	for i := range r.deliverySamples {
		start := r.deliverySamples[i]
		if start.at < after {
			continue
		}
		for j := i + 1; j < len(r.deliverySamples); j++ {
			end := r.deliverySamples[j]
			if end.at > deadline {
				break
			}
			if end.at-start.at < window {
				continue
			}
			rate := (end.bytes - start.bytes) * 8 * uint64(time.Second) / uint64(end.at-start.at)
			if rate > maxRate {
				maxRate = rate
				maxRateAt = end.at
			}
			break
		}
	}
	return maxRate, maxRateAt
}

func (r adaptiveBDPLinkScenarioResult) medianApplicationGoodput(after, window time.Duration) uint64 {
	if len(r.deliverySamples) < 2 || window <= 0 {
		return 0
	}
	var rates []uint64
	for start := after; start+window <= r.deliverySamples[len(r.deliverySamples)-1].at; start += window {
		var first, last *adaptiveBDPDeliverySample
		for i := range r.deliverySamples {
			sample := &r.deliverySamples[i]
			if first == nil && sample.at >= start {
				first = sample
			}
			if sample.at >= start+window {
				last = sample
				break
			}
		}
		if first == nil || last == nil || last.at <= first.at {
			continue
		}
		rates = append(rates, (last.bytes-first.bytes)*8*uint64(time.Second)/uint64(last.at-first.at))
	}
	if len(rates) == 0 {
		return 0
	}
	slices.Sort(rates)
	return rates[len(rates)/2]
}

func runAdaptiveBDPDeterministicBulkTransfer(t *testing.T, scenario adaptiveBDPLinkScenario) adaptiveBDPLinkScenarioResult {
	t.Helper()
	clientAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 9001}
	serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 9002}
	link := simnet.NewDeterministicLink(scenario.linkConfig)
	if scenario.configureLink != nil {
		scenario.configureLink(link)
	}
	router := simnet.NewDeterministicRouter(link, func(packet simnet.Packet) simnet.LinkDirection {
		if packet.From.String() == clientAddr.String() {
			return simnet.LinkForward
		}
		return simnet.LinkReverse
	})
	// The deterministic router can deliver an entire virtual-time batch at
	// once. A large explicit local queue avoids accidental scheduler-dependent
	// drops from SimConn's production-sized default UDP test buffer.
	clientPacketConn := simnet.NewBufferedSimConn(clientAddr, router, 4096)
	serverPacketConn := simnet.NewBufferedSimConn(serverAddr, router, 4096)
	defer clientPacketConn.Close()
	defer serverPacketConn.Close()

	stopPump, queueDelays := startDeterministicLinkPump(router)
	pumpStopped := false
	defer func() {
		if !pumpStopped {
			stopPump()
		}
	}()

	config := getQuicConfig(&quic.Config{
		MaxIdleTimeout:             30 * time.Second,
		MaxStreamReceiveWindow:     128 * 1024 * 1024,
		MaxConnectionReceiveWindow: 256 * 1024 * 1024,
		CwndTuning: quic.CwndTuning{
			Enable:                        true,
			EnableAdaptiveBDPTelemetry:    true,
			Algorithm:                     quic.CongestionControlAdaptiveBDP,
			InitialWindowPackets:          scenario.initialWindowPackets,
			MinWindowPackets:              scenario.minWindowPackets,
			StartupTargetRateBps:          scenario.startupTargetRateBps,
			MinRTTFilterWindow:            scenario.minRTTFilterWindow,
			MaxWindowPackets:              scenario.maxWindowPackets,
			QueueTarget:                   scenario.queueTarget,
			NoCongestionRateFloorFraction: scenario.noCongestionRateFloorFraction,
		},
	})
	ln, err := quic.Listen(serverPacketConn, getTLSConfig(), config)
	require.NoError(t, err)
	defer ln.Close()

	timeout := scenario.timeout
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	clientConn, err := quic.Dial(ctx, clientPacketConn, serverAddr, getTLSClientConfig(), config)
	require.NoError(t, err)
	defer clientConn.CloseWithError(0, "")
	serverConn, err := ln.Accept(ctx)
	require.NoError(t, err)
	defer serverConn.CloseWithError(0, "")

	payload := bytes.Repeat([]byte("a"), scenario.payloadBytes)
	type readResult struct {
		data    []byte
		samples []adaptiveBDPDeliverySample
		err     error
	}
	readResultChan := make(chan readResult, 1)
	go func() {
		serverStream, err := serverConn.AcceptStream(ctx)
		if err != nil {
			readResultChan <- readResult{err: err}
			return
		}
		var data bytes.Buffer
		samples := []adaptiveBDPDeliverySample{{at: link.Now()}}
		buf := make([]byte, 32*1024)
		for {
			n, readErr := serverStream.Read(buf)
			if n > 0 {
				_, _ = data.Write(buf[:n])
				samples = append(samples, adaptiveBDPDeliverySample{at: link.Now(), bytes: uint64(data.Len())})
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					readErr = nil
				}
				readResultChan <- readResult{data: data.Bytes(), samples: samples, err: readErr}
				return
			}
		}
	}()
	stream, err := clientConn.OpenStreamSync(ctx)
	require.NoError(t, err)
	written := 0
	if scenario.initialWriteBytes > 0 {
		end := min(scenario.initialWriteBytes, len(payload))
		n, writeErr := stream.Write(payload[:end])
		require.NoError(t, writeErr)
		require.Equal(t, end, n)
		written = end
	}
	nextPause := 0
	for scenario.pacedWriteInterval > 0 && link.Now() < scenario.pacedWriteUntil && written < len(payload) {
		for nextPause < len(scenario.pacedWritePauses) && link.Now() >= scenario.pacedWritePauses[nextPause].after {
			<-time.After(scenario.pacedWritePauses[nextPause].duration)
			nextPause++
		}
		chunkBytes := scenario.pacedWriteBytes
		if chunkBytes <= 0 {
			chunkBytes = 1200
		}
		end := min(written+chunkBytes, len(payload))
		var n int
		if scenario.pacedWriteWithLimit {
			credit := end - written
			var writeErr error
			n, writeErr = stream.WriteWithLimit(payload[written:], func(maxBytes int) int {
				allowed := min(maxBytes, credit)
				credit -= allowed
				return allowed
			})
			if end < len(payload) {
				require.ErrorIs(t, writeErr, quic.ErrWriteLimitReached)
			} else {
				require.NoError(t, writeErr)
			}
		} else {
			var writeErr error
			n, writeErr = stream.Write(payload[written:end])
			require.NoError(t, writeErr)
		}
		require.Equal(t, end-written, n)
		written = end
		<-time.After(scenario.pacedWriteInterval)
	}
	if written < len(payload) {
		n, writeErr := stream.Write(payload[written:])
		require.NoError(t, writeErr)
		require.Equal(t, len(payload)-written, n)
	}
	require.NoError(t, stream.Close())

	read := <-readResultChan
	require.NoError(t, read.err)
	require.Equal(t, payload, read.data)

	// Conn.AdaptiveBDPDebugInfo is sampled only after virtual-network
	// quiescence, avoiding a test-data race with the ACK goroutine.
	stopPump()
	pumpStopped = true
	synctest.Wait()
	info, ok := clientConn.AdaptiveBDPDebugInfo()
	require.True(t, ok)
	assertAdaptiveBDPTelemetryInvariants(t, info)
	result := adaptiveBDPLinkScenarioResult{info: info, forward: link.Counters(simnet.LinkForward), reverse: link.Counters(simnet.LinkReverse), payloadBytes: uint64(len(payload)), elapsed: link.Now(), queueDelays: queueDelays(), deliverySamples: read.samples}
	t.Logf("adaptive-bdp deterministic result: elapsed=%s goodput_bps=%d delivered_bytes=%d submitted_bytes=%d scripted_losses=%d random_losses=%d tail_drops=%d peak_queue_bytes=%d queue_delay_p50=%s queue_delay_p95=%s queue_delay_p99=%s reverse_peak_queue_bytes=%d min_rtt=%s cwnd=%d pacing_Bps=%d bandwidth_Bps=%d", result.elapsed, result.goodputBitsPerSecond(), result.forward.DeliveredBytes, result.forward.SubmittedBytes, result.forward.ScriptedLosses, result.forward.RandomLosses, result.forward.TailDrops, result.forward.PeakQueueBytes, result.queueDelayPercentile(50), result.queueDelayPercentile(95), result.queueDelayPercentile(99), result.reverse.PeakQueueBytes, result.info.MinRTT, result.info.CongestionWindow, result.info.PacingRateBytesPerSecond, result.info.BandwidthBytesPerSecond)
	return result
}

func assertAdaptiveBDPTelemetryInvariants(t *testing.T, info quic.AdaptiveBDPDebugInfo) {
	t.Helper()
	require.NotEmpty(t, info.Telemetry)
	require.Greater(t, info.MaxOutstandingSentPackets, uint64(0))
	require.Greater(t, info.MaxTrackedSentPackets, info.MaxOutstandingSentPackets, "packet-history limits must not invert")
	require.LessOrEqual(t, info.TrackedSentPackets, info.MaxTrackedSentPackets)
	var lastRound uint64
	for _, sample := range info.Telemetry {
		require.Greater(t, sample.PacingRateBytesPerSecond, uint64(0), "telemetry must never contain a zero pacing rate")
		require.GreaterOrEqual(t, sample.CongestionWindow, info.MinCwnd)
		require.LessOrEqual(t, sample.CongestionWindow, info.MaxCwnd)
		require.False(t, math.IsNaN(sample.PacingGain) || math.IsInf(sample.PacingGain, 0))
		require.False(t, math.IsNaN(sample.CwndGain) || math.IsInf(sample.CwndGain, 0))
		if sample.Event == "round" {
			require.Greater(t, sample.RoundCount, lastRound, "completed-round telemetry must be strictly monotonic")
			lastRound = sample.RoundCount
		}
	}
}

const adaptiveBDPVirtualTick = 100 * time.Microsecond

func startDeterministicLinkPump(router *simnet.DeterministicRouter) (func(), func() []time.Duration) {
	stop := make(chan struct{})
	done := make(chan struct{})
	var queueDelays []time.Duration
	go func() {
		defer close(done)
		var now time.Duration
		for {
			select {
			case <-stop:
				return
			case <-time.After(adaptiveBDPVirtualTick):
				now += adaptiveBDPVirtualTick
				router.AdvanceTo(now)
				queueDelays = append(queueDelays, router.Link().QueueDelay(simnet.LinkForward))
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}, func() []time.Duration { return slices.Clone(queueDelays) }
}

func firstAdaptiveBDPTelemetry(samples []quic.AdaptiveBDPTelemetrySample, match func(quic.AdaptiveBDPTelemetrySample) bool) (quic.AdaptiveBDPTelemetrySample, bool) {
	for _, sample := range samples {
		if match(sample) {
			return sample, true
		}
	}
	return quic.AdaptiveBDPTelemetrySample{}, false
}
