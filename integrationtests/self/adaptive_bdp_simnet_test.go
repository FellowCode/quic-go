package self_test

import (
	"bytes"
	"context"
	"errors"
	"io"
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
		timeout  time.Duration
	}{
		{"C01", 1_000_000, 20 * time.Millisecond, 1, 0, 12 * time.Second},
		{"C02", 10_000_000, 50 * time.Millisecond, 1, 0, 3 * time.Second},
		{"C03", 30_000_000, 150 * time.Millisecond, 1, 0, 3 * time.Second},
		{"C04", 100_000_000, 20 * time.Millisecond, 2, 0, 3 * time.Second},
		{"C05", 100_000_000, 200 * time.Millisecond, 1, 0, 3 * time.Second},
		// C06 is also run with the default 10,000-packet cap, as required by
		// the validation plan. The enlarged cap demonstrates the high-BDP path.
		{"C06-default", 1_000_000_000, 100 * time.Millisecond, 1, 0, 3 * time.Second},
		{"C06-large-cwnd", 1_000_000_000, 100 * time.Millisecond, 1, 100_000, 3 * time.Second},
	} {
		t.Run(tc.id, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queueBytes := tc.queueBDP * tc.capacity * uint64(tc.rtt) / uint64(time.Second) / 8
				config := simnet.DeterministicDirectionConfig{
					BandwidthBitsPerSecond: tc.capacity,
					BaseLatency:            tc.rtt / 2,
					QueueLimitBytes:        queueBytes,
				}
				result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
					linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
					startupTargetRateBps: tc.capacity,
					payloadBytes:         128 * 1024,
					maxWindowPackets:     tc.maxCwnd,
					timeout:              tc.timeout,
				})
				require.Greater(t, result.goodputBitsPerSecond(), uint64(0), "clean path must deliver application data")
				require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0), "clean path must not deadlock pacing")
				require.GreaterOrEqual(t, result.info.CongestionWindow, result.info.MinCwnd)
				require.LessOrEqual(t, result.info.CongestionWindow, result.info.MaxCwnd)
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

		stopPump()
		pumpStopped = true
		synctest.Wait()
		info, ok := clientConn.AdaptiveBDPDebugInfo()
		require.True(t, ok)
		require.Greater(t, info.PacingRateBytesPerSecond, uint64(0), "idle upload restart must not deadlock pacing")
		require.GreaterOrEqual(t, info.CongestionWindow, info.MinCwnd)
	})
}

func TestAdaptiveBDPDeterministicLinkMigrationUsesNewPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clientAddr1 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 21), Port: 9021}
		clientAddr2 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 22), Port: 9022}
		serverAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 23), Port: 9023}
		config := simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 10_000_000, BaseLatency: 10 * time.Millisecond, QueueLimitBytes: 128 * 1024}
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

		quicConfig := getQuicConfig(&quic.Config{CwndTuning: quic.CwndTuning{Enable: true, Algorithm: quic.CongestionControlAdaptiveBDP, StartupTargetRateBps: 10_000_000}})
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
		_, err = stream.Write(bytes.Repeat([]byte("m"), 64*1024))
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
		_, err = stream.Write([]byte("post-migration"))
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		require.NoError(t, <-readDone)
		synctest.Wait()
		after, ok := clientConn.AdaptiveBDPDebugInfo()
		require.True(t, ok)
		require.Equal(t, clientAddr2.String(), clientConn.LocalAddr().String())
		require.Greater(t, after.PacingRateBytesPerSecond, uint64(0))
		require.GreaterOrEqual(t, after.CongestionWindow, after.MinCwnd)
		stopPump()
		pumpStopped = true
	})
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
		before := simnet.DeterministicDirectionConfig{BandwidthBitsPerSecond: 100_000_000, BaseLatency: 15 * time.Millisecond, QueueLimitBytes: 2 * 1024 * 1024}
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
			payloadBytes:         4 * 1024 * 1024,
			configureLink: func(link *simnet.DeterministicLink) {
				link.ScheduleChange(simnet.LinkForward, 200*time.Millisecond, after)
				link.ScheduleChange(simnet.LinkReverse, 200*time.Millisecond, after)
			},
		})
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0))
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
		require.Greater(t, result.forward.PeakQueueBytes, uint64(750*1024), "scenario must create the configured deep queue")
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

func TestAdaptiveBDPDeterministicLinkQ05OutageRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The outage starts after handshake and lasts for more than three PTOs
		// on this 200 ms path, so the real loss detector has persistent-loss
		// evidence before traffic is restored.
		config := simnet.DeterministicDirectionConfig{
			BandwidthBitsPerSecond: 10_000_000,
			BaseLatency:            100 * time.Millisecond,
			QueueLimitBytes:        256 * 1024,
			LossIntervals:          []simnet.LossInterval{{Start: 300 * time.Millisecond, End: 2500 * time.Millisecond}},
		}
		result := runAdaptiveBDPDeterministicBulkTransfer(t, adaptiveBDPLinkScenario{
			linkConfig:           simnet.DeterministicLinkConfig{Forward: config, Reverse: config},
			startupTargetRateBps: 10_000_000,
			payloadBytes:         2 * 1024 * 1024,
			timeout:              6 * time.Second,
		})
		require.Greater(t, result.forward.ScriptedLosses, uint64(0), "Q05 must exercise the complete outage")
		require.Greater(t, result.goodputBitsPerSecond(), uint64(0), "traffic must recover after the outage")
		require.Greater(t, result.info.PacingRateBytesPerSecond, uint64(0), "outage recovery must not deadlock pacing")
		require.GreaterOrEqual(t, result.info.CongestionWindow, result.info.MinCwnd)
	})
}

type adaptiveBDPLinkScenario struct {
	linkConfig           simnet.DeterministicLinkConfig
	startupTargetRateBps uint64
	payloadBytes         int
	minRTTFilterWindow   time.Duration
	maxWindowPackets     uint32
	timeout              time.Duration
	configureLink        func(*simnet.DeterministicLink)
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

type adaptiveBDPLinkScenarioResult struct {
	info         quic.AdaptiveBDPDebugInfo
	forward      simnet.LinkCounters
	reverse      simnet.LinkCounters
	payloadBytes uint64
	elapsed      time.Duration
	queueDelays  []time.Duration
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

	config := getQuicConfig(&quic.Config{CwndTuning: quic.CwndTuning{
		Enable:               true,
		Algorithm:            quic.CongestionControlAdaptiveBDP,
		StartupTargetRateBps: scenario.startupTargetRateBps,
		MinRTTFilterWindow:   scenario.minRTTFilterWindow,
		MaxWindowPackets:     scenario.maxWindowPackets,
	}})
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
		data []byte
		err  error
	}
	readResultChan := make(chan readResult, 1)
	go func() {
		serverStream, err := serverConn.AcceptStream(ctx)
		if err != nil {
			readResultChan <- readResult{err: err}
			return
		}
		data, err := io.ReadAll(serverStream)
		readResultChan <- readResult{data: data, err: err}
	}()
	stream, err := clientConn.OpenStreamSync(ctx)
	require.NoError(t, err)
	_, err = stream.Write(payload)
	require.NoError(t, err)
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
	result := adaptiveBDPLinkScenarioResult{info: info, forward: link.Counters(simnet.LinkForward), reverse: link.Counters(simnet.LinkReverse), payloadBytes: uint64(len(payload)), elapsed: link.Now(), queueDelays: queueDelays()}
	t.Logf("adaptive-bdp deterministic result: elapsed=%s goodput_bps=%d delivered_bytes=%d submitted_bytes=%d scripted_losses=%d random_losses=%d tail_drops=%d peak_queue_bytes=%d queue_delay_p50=%s queue_delay_p95=%s queue_delay_p99=%s reverse_peak_queue_bytes=%d min_rtt=%s cwnd=%d pacing_Bps=%d bandwidth_Bps=%d", result.elapsed, result.goodputBitsPerSecond(), result.forward.DeliveredBytes, result.forward.SubmittedBytes, result.forward.ScriptedLosses, result.forward.RandomLosses, result.forward.TailDrops, result.forward.PeakQueueBytes, result.queueDelayPercentile(50), result.queueDelayPercentile(95), result.queueDelayPercentile(99), result.reverse.PeakQueueBytes, result.info.MinRTT, result.info.CongestionWindow, result.info.PacingRateBytesPerSecond, result.info.BandwidthBytesPerSecond)
	return result
}

func startDeterministicLinkPump(router *simnet.DeterministicRouter) (func(), func() []time.Duration) {
	const virtualTick = 100 * time.Microsecond
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
			case <-time.After(virtualTick):
				now += virtualTick
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
