# AdaptiveBDP F15 race, soak, and release report

## Verdict

**F15: PASS. Production release gate: GO.**

This verdict covers the AdaptiveBDP work packages F11-F15. The release commit
is the commit containing this report. F14's deterministic network evidence is
recorded separately in `ADAPTIVE_BDP_F14_REPORT.md` and its schema-v2 JSON
artifact.

## Source and environments

- Starting revision: `2ea2711c`
- Windows: Go 1.26.2, `windows/amd64`, CGO disabled
- Linux: Go 1.26.5, `linux/amd64`, CGO enabled, kernel 6.6
- Linux verification image:
  `golang:1.26-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651`
- CI target: GitHub-hosted `ubuntu-latest` and `windows-latest`, Go 1.26.x

The local Linux race verification used Docker `linux/amd64`. Because the
container ran as root, it was granted `CAP_NET_ADMIN` for the two root-only
socket-buffer tests. GitHub-hosted unit jobs run as a non-root user and retain
the repository's existing separate `sudo` root-test step.

## Root cause

F15 was incomplete for four concrete reasons:

1. no existing job invoked the required Linux/CGO core race command, and an
   existing shuffled-test step was misleadingly named as a race job;
2. no dedicated CI workflow enforced the core race, static, cross-platform
   full-suite, 100-repeat stress, deterministic-network, and soak gates;
3. no repeated-transition soak exercised capacity, RTT, loss, idle, migration,
   queue, packet-history, ProbeRTT, and resource-growth invariants together;
4. packet-history limits and current occupancy were not available in runtime
   AdaptiveBDP diagnostics, so a live limit inversion could not fail the soak.

## Implementation

- `.github/workflows/adaptive-bdp-release.yml` adds all mandatory F15 jobs.
- `.github/workflows/unit.yml` labels its existing non-race job accurately;
  the dedicated workflow owns the explicitly scoped Linux/CGO core race gate.
- `integrationtests/self/adaptive_bdp_soak_test.go` adds the configurable
  repeated-transition soak.
- The real-QUIC migration scenario was factored into a reusable helper so each
  soak cycle creates a model, migrates, and proves new-controller state.
- The bulk-transfer harness supports explicit write pauses, providing real
  idle/restart epochs rather than inferring idle from sparse traffic.
- Public/internal AdaptiveBDP debug info now reports maximum outstanding,
  maximum tracked, and current tracked packet counts.
- The shuffled release run also removed two test-order assumptions: the
  one-third handshake-loss model now uses a local seeded RNG and honors its
  requested direction, and graceful listener shutdown waits for the long
  request handler to start instead of sleeping for 10 ms.
- A rare T02 deadline miss showed that the one-second ProbeUp interval left
  almost no application-goodput margin at the five-second boundary. The
  default is now 900 ms, with a unit assertion and 100 consecutive isolated
  transition passes.
- Migration validation now sends and confirms post-switch traffic before
  sampling the new controller, eliminating a race where path validation alone
  had not yet completed its first telemetry round.

## Soak design and invariants

The CI soak uses 25 measured cycles plus one warmup cycle. Every cycle includes:

- eight scheduled capacity/RTT/loss configuration transitions;
- two idle write pauses;
- two exact ten-packet forward loss bursts;
- a separate real AddPath/Probe/Switch migration connection.

One CI run therefore covers 208 scheduled link transitions, 52 idle epochs,
520 exact scripted packet losses, and 26 migrations. Windows additionally ran
the 25-cycle soak twice, for 50 measured cycles.

The soak fails on:

- zero pacing;
- cwnd outside configured bounds;
- `maxTracked <= maxOutstanding` or current tracking above the maximum;
- NaN or infinite pacing/cwnd gains;
- retry before the two-second backoff after an inconclusive ProbeRTT;
- final bandwidth retaining the old 100 Mbit/s model after a 500 ms filter
  window on the final 10 Mbit/s epoch;
- duplicate telemetry-observable round-gated transitions;
- peak queue above the configured 1 MiB maximum;
- more than eight retained goroutines, 32 MiB live heap, or 50,000 live
  objects above the warmed baseline across repeated connections.

## Commands and results

### Linux amd64 / CGO

All commands passed:

```bash
CGO_ENABLED=1 go test -count=1 -race ./internal/congestion ./internal/ackhandler .
go vet ./...
go test -count=1 ./...
go test -count=100 ./internal/congestion -run AdaptiveBDP
go test -count=3 ./integrationtests/self -run TestAdaptiveBDPDeterministicLink
QUIC_GO_ADAPTIVE_BDP_SOAK_CYCLES=25 \
  go test -count=1 -timeout=30m ./integrationtests/self \
  -run '^TestAdaptiveBDPSoakRepeatedTransitions$'
```

The new workflow passed standalone `actionlint`. Repository-wide actionlint
still reports pre-existing findings in unrelated workflows; none are in
`adaptive-bdp-release.yml`.

### Windows amd64

All commands passed:

```powershell
go test -count=1 ./internal/congestion -run ProbeRTT
go test -count=100 ./internal/congestion -run ProbeRTT
go test -count=1 . -run AdaptiveBDPCwndTuningValidation
go test -count=1 ./internal/ackhandler -run DynamicOutstandingPacketLimit
go test -count=1 ./internal/congestion -run MaxWindowPackets
go test -count=20 ./integrationtests/self -run TestGracefulShutdownLongLivedRequest
go test -count=20 ./integrationtests/self -run TestMITCorruptPackets
go test -shuffle=on -count=10 ./integrationtests/self
go test -count=100 ./internal/congestion -run AdaptiveBDP
go vet ./...
go test -count=1 ./...
```

The Windows soak also passed twice:

```powershell
$env:QUIC_GO_ADAPTIVE_BDP_SOAK_CYCLES = '25'
go test -count=2 -timeout=30m ./integrationtests/self `
  -run '^TestAdaptiveBDPSoakRepeatedTransitions$'
```

## F13 evidence carried into release

The historical pre-fix revision `1f90e224` was rechecked on the current
Windows host:

- `TestGracefulShutdownLongLivedRequest`: 1 failure in 20 runs (5%);
- `TestMITCorruptPackets`: 0 failures in 20 isolated runs, while the original
  audit had observed one timeout in a full-suite run.

At the release tree both tests passed 20/20. The shutdown root cause was a
platform-invalid symmetric timing tolerance plus a leaked ticker; the repaired
assertion checks the actual contract (never terminate before the shutdown
deadline) and stops the ticker. The MITM test used concurrent global random
state; it now uses a fixed per-test generator protected by a mutex.

The first final shuffled run found two additional order/load assumptions:

- the handshake one-third-loss callback used the package-global RNG and
  ignored its requested direction;
- listener graceful shutdown could begin before the long request entered its
  handler.

After repair, the handshake-loss matrix passed 50/50 and listener graceful
shutdown passed 100/100. The first repeated shuffled retry then exposed a rare
T02 boundary miss: pacing recovered at 4.948 seconds after the link change,
but the best 200 ms application window reached only 79.844 Mbit/s. Reducing
the default ProbeUp interval from one second to 900 ms moved discovery safely
inside the bound; T02 passed 100/100 and the full F14 matrix passed 3/3.

The 25-cycle soak subsequently exposed a migration observation race on cycle
20: AddPath/Probe/Switch had completed, but the new controller had not yet
produced its first ACK-round telemetry. Confirmed post-switch traffic made the
observation causal; the migration helper passed 100/100, soak25 passed, and
the final shuffled integration suite passed 10/10.

## Review and remaining limitations

No unresolved Critical or High AdaptiveBDP audit issue remains. The final diff
was checked with `git diff --check`, formatted with `gofmt`, covered by
Windows/Linux full-suite runs, and committed.

Known limitations:

- deterministic simnet validates controller behavior, not every real Internet,
  VPN, NIC-offload, or middlebox implementation;
- unequal-RTT fairness remains reported observationally because the validation
  plan defines no numeric acceptance threshold;
- C01 requires the documented low-BDP tuning from the F14 report;
- the mandatory race gate intentionally covers the core congestion,
  ackhandler, and root packages. A diagnostic `go test -race ./...` is not a
  release command: it exposes pre-existing test-harness races in FIPS/simnet
  tests plus race-instrumentation timing/allocation assumptions outside the
  AdaptiveBDP audit scope;
- GitHub Actions will re-run the committed gates after push; local Linux
  Docker runs provide the pre-push Linux/amd64 evidence.

## Package handoff

- Package: F15
- Result: PASS / GO
- Tests added: repeated-transition soak, live packet-history diagnostics,
  deterministic directional-loss coverage, ProbeUp default coverage, and
  confirmed post-migration controller-reset coverage
- Remaining release blocker: none
- Next package: none; F11-F15 release package is complete
