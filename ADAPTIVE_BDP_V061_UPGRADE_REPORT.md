# AdaptiveBDP upstream v0.61 upgrade report

## Verdict

**Local RC gate: PASS.**

The fork now contains upstream `master` at
`9bfbf4cd052b5927e6ba31f2376493f057b1142e` (`v0.61.0` plus two commits)
while retaining the complete AdaptiveBDP history through
`882506bfa7f7d6409fea2120cd506cf5d7f5fbeb`.

The integration was performed without rebasing or rewriting the published
AdaptiveBDP commits:

- `f2f38942`: merge upstream `v0.61.0` with AdaptiveBDP;
- `2ca34f3a`: merge the two post-release upstream `master` commits.

The pre-upgrade fork state is preserved by the annotated tag
`adaptive-bdp-pre-v0.61-882506bf`.

## Merge review

The `v0.61.0` merge produced two textual conflicts:

- `connection.go`;
- `connection_test.go`.

Both conflicts were limited to imports. The resolution retained
`internal/congestion` for the AdaptiveBDP public debug and configuration
adapters and removed the obsolete `internal/flowcontrol` import after upstream
moved flow-control implementations into the root package.

The integration retains:

- selection and construction through
  `NewSentPacketHandlerWithCongestionConfig`;
- AdaptiveBDP delivery-rate sampling and application-limited classification;
- persistent-congestion accumulation and model reset;
- path-migration controller reconstruction with the original tuning;
- the public `CwndTuning` and `Conn.AdaptiveBDPDebugInfo` APIs;
- deterministic-link, stress, soak, and packet-history release gates.

It also adopts the upstream stream APIs, RESET_STREAM_AT draft-09 behavior,
qlog checksum types, flow-control relocation, MTU payload-size fix, dependency
updates, and the post-release `Conn.NextConnection` close error fix.

## Additional regression coverage

`TestWriteWithLimitAdaptiveBDP` exercises upstream `WriteWithLimit` while the
sending connection uses AdaptiveBDP and verifies live pacing and congestion
window bounds.

`TestAdaptiveBDPDeterministicLinkWriteWithLimit` sends a bulk transfer through
a real higher-level write limiter over the deterministic bottleneck. It
verifies delivery without synthetic loss or congestion evidence, non-zero
pacing, bounded cwnd, and no bandwidth downshift.

The original F15 repeated-transition soak profile was intentionally left
unchanged. The new limiter behavior is isolated in its own deterministic
regression so the historical F15 acceptance thresholds remain comparable.

## Verification

### Windows amd64, Go 1.26.2

The following commands passed:

```powershell
go test -count=1 ./internal/congestion ./internal/ackhandler .
go vet ./...
go test -count=1 ./...
go test -count=100 ./internal/congestion -run AdaptiveBDP
go test -count=3 ./integrationtests/self -run TestAdaptiveBDPDeterministicLink
$env:QUIC_GO_ADAPTIVE_BDP_SOAK_CYCLES = '25'
go test -count=1 -timeout=30m ./integrationtests/self `
  -run '^TestAdaptiveBDPSoakRepeatedTransitions$'
$env:GODEBUG = 'fips140=only'
go test -count=1 ./internal/handshake `
  -run 'TestToken|TestRetry|TestInitial|TestDecode|TestEncrypt'
```

The nested `integrationtests/fips` module also passed with
`GODEBUG=fips140=only`.

### Linux amd64, Go 1.26.5, CGO

The mandatory core race gate passed in `golang:1.26-bookworm` with
`CAP_NET_ADMIN`, which is required by the root socket-buffer tests:

```bash
CGO_ENABLED=1 go test -count=1 -race \
  ./internal/congestion ./internal/ackhandler .
```

### Linux amd64, Go 1.25

The minimum-supported-version full repository suite passed in
`golang:1.25-bookworm`:

```bash
go test -count=1 ./...
```

### Workflow validation

The dedicated AdaptiveBDP workflow was updated to
`actions/checkout@v7` and `actions/setup-go@v7`.

Standalone validation passed:

```bash
actionlint .github/workflows/adaptive-bdp-release.yml
go fix -diff ./...
```

The first remote RC lint run identified Go 1.26 `go fix` rewrites in legacy
AdaptiveBDP sources and tests. The mechanical rewrites were applied before
RC.2: integer loops now use range-over-integer where applicable, and one
non-negative duration clamp uses `max`. The full repository suite and
AdaptiveBDP deterministic tests passed after the rewrite.

RC.3 resolves the remaining upstream `golangci-lint` findings: explicit
baseline cases were added to exhaustive switches, redundant conversions were
removed, a boolean expression was simplified, and unused AdaptiveBDP helpers
were deleted. These changes do not alter controller decisions or tuning
constants. The full Windows suite, AdaptiveBDP stress/deterministic/soak
gates, Linux Go 1.26 race core, and Linux Go 1.25 full suite all passed again
after this cleanup.

## Release handoff

The source tree is ready for RC validation. Remote GitHub Actions and any deployment-specific
VPN / NIC-offload smoke tests should run against that exact tag before
promoting it to `v0.61.0-adaptive.1`.
