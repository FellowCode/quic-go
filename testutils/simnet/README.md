# simnet

This package is based on @MarcoPolo's [simnet](https://github.com/marcopolo/simnet) package.

A small Go library for simulating packet networks in-process. It provides
drop-in `net.PacketConn` endpoints connected through configurable virtual links
with latency and MTU constraints. Useful for testing networking code
without sockets or root privileges.

- **Drop-in API**: implements `net.PacketConn`
- **Realistic links**: per-direction latency and MTU
- **Packet queuing**: priority queue for scheduled packet delivery
- **Routers**: perfect delivery, fixed-latency, simple firewall/NAT-like routing
- **Deterministic testing**: opt-in `synctest`-based tests for time control

## Deterministic bottleneck links

`DeterministicLink` is the virtual-time bottleneck used by AdaptiveBDP network
validation. It has no background goroutines and never waits for wall-clock
time: submit a packet with `Send`, then move the simulation with `AdvanceTo`.
Each direction independently supports serialization-rate shaping, finite byte
queues with tail drop, ECN thresholds, base-latency changes, fixed-seed random
loss, scripted loss/outage intervals, reordering, duplication, occupancy, and
per-direction counters. `ScheduleChange` applies a complete direction config
at an exact virtual time, so capacity and base-RTT transitions are reproducible.

`NewDeterministicRouter` adapts this link to `net.PacketConn` endpoints. Tests
explicitly call `AdvanceTo` between protocol steps, and can sample returned
delivery events and link counters as scenario telemetry.


