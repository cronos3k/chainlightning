# ChainLightning

**High-performance multi-WAN UDP bonding tunnel for Linux**

Aggregate bandwidth across multiple internet connections (ADSL, Starlink, LTE, fiber) into a single high-throughput link. ChainLightning creates a TUN interface that transparently bonds traffic across all your WAN links.

## Performance

Tested with 5 WAN links (3x ADSL + 2x Starlink):

| Test | Streams | Throughput |
|------|---------|------------|
| Download | 4 | **198 Mbps** |
| Upload | 4 | **54 Mbps** |
| Bidirectional download | 1 | **106 Mbps** |
| Bidirectional upload | 1 | **25 Mbps** |

## Features

- **Multi-WAN aggregation** - Bond 5+ links of any type (DSL, fiber, Starlink, LTE, cable)
- **Adaptive rate control** - Glorytun/MUD-style timing-based congestion detection with directional loss tracking and exponential decay
- **Traffic-Aware Probe Attenuation (TAPA)** - Intelligent probe confidence scaling prevents phantom loss when links are heavily loaded
- **Intelligent scheduling** - Four strategies: tiered fill, weighted round-robin, single-best, pure round-robin
- **Flow classification** - Automatically routes small flows to single best link, bulk transfers across all links
- **Automatic failover** - Detects link failures within 5 seconds, redistributes traffic, recovers automatically
- **Real-time link monitoring** - Per-link RTT, loss (send and receive direction), congestion state, weight
- **Chunk aggregation** - Configurable aggregation reduces per-packet overhead for bulk transfers
- **Realtime traffic priority** - VoIP/gaming packets routed to low-latency links only
- **Hot-reloadable config** - YAML configuration, no recompilation needed
- **A/B testing framework** - Compare configurations with automated bandwidth tests

## Architecture

```
                          Client Router                          VPS Server
                     ┌─────────────────────┐            ┌─────────────────────┐
                     │                     │            │                     │
   LAN traffic ──────┤  tun-bond (10.99.0.2)│            │  tun-bond (10.99.0.1)├──── Internet
                     │         │           │            │         │           │
                     │    ┌────┴────┐      │            │    ┌────┴────┐      │
                     │    │ Bonding │      │            │    │ Bonding │      │
                     │    │  Core   │      │            │    │  Core   │      │
                     │    └────┬────┘      │            │    └────┬────┘      │
                     │   ┌─┬──┴──┬─┬─┐    │            │   ┌─┬──┴──┬─┬─┐    │
                     │   │ │  │  │ │ │    │            │   │ │  │  │ │ │    │
                     └───┤L0│L1│L2│L3│L4├───┘            └───┤ UDP Listeners ├───┘
                         └┬─┴┬─┴┬─┴┬─┴┬┘                    └───────────────┘
                          │  │  │  │  │
                     ADSL1 Star1 ADSL2 Star2 ADSL3
```

### Crate Structure

```
chainlightning_v4/
├── common/          # Shared types: protocol, config, metrics
├── core/            # Core algorithms: rate controller, scheduler, receiver
├── testing/         # Test framework and scenarios
├── client/          # Client binary (runs on your router)
├── server/          # Server binary (runs on your VPS)
├── config.example.yaml
└── Cargo.toml       # Workspace manifest
```

## Quick Start

### Prerequisites

- Linux (both client and server)
- Rust 1.75+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- TUN/TAP kernel support (`modprobe tun`)
- Root or `CAP_NET_ADMIN` capability

### Build

```bash
git clone https://github.com/cronos3k/chainlightning.git
cd chainlightning
cargo build --release
```

Binaries are at `target/release/server` and `target/release/client`.

### Deploy

1. Copy `server` binary and `config.yaml` to your VPS
2. Copy `client` binary and `config.yaml` to your router
3. Start server: `sudo ./server`
4. Start client: `sudo ./client`

See [INSTALLATION.md](INSTALLATION.md) for detailed setup instructions including routing, systemd services, and troubleshooting.

## Configuration

Copy `config.example.yaml` to `config.yaml` and customize for your network. Key sections:

### Link Scheduler

```yaml
link_scheduler:
  strategy: "tiered_fill"    # or "weighted", "round_robin", "single_best"
  enable_sync: true
  flow_affinity: true
  link_tiers:
    - link_id: 0
      priority: 1              # Lower = higher priority
      capacity_down_bps: 7812500
      capacity_up_bps: 1587500
      link_type: "adsl"
```

### Flow Classifier

Small flows (below 66% of fastest link capacity) stay on a single link for best latency. Bulk transfers automatically spread across all links.

```yaml
flow_classifier:
  single_link_threshold: 0.66
  multi_link_threshold: 0.90
  monitor_duration_ms: 2000
```

### Realtime Traffic

VoIP, gaming, and SSH traffic is automatically routed to low-latency links only:

```yaml
realtime:
  realtime_udp_ports: [5060, 5061, 3478, 16384, 27015]
  realtime_tcp_ports: [22]
  force_adsl_only: true
```

## How Rate Control Works

ChainLightning uses a **Glorytun/MUD-inspired** adaptive rate control algorithm with the following key concepts:

### Timing-Based Congestion Detection

Instead of measuring throughput (which fails with low traffic), the rate controller compares **how fast we send** vs **how fast the remote receives**. If the remote receives more than 12.5% slower than we send, congestion is detected and the rate is reduced.

### Directional Loss Tracking

Loss is tracked separately for each direction:
- **Send loss**: packets we sent vs packets the remote received from us
- **Receive loss**: packets the remote sent vs packets we received

This prevents asymmetric traffic (upload-heavy or download-heavy) from being misinterpreted as packet loss.

### TAPA (Traffic-Aware Probe Attenuation)

When a link is heavily loaded, probe measurements become unreliable (probes compete with data traffic for bandwidth). TAPA calculates a **probe confidence** score based on traffic load:

- Below 20% capacity: confidence = 1.0 (fully trusted)
- Above 70% capacity: confidence = 0.1 (minimal trust)
- Between: linear interpolation

Low confidence gates loss accumulation, bypasses congestion detection, and dampens state transitions. This prevents the "phantom loss" problem where heavy traffic causes false loss readings.

### State Machine

Each link operates in one of four states:

```
Running  ──(loss)──>  Lossy  ──(severe loss)──>  Down
   ^                    │                          │
   │                    │                          v
   └──(loss clears)─────┘                       Probing
                                                   │
   <──────────(recovery probes succeed)────────────┘
```

### Safeguards

- Rate floor: 10% of configured max (prevents death spiral)
- Rate changes capped at +/-10% per cycle (no sudden jumps)
- 1000 packet minimum before loss calculation (no premature judgments)
- 15/16 exponential decay on loss accumulators (old problems fade)
- DOWN links still receive probes (recovery always possible)

## Monitoring

ChainLightning logs link status every 5 seconds:

```
RateCtrl: L0[60/60Mbps|30ms|SL0.0%|RL0.0%|c1.00|RUN|w:60]
          L1[198/220Mbps|36ms|SL0.0%|RL1.6%|c1.00|RUN|w:198]
```

Fields: `Link[rate/max|RTT|SendLoss|RecvLoss|confidence|state|weight]`

## Testing

Run the full test suite:

```bash
cargo test --workspace
```

This runs 42 tests including:
- Unit tests for rate controller, protocol, scheduler
- Integration tests simulating 1GB downloads with link degradation
- Congestion detection and recovery scenarios
- Link failure and automatic recovery
- Rate floor (death spiral prevention)
- Probe wire format roundtrip

## Project Structure

| Crate | Purpose |
|-------|---------|
| `chainlightning_common` | Protocol definitions, config parsing, metrics |
| `chainlightning_core` | Rate controller, link scheduler, flow classifier, chunk aggregator, receiver |
| `chainlightning_testing` | A/B test framework and scenario definitions |
| `chainlightning_client` | Client binary - connects to server via multiple WAN links |
| `chainlightning_server` | Server binary - accepts connections, forwards to internet |

## Contributing

Contributions are welcome. Please:

1. Fork the repository
2. Create a feature branch
3. Run `cargo test --workspace` and ensure all tests pass
4. Submit a pull request with a clear description

## License

MIT License. See [LICENSE](LICENSE) for details.
