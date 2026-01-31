# ChainLightning Installation Guide

Step-by-step instructions for setting up ChainLightning on a client router and VPS server.

## Network Topology

```
                    Your LAN
                       │
              ┌────────┴────────┐
              │  Client Router  │
              │   (Linux box)   │
              └──┬──┬──┬──┬──┬──┘
                 │  │  │  │  │
            WAN0 WAN1 WAN2 WAN3 WAN4
            (ADSL)(Star)(ADSL)(Star)(ADSL)
                 │  │  │  │  │
                 └──┴──┴──┴──┘
                       │
                   Internet
                       │
              ┌────────┴────────┐
              │   VPS Server    │
              │  (Hetzner, DO,  │
              │   Vultr, etc.)  │
              └─────────────────┘
```

The client router has multiple WAN interfaces. ChainLightning bonds them into a single tunnel interface (`tun-bond`) that appears as a regular network interface to your LAN.

## Prerequisites

### Both Machines

- Linux (Debian/Ubuntu recommended, any distro works)
- Rust toolchain 1.75+ (install: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- TUN kernel module: `sudo modprobe tun`
- Build tools: `sudo apt install build-essential pkg-config`

### Server (VPS)

- Public IP address
- UDP ports 9001-9005 open in firewall
- IP forwarding enabled: `sysctl -w net.ipv4.ip_forward=1`

### Client (Router)

- Multiple WAN interfaces (physical or VLAN)
- Root access or CAP_NET_ADMIN capability
- IP forwarding enabled for LAN routing

## Step 1: Build

On both machines:

```bash
git clone https://github.com/cronos3k/chainlightning.git
cd chainlightning
cargo build --release
```

## Step 2: Server Setup

### Firewall

Open the UDP ports used for bonding (default: 9001-9005):

```bash
# UFW
sudo ufw allow 9001:9005/udp

# iptables
sudo iptables -A INPUT -p udp --dport 9001:9005 -j ACCEPT

# nftables
sudo nft add rule inet filter input udp dport 9001-9005 accept
```

### IP Forwarding and NAT

Enable forwarding and masquerading for tunnel traffic:

```bash
# Enable IP forwarding
echo 'net.ipv4.ip_forward = 1' | sudo tee -a /etc/sysctl.d/99-chainlightning.conf
sudo sysctl -p /etc/sysctl.d/99-chainlightning.conf

# NAT for tunnel traffic (replace eth0 with your server's internet interface)
sudo iptables -t nat -A POSTROUTING -s 10.99.0.0/24 -o eth0 -j MASQUERADE
```

### Configuration

Copy `config.example.yaml` to `config.yaml` and adjust link capacities to match your client's WAN links.

### Run

```bash
sudo ./target/release/server
```

The server creates a `tun-bond` interface with IP `10.99.0.1/24` and listens on UDP ports 9001-9005.

## Step 3: Client Setup

### Configuration

Edit `config.yaml`:

1. Set `SERVER_IP` in the client binary configuration (or environment variable) to your VPS public IP
2. Adjust `link_tiers` to match your WAN interfaces:
   - Set correct `capacity_down_bps` and `capacity_up_bps` for each link
   - Set `link_type` ("adsl", "starlink", "fiber", "lte")
   - Set `realtime_eligible` based on latency characteristics

### Identify WAN Interfaces

List your network interfaces:

```bash
ip link show
ip route show table all
```

Note which interface connects to which ISP. The client binds to each WAN interface in order (L0, L1, L2, ...).

### Run

```bash
sudo ./target/release/client
```

The client creates a `tun-bond` interface with IP `10.99.0.2/24`, connects to the server on each WAN link, and starts bonding.

### Verify

```bash
# Check tunnel interface exists
ip addr show tun-bond

# Ping through tunnel
ping 10.99.0.1

# Check link status in logs
tail -f /tmp/bonding.log | grep RateCtrl
```

## Step 4: Routing

### Route All Traffic Through Tunnel

```bash
# Default route via tunnel
sudo ip route add default via 10.99.0.1 dev tun-bond table 100
sudo ip rule add from 10.99.0.0/24 table 100

# Make sure WAN links still use their own gateways for tunnel traffic
# (Add routes for each WAN's gateway so tunnel UDP packets don't loop)
sudo ip route add <SERVER_PUBLIC_IP>/32 via <WAN0_GATEWAY> dev <WAN0_IFACE>
```

### Route Specific Subnets

```bash
# Only route LAN traffic through tunnel
sudo ip route add default via 10.99.0.1 dev tun-bond table 100
sudo ip rule add from 192.168.1.0/24 table 100  # Your LAN subnet
```

### Policy-Based Routing

For selective routing, use iptables marks:

```bash
# Mark traffic from specific hosts
sudo iptables -t mangle -A PREROUTING -s 192.168.1.100 -j MARK --set-mark 1
sudo ip rule add fwmark 1 table 100
sudo ip route add default via 10.99.0.1 dev tun-bond table 100
```

## Step 5: Systemd Service (Production)

### Server Service

Create `/etc/systemd/system/chainlightning-server.service`:

```ini
[Unit]
Description=ChainLightning Bonding Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/opt/chainlightning/server
WorkingDirectory=/opt/chainlightning
Restart=always
RestartSec=5
LimitNOFILE=65536

# NAT setup
ExecStartPre=/sbin/iptables -t nat -A POSTROUTING -s 10.99.0.0/24 -o eth0 -j MASQUERADE
ExecStopPost=/sbin/iptables -t nat -D POSTROUTING -s 10.99.0.0/24 -o eth0 -j MASQUERADE

[Install]
WantedBy=multi-user.target
```

### Client Service

Create `/etc/systemd/system/chainlightning-client.service`:

```ini
[Unit]
Description=ChainLightning Bonding Client
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/opt/chainlightning/client
WorkingDirectory=/opt/chainlightning
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### Enable and Start

```bash
sudo systemctl daemon-reload
sudo systemctl enable chainlightning-server  # or client
sudo systemctl start chainlightning-server
sudo systemctl status chainlightning-server
```

## Testing

### Basic Connectivity

```bash
# From client, ping server through tunnel
ping -c 10 10.99.0.1

# Check all links are active
grep "RateCtrl" /tmp/bonding.log | tail -1
# Expected: all links show "RUN" state
```

### Bandwidth Test

Install iperf3 on both machines:

```bash
sudo apt install iperf3
```

Run tests:

```bash
# On server
iperf3 -s -B 10.99.0.1

# On client - download test
iperf3 -c 10.99.0.1 -R -P 4 -t 30

# On client - upload test
iperf3 -c 10.99.0.1 -P 4 -t 30

# On client - bidirectional test
iperf3 -c 10.99.0.1 --bidir -t 30
```

### Monitor Link Health

Watch the rate controller status:

```bash
# Real-time monitoring
tail -f /tmp/bonding.log | grep RateCtrl

# Example output:
# RateCtrl: L0[60/60Mbps|30ms|SL0.0%|RL0.0%|c1.00|RUN|w:60]
#           L1[198/220Mbps|36ms|SL0.0%|RL1.6%|c1.00|RUN|w:198]
#
# Fields: Link[currentRate/maxRate|RTT|SendLoss|RecvLoss|confidence|state|weight]
```

## Troubleshooting

### TUN interface not created

```bash
# Check TUN module
lsmod | grep tun
sudo modprobe tun

# Check permissions
ls -la /dev/net/tun
# Should be: crw-rw-rw- 1 root root 10, 200 ...
```

### Client can't connect to server

```bash
# Test UDP connectivity on each port
for port in 9001 9002 9003 9004 9005; do
    echo "test" | nc -u -w1 <SERVER_IP> $port && echo "Port $port: OK"
done

# Check server is listening
ss -ulnp | grep 900
```

### Low throughput

1. Check if all links show "RUN" state in logs
2. Verify link capacities in config match actual ISP speeds
3. Run `iperf3` directly on each WAN interface (without tunnel) to baseline
4. Check for packet loss: `ping -c 100 10.99.0.1` should show 0% loss
5. Increase `reorder_timeout_ms` if links have very different latencies

### Links going DOWN

If links frequently go DOWN and recover:

1. Check physical connectivity on that WAN interface
2. Increase `down_timeout_ms` in rate control config
3. Check if the ISP has intermittent connectivity issues
4. Look at RTT values - sudden RTT spikes indicate link problems

### High retransmits in iperf3

Some retransmits are normal with multi-path bonding due to reordering. If excessive:

1. Enable sync: `enable_sync: true`
2. Increase `reorder_timeout_ms`
3. Try `strategy: "tiered_fill"` which prefers low-latency links first
