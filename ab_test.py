#!/usr/bin/env python3
"""
A/B Testing Framework for ChainLightning v4 Configuration Optimization.

This script automates testing different configuration parameters across
the server and client machines. It rebuilds, restarts services, and runs
iperf3 + ping benchmarks for each configuration variant.

Requirements:
  - Python 3.8+
  - paramiko (pip install paramiko)
  - iperf3 installed on both server and client machines
  - SSH access to both machines

Configuration:
  Set the following environment variables before running:
    SSH_SERVER_HOST   - Server (VPS) IP address
    SSH_SERVER_USER   - Server SSH username (default: root)
    SSH_CLIENT_HOST   - Client (router) IP address
    SSH_CLIENT_USER   - Client SSH username (default: dev)
    SSH_KEY_PATH      - Path to SSH private key (optional)
    SUDO_PASSWORD     - Client sudo password (if needed)

  Or provide an ssh_config.json file (see --config flag).

Usage:
  python ab_test.py
  python ab_test.py --config /path/to/ssh_config.json
"""

import os
import sys
import time
import json
import re
import argparse
import subprocess
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class TestResult:
    config_name: str
    params: Dict[str, Any]
    download_mbps: float
    upload_mbps: float
    ping_loss_pct: float
    ping_avg_ms: float
    retransmits: int

class SSHRunner:
    """Simple SSH command runner using subprocess ssh or paramiko."""

    def __init__(self, config_path: Optional[str] = None):
        self.hosts = {}

        if config_path and os.path.exists(config_path):
            with open(config_path) as f:
                self.hosts = json.load(f)
        else:
            # Load from environment variables
            self.hosts = {
                'server': {
                    'host': os.environ.get('SSH_SERVER_HOST', ''),
                    'user': os.environ.get('SSH_SERVER_USER', 'root'),
                    'key': os.environ.get('SSH_KEY_PATH', ''),
                },
                'client': {
                    'host': os.environ.get('SSH_CLIENT_HOST', ''),
                    'user': os.environ.get('SSH_CLIENT_USER', 'dev'),
                    'key': os.environ.get('SSH_KEY_PATH', ''),
                }
            }

        if not self.hosts.get('server', {}).get('host'):
            print("ERROR: SSH_SERVER_HOST not set. Configure via environment or --config.")
            sys.exit(1)
        if not self.hosts.get('client', {}).get('host'):
            print("ERROR: SSH_CLIENT_HOST not set. Configure via environment or --config.")
            sys.exit(1)

    def execute(self, target: str, command: str, timeout: int = 120) -> Dict[str, str]:
        """Execute command on target host via SSH."""
        # Map legacy names
        host_key = target
        if target == 'datacenter':
            host_key = 'server'
        elif target == 'srvr':
            host_key = 'client'

        cfg = self.hosts.get(host_key, self.hosts.get(target, {}))
        host = cfg.get('host', '')
        user = cfg.get('user', 'root')
        key = cfg.get('key', '')

        ssh_cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=10']
        if key:
            ssh_cmd.extend(['-i', key])
        ssh_cmd.append(f'{user}@{host}')
        ssh_cmd.append(command)

        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=timeout)
            return {'stdout': result.stdout, 'stderr': result.stderr, 'returncode': str(result.returncode)}
        except subprocess.TimeoutExpired:
            return {'stdout': '', 'stderr': 'Command timed out', 'returncode': '1'}
        except Exception as e:
            return {'stdout': '', 'stderr': str(e), 'returncode': '1'}

    def close(self):
        pass


class ABTester:
    def __init__(self, config_path: Optional[str] = None):
        self.ssh = SSHRunner(config_path)
        self.results: List[TestResult] = []
        self.sudo_password = os.environ.get('SUDO_PASSWORD', '')

    def update_config(self, params: Dict[str, Any]):
        """Update config.rs with new parameters and rebuild"""
        # Read current config
        r = self.ssh.execute('datacenter', 'cat /root/chainlightning_v4/common/src/config.rs')
        config = r['stdout']

        # Update parameters using sed
        for key, value in params.items():
            if key == 'min_chunk_size':
                cmd = f"sed -i 's/min_chunk_size: [0-9]*/min_chunk_size: {value}/' /root/chainlightning_v4/common/src/config.rs"
            elif key == 'max_chunk_size':
                cmd = f"sed -i 's/max_chunk_size: [0-9]*/max_chunk_size: {value}/' /root/chainlightning_v4/common/src/config.rs"
            elif key == 'aggregation_timeout_ms':
                cmd = f"sed -i 's/aggregation_timeout_ms: [0-9]*/aggregation_timeout_ms: {value}/' /root/chainlightning_v4/common/src/config.rs"
            elif key == 'reorder_timeout_ms':
                cmd = f"sed -i 's/reorder_timeout_ms: [0-9]*/reorder_timeout_ms: {value}/' /root/chainlightning_v4/common/src/config.rs"
            elif key == 'enable_sync':
                val = 'true' if value else 'false'
                cmd = f"sed -i 's/enable_sync: [a-z]*/enable_sync: {val}/' /root/chainlightning_v4/common/src/config.rs"
            else:
                continue
            self.ssh.execute('datacenter', cmd)

        # Copy to client
        self.ssh.execute('datacenter', 'cat /root/chainlightning_v4/common/src/config.rs > /tmp/config.rs')
        r = self.ssh.execute('datacenter', 'cat /tmp/config.rs')

        # Write to client via base64
        import base64
        config_b64 = base64.b64encode(r['stdout'].encode()).decode()
        self.ssh.execute('srvr', f'echo "{config_b64}" | base64 -d > /home/dev/chainlightning_v4/common/src/config.rs')

    def rebuild(self):
        """Rebuild on both machines"""
        print("  Building on datacenter...")
        r = self.ssh.execute('datacenter',
            'cd /root/chainlightning_v4 && /root/.cargo/bin/cargo build --release 2>&1 | tail -5',
            timeout=300)
        if 'error' in r['stdout'].lower():
            print(f"  BUILD ERROR: {r['stdout']}")
            return False

        print("  Building on srvr...")
        r = self.ssh.execute('srvr',
            'cd /home/dev/chainlightning_v4 && /home/dev/.cargo/bin/cargo build --release 2>&1 | tail -5',
            timeout=300)
        if 'error' in r['stdout'].lower():
            print(f"  BUILD ERROR: {r['stdout']}")
            return False
        return True

    def restart_services(self):
        """Restart server and client"""
        # Kill existing
        self.ssh.execute('datacenter', 'pkill -9 server || true; ip link del tun-bond 2>/dev/null || true')
        sudo_prefix = f'echo {self.sudo_password} | sudo -S' if self.sudo_password else 'sudo'
        self.ssh.execute('srvr', f'{sudo_prefix} pkill -9 client || true; {sudo_prefix} ip link del tun-bond 2>/dev/null || true')
        time.sleep(1)

        # Start server
        self.ssh.execute('datacenter',
            'cd /root/chainlightning_v4 && nohup ./target/release/server > /tmp/chainlightning_server.log 2>&1 &')
        time.sleep(2)

        # Start client
        self.ssh.execute('srvr',
            f'cd /home/dev/chainlightning_v4 && {sudo_prefix} nohup ./target/release/client > /tmp/chainlightning_client.log 2>&1 &')
        time.sleep(3)

        # Verify started
        r = self.ssh.execute('datacenter', 'pgrep server')
        if not r['stdout'].strip():
            print("  ERROR: Server not running")
            return False
        r = self.ssh.execute('srvr', 'pgrep client')
        if not r['stdout'].strip():
            print("  ERROR: Client not running")
            return False
        return True

    def run_ping_test(self) -> tuple:
        """Run ping test, return (loss_pct, avg_ms)"""
        r = self.ssh.execute('srvr', 'ping -c 20 -i 0.2 10.99.0.1')
        output = r['stdout']

        # Parse loss
        loss_match = re.search(r'(\d+)% packet loss', output)
        loss_pct = float(loss_match.group(1)) if loss_match else 100.0

        # Parse avg RTT
        rtt_match = re.search(r'rtt min/avg/max/mdev = [\d.]+/([\d.]+)/', output)
        avg_ms = float(rtt_match.group(1)) if rtt_match else 999.0

        return loss_pct, avg_ms

    def run_iperf_test(self, duration=10, streams=4, reverse=False) -> tuple:
        """Run iperf test, return (mbps, retransmits)"""
        # Make sure iperf server is running
        self.ssh.execute('datacenter', 'pkill iperf3 2>/dev/null; nohup iperf3 -s -B 10.99.0.1 > /dev/null 2>&1 &')
        time.sleep(1)

        cmd = f'iperf3 -c 10.99.0.1 -t {duration} -P {streams}'
        if reverse:
            cmd += ' -R'

        r = self.ssh.execute('srvr', cmd, timeout=duration + 30)
        output = r['stdout']

        # Parse results - look for SUM receiver line
        mbps = 0.0
        retrans = 0

        # Find SUM line with receiver
        for line in output.split('\n'):
            if '[SUM]' in line and 'receiver' in line:
                # Extract Mbits/sec
                mbps_match = re.search(r'([\d.]+)\s*Mbits/sec', line)
                if mbps_match:
                    mbps = float(mbps_match.group(1))
            elif '[SUM]' in line and 'sender' in line:
                # Extract retransmits
                parts = line.split()
                for i, p in enumerate(parts):
                    if 'Mbits/sec' in p or p == 'Mbits/sec':
                        # Retransmits is usually after bitrate
                        try:
                            retrans = int(parts[i+1])
                        except:
                            pass

        return mbps, retrans

    def run_test(self, config_name: str, params: Dict[str, Any]) -> TestResult:
        """Run a complete A/B test with given parameters"""
        print(f"\n=== Testing: {config_name} ===")
        print(f"  Params: {params}")

        # Update config
        print("  Updating config...")
        self.update_config(params)

        # Rebuild
        if not self.rebuild():
            return None

        # Restart
        print("  Restarting services...")
        if not self.restart_services():
            return None

        # Wait for stabilization
        time.sleep(3)

        # Run tests
        print("  Running ping test...")
        loss_pct, avg_ms = self.run_ping_test()
        print(f"    Ping: {loss_pct}% loss, {avg_ms:.1f}ms avg")

        print("  Running upload test...")
        upload_mbps, upload_retrans = self.run_iperf_test(duration=10, reverse=False)
        print(f"    Upload: {upload_mbps:.1f} Mbps, {upload_retrans} retransmits")

        print("  Running download test...")
        download_mbps, download_retrans = self.run_iperf_test(duration=10, reverse=True)
        print(f"    Download: {download_mbps:.1f} Mbps, {download_retrans} retransmits")

        result = TestResult(
            config_name=config_name,
            params=params,
            download_mbps=download_mbps,
            upload_mbps=upload_mbps,
            ping_loss_pct=loss_pct,
            ping_avg_ms=avg_ms,
            retransmits=upload_retrans + download_retrans
        )

        self.results.append(result)
        return result

    def print_results(self):
        """Print all results sorted by download speed"""
        print("\n" + "="*80)
        print("A/B TEST RESULTS (sorted by download speed)")
        print("="*80)

        sorted_results = sorted(self.results, key=lambda r: r.download_mbps, reverse=True)

        for i, r in enumerate(sorted_results, 1):
            print(f"\n{i}. {r.config_name}")
            print(f"   Download: {r.download_mbps:.1f} Mbps | Upload: {r.upload_mbps:.1f} Mbps")
            print(f"   Ping: {r.ping_loss_pct}% loss, {r.ping_avg_ms:.1f}ms | Retrans: {r.retransmits}")
            print(f"   Params: {r.params}")

    def close(self):
        self.ssh.close()


def main():
    parser = argparse.ArgumentParser(description='ChainLightning A/B Configuration Tester')
    parser.add_argument('--config', type=str, default=None,
                        help='Path to ssh_config.json with host definitions')
    args = parser.parse_args()

    tester = ABTester(config_path=args.config)

    # Define test configurations
    configs = [
        # Baseline (current)
        ("baseline", {
            "min_chunk_size": 1400,
            "max_chunk_size": 65536,
            "aggregation_timeout_ms": 5,
            "reorder_timeout_ms": 50,
            "enable_sync": True
        }),

        # Smaller chunks, faster timeouts
        ("small_fast", {
            "min_chunk_size": 1400,
            "max_chunk_size": 16384,
            "aggregation_timeout_ms": 2,
            "reorder_timeout_ms": 30,
            "enable_sync": True
        }),

        # No aggregation (immediate send)
        ("no_aggregation", {
            "min_chunk_size": 1,
            "max_chunk_size": 1500,
            "aggregation_timeout_ms": 1,
            "reorder_timeout_ms": 30,
            "enable_sync": False
        }),

        # Larger chunks for bulk transfer
        ("large_chunks", {
            "min_chunk_size": 8192,
            "max_chunk_size": 131072,
            "aggregation_timeout_ms": 10,
            "reorder_timeout_ms": 50,
            "enable_sync": True
        }),

        # Medium balanced
        ("medium_balanced", {
            "min_chunk_size": 4096,
            "max_chunk_size": 32768,
            "aggregation_timeout_ms": 3,
            "reorder_timeout_ms": 40,
            "enable_sync": True
        }),

        # No sync (faster but more reorder)
        ("no_sync", {
            "min_chunk_size": 1400,
            "max_chunk_size": 65536,
            "aggregation_timeout_ms": 5,
            "reorder_timeout_ms": 50,
            "enable_sync": False
        }),

        # Very aggressive (minimal latency)
        ("aggressive", {
            "min_chunk_size": 1,
            "max_chunk_size": 8192,
            "aggregation_timeout_ms": 1,
            "reorder_timeout_ms": 20,
            "enable_sync": False
        }),
    ]

    try:
        for name, params in configs:
            result = tester.run_test(name, params)
            if result:
                # Save intermediate results
                with open('ab_results.json', 'w') as f:
                    json.dump([{
                        'name': r.config_name,
                        'params': r.params,
                        'download_mbps': r.download_mbps,
                        'upload_mbps': r.upload_mbps,
                        'ping_loss_pct': r.ping_loss_pct,
                        'ping_avg_ms': r.ping_avg_ms,
                        'retransmits': r.retransmits
                    } for r in tester.results], f, indent=2)

        tester.print_results()

    finally:
        tester.close()


if __name__ == '__main__':
    main()
