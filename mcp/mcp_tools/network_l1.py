"""
Network Layer 1 (Physical Layer) MCP Tool
Network interface hardware status and physical layer metrics (NOT NODE CPU/MEMORY!)
"""

import asyncio
import logging
from datetime import datetime, timezone

from .models import NetworkL1Request, NetworkMetricsResponse
from tools.net.network_l1 import NetworkL1Collector
from tools.utils.promql_utility import mcpToolsUtility
from tools.utils.promql_basequery import PrometheusQueryError

logger = logging.getLogger(__name__)


def register_network_l1_tool(mcp, get_components_func):
    """Register the network L1 tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_l1_stats(request: NetworkL1Request) -> NetworkMetricsResponse:
        """
        🔌 LAYER 1 INTERFACE: Network interface hardware and physical layer metrics.
        
        📋 WHAT THIS TOOL COLLECTS (Per Network Interface, Per Node):
        
        INTERFACE STATUS (Physical Layer):
        - Operational status: up/down for EACH interface (eth0, ens3, ens4, etc.)
        - Carrier detection: Physical link presence (is cable connected?)
        - Status per interface: bond0, br-ex, ovs-system, etc.
        
        INTERFACE CONFIGURATION:
        - Interface speeds: Configured speeds in bits/second (1Gbps, 10Gbps, 25Gbps)
        - MTU settings: Maximum Transmission Unit in bytes (1500 standard, 9000 jumbo)
        - Speed per interface: Different NICs may have different speeds
        
        ARP TABLE STATISTICS:
        - ARP entry counts: Address resolution table size (avg/max over time)
        - Per interface: ARP entries for each network interface
        
        🎯 NETWORK INTERFACES (Examples):
        - eth0, eth1, eth2 - Physical NICs
        - ens3, ens4, ens5 - Predictable NIC names
        - bond0 - Bonded interfaces
        - br-ex, br-int - OVS bridges
        - ovs-system - OVS internal
        
        ⚠️  THIS IS ABOUT NETWORK INTERFACE HARDWARE ONLY, NOT:
        ❌ Node CPU usage → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Node memory usage → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Network traffic volume → Use get_network_io_node (Layer 2-4 traffic)
        ❌ Bandwidth utilization → Use get_network_io_node (traffic metrics)
        ❌ Packet drops/errors → Use get_network_io_node (I/O performance)
        ❌ Pod/container resources → Use container MCP server
        ❌ Disk I/O → Use storage MCP server
        
        💡 USE THIS WHEN:
        - Checking if network interfaces are up (link status verification)
        - Validating interface speeds (1G vs 10G vs 25G configuration)
        - Verifying physical link status (carrier detection - is cable plugged in?)
        - Checking MTU configuration (jumbo frames enabled/disabled?)
        - Monitoring ARP table growth (address exhaustion detection)
        - Pre-maintenance validation (ensure all interfaces configured correctly)
        - Troubleshooting "no network connectivity" (check physical layer first)
        
        🔍 COMMON TROUBLESHOOTING SCENARIOS:
        - "Is the network interface up?" → Check operational status per interface
        - "Do we have a physical link?" → Check carrier detection
        - "What speed is eth0 running at?" → Check interface speeds
        - "Is interface configured for jumbo frames?" → Check MTU (9000 vs 1500)
        - "Is ARP table full?" → Check ARP entry counts
        - "Interface shows up but no connectivity?" → Check carrier detection
        - "Speed mismatch between nodes?" → Compare interface speeds across nodes
        
        📊 RETURNS (Grouped by Node Role):
        - controlplane: All master nodes with per-interface details
        - infra: All infrastructure nodes with per-interface details
        - workload: All workload nodes with per-interface details
        - worker: Top 3 worker nodes (by name) with per-interface details
        
        Each node shows:
        - interfaces: Dict of interface_name → {status/speed/MTU/ARP}
        - Per-interface data: eth0={status: "Yes", speed: 10000000000, ...}
        
        ⏱️  TIME PARAMETERS:
        - duration: '5m' (default), '15m', '1h', '6h' - time window for ARP metrics
        - start_time/end_time: Optional explicit time range (ISO format)
        - timeout_seconds: 120 (default) - server-side timeout
        
        🎯 KEY DISTINCTION FROM OTHER TOOLS:
        - network_l1 (THIS TOOL) = Interface hardware status (eth0 up? speed? MTU?)
        - network_io = Traffic metrics (how many packets? bandwidth usage?)
        - node_usage = System resources (CPU %, memory %, NOT network!)
        
        Example: "Check network interface status and speeds on control plane nodes"
        """
        components = get_components_func()
        prometheus_client = components['prometheus_client']
        auth_manager = components['auth_manager']
        config = components['config']
        initialize_components = components['initialize_components']
        
        duration = request.duration
        timeout_seconds = request.timeout_seconds or 120
        
        try:
            if not prometheus_client or not auth_manager or not config:
                await initialize_components()
                components = get_components_func()
                prometheus_client = components['prometheus_client']
                auth_manager = components['auth_manager']
                config = components['config']
            
            utility = mcpToolsUtility()
            collector = NetworkL1Collector(prometheus_client, config, utility)
            
            try:
                results = await asyncio.wait_for(
                    collector.collect_all_metrics(duration), 
                    timeout=timeout_seconds
                )
            except Exception as first_err:
                # Handle token expiration
                err_text = str(first_err)
                if ('401' in err_text) or ('Unauthorized' in err_text) or isinstance(first_err, PrometheusQueryError):
                    try:
                        await auth_manager.initialize()
                        if prometheus_client:
                            prometheus_client.token = getattr(auth_manager, 'prometheus_token', None)
                            if getattr(prometheus_client, 'session', None):
                                try:
                                    await prometheus_client.close()
                                except Exception:
                                    pass
                        results = await asyncio.wait_for(
                            collector.collect_all_metrics(duration), 
                            timeout=timeout_seconds
                        )
                    except Exception as retry_err:
                        raise retry_err
                else:
                    raise first_err
            
            return NetworkMetricsResponse(
                status="success" if 'error' not in results else "error",
                data=results,
                error=results.get('error'),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_l1",
                duration=duration
            )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error=f"Timeout collecting network L1 metrics after {timeout_seconds}s",
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_l1",
                duration=duration
            )
        except Exception as e:
            logger.error(f"Error collecting network L1 metrics: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_l1",
                duration=duration
            )