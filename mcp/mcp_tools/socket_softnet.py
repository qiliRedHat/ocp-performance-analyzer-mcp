"""
Socket Softnet Statistics MCP Tool
Kernel network stack packet processing performance (NOT NODE CPU USAGE!)
"""

import asyncio
import logging
from datetime import datetime, timezone

from .models import NetworkSocketSoftnetRequest, NetworkMetricsResponse
from .utils import sanitize_json_compat
from tools.net.network_socket4softnet import socketStatSoftNetCollector

logger = logging.getLogger(__name__)


def register_socket_softnet_tool(mcp, get_components_func):
    """Register the softnet tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_socket_softnet_stats(request: NetworkSocketSoftnetRequest) -> NetworkMetricsResponse:
        """
        ⚙️ SOFTNET LAYER: Kernel network stack packet processing performance.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node, Kernel Packet Processing Data):
        
        PACKET PROCESSING STATISTICS:
        - Packets processed: Successfully processed packets at softnet layer (rate per second)
        - Packets dropped: Dropped due to backlog/quota exhaustion (kernel can't keep up)
        - Processing success rate: Ratio of processed to total packets
        
        KERNEL PROCESSING LIMITS:
        - Out-of-quota events: Processing budget exhausted (net.core.netdev_budget)
        - Budget hits: Number of times kernel hit processing limit per cycle
        - Backlog overflow: Packets dropped due to backlog queue full
        
        CPU PACKET DISTRIBUTION:
        - CPU RPS statistics: Receive Packet Steering distribution across CPUs
        - Load balancing: How well packets are distributed to multiple CPUs
        - CPU affinity: Packet processing CPU assignment
        
        FLOW CONTROL:
        - Flow limit counts: Flow-based congestion control indicators
        - Per-flow quotas: Flow-level processing limits
        
        🎯 KERNEL PROCESSING FOCUS:
        - Packet processing at kernel softirq level (not hardware)
        - CPU distribution effectiveness (RPS/RFS)
        - Processing quota management (kernel budget)
        - Softnet backlog handling
        
        ⚠️  THIS IS ABOUT KERNEL PACKET PROCESSING ONLY, NOT:
        ❌ Network traffic volume → Use get_network_io_node (for bandwidth)
        ❌ Node CPU usage → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Packet drops at NIC → Use get_network_io_node (for interface drops)
        ❌ Application performance → Use container MCP server
        ❌ Socket counts → Use get_network_socket_tcp_stats (for connections)
        ❌ Protocol errors → Use get_network_netstat_tcp_stats (for retransmissions)
        
        💡 USE THIS WHEN:
        - Troubleshooting packet drops at kernel level (not NIC)
        - Analyzing CPU packet processing distribution (RPS effectiveness)
        - Detecting softnet backlog exhaustion (queue overflow)
        - Tuning kernel network stack parameters (net.core.netdev_budget)
        - Validating RPS (Receive Packet Steering) configuration
        - High packet rate performance analysis (millions of pps)
        - Investigating kernel network stack bottlenecks
        
        🔍 COMMON TROUBLESHOOTING SCENARIOS:
        - "Kernel dropping packets" → Check softnet dropped count
        - "CPU not distributing packet load" → Check RPS statistics
        - "Network stack bottleneck" → Check out-of-quota events
        - "High packet loss in kernel" → Compare processed vs dropped
        - "After RPS tuning" → Validate CPU distribution
        - "netdev_budget exhausted" → Check out-of-quota frequency
        
        📊 RETURNS (Grouped by Node Role):
        - controlplane: All master nodes with softnet stats (avg/max)
        - infra: All infrastructure nodes with softnet stats
        - workload: All workload nodes with softnet stats
        - worker: Top 3 workers by maximum packet processing (avg/max)
        
        Each node shows per metric:
        - avg: Average value over time period
        - max: Maximum value over time period
        - unit: packets_per_second or count
        
        Metrics included:
        - node_softnet_packages_processed_total: Packets processed
        - node_softnet_packages_dropped_total: Packets dropped
        - softnet_out_of_quota: Budget exhaustion events
        - softnet_cpu_rps: RPS distribution stats
        - softnet_flow_limit_count: Flow limit hits
        
        ⏱️  TIME PARAMETERS:
        - duration: '1h' (default), '6h', '1d' - time window for metrics
        - step: '15s' (default) - query resolution interval
        - start_time/end_time: Optional explicit time range (ISO format)
        
        🎯 KEY DISTINCTION FROM OTHER TOOLS:
        - socket_softnet (THIS TOOL) = Kernel packet processing (softirq level)
        - network_io = NIC-level metrics (interface drops, bandwidth)
        - node_usage = System CPU (overall CPU %, DIFFERENT SERVER!)
        - netstat_tcp = Protocol errors (retransmissions, timeouts)
        
        💡 KERNEL PARAMETERS MONITORED:
        - net.core.netdev_budget: Per-cycle packet budget
        - net.core.netdev_max_backlog: Backlog queue size
        - net.core.rps_sock_flow_entries: RPS flow entries
        
        Example: "Show me kernel packet processing stats and softnet drops"
        """
        components = get_components_func()
        prometheus_client = components['prometheus_client']
        auth_manager = components['auth_manager']
        config = components['config']
        initialize_components = components['initialize_components']
        
        duration = request.duration
        step = request.step
        
        try:
            if not prometheus_client or not auth_manager or not config:
                await initialize_components()
                components = get_components_func()
                prometheus_client = components['prometheus_client']
                auth_manager = components['auth_manager']
                config = components['config']
            
            end_time = datetime.now(timezone.utc)
            duration_delta = prometheus_client.parse_duration(duration)
            start_time = end_time - duration_delta
            
            start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            async with socketStatSoftNetCollector(auth_manager, config) as collector:
                softnet_data = await asyncio.wait_for(
                    collector.collect_all_softnet_metrics(start_str, end_str, step),
                    timeout=45.0
                )
            
            return NetworkMetricsResponse(
                status="success",
                data=sanitize_json_compat(softnet_data),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_softnet",
                duration=duration
            )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error="Timeout collecting softnet statistics",
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_softnet",
                duration=duration
            )
        except Exception as e:
            logger.error(f"Error collecting softnet stats: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_softnet",
                duration=duration
            )