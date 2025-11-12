"""
UDP Socket Statistics MCP Tool
UDP socket allocation and datagram resource tracking (NOT TRAFFIC VOLUME!)
"""

import asyncio
import logging
from datetime import datetime, timezone

from .models import NetworkSocketUDPRequest, NetworkMetricsResponse
from tools.net.network_socket4udp import SocketStatUDPCollector
from tools.utils.promql_utility import mcpToolsUtility

logger = logging.getLogger(__name__)


def register_socket_udp_tool(mcp, get_components_func):
    """Register the UDP socket tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_socket_udp_stats(request: NetworkSocketUDPRequest) -> NetworkMetricsResponse:
        """
        📡 UDP SOCKETS: UDP socket allocation and datagram resource tracking.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node, Socket Resource Data):
        
        UDP SOCKET ALLOCATIONS:
        - UDP sockets in-use: Active UDP datagram sockets (connectionless)
        - UDP lite sockets in-use: UDP lite variant socket allocations
        
        🎯 UDP SOCKET RESOURCE FOCUS:
        - How many UDP socket structures exist (memory allocations)
        - UDP socket resource consumption (datagram sockets)
        - UDP lite usage (lightweight UDP variant)
        
        ⚠️  THIS IS ABOUT UDP SOCKET RESOURCES ONLY, NOT:
        ❌ UDP traffic volume → Use get_network_io_node (for packets/bandwidth)
        ❌ UDP protocol errors → Use get_network_netstat_udp_stats (for packet errors)
        ❌ Network bandwidth → Use get_network_io_node (for throughput)
        ❌ Socket memory usage → Use get_network_socket_memory (for buffer memory)
        ❌ Node CPU/memory → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Interface status → Use get_network_l1_stats (for eth0 up/down)
        
        💡 USE THIS WHEN:
        - Monitoring UDP socket resource consumption
        - Detecting UDP socket exhaustion
        - Analyzing datagram-based application load (DNS, service discovery)
        - Capacity planning for UDP-heavy workloads
        - Troubleshooting DNS/service discovery socket issues
        - Tracking UDP socket allocation trends
        
        🔍 COMMON TROUBLESHOOTING SCENARIOS:
        - "DNS resolution slow" → Check UDP socket counts (not traffic)
        - "Service discovery issues" → Check UDP socket availability
        - "UDP-based app errors" → Check UDP socket exhaustion
        - "Cannot create UDP socket" → Check UDP sockets in-use vs limits
        
        📊 RETURNS (Grouped by Node Role):
        - controlplane: All master nodes with UDP socket stats (avg/max)
        - infra: All infrastructure nodes with UDP socket stats
        - workload: All workload nodes with UDP socket stats
        - worker: Top 3 workers by maximum UDP socket usage (avg/max)
        
        Each node shows per metric:
        - avg: Average value over time period
        - max: Maximum value over time period
        - unit: count (socket counts)
        
        Metrics included:
        - socket_udp_inuse: Active UDP sockets
        - socket_udp_lite_inuse: UDP lite sockets
        
        ⏱️  TIME PARAMETERS:
        - duration: '1h' (default), '6h', '1d' - time window for metrics
        - start_time/end_time: Optional explicit time range (ISO format)
        
        🎯 KEY DISTINCTION FROM OTHER TOOLS:
        - socket_udp (THIS TOOL) = UDP socket counts (how many UDP sockets)
        - network_io = Traffic metrics (UDP packets, bandwidth)
        - netstat_udp = Protocol errors (UDP packet errors, buffer errors)
        - socket_tcp = TCP socket counts (different protocol)
        - node_usage = System resources (CPU %, memory %, NOT sockets!)
        
        Example: "Show me UDP socket usage across all nodes"
        """
        components = get_components_func()
        prometheus_client = components['prometheus_client']
        config = components['config']
        initialize_components = components['initialize_components']
        
        duration = request.duration
        
        try:
            if not prometheus_client or not config:
                await initialize_components()
                components = get_components_func()
                prometheus_client = components['prometheus_client']
                config = components['config']
            
            utility = mcpToolsUtility()
            collector = SocketStatUDPCollector(prometheus_client, config, utility)
            
            udp_data = await asyncio.wait_for(
                collector.collect_all_metrics(), 
                timeout=30.0
            )
            
            return NetworkMetricsResponse(
                status="success",
                data=udp_data,
                timestamp=udp_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
                category="network_socket_udp"
            )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error="Timeout collecting UDP socket statistics",
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_udp"
            )
        except Exception as e:
            logger.error(f"Error collecting UDP socket stats: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_udp"
            )