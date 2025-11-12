"""
TCP Socket Statistics MCP Tool
TCP socket allocation and connection resource tracking (NOT TRAFFIC VOLUME!)
"""

import asyncio
import logging
from datetime import datetime, timezone

from .models import NetworkSocketTCPRequest, NetworkMetricsResponse
from tools.net.network_socket4tcp import socketStatTCPCollector

logger = logging.getLogger(__name__)


def register_socket_tcp_tool(mcp, get_components_func):
    """Register the TCP socket tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_socket_tcp_stats(request: NetworkSocketTCPRequest) -> NetworkMetricsResponse:
        """
        🔌 TCP SOCKETS: TCP socket allocation and connection resource tracking.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node, Socket Resource Data):
        
        SOCKET ALLOCATIONS:
        - TCP sockets allocated: Total kernel TCP socket allocations (memory structures)
        - TCP sockets in-use: Active TCP connections (established + listening)
        - Socket allocation counts: How many socket structures exist
        
        CONNECTION STATES:
        - Orphan connections: TCP connections not attached to any process (leaked)
        - TIME_WAIT connections: TCP connections in TIME_WAIT state (cleanup phase)
        - Established connections: Currently active TCP connections
        
        SOCKET RESOURCES:
        - Total sockets used: All socket types (TCP + UDP + RAW + UNIX)
        - Fragment buffers in-use: IP fragment reassembly buffer allocations
        - Raw sockets in-use: Raw socket allocations (ICMP, custom protocols)
        
        🎯 SOCKET RESOURCE FOCUS:
        - How many socket structures exist (memory allocations)
        - How many connections are tracked (connection counts)
        - Are connections leaking (orphan detection)
        - Is TIME_WAIT accumulating (port exhaustion risk)
        
        ⚠️  THIS IS ABOUT TCP SOCKET RESOURCES ONLY, NOT:
        ❌ Network traffic volume → Use get_network_io_node (for bandwidth/packets)
        ❌ TCP protocol errors → Use get_network_netstat_tcp_stats (for retransmissions)
        ❌ Interface status → Use get_network_l1_stats (for eth0 up/down)
        ❌ Socket memory usage → Use get_network_socket_memory (for buffer memory)
        ❌ Node CPU/memory → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Bandwidth utilization → Use get_network_io_node (for traffic metrics)
        
        💡 USE THIS WHEN:
        - Detecting socket exhaustion ("too many open files" errors)
        - Identifying connection leaks (orphan connection growth)
        - Troubleshooting port exhaustion (TIME_WAIT accumulation)
        - Monitoring socket resource consumption per node
        - Capacity planning for socket limits (fs.file-max, net.ipv4.ip_local_port_range)
        - Investigating "no buffer space available" errors
        - Tracking socket allocation trends
        
        🔍 COMMON TROUBLESHOOTING SCENARIOS:
        - "Cannot create socket" → Check sockets allocated vs system limits
        - "Too many open files" → Check TCP sockets in-use counts
        - "Connection refused" → Check TCP in-use and listening sockets
        - "Port exhaustion" → Check TIME_WAIT connection accumulation
        - "Memory exhaustion" → Check socket allocations and fragment buffers
        - "Zombie connections" → Check orphan connection counts
        - "Socket leak suspected" → Monitor orphan connections over time
        
        📊 RETURNS (Grouped by Node Role):
        - controlplane: All master nodes with socket stats (avg/max)
        - infra: All infrastructure nodes with socket stats
        - workload: All workload nodes with socket stats
        - worker: Top 3 workers by maximum socket usage (avg/max)
        
        Each node shows per metric:
        - avg: Average value over time period
        - max: Maximum value over time period
        - unit: count (socket counts)
        
        Metrics included:
        - socket_tcp_allocated: Total TCP allocations
        - socket_tcp_inuse: Active TCP connections
        - socket_tcp_orphan: Orphaned connections
        - socket_tcp_tw: TIME_WAIT connections
        - socket_used: Total sockets (all types)
        - socket_frag_inuse: Fragment buffers
        - socket_raw_inuse: Raw sockets
        
        ⏱️  TIME PARAMETERS:
        - duration: '1h' (default), '6h', '1d' - time window for metrics
        - start_time/end_time: Optional explicit time range (ISO format)
        
        🎯 KEY DISTINCTION FROM OTHER TOOLS:
        - socket_tcp (THIS TOOL) = Socket resource counts (how many connections)
        - network_io = Traffic metrics (bandwidth, packets, drops)
        - netstat_tcp = Protocol errors (retransmissions, timeouts)
        - socket_memory = Buffer memory (kernel socket memory usage)
        - node_usage = System resources (CPU %, memory %, NOT sockets!)
        
        Example: "Show me TCP socket allocations and orphan connections"
        """
        components = get_components_func()
        config = components['config']
        auth_manager = components['auth_manager']
        initialize_components = components['initialize_components']
        
        duration = request.duration
        
        try:
            if not config or not auth_manager:
                await initialize_components()
                components = get_components_func()
                config = components['config']
                auth_manager = components['auth_manager']
            
            async with socketStatTCPCollector(config, auth_manager) as collector:
                socket_data = await asyncio.wait_for(
                    collector.collect_all_tcp_metrics(), 
                    timeout=30.0
                )
                
                return NetworkMetricsResponse(
                    status="success",
                    data=socket_data,
                    timestamp=socket_data.get('collection_time', datetime.now(timezone.utc).isoformat()),
                    category="network_socket_tcp"
                )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error="Timeout collecting TCP socket statistics",
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_tcp"
            )
        except Exception as e:
            logger.error(f"Error collecting TCP socket stats: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_tcp"
            )