"""
Socket Memory MCP Tool
Kernel socket buffer memory consumption tracking (NOT NODE SYSTEM MEMORY!)
"""

import asyncio
import logging
from datetime import datetime, timezone

from .models import NetworkSocketMemRequest, NetworkMetricsResponse
from tools.net.network_socket4mem import socketStatMemCollector

logger = logging.getLogger(__name__)


def register_socket_memory_tool(mcp, get_components_func):
    """Register the socket memory tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_socket_memory(request: NetworkSocketMemRequest) -> NetworkMetricsResponse:
        """
        💾 SOCKET MEMORY: Kernel socket buffer memory consumption tracking.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node, Socket Buffer Memory Data):
        
        SOCKET BUFFER MEMORY:
        - TCP kernel buffer memory: TCP socket send/receive buffer memory (pages + bytes)
        - UDP kernel buffer memory: UDP socket send/receive buffer memory (pages + bytes)
        - Fragment buffer memory: IP fragment reassembly buffer memory usage
        
        MEMORY UNITS:
        - Memory pages: Kernel memory pages allocated for socket buffers
        - Bytes: Total memory in bytes (converted to GB for readability)
        - Allocation patterns: How socket memory grows over time
        
        🎯 SOCKET BUFFER MEMORY FOCUS:
        - Kernel memory for socket operations (not application memory)
        - TCP/UDP buffer sizes (tunable via sysctl)
        - Fragment reassembly memory (IP fragmentation handling)
        
        ⚠️  THIS IS ABOUT SOCKET BUFFER MEMORY ONLY, NOT:
        ❌ Node system memory → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Application memory → Use container MCP server
        ❌ Socket counts → Use get_network_socket_tcp_stats (for connection counts)
        ❌ Network traffic → Use get_network_io_node (for bandwidth)
        ❌ Node memory usage → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Pod memory → Use container MCP server
        
        💡 USE THIS WHEN:
        - Troubleshooting "no buffer space available" errors
        - Tuning kernel socket buffer parameters (net.ipv4.tcp_wmem, tcp_rmem)
        - Detecting socket memory leaks
        - Monitoring fragment buffer exhaustion (PMTU discovery issues)
        - Capacity planning for high-throughput workloads
        - Investigating socket memory pressure
        - Validating sysctl tuning effects
        
        🔍 COMMON TROUBLESHOOTING SCENARIOS:
        - "No buffer space available" → Check TCP/UDP buffer memory exhaustion
        - "Fragment drops" → Check fragment buffer memory usage
        - "Socket memory exhausted" → Check kernel buffer allocations vs limits
        - "High latency" → Check if buffer memory is exhausted (pressure)
        - "After sysctl tuning" → Validate memory allocation changes
        - "Memory leak suspected" → Monitor socket memory growth trends
        
        📊 RETURNS (Grouped by Node Role):
        - controlplane: All master nodes with memory stats (avg/max in GB)
        - infra: All infrastructure nodes with memory stats
        - workload: All workload nodes with memory stats
        - worker: Top 3 workers by maximum memory usage (avg/max in GB)
        
        Each node shows per metric:
        - avg: Average memory usage (GB)
        - max: Maximum memory usage (GB)
        - unit: GB (gigabytes)
        
        Metrics included:
        - node_sockstat_FRAG_memory: Fragment buffer memory
        - TCP_Kernel_Buffer_Memory_Pages: TCP buffer pages
        - UDP_Kernel_Buffer_Memory_Pages: UDP buffer pages
        - node_sockstat_TCP_mem_bytes: TCP buffer bytes
        - node_sockstat_UDP_mem_bytes: UDP buffer bytes
        
        ⏱️  TIME PARAMETERS:
        - duration: '1h' (default), '6h', '1d' - time window for metrics
        - start_time/end_time: Optional explicit time range (ISO format)
        
        🎯 KEY DISTINCTION FROM OTHER TOOLS:
        - socket_memory (THIS TOOL) = Socket buffer memory (kernel TCP/UDP buffers)
        - node_usage = System memory (total node RAM usage, DIFFERENT SERVER!)
        - socket_tcp = Socket counts (how many connections, not memory)
        - network_io = Traffic metrics (bandwidth, not memory)
        
        💡 KERNEL PARAMETERS MONITORED:
        - net.ipv4.tcp_wmem (TCP write buffer)
        - net.ipv4.tcp_rmem (TCP read buffer)
        - net.ipv4.udp_wmem_min (UDP write buffer)
        - net.ipv4.udp_rmem_min (UDP read buffer)
        
        Example: "Show me socket buffer memory to troubleshoot buffer exhaustion"
        """
        components = get_components_func()
        auth_manager = components['auth_manager']
        config = components['config']
        initialize_components = components['initialize_components']
        
        duration = request.duration
        
        try:
            if not auth_manager or not config:
                await initialize_components()
                components = get_components_func()
                auth_manager = components['auth_manager']
                config = components['config']
            
            async with socketStatMemCollector(config=config, auth=auth_manager) as collector:
                socket_mem_data = await asyncio.wait_for(
                    collector.collect_all_metrics(), 
                    timeout=30.0
                )
                
                return NetworkMetricsResponse(
                    status="success",
                    data=socket_mem_data,
                    timestamp=socket_mem_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
                    category="network_socket_mem"
                )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error="Timeout collecting socket memory metrics",
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_mem"
            )
        except Exception as e:
            logger.error(f"Error collecting socket memory: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_mem"
            )