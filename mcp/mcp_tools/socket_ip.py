"""
Socket IP Statistics MCP Tool
IP protocol traffic and ICMP statistics (NOT APPLICATION TRAFFIC ANALYSIS!)
"""

import asyncio
import logging
from datetime import datetime, timezone

from .models import NetworkSocketIPRequest, NetworkMetricsResponse
from tools.net.network_socket4ip import socketStatIPCollector

logger = logging.getLogger(__name__)


def register_socket_ip_tool(mcp, get_components_func):
    """Register the socket IP tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_socket_ip_stats(request: NetworkSocketIPRequest) -> NetworkMetricsResponse:
        """
        🌐 IP LAYER: IP protocol traffic and ICMP statistics.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node, IP Protocol Data):
        
        IP TRAFFIC VOLUME:
        - IP incoming octets: Incoming IP traffic in bytes per second (all IP traffic)
        - IP outgoing octets: Outgoing IP traffic in bytes per second
        - IP-level throughput: Total IP layer data transfer
        
        ICMP MESSAGES:
        - ICMP incoming messages: ICMP messages received (pings, errors, redirects)
        - ICMP outgoing messages: ICMP messages sent
        - ICMP message rate: Messages per second
        
        ICMP ERRORS:
        - ICMP error messages: ICMP error count (unreachable, timeout, redirects)
        - Network path issues: ICMP-reported problems
        - Connectivity problems: ICMP failure indicators
        
        🎯 IP PROTOCOL FOCUS:
        - IP-level traffic volume (protocol layer, not application)
        - ICMP connectivity monitoring (ping, traceroute)
        - ICMP error detection (network path issues)
        
        ⚠️  THIS IS ABOUT IP PROTOCOL STATISTICS ONLY, NOT:
        ❌ Network bandwidth utilization → Use get_network_io_node (for saturation)
        ❌ TCP/UDP errors → Use get_network_netstat_tcp_stats or _udp_stats
        ❌ Interface traffic → Use get_network_io_node (for interface-level metrics)
        ❌ Node CPU/memory → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Application traffic → Use application-specific monitoring
        ❌ Socket counts → Use get_network_socket_tcp_stats (for connections)
        
        💡 USE THIS WHEN:
        - Analyzing IP-level traffic patterns (protocol layer)
        - Troubleshooting ICMP connectivity (ping failures, timeouts)
        - Detecting network path issues via ICMP errors
        - Monitoring bandwidth at IP protocol layer
        - Establishing network health baseline (ICMP echo)
        - Cross-node traffic distribution analysis
        - Investigating "ping not working" issues
        
        🔍 COMMON TROUBLESHOOTING SCENARIOS:
        - "Ping not working" → Check ICMP incoming/outgoing messages
        - "Network unreachable errors" → Check ICMP error counts
        - "IP traffic asymmetry" → Compare incoming vs outgoing octets
        - "Path MTU discovery issues" → Check ICMP error patterns
        - "Traceroute failing" → Check ICMP message rates
        - "ICMP blocked?" → Check ICMP message counts (should be > 0)
        
        📊 RETURNS (Grouped by Node Role):
        - controlplane: All master nodes with IP stats (avg/max)
        - infra: All infrastructure nodes with IP stats
        - workload: All workload nodes with IP stats
        - worker: Top 3 workers by maximum traffic (avg/max)
        
        Each node shows per metric:
        - avg: Average value over time period
        - max: Maximum value over time period
        - unit: bytes_per_second or messages_per_second
        
        Metrics included:
        - netstat_ip_in_octets: IP incoming bytes/sec
        - netstat_ip_out_octets: IP outgoing bytes/sec
        - node_netstat_Icmp_InMsgs: ICMP incoming messages/sec
        - node_netstat_Icmp_OutMsgs: ICMP outgoing messages/sec
        - node_netstat_Icmp_InErrors: ICMP errors/sec
        
        ⏱️  TIME PARAMETERS:
        - duration: '1h' (default), '6h', '1d' - time window for metrics
        - step: '15s' (default) - query resolution interval
        - start_time/end_time: Optional explicit time range (ISO format)
        
        🎯 KEY DISTINCTION FROM OTHER TOOLS:
        - socket_ip (THIS TOOL) = IP protocol stats (ICMP, IP octets)
        - network_io = Interface-level metrics (eth0 bandwidth, drops)
        - netstat_tcp = TCP protocol errors (retransmissions)
        - netstat_udp = UDP protocol errors (packet errors)
        - node_usage = System resources (CPU %, memory %, DIFFERENT SERVER!)
        
        💡 ICMP TYPES MONITORED:
        - Echo Request/Reply (ping)
        - Destination Unreachable
        - Time Exceeded (traceroute)
        - Redirect
        - Source Quench
        
        Example: "Show me IP traffic and ICMP error statistics"
        """
        components = get_components_func()
        prometheus_client = components['prometheus_client']
        auth_manager = components['auth_manager']
        config = components['config']
        initialize_components = components['initialize_components']
        
        duration = request.duration
        
        try:
            if not prometheus_client or not auth_manager or not config:
                await initialize_components()
                components = get_components_func()
                prometheus_client = components['prometheus_client']
                auth_manager = components['auth_manager']
                config = components['config']
            
            async with socketStatIPCollector(auth_manager, config) as collector:
                end_time = datetime.now(timezone.utc)
                duration_delta = prometheus_client.parse_duration(duration)
                start_time = end_time - duration_delta
                
                start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                
                metrics_data = await asyncio.wait_for(
                    collector.collect_all_socket_ip_metrics(start_str, end_str, '15s'),
                    timeout=45.0
                )
            
            return NetworkMetricsResponse(
                status="success",
                data=metrics_data,
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_ip",
                duration=duration
            )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error="Timeout collecting socket IP metrics",
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_ip",
                duration=duration
            )
        except Exception as e:
            logger.error(f"Error collecting socket IP metrics: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_socket_ip",
                duration=duration
            )