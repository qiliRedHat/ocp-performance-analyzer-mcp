"""
UDP Netstat MCP Tool
UDP protocol-level statistics and error tracking (NOT SOCKET COUNTS!)
"""

import asyncio
import logging
from datetime import datetime, timezone

from .models import NetStatUDPRequest, NetworkMetricsResponse
from tools.net.network_netstat4udp import netStatUDPCollector

logger = logging.getLogger(__name__)


def register_netstat_udp_tool(mcp, get_components_func):
    """Register the UDP netstat tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_netstat_udp_stats(request: NetStatUDPRequest) -> NetworkMetricsResponse:
        """
        📡 UDP PROTOCOL: UDP protocol-level statistics and error tracking.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node, UDP Protocol Behavior Data):
        
        UDP PACKET TRAFFIC:
        - UDP packets in: UDP datagrams received (protocol-level traffic)
        - UDP packets out: UDP datagrams transmitted
        - Datagram rate: Packets per second
        
        UDP ERROR CONDITIONS:
        - Receive errors: Packet corruption or processing errors (checksum, malformed)
        - No-listen errors: Packets sent to ports with no listener (connection refused)
        - UDP lite errors: UDP lite protocol variant errors
        - Buffer errors: Receive/transmit buffer exhaustion
        
        UDP BUFFER ISSUES:
        - RX buffer errors: Receive buffer full (dropped due to buffer exhaustion)
        - TX buffer errors: Transmit buffer full (send buffer exhaustion)
        - Buffer overflow: Cannot allocate buffer space
        
        🎯 UDP PROTOCOL BEHAVIOR FOCUS:
        - Datagram-level traffic patterns (connectionless)
        - UDP error detection (packet loss, corruption)
        - Buffer management issues
        - Port listener problems (no process on port)
        
        ⚠️  THIS IS ABOUT UDP PROTOCOL ERRORS ONLY, NOT:
        ❌ UDP socket counts → Use get_network_socket_udp_stats (for socket allocations)
        ❌ Network traffic volume → Use get_network_io_node (for bandwidth)
        ❌ Interface drops → Use get_network_io_node (for NIC-level drops)
        ❌ Node CPU/memory → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Socket memory → Use get_network_socket_memory (for buffer memory)
        ❌ Connection counts → Use get_network_socket_udp_stats (for in-use sockets)
        
        💡 USE THIS WHEN:
        - Troubleshooting UDP packet loss (error detection)
        - Analyzing datagram-based application issues (DNS, DHCP, NTP)
        - Detecting UDP buffer exhaustion
        - Monitoring DNS/service discovery health
        - Identifying port listener problems (no process listening)
        - UDP-based application performance analysis
        - Investigating "destination unreachable" for UDP
        
        🔍 COMMON TROUBLESHOOTING SCENARIOS:
        - "UDP packets lost" → Check UDP receive errors
        - "No process listening on port" → Check no-listen error counts
        - "DNS issues" → Check UDP error rates (DNS uses UDP)
        - "Buffer overflow" → Check UDP buffer error counts
        - "NTP not working" → Check UDP no-listen errors
        - "DHCP failures" → Check UDP receive errors
        - "High UDP error rate" → Check all UDP error types
        
        📊 RETURNS (Grouped by Node Role):
        - controlplane: All master nodes with UDP protocol stats (avg/max)
        - infra: All infrastructure nodes with UDP protocol stats
        - workload: All workload nodes with UDP protocol stats
        - worker: Top 3 workers by maximum metric values (avg/max)
        
        Each node shows per metric:
        - avg: Average value over time period
        - max: Maximum value over time period
        - unit: packets_per_second or errors_per_second
        
        Metrics included:
        - nestat_udp_in: UDP packets received
        - netstat_udp_out: UDP packets sent
        - udp_error_rx_in_errors: Receive errors
        - udp_error_no_listen: No listener on port
        - udp_error_lite_rx_in_errors: UDP lite errors
        - udp_error_rx_in_buffer_errors: RX buffer errors
        - udp_error_tx_buffer_errors: TX buffer errors
        
        ⏱️  TIME PARAMETERS:
        - duration: '1h' (default), '6h', '1d' - time window for metrics
        - start_time/end_time: Optional explicit time range (ISO format)
        
        🎯 KEY DISTINCTION FROM OTHER TOOLS:
        - netstat_udp (THIS TOOL) = Protocol errors (packet errors, buffer errors)
        - socket_udp = Socket resource counts (how many UDP sockets exist)
        - network_io = Traffic metrics (UDP packets, bandwidth)
        - socket_memory = Buffer memory (kernel UDP buffer usage)
        - node_usage = System resources (CPU %, memory %, DIFFERENT SERVER!)
        
        💡 KEY METRICS TO WATCH:
        - No-listen errors: Indicates misconfiguration (wrong port)
        - Buffer errors: Indicates buffer tuning needed
        - Receive errors: Indicates packet corruption or network issues
        - Error rate: Should be < 0.1% of total UDP traffic
        
        💡 COMMON UDP APPLICATIONS:
        - DNS (port 53): Name resolution
        - DHCP (ports 67/68): IP address assignment
        - NTP (port 123): Time synchronization
        - SNMP (port 161/162): Network management
        - Syslog (port 514): Logging
        
        Example: "Show me UDP packet errors and buffer exhaustion"
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
            
            collector = netStatUDPCollector(auth_client=auth_manager, config=config)
            udp_metrics = await asyncio.wait_for(
                collector.collect_all_metrics(),
                timeout=45.0
            )
            
            return NetworkMetricsResponse(
                status="success",
                data=udp_metrics,
                timestamp=udp_metrics.get('timestamp', datetime.now(timezone.utc).isoformat()),
                category="network_netstat_udp",
                duration=duration
            )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error="Timeout collecting netstat UDP metrics",
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_netstat_udp",
                duration=duration
            )
        except Exception as e:
            logger.error(f"Error collecting netstat UDP metrics: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_netstat_udp",
                duration=duration
            )