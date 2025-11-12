"""
TCP Netstat MCP Tool
TCP protocol-level statistics, errors, and retransmissions (NOT SOCKET COUNTS!)
"""

import asyncio
import logging
from datetime import datetime, timezone

from .models import NetStatTCPRequest, NetworkMetricsResponse
from .utils import sanitize_json_compat
from tools.net.network_netstat4tcp import netStatTCPCollector

logger = logging.getLogger(__name__)


def register_netstat_tcp_tool(mcp, get_components_func):
    """Register the TCP netstat tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_netstat_tcp_stats(request: NetStatTCPRequest) -> NetworkMetricsResponse:
        """
        📡 TCP PROTOCOL: TCP protocol-level statistics, errors, and retransmissions.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node, TCP Protocol Behavior Data):
        
        TCP SEGMENT TRAFFIC:
        - TCP segments in: TCP segments received (protocol-level traffic)
        - TCP segments out: TCP segments transmitted
        - Segment rate: Segments per second
        
        TCP ERROR CONDITIONS:
        - Listen overflow: SYN queue overflow (too many connection attempts)
        - Listen drops: Connection requests dropped (queue full)
        - Retransmissions: TCP segments retransmitted (packet loss indicator)
        - Sync retrans: SYN retransmissions (connection setup issues)
        
        TCP CONNECTION MANAGEMENT:
        - Established connections: Currently active TCP connections (state: ESTABLISHED)
        - Max connections: Maximum TCP connections supported
        - Connection states: TCP state machine statistics
        
        SYN FLOOD PROTECTION:
        - SYN cookie sent: SYN cookies sent (flood protection active)
        - SYN cookie validated: Valid SYN cookies received
        - SYN cookie failures: Invalid SYN cookie attempts
        
        TCP QUALITY INDICATORS:
        - Timeout events: TCP connection timeouts
        - RST packets sent: Connection resets (abrupt termination)
        - Receive queue drops: Buffer overflow drops
        - Out-of-order packets: Packet reordering detection
        
        🎯 TCP PROTOCOL BEHAVIOR FOCUS:
        - Protocol-level error detection (not socket allocation)
        - Retransmission analysis (network quality indicator)
        - Connection management issues
        - SYN flood attack detection
        
        ⚠️  THIS IS ABOUT TCP PROTOCOL ERRORS ONLY, NOT:
        ❌ TCP socket counts → Use get_network_socket_tcp_stats (for socket allocations)
        ❌ Network traffic volume → Use get_network_io_node (for bandwidth)
        ❌ Interface errors → Use get_network_io_node (for NIC-level errors)
        ❌ Node CPU/memory → Use node_usage MCP server (DIFFERENT SERVER!)
        ❌ Socket memory → Use get_network_socket_memory (for buffer memory)
        ❌ Connection counts → Use get_network_socket_tcp_stats (for in-use sockets)
        
        💡 USE THIS WHEN:
        - Troubleshooting TCP retransmissions (network quality issues)
        - Detecting SYN flood attacks (SYN cookie activation)
        - Analyzing connection establishment failures
        - Monitoring TCP stack health (errors and timeouts)
        - Identifying network path quality issues (retrans rate)
        - Tuning TCP stack parameters (sysctl validation)
        - Investigating "connection drops" complaints
        
        🔍 COMMON TROUBLESHOOTING SCENARIOS:
        - "High retransmission rate" → Check TCP segments retrans (packet loss)
        - "Connection drops" → Check listen drops and overflow
        - "SYN flood attack suspected" → Check SYN cookie statistics
        - "Network quality poor" → Check retransmission rate vs total segments
        - "Connection timeouts" → Check TCP timeout events
        - "Too many resets" → Check RST sent count
        - "Packet reordering" → Check out-of-order queue
        
        📊 RETURNS (Grouped by Node Role):
        - controlplane: All master nodes with TCP protocol stats (avg/max)
        - infra: All infrastructure nodes with TCP protocol stats
        - workload: All workload nodes with TCP protocol stats
        - worker: Top 3 workers by maximum metric values (avg/max)
        
        Each node shows per metric:
        - avg: Average value over time period
        - max: Maximum value over time period
        - unit: segments_per_second, connections, or count
        
        Metrics included:
        - netstat_tcp_in: Segments received
        - netstat_tcp_out: Segments sent
        - netstat_tcp_error_listen_overflow: Listen overflow
        - netstat_tcp_error_listen_drops: Listen drops
        - netstat_tcp_error_segments_retrans: Retransmissions
        - netstat_tcp_error_sync_retrans: SYN retrans
        - node_tcp_sync_cookie_*: SYN cookie stats
        - node_netstat_Tcp_CurrEstab: Current connections
        
        ⏱️  TIME PARAMETERS:
        - duration: '1h' (default), '6h', '1d' - time window for metrics
        - step: '15s' (default) - query resolution interval
        - start_time/end_time: Optional explicit time range (ISO format)
        
        🎯 KEY DISTINCTION FROM OTHER TOOLS:
        - netstat_tcp (THIS TOOL) = Protocol errors (retransmissions, timeouts)
        - socket_tcp = Socket resource counts (how many connections exist)
        - network_io = Traffic metrics (bandwidth, interface drops)
        - socket_memory = Buffer memory (kernel TCP buffer usage)
        - node_usage = System resources (CPU %, memory %, DIFFERENT SERVER!)
        
        💡 KEY METRICS TO WATCH:
        - Retransmission rate: Should be < 1% of total segments
        - SYN cookies: Should be 0 unless under attack
        - Listen drops: Should be 0 (queue properly sized)
        - Timeout events: Should be minimal
        
        Example: "Show me TCP retransmissions and protocol errors"
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
            
            end_time = datetime.now(timezone.utc)
            duration_delta = prometheus_client.parse_duration(duration)
            start_time = end_time - duration_delta
            
            start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            collector = netStatTCPCollector(config, auth_manager, prometheus_client)
            tcp_metrics = await asyncio.wait_for(
                collector.collect_all_metrics(duration),
                timeout=60.0
            )
            
            return NetworkMetricsResponse(
                status="success",
                data=sanitize_json_compat(tcp_metrics),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_netstat_tcp",
                duration=duration
            )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error="Timeout collecting netstat TCP metrics",
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_netstat_tcp",
                duration=duration
            )
        except Exception as e:
            logger.error(f"Error collecting netstat TCP metrics: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
                category="network_netstat_tcp",
                duration=duration
            )