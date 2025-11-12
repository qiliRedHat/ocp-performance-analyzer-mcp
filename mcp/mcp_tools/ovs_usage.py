"""
OVS Usage MCP Tool
Get Open vSwitch performance metrics
"""

import logging
from .models import DurationInput, MetricResponse
from .utils import get_utc_timestamp

logger = logging.getLogger(__name__)


def register_ovs_usage_tool(mcp, get_components_func):
    """Register the OVS usage tool with the MCP server"""
    
    @mcp.tool()
    async def get_ovs_usage(
        request: DurationInput | None = None
    ) -> MetricResponse:
        """
        🔄 OVS USAGE: Open vSwitch resource usage and performance.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node/Pod):
        
        OVS PROCESS CPU:
        - OVS vswitchd CPU usage (percentage)
        - OVSDB server CPU usage (percentage)
        - CPU trends and spikes
        
        OVS MEMORY:
        - OVS database memory size (bytes/MB)
        - OVS vswitchd memory size (bytes/MB)
        - Memory growth patterns
        
        DATAPATH FLOWS:
        - Total datapath flows count
        - Flow table utilization
        - Flow distribution
        
        BRIDGE FLOWS:
        - br-int (integration bridge) flows
        - br-ex (external bridge) flows
        - Per-bridge flow statistics
        
        CONNECTION METRICS:
        - Stream connections (open count)
        - Remote connection overflow
        - Discarded connection counts
        
        MEGAFLOW CACHE:
        - Cache hit rate
        - Cache miss rate
        - Cache efficiency metrics
        
        DATAPATH STATISTICS:
        - Packet rates (packets/second)
        - Error rates (errors/second)
        - Packet processing efficiency
        
        INTERFACE METRICS:
        - RX/TX bytes rates (per interface)
        - RX dropped rate (per interface)
        - Interface-level performance
        
        ⚠️ THIS IS FOR OVS ONLY, NOT:
        ❌ OVN controller metrics → Use get_ovn_latency_stats
        ❌ CNI/CRIO metrics → Use get_kubelet_cni_stats
        ❌ Node resources → Use get_ocp_node_usage
        ❌ Network traffic → Use get_network_io
        
        💡 USE THIS WHEN:
        - Monitoring OVS resource consumption
        - Analyzing flow table utilization
        - Identifying OVS performance bottlenecks
        - Troubleshooting packet drops
        - Optimizing megaflow cache
        - Capacity planning for OVS
        
        📊 RETURNS (Per Node/Pod/Bridge):
        - ovs_vswitchd_cpu: vswitchd CPU percentage
        - ovsdb_server_cpu: OVSDB CPU percentage
        - ovs_db_memory: Database memory size
        - ovs_vswitchd_memory: vswitchd memory size
        - datapath_flows_total: Total flow count
        - bridge_flows_brint: br-int flows
        - bridge_flows_brex: br-ex flows
        - stream_connections: Open connections
        - remote_overflow: Overflow count
        - remote_discarded: Discarded count
        - megaflow_hits: Cache hits
        - megaflow_misses: Cache misses
        - datapath_packets: Packet rate
        - datapath_errors: Error rate
        - interface_rx_bytes: RX rate per interface
        - interface_tx_bytes: TX rate per interface
        - interface_rx_dropped: Drop rate per interface
        
        ⏱️ TIME PARAMETERS:
        - duration: '1h' (default), '30m', '2h', '6h', '12h', '1d'
        
        Example: "Show me OVS resource usage and flow statistics"
        """
        components = get_components_func()
        ovs_collector = components.get('ovs_collector')
        
        if request is None:
            request = DurationInput()
        
        try:
            if ovs_collector is None:
                return MetricResponse(
                    status="error",
                    error="OVS collector not initialized",
                    timestamp=get_utc_timestamp()
                )
            
            # Update collector duration if different
            if ovs_collector.duration != request.duration:
                ovs_collector.duration = request.duration
            
            result = await ovs_collector.collect_all_metrics()
            
            return MetricResponse(
                status=result.get('status', 'unknown'),
                data=result,
                error=result.get('error'),
                timestamp=result.get('timestamp', get_utc_timestamp()),
                category="ovs_metrics",
                duration=request.duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting OVS metrics: {e}")
            return MetricResponse(
                status="error",
                error=str(e),
                timestamp=get_utc_timestamp()
            )