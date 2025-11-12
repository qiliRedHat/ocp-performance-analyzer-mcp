"""
Kubelet CNI MCP Tool
Get Kubelet CNI and CRIO performance metrics
"""

import logging
from .models import TimeRangeInput, MetricResponse
from .utils import get_utc_timestamp

logger = logging.getLogger(__name__)


def register_kubelet_cni_tool(mcp, get_components_func):
    """Register the kubelet CNI tool with the MCP server"""
    
    @mcp.tool()
    async def get_kubelet_cni_stats(
        request: TimeRangeInput | None = None
    ) -> MetricResponse:
        """
        🔌 KUBELET CNI: CNI and CRIO resource usage with cluster summary.
        
        📋 WHAT THIS TOOL COLLECTS (Per Node):
        
        CNI/CRIO CPU METRICS:
        - CPU usage percentage for CNI processes
        - CPU usage percentage for CRIO runtime
        - CPU trends over time period
        
        CNI/CRIO MEMORY METRICS:
        - Memory usage in bytes/MB
        - Memory trends and spikes
        - Memory efficiency indicators
        
        NETWORK METRICS:
        - Network usage (bytes/packets)
        - Network drops and errors
        - Network utilization percentage
        
        CONTAINER METRICS:
        - Container thread counts
        - I/O operations (read/write IOPS)
        - Container resource efficiency
        
        CLUSTER SUMMARY (Integrated):
        - Total nodes by role distribution
        - Max and average CPU/memory usage
        - Network drops/errors totals
        - Thread counts and IOPS aggregates
        - Overall health status
        - Performance recommendations
        
        NODE GROUPING:
        - Metrics grouped by role (controlplane, worker, infra, workload)
        - Per-node detailed statistics
        - Role-based performance analysis
        
        ⚠️ THIS IS FOR CNI/CRIO ONLY, NOT:
        ❌ OVN-K pod metrics → Use get_ovnk_pods_usage
        ❌ OVS metrics → Use get_ovs_usage
        ❌ Node CPU/memory → Use get_ocp_node_usage
        ❌ Network I/O → Use get_network_io
        
        💡 USE THIS WHEN:
        - Diagnosing CNI performance issues
        - Monitoring CRIO resource consumption
        - Identifying container runtime bottlenecks
        - Analyzing CNI operation efficiency
        - Troubleshooting pod networking problems
        
        📊 RETURNS (Per Node Group):
        - cni_cpu_usage: CNI process CPU percentage
        - crio_cpu_usage: CRIO runtime CPU percentage
        - cni_memory: CNI memory consumption
        - crio_memory: CRIO memory consumption
        - network_usage: Bytes and packets
        - network_drops: Drop counts
        - network_errors: Error counts
        - container_threads: Thread counts
        - io_operations: Read/write IOPS
        
        CLUSTER SUMMARY:
        - total_nodes: Count and distribution
        - cpu_usage_indicators: Max/avg values
        - memory_usage_indicators: Max/avg values
        - network_totals: Drops and errors
        - health_status: Overall assessment
        - recommendations: Performance guidance
        
        ⏱️ TIME PARAMETERS:
        - duration: '1h' (default), '30m', '2h', '6h', '12h'
        - start_time: ISO format start time (optional)
        - end_time: ISO format end time (optional)
        
        Example: "Show me CNI and CRIO metrics with cluster health summary"
        """
        components = get_components_func()
        auth_manager = components.get('auth_manager')
        
        try:
            if auth_manager is None:
                return MetricResponse(
                    status="error",
                    error="Authentication manager not initialized",
                    timestamp=get_utc_timestamp()
                )
            
            # Parse request parameters
            duration = "1h"
            start_time = None
            end_time = None
            
            if request:
                if request.start_time and request.end_time:
                    start_time = request.start_time
                    end_time = request.end_time
                elif request.duration:
                    duration = request.duration
            
            # Create collector for this request
            from tools.ovnk.ovnk_kubelet_cni import KubeletCNICollector
            collector = KubeletCNICollector(
                ocp_auth=auth_manager,
                duration=duration,
                start_time=start_time,
                end_time=end_time
            )
            
            # Collect all metrics with integrated summary
            result = await collector.collect_all_metrics()
            
            return MetricResponse(
                status=result.get('status', 'unknown'),
                data=result,
                error=result.get('error'),
                timestamp=result.get('timestamp', get_utc_timestamp()),
                category="kubelet_cni",
                duration=duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting Kubelet CNI metrics: {e}")
            return MetricResponse(
                status="error",
                error=str(e),
                timestamp=get_utc_timestamp()
            )