"""
Multus Pods Usage MCP Tool
Get Multus CNI pod resource usage metrics
"""

import logging
from .models import MultusPodMetricsRequest, MetricResponse
from .utils import get_utc_timestamp

logger = logging.getLogger(__name__)


def register_multus_pods_usage_tool(mcp, get_components_func):
    """Register the Multus pods usage tool with the MCP server"""
    
    @mcp.tool()
    async def get_multus_pods_usage(
        request: MultusPodMetricsRequest | None = None
    ) -> MetricResponse:
        """
        📦 MULTUS PODS: Multus CNI pod resource usage metrics.
        
        📋 WHAT THIS TOOL COLLECTS:
        
        MULTUS DAEMON PODS:
        - multus-* pods (all Multus daemon pods)
        - All containers within Multus pods
        - Per-container resource metrics
        
        NETWORK METRICS PODS:
        - network-metrics-* pods (if present)
        - Network monitoring containers
        
        RESOURCE METRICS:
        - CPU usage (percentage)
        - Memory RSS (Resident Set Size in bytes/MB)
        - Memory working set (bytes/MB)
        
        NAMESPACE: openshift-multus
        
        POD-LEVEL DETAILS:
        - Per-pod resource consumption
        - Node mapping for each pod
        - Container-specific metrics
        - Timestamp information
        
        ℹ️ ABOUT MULTUS:
        Multus is responsible for managing multiple network interfaces
        for pods in OpenShift, enabling multi-homed pod networking.
        
        ⚠️ THIS IS FOR MULTUS PODS ONLY, NOT:
        ❌ OVN-K pods → Use get_ovnk_pods_usage
        ❌ Node resources → Use get_ocp_node_usage
        ❌ CNI/CRIO → Use get_kubelet_cni_stats
        ❌ Network traffic → Use get_network_io
        
        💡 USE THIS WHEN:
        - Monitoring Multus CNI resource consumption
        - Troubleshooting multi-homed pod networking
        - Identifying Multus performance issues
        - Capacity planning for Multus infrastructure
        - Detecting resource bottlenecks in Multus
        - Analyzing network interface management overhead
        
        📊 RETURNS (Per Container):
        - cpu_usage: Container CPU percentage
        - memory_rss: Resident Set Size in bytes/MB
        - memory_working_set: Working set in bytes/MB
        - pod_name: Pod identifier
        - node: Node location
        - container: Container name
        - namespace: openshift-multus
        
        ⏱️ TIME PARAMETERS:
        - duration: '1h' (default), '30m', '2h', '6h', '12h'
        - start_time: ISO format start time (optional)
        - end_time: ISO format end time (optional)
        
        Example: "Show me Multus pod resource usage metrics"
        """
        components = get_components_func()
        pods_collector = components.get('pods_collector')
        
        if request is None:
            request = MultusPodMetricsRequest()
        
        try:
            if pods_collector is None:
                return MetricResponse(
                    status="error",
                    error="Pods collector not initialized",
                    timestamp=get_utc_timestamp()
                )
            
            # Collect metrics for all multus pods
            result = await pods_collector.collect_all_metrics(
                pod_pattern="multus.*|network-metrics.*",
                container_pattern=".*",
                namespace_pattern="openshift-multus",
                start_time=request.start_time,
                end_time=request.end_time
            )
            
            # Enhance result with namespace context
            if result and result.get('status') == 'success':
                result['namespace'] = 'openshift-multus'
                result['description'] = 'Multus CNI pod resource usage metrics'
            
            return MetricResponse(
                status=result.get('status', 'unknown'),
                data=result,
                error=result.get('error'),
                timestamp=result.get('timestamp', get_utc_timestamp()),
                category="multus_pods_usage",
                duration=request.duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting Multus pods usage metrics: {e}")
            return MetricResponse(
                status="error",
                error=str(e),
                timestamp=get_utc_timestamp()
            )