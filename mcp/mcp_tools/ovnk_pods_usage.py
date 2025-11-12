"""
OVN-K Pods Usage MCP Tool
Get OVN-Kubernetes pod resource usage metrics
"""

import logging
from .models import OVNKPodMetricsRequest, MetricResponse
from .utils import get_utc_timestamp

logger = logging.getLogger(__name__)


def register_ovnk_pods_usage_tool(mcp, get_components_func):
    """Register the OVN-K pods usage tool with the MCP server"""
    
    @mcp.tool()
    async def get_ovnk_pods_usage(
        request: OVNKPodMetricsRequest | None = None
    ) -> MetricResponse:
        """
        📦 OVN-K PODS: OVN-Kubernetes pod resource usage metrics.
        
        📋 WHAT THIS TOOL COLLECTS:
        
        OVNKUBE-CONTROLLER PODS:
        - ovnkube-controller container:
          * CPU usage (percentage)
          * Memory RSS (bytes/MB)
          * Memory working set (bytes/MB)
        
        OVNKUBE-NODE PODS (if include_node=True):
        - ovn-controller container:
          * CPU and memory metrics
        - northd container:
          * CPU and memory metrics
        - nbdb (Northbound DB) container:
          * CPU and memory metrics
        - sbdb (Southbound DB) container:
          * CPU and memory metrics
        
        NAMESPACE: openshift-ovn-kubernetes
        
        POD-LEVEL DETAILS:
        - Per-pod resource consumption
        - Node mapping for each pod
        - Container-specific metrics
        - Timestamp information
        
        ⚠️ THIS IS FOR OVN-K PODS ONLY, NOT:
        ❌ Node resources → Use get_ocp_node_usage
        ❌ OVS resources → Use get_ovs_usage
        ❌ CNI/CRIO → Use get_kubelet_cni_stats
        ❌ Multus pods → Use get_multus_pods_usage
        ❌ Network traffic → Use get_network_io
        
        💡 USE THIS WHEN:
        - Monitoring OVN-K pod resource consumption
        - Identifying resource-hungry containers
        - Troubleshooting OVN-K performance issues
        - Capacity planning for OVN-K infrastructure
        - Detecting memory leaks in OVN components
        - Analyzing controller vs. node resource usage
        
        📊 RETURNS (Per Container):
        - cpu_usage: Container CPU percentage
        - memory_rss: Resident Set Size in bytes/MB
        - memory_working_set: Working set in bytes/MB
        - pod_name: Pod identifier
        - node: Node location
        - container: Container name
        
        ⏱️ TIME PARAMETERS:
        - duration: '1h' (default), '30m', '2h', '6h', '12h'
        - start_time: ISO format start time (optional)
        - end_time: ISO format end time (optional)
        - include_node: Include ovnkube-node pods (default: True)
        
        Example: "Show me OVN-Kubernetes pod resource usage"
        """
        components = get_components_func()
        pods_collector = components.get('pods_collector')
        
        if request is None:
            request = OVNKPodMetricsRequest()
        
        try:
            if pods_collector is None:
                return MetricResponse(
                    status="error",
                    error="Pods collector not initialized",
                    timestamp=get_utc_timestamp()
                )
            
            # Collect metrics for ovnkube-node containers
            node_result = None
            if request.include_node:
                node_result = await pods_collector.collect_all_metrics(
                    pod_pattern="ovnkube-node.*",
                    container_pattern="(ovnkube-controller|ovn-controller|northd|nbdb|sbdb)",
                    namespace_pattern="openshift-ovn-kubernetes",
                    start_time=request.start_time,
                    end_time=request.end_time
                )
            
            # Combine results
            combined_data = {
                'namespace': 'openshift-ovn-kubernetes',
                'duration': request.duration,
                'ovnkube_node_containers': node_result if node_result else {
                    'status': 'skipped'
                }
            }
            
            # Determine overall status
            overall_status = "success"
            errors = []
            
            if node_result and node_result.get('status') == 'error':
                errors.append(f"Node containers: {node_result.get('error')}")
            
            if errors:
                overall_status = "partial" if len(errors) < 2 else "error"
                combined_data['errors'] = errors
            
            return MetricResponse(
                status=overall_status,
                data=combined_data,
                error="; ".join(errors) if errors else None,
                timestamp=get_utc_timestamp(),
                category="ovnk_pods_usage",
                duration=request.duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting OVN-K pods usage metrics: {e}")
            return MetricResponse(
                status="error",
                error=str(e),
                timestamp=get_utc_timestamp()
            )