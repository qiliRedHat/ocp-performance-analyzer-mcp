"""
OVN Latency MCP Tool
Get OVN-Kubernetes latency metrics
"""

import logging
from .models import DurationInput, MetricResponse
from .utils import get_utc_timestamp

logger = logging.getLogger(__name__)


def register_ovn_latency_tool(mcp, get_components_func):
    """Register the OVN latency tool with the MCP server"""
    
    @mcp.tool()
    async def get_ovn_latency_stats(
        request: DurationInput | None = None
    ) -> MetricResponse:
        """
        ⏱️ OVN LATENCY: Comprehensive OVN-Kubernetes operation latency.
        
        📋 WHAT THIS TOOL COLLECTS:
        
        CNI OPERATION LATENCY (P99):
        - CNI ADD operation latency (pod creation)
        - CNI DEL operation latency (pod deletion)
        - Per-pod CNI request timing
        
        POD NETWORKING LATENCY (P99):
        - Pod annotation latency
        - Pod first seen to LSP created
        - LSP (Logical Switch Port) creation latency
        - Port binding latency
        
        SERVICE LATENCY:
        - Service sync latency (average and P99)
        - Service update propagation time
        
        NETWORK CONFIG LATENCY (P99):
        - Network configuration application duration
        - Config propagation time
        
        CONTROLLER TIMING:
        - Controller ready duration
        - Node ready duration
        - Controller sync duration (average and P95)
        
        POD-LEVEL DETAILS:
        - All metrics include pod-level details
        - Node mapping for each pod
        - Timestamp information
        
        ⚠️ THIS IS FOR LATENCY ONLY, NOT:
        ❌ Resource usage → Use get_ovnk_pods_usage or get_ovs_usage
        ❌ Database size → Use get_ovn_database_info
        ❌ Network throughput → Use get_network_io
        ❌ Node resources → Use get_ocp_node_usage
        
        💡 USE THIS WHEN:
        - Diagnosing slow pod startup
        - Troubleshooting CNI delays
        - Analyzing service update lag
        - Identifying networking bottlenecks
        - Performance baseline establishment
        - SLA compliance verification
        
        📊 RETURNS:
        - cni_request_latency_add: P99 ADD operation
        - cni_request_latency_del: P99 DEL operation
        - pod_annotation_latency: P99 annotation time
        - pod_first_seen_lsp_latency: P99 first seen to LSP
        - pod_lsp_created_latency: P99 LSP creation
        - pod_port_binding_latency: P99 port binding
        - service_sync_latency: Avg and P99
        - network_config_duration: P99 config apply
        - controller_ready_duration: Controller startup
        - node_ready_duration: Node readiness
        - controller_sync_duration: Avg and P95 sync
        
        ⏱️ TIME PARAMETERS:
        - duration: '1h' (default), '30m', '2h', '6h', '12h', '1d'
        
        Example: "Show me OVN latency metrics to diagnose slow pod creation"
        """
        components = get_components_func()
        latency_collector = components.get('latency_collector')
        
        if request is None:
            request = DurationInput()
        
        try:
            if latency_collector is None:
                return MetricResponse(
                    status="error",
                    error="Latency collector not initialized",
                    timestamp=get_utc_timestamp()
                )
            
            # Update collector duration if different
            if latency_collector.duration != request.duration:
                latency_collector.duration = request.duration
            
            result = await latency_collector.collect_all_metrics()
            
            return MetricResponse(
                status=result.get('status', 'unknown'),
                data=result,
                error=result.get('error'),
                timestamp=result.get('timestamp', get_utc_timestamp()),
                category="ovn_latency",
                duration=request.duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting latency metrics: {e}")
            return MetricResponse(
                status="error",
                error=str(e),
                timestamp=get_utc_timestamp()
            )