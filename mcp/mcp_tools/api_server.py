"""
API Server MCP Tool
Get Kubernetes API server performance metrics
"""

import logging
from .models import DurationInput, MetricResponse
from .utils import get_utc_timestamp

logger = logging.getLogger(__name__)


def register_api_server_tool(mcp, get_components_func):
    """Register the API server tool with the MCP server"""
    
    @mcp.tool()
    async def get_api_server_stats(
        request: DurationInput | None = None
    ) -> MetricResponse:
        """
        🌐 API SERVER: Kubernetes API server performance metrics.
        
        📋 WHAT THIS TOOL COLLECTS:
        
        API LATENCY METRICS:
        - Read-only API calls latency (average and max)
        - Mutating API calls latency (average and max)
        - Request latency by verb (GET, POST, PUT, DELETE, etc.) - P99
        - Request duration by resource (pods, services, etc.) - P99
        
        API THROUGHPUT:
        - API request rate (requests per second)
        - Request distribution by verb
        - Request distribution by resource
        
        ERROR METRICS:
        - API request errors (count and rate)
        - Error distribution by type
        - Failed request patterns
        
        INFLIGHT REQUESTS:
        - Current inflight requests (active connections)
        - Request queue depth
        - Connection saturation
        
        ETCD PERFORMANCE:
        - etcd request duration (P99)
        - etcd operation latency by type
        - Backend storage performance
        
        PRIORITY AND FAIRNESS:
        - Request wait duration (P99) - time in queue
        - Request execution duration (P99) - processing time
        - Request dispatch rate - admission rate
        - Requests in queue - backlog size
        - Priority level metrics
        - Fair queuing statistics
        
        DETAILED LABELS:
        - Resource type (pods, services, deployments, etc.)
        - Verb (get, list, create, update, delete, patch, watch)
        - Scope (cluster, namespace, resource)
        - Operation type (read, write, watch)
        
        ⚠️ THIS IS FOR API SERVER ONLY, NOT:
        ❌ etcd storage metrics → Separate etcd monitoring
        ❌ Node resources → Use get_ocp_node_usage
        ❌ Network traffic → Use get_network_io
        ❌ OVN-K performance → Use OVN-specific tools
        
        💡 USE THIS WHEN:
        - Diagnosing API server slowness
        - Identifying high-latency API calls
        - Monitoring API request rates
        - Analyzing request queuing issues
        - Troubleshooting timeout errors
        - Performance baseline establishment
        - Capacity planning for API server
        
        📊 RETURNS:
        - readonly_latency: Read API call latency (avg/max)
        - mutating_latency: Write API call latency (avg/max)
        - request_rate: Requests per second
        - request_errors: Error counts
        - inflight_requests: Current active requests
        - etcd_duration: etcd operation latency (P99)
        - request_latency_by_verb: Latency per verb (P99)
        - request_duration_by_resource: Duration per resource (P99)
        - pf_wait_duration: Priority/Fairness wait time (P99)
        - pf_execution_duration: Execution time (P99)
        - pf_dispatch_rate: Admission rate
        - pf_queued_requests: Backlog size
        
        ⏱️ TIME PARAMETERS:
        - duration: '5m' (default), '15m', '30m', '1h', '2h', '6h'
        
        Example: "Show me API server performance and latency metrics"
        """
        components = get_components_func()
        api_collector = components.get('api_collector')
        
        if request is None:
            request = DurationInput(duration="5m")
        
        try:
            if api_collector is None:
                return MetricResponse(
                    status="error",
                    error="API collector not initialized",
                    timestamp=get_utc_timestamp()
                )
            
            # Update collector duration if different
            if api_collector.duration != request.duration:
                api_collector.duration = request.duration
            
            result = await api_collector.collect_all_metrics()
            
            return MetricResponse(
                status=result.get('status', 'unknown'),
                data=result,
                error=result.get('error'),
                timestamp=result.get('timestamp', get_utc_timestamp()),
                category="api_server",
                duration=request.duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting API server metrics: {e}")
            return MetricResponse(
                status="error",
                error=str(e),
                timestamp=get_utc_timestamp()
            )