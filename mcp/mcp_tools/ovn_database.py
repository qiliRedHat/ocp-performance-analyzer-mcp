"""
OVN Database MCP Tool
Get OVN database size and health metrics
"""

import logging
from .models import TimeRangeInput, MetricResponse
from .utils import get_utc_timestamp

logger = logging.getLogger(__name__)


def register_ovn_database_tool(mcp, get_components_func):
    """Register the OVN database tool with the MCP server"""
    
    @mcp.tool()
    async def get_ovn_database_info(
        request: TimeRangeInput | None = None
    ) -> MetricResponse:
        """
        🗄️ OVN DATABASE: Cluster-wide OVN database size and health.
        
        📋 WHAT THIS TOOL COLLECTS:
        
        DATABASE METRICS:
        - Total OVN pods count across cluster
        - Northbound database size (MB) per node
        - Southbound database size (MB) per node
        - Database growth trends
        
        NODE GROUPING:
        - Statistics grouped by node role
        - Per-node database sizes
        - Cluster-wide totals and averages
        
        HEALTH RECOMMENDATIONS:
        - Size threshold warnings
        - Performance impact assessment
        - Maintenance suggestions
        
        ⚠️ THIS IS FOR DATABASE SIZE ONLY, NOT:
        ❌ OVN controller performance → Use get_ovn_latency_stats
        ❌ OVS performance → Use get_ovs_usage
        ❌ Network traffic → Use get_network_io
        
        💡 USE THIS WHEN:
        - Monitoring database growth
        - Identifying database bloat
        - Planning database maintenance
        - Troubleshooting OVN performance issues
        - Capacity planning for OVN infrastructure
        
        📊 RETURNS:
        - total_ovn_pods: Count of OVN database pods
        - northbound_db_size: NB database sizes by node
        - southbound_db_size: SB database sizes by node
        - grouped_statistics: Metrics by node role
        - health_recommendations: Size-based guidance
        
        Example: "Check OVN database sizes and health status"
        """
        components = get_components_func()
        ovn_db_collector = components.get('ovn_db_collector')
        
        try:
            if ovn_db_collector is None:
                return MetricResponse(
                    status="error",
                    error="OVN DB collector not initialized",
                    timestamp=get_utc_timestamp()
                )
            
            result = await ovn_db_collector.get_cluster_summary()
            
            return MetricResponse(
                status=result.get('status', 'unknown'),
                data=result,
                error=result.get('error'),
                timestamp=result.get('timestamp', get_utc_timestamp()),
                category="ovn_database_summary"
            )
            
        except Exception as e:
            logger.error(f"Error collecting OVN database summary: {e}")
            return MetricResponse(
                status="error",
                error=str(e),
                timestamp=get_utc_timestamp()
            )