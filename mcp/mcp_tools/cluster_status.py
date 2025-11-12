"""
etcd Cluster Status MCP Tool
Get real-time cluster health and member information
"""

import logging
from datetime import datetime
import pytz

from .models import ETCDClusterStatusResponse

logger = logging.getLogger(__name__)


def register_cluster_status_tool(mcp, get_components_func):
    """Register the cluster status tool with the MCP server"""
    
    @mcp.tool()
    async def get_etcd_cluster_status() -> ETCDClusterStatusResponse:
        """
        Get comprehensive etcd cluster status including health, member information, and leadership details.
        
        This tool provides real-time etcd cluster status by executing etcdctl commands to check:
        - Cluster health status (healthy/degraded endpoints)
        - Member list with active and learner members
        - Endpoint status including leader information, database sizes, and Raft terms
        - Leadership information and changes
        - Basic cluster metrics
        
        Returns:
            ETCDClusterStatusResponse: Complete cluster status including health, members, endpoints, 
                                      leadership, and basic metrics
        """
        components = get_components_func()
        cluster_collector = components.get('cluster_collector')
        
        try:
            if not cluster_collector:
                return ETCDClusterStatusResponse(
                    status="error",
                    error="Cluster collector not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat()
                )
            
            result = await cluster_collector.get_cluster_status()
            
            return ETCDClusterStatusResponse(
                status=result.get('status', 'unknown'),
                data=result.get('data'),
                error=result.get('error'),
                timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat())
            )
            
        except Exception as e:
            logger.error(f"Error getting cluster status: {e}")
            return ETCDClusterStatusResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat()
            )