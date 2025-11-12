"""
Health Check MCP Tool
Verifies MCP server and collector initialization status
"""

import logging
from datetime import datetime
from typing import Optional, Any, Dict
import pytz

from .models import ServerHealthResponse, MCPBaseModel

logger = logging.getLogger(__name__)


class EmptyRequest(MCPBaseModel):
    """Empty request model for tools that require no parameters"""
    pass


def register_health_check_tool(mcp, get_components_func):
    """Register the health check tool with the MCP server"""
    
    @mcp.tool()
    async def get_server_health(request: Optional[EmptyRequest] = None) -> ServerHealthResponse:
        """
        Get server health status and collector initialization status.
        
        This tool provides diagnostic information about the MCP analyzer server:
        - Overall server health status (healthy/unhealthy)
        - Individual collector initialization status
        - Component availability for troubleshooting
        
        Works with different server types (etcd, OVN-K, network analyzer) by checking
        for components that actually exist in each server type.
        
        Returns:
            ServerHealthResponse: Health status with detailed component availability
        """
        components = get_components_func()
        
        # Required components that must exist for any server
        required_components = [
            'auth_manager',
            'config'
        ]
        
        # Optional collectors - different servers have different collectors
        # Check all possible collectors and report what exists
        all_possible_collectors = [
            # etcd-specific collectors
            'cluster_collector',
            'general_collector',
            'compact_defrag_collector',
            'wal_fsync_collector',
            'backend_commit_collector',
            'disk_io_collector',
            # OVN-K and network analyzer collectors
            'ovn_db_collector',
            'kubelet_cni_collector',
            'latency_collector',
            'ovs_collector',
            'pods_collector',
            'api_collector',
            # Common collectors
            'network_collector',
            'cluster_info_collector',
            'node_usage_collector',
            # Network analyzer specific
            'network_l1_collector',
            'socket_tcp_collector',
            'socket_udp_collector',
            'socket_memory_collector',
            'socket_softnet_collector',
            'socket_ip_collector',
            'netstat_tcp_collector',
            'netstat_udp_collector'
        ]
        
        # Check required components
        required_available = all(
            components.get(comp) is not None 
            for comp in required_components
        )
        
        # Count available collectors
        available_collectors = [
            name for name in all_possible_collectors
            if components.get(name) is not None
        ]
        
        # Server is healthy if required components exist and at least one collector is available
        collectors_initialized = required_available and len(available_collectors) > 0
        
        # Build details dictionary with all components
        details = {}
        
        # Add required components
        for comp in required_components:
            details[comp] = components.get(comp) is not None
        
        # Add all possible collectors (show which ones exist)
        for collector in all_possible_collectors:
            details[collector] = components.get(collector) is not None
        
        return ServerHealthResponse(
            status="healthy" if collectors_initialized else "unhealthy",
            timestamp=datetime.now(pytz.UTC).isoformat(),
            collectors_initialized=collectors_initialized,
            details=details
        )