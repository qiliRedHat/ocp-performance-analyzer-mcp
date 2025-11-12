"""
OpenShift Cluster Information MCP Tool
Get comprehensive cluster infrastructure details
"""

import logging
from datetime import datetime
from typing import Optional
import pytz

from .models import OCPClusterInfoResponse, OCPClusterInfoRequest

logger = logging.getLogger(__name__)


def register_cluster_info_tool(mcp, get_components_func):
    """Register the cluster info tool with the MCP server"""
    
    @mcp.tool()
    async def get_ocp_cluster_info(request: Optional[OCPClusterInfoRequest] = None) -> OCPClusterInfoResponse:
        """
        Get comprehensive OpenShift cluster information and infrastructure details.
        
        Collects detailed information about the OpenShift cluster hosting the etcd cluster:
        - Cluster identification (name, version, platform - AWS/Azure/GCP/etc.)
        - Node information (master, infra, worker nodes with specs and status)
        - Resource counts (namespaces, pods, services, secrets, configmaps)
        - Network policy counts (NetworkPolicies, AdminNetworkPolicies, etc.)
        - Network resources (EgressFirewalls, EgressIPs, UserDefinedNetworks)
        - Cluster operator status (unavailable operators)
        - Machine Config Pool (MCP) status
        
        This provides context for etcd performance by showing the cluster environment.
        
        Returns:
            OCPClusterInfoResponse: Comprehensive cluster information including cluster details, 
                                   node inventory, resource statistics, and operator status
        """
        components = get_components_func()
        cluster_info_collector = components.get('cluster_info_collector')
        auth_manager = components.get('auth_manager')
        config = components.get('config')
        
        try:
            if cluster_info_collector is None:
                # Lazy initialize
                try:
                    from tools.ocp.cluster_info import ClusterInfoCollector
                    
                    collector = ClusterInfoCollector(
                        kubeconfig_path=(config.kubeconfig_path if config else None)
                    )
                    
                    if auth_manager:
                        # Reuse existing initialized auth to avoid duplicate initialization and SSL issues
                        collector.auth_manager = auth_manager
                        collector.k8s_client = auth_manager.kube_client
                    else:
                        await collector.initialize()
                    
                    # Update global reference
                    components['cluster_info_collector'] = collector
                    cluster_info_collector = collector
                    
                except Exception as init_err:
                    return OCPClusterInfoResponse(
                        status="error",
                        error=f"Failed to initialize ClusterInfoCollector: {init_err}",
                        timestamp=datetime.now(pytz.UTC).isoformat()
                    )

            info = await cluster_info_collector.collect_cluster_info()
            
            return OCPClusterInfoResponse(
                status="success",
                data=cluster_info_collector.to_dict(info),
                timestamp=datetime.now(pytz.UTC).isoformat()
            )
            
        except Exception as e:
            logger.error(f"Error collecting OCP cluster info: {e}")
            return OCPClusterInfoResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat()
            )