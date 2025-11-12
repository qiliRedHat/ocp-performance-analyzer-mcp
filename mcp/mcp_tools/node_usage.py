"""
etcd Node Usage MCP Tool
Get comprehensive node resource usage metrics for master nodes
"""

import logging
from datetime import datetime
from typing import Optional
import pytz

from .models import ETCDNodeUsageResponse, DurationInput

logger = logging.getLogger(__name__)


def register_node_usage_tool(mcp, get_components_func):
    """Register the node usage tool with the MCP server"""
    
    @mcp.tool()
    async def get_ocp_node_usage(request: Optional[DurationInput] = None) -> ETCDNodeUsageResponse:
        """
        Get comprehensive node usage metrics for all node groups (controlplane, infra, worker, workload).
        
        Monitors resource utilization metrics at the node and cgroup level for all node groups:
        - Node CPU usage by mode (user, system, idle, iowait, etc.)
        - Node memory used (active memory consumption)
        - Node memory cache and buffer (filesystem cache and buffers)
        - Cgroup CPU usage (CPU consumption per control group)
        - Cgroup RSS usage (Resident Set Size memory per control group)
        
        These metrics provide insights into:
        - Overall node resource utilization and capacity across all node roles
        - CPU contention and workload distribution patterns
        - Memory pressure and caching efficiency
        - Container-level resource consumption via cgroups
        - Potential resource bottlenecks affecting cluster performance
        
        Node resource constraints can directly impact cluster stability and performance.
        High CPU usage (>80%) or memory pressure can cause timeouts and degraded performance.
        
        The tool queries all node groups (controlplane, infra, workload, worker) and returns
        results grouped by role. For worker nodes, only the top 3 nodes by CPU usage are returned
        to focus on the most resource-intensive workers. If a specific node group has no nodes 
        or fails to collect, it will be marked with an error status but other groups will still be returned.
        
        Args:
            request: Optional request object with duration field. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            ETCDNodeUsageResponse: Node usage metrics including CPU usage by mode, memory consumption, 
                                  cache/buffer statistics, and cgroup-level resource utilization for all node groups.
                                  Results are organized in a 'node_groups' dictionary with keys: controlplane, infra, workload, worker.
        """
        # Extract duration from request, default to "1h" if not provided
        duration = request.duration if request and request.duration else "1h"
        
        components = get_components_func()
        auth_manager = components.get('auth_manager')
        node_usage_collector = components.get('node_usage_collector')
        config = components.get('config')
        
        try:
            if not node_usage_collector:
                # Lazy initialize
                if auth_manager is None:
                    from ocauth.openshift_auth import OpenShiftAuth
                    auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                    try:
                        await auth_manager.initialize()
                    except Exception:
                        return ETCDNodeUsageResponse(
                            status="error",
                            error="Failed to initialize OpenShift auth for node usage",
                            timestamp=datetime.now(pytz.UTC).isoformat(),
                            duration=duration
                        )
                
                try:
                    from tools.node.node_usage import nodeUsageCollector
                    prometheus_config = {
                        'url': auth_manager.prometheus_url,
                        'token': getattr(auth_manager, 'prometheus_token', None),
                        'verify_ssl': False
                    }
                    node_usage_collector = nodeUsageCollector(auth_manager, prometheus_config)
                    components['node_usage_collector'] = node_usage_collector
                except Exception as e:
                    return ETCDNodeUsageResponse(
                        status="error",
                        error=f"Failed to initialize nodeUsageCollector: {e}",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        duration=duration
                    )
            
            # Collect metrics for all node groups
            node_groups = {}
            overall_status = 'success'
            errors = []
            
            # Query each node group: controlplane (master), infra, workload, worker
            for node_group in ['controlplane', 'infra', 'workload', 'worker']:
                try:
                    # For worker nodes, return top 3 by CPU usage
                    if node_group == 'worker':
                        group_result = await node_usage_collector.collect_all_metrics(
                            node_group=node_group, 
                            duration=duration,
                            top_n_workers=3
                        )
                    else:
                        group_result = await node_usage_collector.collect_all_metrics(
                            node_group=node_group, 
                            duration=duration
                        )
                    
                    if group_result.get('status') == 'success':
                        node_groups[node_group] = group_result
                    else:
                        # Store error but don't fail the entire request
                        error_msg = group_result.get('error', 'Unknown error')
                        node_groups[node_group] = {
                            'status': 'error',
                            'error': error_msg
                        }
                        errors.append(f"{node_group}: {error_msg}")
                        logger.warning(f"Error collecting {node_group} node metrics: {error_msg}")
                except Exception as e:
                    # Store error but continue with other groups
                    error_msg = str(e)
                    node_groups[node_group] = {
                        'status': 'error',
                        'error': error_msg
                    }
                    errors.append(f"{node_group}: {error_msg}")
                    logger.error(f"Error collecting {node_group} node metrics: {e}")
            
            # Determine overall status
            successful_groups = [g for g, data in node_groups.items() if data.get('status') == 'success']
            if not successful_groups:
                overall_status = 'error'
            elif errors:
                overall_status = 'partial_success'
            
            # Build combined result
            combined_result = {
                'status': overall_status,
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'duration': duration,
                'category': 'node_usage',
                'node_groups': node_groups
            }
            
            if errors:
                combined_result['errors'] = errors
            if overall_status == 'error':
                combined_result['error'] = 'Failed to collect metrics for any node group'
            
            return ETCDNodeUsageResponse(
                status=overall_status,
                data=combined_result,
                error=combined_result.get('error'),
                timestamp=combined_result['timestamp'],
                duration=duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting node usage metrics: {e}")
            return ETCDNodeUsageResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )