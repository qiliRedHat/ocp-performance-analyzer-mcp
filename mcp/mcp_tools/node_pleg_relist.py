"""
Node PLEG Relist MCP Tool
Get PLEG (Pod Lifecycle Event Generator) relist latency metrics for node groups
"""

import logging
from datetime import datetime
from typing import Optional
import pytz

from .models import NodeHealthResponse, DurationInput

logger = logging.getLogger(__name__)


def register_pleg_relist_tool(mcp, get_components_func):
    """Register the PLEG relist tool with the MCP server"""

    @mcp.tool()
    async def get_ocp_node_pleg_latency(request: Optional[DurationInput] = None) -> NodeHealthResponse:
        """
        Get PLEG (Pod Lifecycle Event Generator) relist latency metrics for all node groups.

        This tool specifically monitors PLEG performance across all nodes:
        - P99 latency: 99th percentile relist operation duration
        - Max latency: Maximum relist latency observed
        - Min latency: Minimum relist latency observed

        PLEG (Pod Lifecycle Event Generator) is a critical kubelet component that:
        - Monitors pod lifecycle state changes
        - Detects container state transitions
        - Reports pod status to the API server
        - Triggers pod sync operations

        Performance thresholds:
        - Normal: < 100ms (0.1s)
        - Warning: 100ms - 1000ms (0.1s - 1s)
        - Critical: > 1000ms (> 1s)

        High PLEG relist latency indicates:
        - Kubelet performance degradation
        - Slow pod lifecycle operations
        - Delayed pod status updates
        - Potential node stability issues
        - Container runtime problems

        Common causes of high PLEG latency:
        - High pod density on nodes
        - Container runtime performance issues
        - Disk I/O bottlenecks
        - CPU contention on nodes
        - Network storage latency
        - Large number of container state changes

        The tool queries all node groups (controlplane, infra, workload, worker) and returns
        results grouped by role. For worker nodes, the top 3 nodes by PLEG latency are returned.

        Args:
            request: Optional request object with duration field. Default duration is '1h'.

        """
        duration = request.duration if request and request.duration else "1h"

        components = get_components_func()
        pleg_relist_collector = components.get('pleg_relist_collector')

        try:
            if not pleg_relist_collector:
                return NodeHealthResponse(
                    status="error",
                    error="PLEG relist collector not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )

            node_groups = {}
            overall_status = 'success'
            errors = []

            for node_group in ['controlplane', 'infra', 'workload', 'worker']:
                try:
                    if node_group == 'worker':
                        group_result = await pleg_relist_collector.collect_all_metrics(
                            node_group=node_group,
                            duration=duration,
                            top_n_nodes=3
                        )
                    else:
                        group_result = await pleg_relist_collector.collect_all_metrics(
                            node_group=node_group,
                            duration=duration
                        )

                    if group_result.get('status') == 'success':
                        # Filter to only include PLEG metrics
                        filtered_result = {
                            'status': group_result['status'],
                            'total_nodes': group_result.get('total_nodes', 0),
                            'nodes': group_result.get('nodes', []),
                            'time_range': group_result.get('time_range', {}),
                            'metrics': {}
                        }

                        if 'p99_kubelet_pleg_relist_duration' in group_result.get('metrics', {}):
                            filtered_result['metrics']['p99_kubelet_pleg_relist_duration'] = \
                                group_result['metrics']['p99_kubelet_pleg_relist_duration']

                        if 'top_nodes_by_pleg_latency' in group_result:
                            filtered_result['top_nodes_by_pleg_latency'] = group_result['top_nodes_by_pleg_latency']

                        node_groups[node_group] = filtered_result
                    else:
                        error_msg = group_result.get('error', 'Unknown error')
                        node_groups[node_group] = {
                            'status': 'error',
                            'error': error_msg
                        }
                        errors.append(f"{node_group}: {error_msg}")
                except Exception as e:
                    error_msg = str(e)
                    node_groups[node_group] = {
                        'status': 'error',
                        'error': error_msg
                    }
                    errors.append(f"{node_group}: {error_msg}")

            successful_groups = [g for g, data in node_groups.items() if data.get('status') == 'success']
            if not successful_groups:
                overall_status = 'error'
            elif errors:
                overall_status = 'partial_success'

            combined_result = {
                'status': overall_status,
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'duration': duration,
                'category': 'node_pleg_latency',
                'node_groups': node_groups,
                'health_summary': {
                    'total_node_groups': len(node_groups),
                    'successful_groups': len(successful_groups),
                    'failed_groups': len(errors),
                    'pleg_latency': {
                        'critical_nodes': _count_critical_nodes(node_groups),
                        'warning_nodes': _count_warning_nodes(node_groups),
                        'healthy_nodes': _count_healthy_nodes(node_groups)
                    }
                }
            }

            if errors:
                combined_result['errors'] = errors
            if overall_status == 'error':
                combined_result['error'] = 'Failed to collect PLEG metrics for any node group'

            return NodeHealthResponse(
                status=overall_status,
                data=combined_result,
                error=combined_result.get('error'),
                timestamp=combined_result['timestamp'],
                duration=duration
            )

        except Exception as e:
            logger.error(f"Error collecting PLEG latency metrics: {e}")
            return NodeHealthResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )


def _count_critical_nodes(node_groups):
    """Count nodes with critical PLEG latency (>1s)"""
    count = 0
    for group_data in node_groups.values():
        if group_data.get('status') != 'success':
            continue
        metrics = group_data.get('metrics', {})
        pleg_metrics = metrics.get('p99_kubelet_pleg_relist_duration', {})
        nodes = pleg_metrics.get('nodes', {})
        for node_data in nodes.values():
            p99 = node_data.get('p99', 0)
            if p99 > 1.0:  # Critical: > 1 second
                count += 1
    return count


def _count_warning_nodes(node_groups):
    """Count nodes with warning PLEG latency (0.1s - 1s)"""
    count = 0
    for group_data in node_groups.values():
        if group_data.get('status') != 'success':
            continue
        metrics = group_data.get('metrics', {})
        pleg_metrics = metrics.get('p99_kubelet_pleg_relist_duration', {})
        nodes = pleg_metrics.get('nodes', {})
        for node_data in nodes.values():
            p99 = node_data.get('p99', 0)
            if 0.1 < p99 <= 1.0:  # Warning: 0.1s - 1s
                count += 1
    return count


def _count_healthy_nodes(node_groups):
    """Count nodes with healthy PLEG latency (<0.1s)"""
    count = 0
    for group_data in node_groups.values():
        if group_data.get('status') != 'success':
            continue
        metrics = group_data.get('metrics', {})
        pleg_metrics = metrics.get('p99_kubelet_pleg_relist_duration', {})
        nodes = pleg_metrics.get('nodes', {})
        for node_data in nodes.values():
            p99 = node_data.get('p99', 0)
            if p99 <= 0.1:  # Healthy: <= 0.1 second
                count += 1
    return count
