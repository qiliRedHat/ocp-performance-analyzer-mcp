"""
Node Kubelet Runtime Operations Errors MCP Tool
Get kubelet runtime operations error rate metrics for node groups
"""

import logging
from datetime import datetime
from typing import Optional
import pytz

from .models import NodeHealthResponse, DurationInput

logger = logging.getLogger(__name__)


def register_runtime_errors_tool(mcp, get_components_func):
    """Register the runtime errors tool with the MCP server"""

    @mcp.tool()
    async def get_ocp_node_runtime_errors(request: Optional[DurationInput] = None) -> NodeHealthResponse:
        """
        Get Kubelet Runtime Operations Error Rate metrics for all node groups.

        This tool specifically monitors kubelet runtime operation errors:
        - Average error rate per second by operation type
        - Maximum error rate observed
        - Breakdown by operation type (exec_sync, container_status, etc.)

        Performance thresholds:
        - Healthy: < 0.01 errors/sec (occasional transient errors)
        - Warning: 0.01 - 0.1 errors/sec (investigate)
        - Critical: 0.1 - 1 errors/sec (active issues affecting workloads)
        - Severe: > 1 error/sec (major runtime problems)

        Args:
            request: Optional request object with duration field. Default duration is '1h'.

        Returns:
            NodeHealthResponse: Runtime operations error metrics for all node groups
        """
        duration = request.duration if request and request.duration else "1h"

        components = get_components_func()
        kubelet_runtime_operations_errors_collector = components.get('kubelet_runtime_operations_errors_collector')

        try:
            if not kubelet_runtime_operations_errors_collector:
                return NodeHealthResponse(
                    status="error",
                    error="Kubelet runtime operations errors collector not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )

            node_groups = {}
            overall_status = 'success'
            errors = []

            for node_group in ['controlplane', 'infra', 'workload', 'worker']:
                try:
                    # Collect runtime errors for all node groups
                    group_result = await kubelet_runtime_operations_errors_collector.collect_all_metrics(
                        node_group=node_group,
                        duration=duration
                    )

                    if group_result.get('status') == 'success':
                        # Filter to only include runtime errors metrics
                        filtered_result = {
                            'status': group_result['status'],
                            'total_nodes': group_result.get('total_nodes', 0),
                            'nodes': group_result.get('nodes', []),
                            'time_range': group_result.get('time_range', {}),
                            'metrics': {}
                        }

                        if 'kubelet_runtime_operations_errors_rate' in group_result.get('metrics', {}):
                            filtered_result['metrics']['kubelet_runtime_operations_errors_rate'] = \
                                group_result['metrics']['kubelet_runtime_operations_errors_rate']

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
                'category': 'node_kubelet_runtime_operations_errors',
                'node_groups': node_groups,
                'health_summary': {
                    'total_node_groups': len(node_groups),
                    'successful_groups': len(successful_groups),
                    'failed_groups': len(errors),
                    'runtime_errors': {
                        'severe_nodes': _count_runtime_errors_severe_nodes(node_groups),
                        'critical_nodes': _count_runtime_errors_critical_nodes(node_groups),
                        'warning_nodes': _count_runtime_errors_warning_nodes(node_groups),
                        'healthy_nodes': _count_runtime_errors_healthy_nodes(node_groups)
                    }
                }
            }

            if errors:
                combined_result['errors'] = errors
            if overall_status == 'error':
                combined_result['error'] = 'Failed to collect runtime error metrics for any node group'

            return NodeHealthResponse(
                status=overall_status,
                data=combined_result,
                error=combined_result.get('error'),
                timestamp=combined_result['timestamp'],
                duration=duration
            )

        except Exception as e:
            logger.error(f"Error collecting runtime error metrics: {e}")
            return NodeHealthResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )


def _count_runtime_errors_severe_nodes(node_groups):
    """Count nodes with severe runtime errors (>1 error/sec)"""
    count = 0
    for group_data in node_groups.values():
        if group_data.get('status') != 'success':
            continue
        metrics = group_data.get('metrics', {})
        runtime_errors = metrics.get('kubelet_runtime_operations_errors_rate', {})
        nodes = runtime_errors.get('nodes', {})
        for node_data in nodes.values():
            total = node_data.get('total', {})
            avg_error_rate = total.get('avg', 0)
            if avg_error_rate > 1.0:  # Severe: > 1 error/sec
                count += 1
    return count


def _count_runtime_errors_critical_nodes(node_groups):
    """Count nodes with critical runtime errors (>0.1 errors/sec)"""
    count = 0
    for group_data in node_groups.values():
        if group_data.get('status') != 'success':
            continue
        metrics = group_data.get('metrics', {})
        runtime_errors = metrics.get('kubelet_runtime_operations_errors_rate', {})
        nodes = runtime_errors.get('nodes', {})
        for node_data in nodes.values():
            total = node_data.get('total', {})
            avg_error_rate = total.get('avg', 0)
            if 0.1 < avg_error_rate <= 1.0:  # Critical: 0.1 - 1 error/sec
                count += 1
    return count


def _count_runtime_errors_warning_nodes(node_groups):
    """Count nodes with warning runtime errors (0.01 - 0.1 errors/sec)"""
    count = 0
    for group_data in node_groups.values():
        if group_data.get('status') != 'success':
            continue
        metrics = group_data.get('metrics', {})
        runtime_errors = metrics.get('kubelet_runtime_operations_errors_rate', {})
        nodes = runtime_errors.get('nodes', {})
        for node_data in nodes.values():
            total = node_data.get('total', {})
            avg_error_rate = total.get('avg', 0)
            if 0.01 <= avg_error_rate <= 0.1:  # Warning: 0.01 - 0.1 errors/sec
                count += 1
    return count


def _count_runtime_errors_healthy_nodes(node_groups):
    """Count nodes with healthy runtime errors (<0.01 errors/sec)"""
    count = 0
    for group_data in node_groups.values():
        if group_data.get('status') != 'success':
            continue
        metrics = group_data.get('metrics', {})
        runtime_errors = metrics.get('kubelet_runtime_operations_errors_rate', {})
        nodes = runtime_errors.get('nodes', {})
        for node_data in nodes.values():
            total = node_data.get('total', {})
            avg_error_rate = total.get('avg', 0)
            if avg_error_rate < 0.01:  # Healthy: < 0.01 errors/sec
                count += 1
    return count
