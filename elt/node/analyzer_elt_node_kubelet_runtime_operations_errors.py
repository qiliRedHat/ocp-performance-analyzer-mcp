"""
Extract, Load, Transform module for Kubelet Runtime Operations Errors
Handles runtime operations error data from tools/node/node_kubelet_runtime_operations_errors.py
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class nodeKubeletRuntimeOperationsErrorsELT(utilityELT):
    """Extract, Load, Transform class for kubelet runtime operations errors data"""

    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'kubelet_runtime_operations_errors_rate': {
                'title': 'Kubelet Runtime Operations Error Rate',
                'unit': 'errors/sec',
                'thresholds': {'severe': 1.0, 'critical': 0.1, 'warning': 0.01}  # errors per second
            }
        }

    def extract_kubelet_runtime_operations_errors(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract kubelet runtime operations errors information

        Handles structure with node_groups containing kubelet runtime operations errors
        """

        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict) and 'node_groups' not in data:
            actual_data = data['data']

        logger.debug(f"Extracting runtime errors, actual_data keys: {list(actual_data.keys())}")
        logger.debug(f"Has node_groups: {'node_groups' in actual_data}")

        structured = {
            'overview': [],
            'timestamp': actual_data.get('timestamp', ''),
            'duration': actual_data.get('duration', ''),
            'time_range': {},
        }

        # Initialize role-based tables for runtime errors metric
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            structured[f'kubelet_runtime_operations_errors_rate_{role}'] = []

        # Handle new structure with node_groups
        node_groups = actual_data.get('node_groups', {})

        if node_groups:
            # Process each node group (controlplane, infra, workload, worker)
            for group_name, group_data in node_groups.items():
                if group_data.get('status') != 'success':
                    logger.debug(f"Skipping {group_name} group: {group_data.get('error', 'unknown error')}")
                    continue

                # Get nodes list with roles for this group
                nodes_list = group_data.get('nodes', [])
                node_role_map = {node['name']: node.get('role', group_name) for node in nodes_list}

                # Update time_range from first successful group
                if not structured['time_range'] and 'time_range' in group_data:
                    structured['time_range'] = group_data.get('time_range', {})

                # Extract metrics from this group
                metrics = group_data.get('metrics', {})

                # Extract runtime operations errors
                if 'kubelet_runtime_operations_errors_rate' in metrics:
                    self._extract_runtime_operations_errors(
                        metrics['kubelet_runtime_operations_errors_rate'],
                        structured,
                        node_role_map
                    )

            # Generate overview from all node groups
            self._generate_overview_from_groups(node_groups, structured)
        else:
            # Fallback to old structure (single node group)
            nodes_list = actual_data.get('nodes', [])
            node_role_map = {node['name']: node.get('role', 'worker') for node in nodes_list}

            structured['time_range'] = actual_data.get('time_range', {})

            metrics = actual_data.get('metrics', {})

            # Extract runtime operations errors
            if 'kubelet_runtime_operations_errors_rate' in metrics:
                self._extract_runtime_operations_errors(
                    metrics['kubelet_runtime_operations_errors_rate'],
                    structured,
                    node_role_map
                )

            # Generate overview
            self._generate_overview(actual_data, structured, node_role_map)

        return structured

    def _extract_runtime_operations_errors(self, metric_data: Dict[str, Any],
                                           structured: Dict[str, Any],
                                           node_role_map: Dict[str, str]):
        """Extract runtime operations error rate metrics grouped by role"""

        if metric_data.get('status') != 'success':
            logger.warning(f"Runtime operations errors metric status: {metric_data.get('status')}")
            return

        nodes = metric_data.get('nodes', {})

        # Collect all avg error rates for top identification
        all_error_rates = []
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            total = node_data.get('total', {})
            avg_rate = float(total.get('avg', 0))
            all_error_rates.append((node_name, role, avg_rate))

        # Find top error rate
        top_error_rate = max((v[2] for v in all_error_rates), default=0) if all_error_rates else 0

        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'kubelet_runtime_operations_errors_rate_{role}'

            total = node_data.get('total', {})
            avg_rate = float(total.get('avg', 0))
            max_rate = float(total.get('max', 0))
            unit = total.get('unit', 'errors/sec')

            is_top = (avg_rate == top_error_rate and avg_rate > 0)

            # Get operation types breakdown
            operation_types = node_data.get('operation_types', {})
            op_types_str = ', '.join([
                f"{op_type}: {op_data.get('avg', 0):.3f}"
                for op_type, op_data in sorted(operation_types.items(),
                                               key=lambda x: x[1].get('avg', 0),
                                               reverse=True)[:3]  # Top 3 operation types
            ])

            structured[table_key].append({
                'Node': self.truncate_node_name(node_name),
                'Role': role.title(),
                'Metric': 'Runtime Errors',
                'Avg_Rate': self._format_error_rate(avg_rate, is_top=is_top),
                'Max_Rate': self._format_error_rate(max_rate, is_top=False),
                'Top_Operations': op_types_str if op_types_str else 'None',
                'Unit': unit
            })

    def _format_error_rate(self, value: float, is_top: bool = False) -> str:
        """Format runtime operations error rate with thresholds and top highlighting

        Healthy: < 0.01 errors/sec
        Warning: 0.01 - 0.1 errors/sec
        Critical: 0.1 - 1 errors/sec
        Severe: > 1 error/sec

        Args:
            value: Error rate in errors/sec
            is_top: If True, this is the highest (worst) error rate
        """
        thresholds = self.metric_configs['kubelet_runtime_operations_errors_rate']['thresholds']

        if is_top:
            # Highest error rate = worst performance
            return f'<span class="text-danger font-weight-bold bg-warning-light px-1">🔺 {value:.3f}</span>'
        elif value >= thresholds['severe']:
            return f'<span class="text-danger font-weight-bold">🔴 {value:.3f}</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {value:.3f}</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{value:.3f}</span>'
        else:
            return f'<span class="text-success">✅ {value:.3f}</span>'

    def _generate_overview(self, data: Dict[str, Any],
                        structured: Dict[str, Any],
                        node_role_map: Dict[str, str]):
        """Generate runtime errors overview"""
        # Count nodes by role
        role_counts = {}
        for node_name, role in node_role_map.items():
            role_counts[role] = role_counts.get(role, 0) + 1

        metrics = data.get('metrics', {})
        metrics_collected = len([k for k in metrics.keys() if metrics[k].get('status') == 'success'])

        # Role breakdown
        for role, count in sorted(role_counts.items()):
            structured['overview'].append({
                'Role': role.title(),
                'Nodes': count,
                'Metrics Collected': metrics_collected,
                'Status': self.create_status_badge('success', 'Monitoring')
            })

    def _generate_overview_from_groups(self, node_groups: Dict[str, Any],
                                      structured: Dict[str, Any]):
        """Generate runtime errors overview from node_groups structure"""
        role_counts = {}
        total_metrics_collected = 0

        for group_name, group_data in node_groups.items():
            if group_data.get('status') != 'success':
                continue

            nodes_list = group_data.get('nodes', [])
            metrics = group_data.get('metrics', {})
            metrics_collected = len([k for k in metrics.keys() if metrics[k].get('status') == 'success'])
            total_metrics_collected = max(total_metrics_collected, metrics_collected)

            for node in nodes_list:
                role = node.get('role', group_name)
                role_counts[role] = role_counts.get(role, 0) + 1

        # Add overall top runtime error rate across all groups
        all_error_nodes = []
        for group_name, group_data in node_groups.items():
            if group_data.get('status') != 'success':
                continue

            metrics = group_data.get('metrics', {})
            runtime_errors = metrics.get('kubelet_runtime_operations_errors_rate', {})
            if runtime_errors.get('status') == 'success':
                nodes = runtime_errors.get('nodes', {})
                for node_name, node_data in nodes.items():
                    total = node_data.get('total', {})
                    avg_rate = total.get('avg', 0)
                    role = group_data.get('nodes', [{}])[0].get('role', group_name) if group_data.get('nodes') else group_name
                    # Try to get actual role from nodes list
                    for n in group_data.get('nodes', []):
                        if n.get('name') == node_name:
                            role = n.get('role', group_name)
                            break
                    all_error_nodes.append({
                        'node': node_name,
                        'role': role,
                        'avg_rate': avg_rate
                    })

        if all_error_nodes:
            # Sort by avg_rate descending
            all_error_nodes.sort(key=lambda x: x.get('avg_rate', 0), reverse=True)
            top_error_node = all_error_nodes[0]
            top_error_rate = top_error_node.get('avg_rate', 0)
            # Only show if there are actual errors (> 0)
            if top_error_rate > 0:
                structured['overview'].append({
                    'Metric': 'Highest Runtime Error Rate',
                    'Node': self.truncate_node_name(top_error_node.get('node', '')),
                    'Role': top_error_node.get('role', '').title(),
                    'Value': f"{top_error_rate:.3f} err/s",
                    'Status': self._get_error_rate_status(top_error_rate)
                })

        # Role breakdown
        for role, count in sorted(role_counts.items()):
            structured['overview'].append({
                'Role': role.title(),
                'Nodes': count,
                'Metrics Collected': total_metrics_collected,
                'Status': self.create_status_badge('success', 'Monitoring')
            })

    def _get_error_rate_status(self, error_rate: float) -> str:
        """Get status badge for runtime operations error rate"""
        thresholds = self.metric_configs['kubelet_runtime_operations_errors_rate']['thresholds']

        if error_rate >= thresholds['severe']:
            return self.create_status_badge('error', 'Severe')
        elif error_rate >= thresholds['critical']:
            return self.create_status_badge('error', 'Critical')
        elif error_rate >= thresholds['warning']:
            return self.create_status_badge('warning', 'Warning')
        else:
            return self.create_status_badge('success', 'Healthy')

    def _get_node_role(self, node_name: str, node_role_map: Dict[str, str] = None) -> str:
        """Get role for a node using get_node_role_from_labels, with fallback to node_role_map"""
        # First try to get role from labels using utility method
        role = self.get_node_role_from_labels(node_name)

        # If utility method returns 'worker' (default) and we have a node_role_map, check it
        if role == 'worker' and node_role_map and node_name in node_role_map:
            role = node_role_map[node_name]

        return role

    def summarize_kubelet_runtime_operations_errors(self, data: Dict[str, Any]) -> str:
        """Generate kubelet runtime operations errors summary as HTML"""
        try:
            summary_items: List[str] = []

            overview_data = data.get('overview', [])

            # Calculate total nodes
            total_nodes = 0
            for item in overview_data:
                if item.get('Role') and item.get('Nodes'):
                    nodes_value = item.get('Nodes', 0)
                    if isinstance(nodes_value, (int, float)):
                        total_nodes += int(nodes_value)

            if total_nodes > 0:
                summary_items.append(f"<li>Total Nodes Monitored: {total_nodes}</li>")

            # Runtime error rate info
            for item in overview_data:
                if item.get('Metric') == 'Highest Runtime Error Rate':
                    node = item.get('Node', 'Unknown')
                    role = item.get('Role', 'Unknown')
                    value = item.get('Value', 'N/A')
                    summary_items.append(
                        f"<li>Highest Runtime Error Rate: {node} ({role}) - {value}</li>"
                    )

            # Role breakdown
            for item in overview_data:
                if item.get('Role') and item.get('Nodes'):
                    role = item.get('Role', 'Unknown')
                    nodes = item.get('Nodes', 0)
                    metrics = item.get('Metrics Collected', 0)
                    summary_items.append(f"<li>{role}: {nodes} nodes, {metrics} metrics</li>")

            # Time range
            time_range = data.get('time_range', {})
            if time_range:
                duration = data.get('duration', 'unknown')
                summary_items.append(f"<li>Monitoring Duration: {duration}</li>")
                if 'start' in time_range and 'end' in time_range:
                    summary_items.append(
                        f"<li>Time Range: {time_range.get('start')} to {time_range.get('end')}</li>"
                    )

            # Runtime error thresholds
            summary_items.append("<li><strong>Kubelet Runtime Operations Errors Thresholds:</strong></li>")
            summary_items.append("<li style='margin-left: 20px;'>✅ Healthy: &lt; 0.01 errors/sec</li>")
            summary_items.append("<li style='margin-left: 20px;'>⚠️ Warning: 0.01 - 0.1 errors/sec</li>")
            summary_items.append("<li style='margin-left: 20px;'>🔴 Critical: 0.1 - 1 errors/sec</li>")
            summary_items.append("<li style='margin-left: 20px;'>🔴 Severe: &gt; 1 error/sec</li>")

            return (
                "<div class=\"runtime-errors-summary\">"
                "<h4>Kubelet Runtime Operations Errors Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )

        except Exception as e:
            logger.error(f"Failed to generate kubelet runtime operations errors summary: {e}")
            return "<div class=\"runtime-errors-summary\">Kubelet runtime operations errors metrics collected</div>"

    def transform_to_dataframes(self, structured_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform structured data into pandas DataFrames"""
        dataframes = {}

        try:
            for key, value in structured_data.items():
                if isinstance(value, list) and value:
                    df = pd.DataFrame(value)
                    if not df.empty:
                        # Decode unicode in object columns
                        for col in df.columns:
                            if df[col].dtype == 'object':
                                df[col] = df[col].astype(str).apply(self.decode_unicode_escapes)

                        dataframes[key] = df

        except Exception as e:
            logger.error(f"Failed to transform kubelet runtime operations errors data to DataFrames: {e}")

        return dataframes

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by role"""
        html_tables = {}

        try:
            # Overview table first
            if 'overview' in dataframes and not dataframes['overview'].empty:
                html_tables['overview'] = self.create_html_table(
                    dataframes['overview'],
                    'Kubelet Runtime Operations Errors Overview'
                )

            # Generate tables for each role
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                table_key = f'kubelet_runtime_operations_errors_rate_{role}'
                if table_key in dataframes and not dataframes[table_key].empty:
                    display_name = f"Kubelet Runtime Operations Error Rate - {role.title()} Nodes"
                    html_tables[table_key] = self.create_html_table(
                        dataframes[table_key],
                        display_name
                    )

        except Exception as e:
            logger.error(f"Failed to generate HTML tables for kubelet runtime operations errors: {e}")

        return html_tables
