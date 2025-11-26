"""
Extract, Load, Transform module for Node PLEG Relist Metrics
Handles PLEG relist latency data from tools/node/node_pleg_relist.py
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class nodePlegRelistELT(utilityELT):
    """Extract, Load, Transform class for PLEG relist latency metrics data"""

    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'p99_kubelet_pleg_relist_duration': {
                'title': 'PLEG Relist Duration (P99)',
                'unit': 'second',
                'thresholds': {'critical': 180.0, 'warning': 10.0}  # seconds (3 minutes, 10 seconds)
            }
        }

    def extract_pleg_relist(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract PLEG relist latency information from node_pleg_relist.py output

        Handles structure with node_groups containing PLEG relist latency
        """

        # Handle nested data structure
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict) and 'node_groups' not in data:
            actual_data = data['data']

        logger.debug(f"Extracting PLEG relist, actual_data keys: {list(actual_data.keys())}")
        logger.debug(f"Has node_groups: {'node_groups' in actual_data}")

        structured = {
            'overview': [],
            'timestamp': actual_data.get('timestamp', ''),
            'duration': actual_data.get('duration', ''),
            'time_range': {},
        }

        # Initialize role-based tables for PLEG metric
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            structured[f'p99_kubelet_pleg_relist_duration_{role}'] = []

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

                # Extract PLEG relist duration
                if 'p99_kubelet_pleg_relist_duration' in metrics:
                    self._extract_pleg_relist_duration(
                        metrics['p99_kubelet_pleg_relist_duration'],
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

            # Extract PLEG relist duration
            if 'p99_kubelet_pleg_relist_duration' in metrics:
                self._extract_pleg_relist_duration(
                    metrics['p99_kubelet_pleg_relist_duration'],
                    structured,
                    node_role_map
                )

            # Generate overview
            self._generate_overview(actual_data, structured, node_role_map)

        return structured

    def _extract_pleg_relist_duration(self, metric_data: Dict[str, Any],
                                    structured: Dict[str, Any],
                                    node_role_map: Dict[str, str]):
        """Extract PLEG relist duration metrics grouped by role"""

        if metric_data.get('status') != 'success':
            logger.warning(f"PLEG relist duration metric status: {metric_data.get('status')}")
            return

        nodes = metric_data.get('nodes', {})

        # Collect all p99 values for top identification and critical detection
        all_p99_values = []
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            p99_val = float(node_data.get('p99', 0))
            all_p99_values.append((node_name, role, p99_val))

        # Find top 1 p99 value
        top_p99 = max((v[2] for v in all_p99_values), default=0) if all_p99_values else 0

        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'p99_kubelet_pleg_relist_duration_{role}'

            p99_val = float(node_data.get('p99', 0))
            max_val = float(node_data.get('max', 0))
            min_val = float(node_data.get('min', 0))
            unit = node_data.get('unit', 'second')

            is_top = (p99_val == top_p99 and p99_val > 0)

            structured[table_key].append({
                'Node': self.truncate_node_name(node_name),
                'Role': role.title(),
                'Metric': 'PLEG Relist (P99)',
                'P99_Avg': self._format_pleg_latency(p99_val, is_top=is_top),
                'P99_Max': self._format_pleg_latency(max_val, is_top=False),
                'P99_Min': self._format_pleg_latency(min_val, is_top=False),
                'Unit': unit
            })

    def _format_pleg_latency(self, value: float, is_top: bool = False) -> str:
        """Format PLEG latency value with thresholds and top highlighting

        Normal: < 10s
        Warning: 10s - 180s (3 minutes)
        Critical: > 180s (3 minutes)

        Args:
            value: Latency in seconds
            is_top: If True, this is the highest (worst) latency
        """
        thresholds = self.metric_configs['p99_kubelet_pleg_relist_duration']['thresholds']

        if is_top:
            # Highest latency = worst performance, use alert icon with danger styling
            return f'<span class="text-danger font-weight-bold bg-warning-light px-1">🔺 {value:.4f}s</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {value:.4f}s</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{value:.4f}s</span>'
        else:
            return f'<span class="text-success">{value:.4f}s</span>'

    def _generate_overview(self, data: Dict[str, Any],
                        structured: Dict[str, Any],
                        node_role_map: Dict[str, str]):
        """Generate PLEG overview"""
        # Count nodes by role
        role_counts = {}
        for node_name, role in node_role_map.items():
            role_counts[role] = role_counts.get(role, 0) + 1

        metrics = data.get('metrics', {})
        metrics_collected = len([k for k in metrics.keys() if metrics[k].get('status') == 'success'])

        # Add top nodes info if available
        top_nodes_info = data.get('top_nodes_by_pleg_latency', {})
        if top_nodes_info and top_nodes_info.get('count', 0) > 0:
            top_nodes = top_nodes_info.get('nodes', [])
            if top_nodes:
                top_node = top_nodes[0]
                structured['overview'].append({
                    'Metric': 'Top PLEG Latency',
                    'Node': self.truncate_node_name(top_node.get('node', '')),
                    'Role': top_node.get('role', '').title(),
                    'Value': f"{top_node.get('p99_latency', 0):.4f}s",
                    'Status': self._get_pleg_status(top_node.get('p99_latency', 0))
                })

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
        """Generate PLEG overview from node_groups structure"""
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

        # Add overall top PLEG latency across all groups
        all_pleg_nodes = []
        for group_name, group_data in node_groups.items():
            if group_data.get('status') != 'success':
                continue

            top_nodes_info = group_data.get('top_nodes_by_pleg_latency', {})
            if top_nodes_info and top_nodes_info.get('count', 0) > 0:
                all_pleg_nodes.extend(top_nodes_info.get('nodes', []))

        if all_pleg_nodes:
            # Sort by p99_latency descending
            all_pleg_nodes.sort(key=lambda x: x.get('p99_latency', 0), reverse=True)
            top_node = all_pleg_nodes[0]
            structured['overview'].append({
                'Metric': 'Highest PLEG Latency',
                'Node': self.truncate_node_name(top_node.get('node', '')),
                'Role': top_node.get('role', '').title(),
                'Value': f"{top_node.get('p99_latency', 0):.4f}s",
                'Status': self._get_pleg_status(top_node.get('p99_latency', 0))
            })

        # Role breakdown
        for role, count in sorted(role_counts.items()):
            structured['overview'].append({
                'Role': role.title(),
                'Nodes': count,
                'Metrics Collected': total_metrics_collected,
                'Status': self.create_status_badge('success', 'Monitoring')
            })

    def _get_pleg_status(self, latency: float) -> str:
        """Get status badge for PLEG latency"""
        thresholds = self.metric_configs['p99_kubelet_pleg_relist_duration']['thresholds']

        if latency >= thresholds['critical']:
            return self.create_status_badge('error', 'Critical')
        elif latency >= thresholds['warning']:
            return self.create_status_badge('warning', 'Warning')
        else:
            return self.create_status_badge('success', 'Normal')

    def _get_node_role(self, node_name: str, node_role_map: Dict[str, str] = None) -> str:
        """Get role for a node using get_node_role_from_labels, with fallback to node_role_map"""
        # First try to get role from labels using utility method
        role = self.get_node_role_from_labels(node_name)

        # If utility method returns 'worker' (default) and we have a node_role_map, check it
        if role == 'worker' and node_role_map and node_name in node_role_map:
            role = node_role_map[node_name]

        return role

    def summarize_pleg_relist(self, data: Dict[str, Any]) -> str:
        """Generate PLEG relist summary as HTML"""
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

            # Top PLEG latency info
            for item in overview_data:
                if item.get('Metric') in ['Top PLEG Latency', 'Highest PLEG Latency']:
                    node = item.get('Node', 'Unknown')
                    role = item.get('Role', 'Unknown')
                    value = item.get('Value', 'N/A')
                    summary_items.append(
                        f"<li>Highest PLEG Latency: {node} ({role}) - {value}</li>"
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

            # PLEG thresholds
            summary_items.append("<li><strong>PLEG Latency Thresholds:</strong></li>")
            summary_items.append("<li style='margin-left: 20px;'>✅ Normal: &lt; 10s</li>")
            summary_items.append("<li style='margin-left: 20px;'>⚠️ Warning: 10s - 180s (3 minutes)</li>")
            summary_items.append("<li style='margin-left: 20px;'>🔴 Critical: &gt; 180s (3 minutes)</li>")

            return (
                "<div class=\"pleg-relist-summary\">"
                "<h4>PLEG Relist Latency Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )

        except Exception as e:
            logger.error(f"Failed to generate PLEG summary: {e}")
            return "<div class=\"pleg-relist-summary\">PLEG relist metrics collected</div>"

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
            logger.error(f"Failed to transform PLEG data to DataFrames: {e}")

        return dataframes

    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by role"""
        html_tables = {}

        try:
            # Overview table first
            if 'overview' in dataframes and not dataframes['overview'].empty:
                html_tables['overview'] = self.create_html_table(
                    dataframes['overview'],
                    'PLEG Relist Latency Overview'
                )

            # Generate tables for each role
            for role in ['controlplane', 'infra', 'worker', 'workload']:
                table_key = f'p99_kubelet_pleg_relist_duration_{role}'
                if table_key in dataframes and not dataframes[table_key].empty:
                    display_name = f"PLEG Relist Duration (P99) - {role.title()} Nodes"
                    html_tables[table_key] = self.create_html_table(
                        dataframes[table_key],
                        display_name
                    )

        except Exception as e:
            logger.error(f"Failed to generate HTML tables for PLEG: {e}")

        return html_tables
