"""
PLEG Relist Latency Collector Module
Collects and analyzes PLEG (Pod Lifecycle Event Generator) relist latency metrics
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.metrics_config_reader import Config
from tools.utils.promql_basequery import PrometheusBaseQuery
from tools.utils.promql_utility import mcpToolsUtility


class plegRelistCollector:
    """Collector for PLEG relist latency metrics"""

    def __init__(self, ocp_auth, prometheus_config: Dict[str, Any]):
        self.ocp_auth = ocp_auth
        self.prometheus_config = prometheus_config
        self.logger = logging.getLogger(__name__)

        # Initialize config and load metrics file
        self.config = Config()
        self._load_metrics_config()

        # Initialize utility for node operations and common functions
        self.utility = mcpToolsUtility(ocp_auth)

        # Extract token using utility function
        self._prom_token = mcpToolsUtility.extract_token_from_config(prometheus_config)

    def _load_metrics_config(self):
        """Load node metrics configuration file"""
        try:
            # Calculate project root (go up from tools/node/ to project root)
            project_root = Path(__file__).parent.parent.parent
            metrics_file_path = project_root / 'config' / 'metrics-node.yml'

            self.logger.info(f"Looking for metrics file at: {metrics_file_path.absolute()}")

            if not metrics_file_path.exists():
                self.logger.error(f"✗ Metrics file not found at: {metrics_file_path.absolute()}")
                self.logger.error(f"  Current file location: {Path(__file__).absolute()}")
                self.logger.error(f"  Project root: {project_root.absolute()}")

                # Try alternative paths
                alternative_paths = [
                    Path.cwd() / 'config' / 'metrics-node.yml',
                    Path.cwd().parent / 'config' / 'metrics-node.yml',
                ]

                for alt_path in alternative_paths:
                    if alt_path.exists():
                        self.logger.info(f"✓ Found metrics file at alternative location: {alt_path}")
                        metrics_file_path = alt_path
                        break
                else:
                    self.logger.error("✗ Could not find metrics file in any location")
                    return
            else:
                self.logger.info(f"✓ Found metrics file at: {metrics_file_path}")

            # Load the metrics file
            self.logger.info(f"Loading metrics from: {metrics_file_path.absolute()}")
            result = self.config.load_metrics_file(str(metrics_file_path.absolute()))

            if result.get('success'):
                self.logger.info(f"✓ Loaded {result.get('metrics_loaded', 0)} metrics from {result.get('file_name')}")
                self.logger.info(f"✓ Categories: {result.get('categories_loaded', [])}")
            else:
                self.logger.error(f"✗ Failed to load metrics: {result.get('error')}")

        except Exception as e:
            self.logger.error(f"✗ Error loading metrics config: {e}", exc_info=True)

    async def _query_range_wrap(self, prom: PrometheusBaseQuery, query: str, start: str, end: str, step: str) -> Dict[str, Any]:
        """Wrapper for range query"""
        data = await prom.query_range(query, start, end, step)
        return {'status': 'success', 'data': data}

    def _normalize_instance_to_full_name(self, instance: str, node_name_map: Dict[str, str]) -> str:
        """Normalize instance name to full node name

        Args:
            instance: Instance name from Prometheus (may be short or full)
            node_name_map: Mapping of short names to full names

        Returns:
            Full node name
        """
        # Remove port if present
        instance_clean = instance.split(':')[0]

        # Check if it's already a full name
        if instance_clean in node_name_map.values():
            return instance_clean

        # Check if it's a short name
        if instance_clean in node_name_map:
            return node_name_map[instance_clean]

        # Try to find by prefix match
        for short_name, full_name in node_name_map.items():
            if instance_clean.startswith(short_name) or full_name.startswith(instance_clean):
                return full_name

        # If no match found, return as is
        return instance_clean

    def _verify_metric_loaded(self, metric_name: str) -> bool:
        """Verify a metric is loaded and log details if not found"""
        metric = self.config.get_metric_by_name(metric_name)

        if not metric:
            self.logger.error(f"✗ Metric '{metric_name}' not found in config")

            # Debug: show what we have
            all_metrics = self.config.get_all_metrics()
            self.logger.error(f"  Total metrics loaded: {self.config.get_metrics_count()}")
            self.logger.error(f"  Available categories: {list(all_metrics.keys())}")

            # Show all available metric names
            all_names = []
            for category, metrics_list in all_metrics.items():
                for m in metrics_list:
                    all_names.append(m.get('name', 'unnamed'))

            self.logger.error(f"  Available metric names: {all_names}")
            return False
        else:
            self.logger.debug(f"✓ Metric '{metric_name}' found in category '{metric.get('category')}'")
            return True

    async def _get_nodes_by_group(self, node_group: str = 'master') -> tuple[List[str], Dict[str, str], Dict[str, str]]:
        """Get list of node names by group type with full names and role mapping

        Args:
            node_group: Node group type (master, controlplane, worker, infra, workload)

        Returns:
            Tuple of (full_node_names, short_to_full_map, node_to_role_map)
        """
        try:
            groups = await self.utility.get_node_groups()

            # Normalize group name (master -> controlplane)
            if node_group.lower() in ['master', 'controlplane', 'control-plane']:
                node_group = 'controlplane'

            node_list = groups.get(node_group, []) if isinstance(groups, dict) else []

            # Canonicalize nodes by short prefix, preferring FQDN when available
            nodes_by_short: Dict[str, str] = {}
            node_to_role: Dict[str, str] = {}
            for node_info in node_list:
                raw_name = node_info.get('name', '')
                if not raw_name:
                    continue
                # Remove any port suffix
                raw_name = raw_name.split(':')[0]
                role = node_info.get('role', node_group)

                short_name = raw_name.split('.')[0]
                existing = nodes_by_short.get(short_name)
                if existing is None:
                    nodes_by_short[short_name] = raw_name
                    node_to_role[raw_name] = role
                else:
                    # Prefer the FQDN (name containing a dot)
                    if '.' in raw_name and '.' not in existing:
                        nodes_by_short[short_name] = raw_name
                        node_to_role[raw_name] = role
                    else:
                        # Keep existing canonical name; still record role for visibility
                        node_to_role.setdefault(existing, role)

            # Build short->full mapping and final canonical full name list
            short_to_full: Dict[str, str] = {}
            full_names: List[str] = []
            for short_name, canonical in nodes_by_short.items():
                short_to_full[short_name] = canonical
                # Also map canonical to itself to simplify normalization checks
                short_to_full[canonical] = canonical
                full_names.append(canonical)

            self.logger.info(f"Retrieved {len(full_names)} {node_group} nodes (canonicalized to full names when available): {full_names}")
            return full_names, short_to_full, node_to_role

        except Exception as e:
            self.logger.error(f"Error getting {node_group} nodes: {e}", exc_info=True)
            return [], {}, {}

    def _get_top_n_nodes_by_metric(self, nodes_data: Dict[str, Any], metric_key: str = 'p99',
                                    n: int = 3) -> List[Dict[str, Any]]:
        """Get top N nodes sorted by a specific metric value

        Args:
            nodes_data: Dictionary of node data
            metric_key: Key to extract metric from (e.g., 'p99', 'avg', 'max')
            n: Number of top nodes to return

        Returns:
            List of top N nodes with their data
        """
        try:
            node_values = []

            for node_name, node_data in nodes_data.items():
                if isinstance(node_data, dict) and metric_key in node_data:
                    value = node_data[metric_key]
                    node_values.append({
                        'node': node_name,
                        'value': value,
                        'data': node_data
                    })

            # Sort by value (descending) and take top N
            sorted_nodes = sorted(node_values, key=lambda x: x['value'], reverse=True)[:n]

            return sorted_nodes

        except Exception as e:
            self.logger.error(f"Error getting top N nodes: {e}", exc_info=True)
            return []

    async def collect_all_metrics(self, node_group: str = 'master', duration: Optional[str] = None,
                                  start_time: Optional[str] = None, end_time: Optional[str] = None,
                                  top_n_nodes: int = 3) -> Dict[str, Any]:
        """Collect PLEG relist latency metrics for specified node group

        Args:
            node_group: Node group to query (master/controlplane/worker/infra/workload)
            duration: Duration string (e.g., '1h', '30m') - used if start_time/end_time not provided
            start_time: Start time (ISO format or Unix timestamp)
            end_time: End time (ISO format or Unix timestamp)
            top_n_nodes: Number of top nodes to return by PLEG latency

        Returns:
            Dictionary containing collected metrics with full node names and roles
        """
        try:
            self.logger.info(f"Starting PLEG relist latency collection for group: {node_group}")

            # Get nodes for the specified group with full names and role mapping
            nodes, node_name_map, node_role_map = await self._get_nodes_by_group(node_group)
            if not nodes:
                return {
                    'status': 'error',
                    'error': f'No {node_group} nodes found',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }

            self.logger.info(f"Found {len(nodes)} {node_group} nodes: {nodes}")

            # Build prometheus config using utility function
            prom_config = mcpToolsUtility.build_prometheus_config(
                self.prometheus_config,
                self.ocp_auth
            )

            if not prom_config.get('url'):
                return {
                    'status': 'error',
                    'error': 'Prometheus URL not configured',
                    'hint': "Provide prometheus_config['url'] or set PROMETHEUS_URL env var",
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }

            async with PrometheusBaseQuery(prom_config['url'], self._prom_token) as prom:
                # Get time range - support both duration and explicit time range
                if start_time and end_time:
                    start_str, end_str = start_time, end_time
                    self.logger.info(f"Using explicit time range: {start_str} to {end_str}")
                else:
                    if duration is None:
                        duration = '1h'  # Default duration
                    start_str, end_str = prom.get_time_range_from_duration(duration)
                    self.logger.info(f"Using duration {duration}: {start_str} to {end_str}")

                # Collect PLEG metrics
                pleg_relist = await self.collect_pleg_relist_latency(
                    nodes, start_str, end_str, node_name_map
                )

            # Build node details with role information
            node_details = []
            for full_name in nodes:
                role = node_role_map.get(full_name, node_group)
                node_details.append({
                    'name': full_name,
                    'role': role
                })

            # Get top N nodes by PLEG latency
            top_nodes = []
            if pleg_relist.get('status') == 'success':
                pleg_nodes = pleg_relist.get('nodes', {})
                top_nodes_data = self._get_top_n_nodes_by_metric(
                    pleg_nodes,
                    metric_key='p99',
                    n=top_n_nodes
                )

                self.logger.info(f"Top {top_n_nodes} nodes by PLEG relist latency:")
                for idx, node in enumerate(top_nodes_data, 1):
                    node_name = node['node']
                    role = node_role_map.get(node_name, node_group)
                    self.logger.info(f"  {idx}. {node_name}: {node['value']}s")
                    top_nodes.append({
                        'rank': idx,
                        'node': node_name,
                        'role': role,
                        'p99_latency': node['value'],
                        'unit': 'second'
                    })

            # Aggregate results
            result = {
                'status': 'success',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'query_params': {
                    'node_group': node_group,
                    'duration': duration if not (start_time and end_time) else None,
                    'start_time': start_str,
                    'end_time': end_str
                },
                'time_range': {
                    'start': start_str,
                    'end': end_str
                },
                'total_nodes': len(nodes),
                'nodes': node_details,
                'metrics': {
                    'p99_kubelet_pleg_relist_duration': pleg_relist
                }
            }

            # Add top nodes section if applicable
            if top_nodes:
                result['top_nodes_by_pleg_latency'] = {
                    'count': len(top_nodes),
                    'sort_by': 'p99_latency',
                    'nodes': top_nodes
                }

            return result

        except Exception as e:
            self.logger.error(f"Error collecting PLEG relist latency metrics: {e}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

    async def collect_pleg_relist_latency(self, nodes: List[str],
                                          start: str, end: str,
                                          node_name_map: Dict[str, str],
                                          step: str = '15s') -> Dict[str, Any]:
        """Collect p99 kubelet PLEG relist duration for specified nodes

        PLEG (Pod Lifecycle Event Generator) relist latency indicates how long it takes
        the kubelet to relist pods. High latency can indicate performance issues.

        Args:
            nodes: List of node names to query
            start: Start time for query
            end: End time for query
            node_name_map: Mapping of short names to full node names
            step: Query step interval

        Returns:
            Dictionary containing PLEG relist latency metrics
        """
        try:
            if not self._verify_metric_loaded('p99_kubelet_pleg_relist_duration'):
                return {'status': 'error', 'error': 'Metric configuration not found'}

            metric_config = self.config.get_metric_by_name('p99_kubelet_pleg_relist_duration')

            # Build prometheus config using utility function
            prom_config = mcpToolsUtility.build_prometheus_config(
                self.prometheus_config,
                self.ocp_auth
            )

            if not prom_config.get('url'):
                return {
                    'status': 'error',
                    'error': 'Prometheus URL not configured',
                    'hint': "Provide prometheus_config['url'] or set PROMETHEUS_URL env var"
                }

            async with PrometheusBaseQuery(prom_config['url'], self._prom_token) as prom:
                # Build query with node pattern using utility function
                node_pattern = mcpToolsUtility.get_node_pattern(nodes)
                # Note: kubelet metrics use 'node' label, not 'instance'
                query = f'histogram_quantile(0.99, sum(rate(kubelet_pleg_relist_duration_seconds_bucket{{node=~"{node_pattern}"}}[5m])) by (node, le))'

                self.logger.debug(f"Querying PLEG relist duration: {query}")

                # Execute range query
                result = await self._query_range_wrap(prom, query, start, end, step)

                if result['status'] != 'success':
                    self.logger.error(f"Query failed: {result.get('error')}")
                    return {'status': 'error', 'error': result.get('error', 'Query failed')}

                # Process raw results
                raw_results = result.get('data', {}).get('result', [])
                self.logger.info(f"Got {len(raw_results)} PLEG relist series")

                # Calculate per-node statistics
                nodes_result = {}
                for item in raw_results:
                    # Try 'node' label first (kubelet metrics), fall back to 'instance' label
                    metric_labels = item.get('metric', {})
                    node_identifier = metric_labels.get('node') or metric_labels.get('instance', 'unknown')
                    values = item.get('values', [])

                    # Normalize to full node name
                    full_name = self._normalize_instance_to_full_name(node_identifier, node_name_map)

                    # Extract numeric values using utility function
                    numeric_values = mcpToolsUtility.extract_numeric_values(values)

                    if numeric_values:
                        # Calculate min/max/avg directly from time series values
                        avg_val = sum(numeric_values) / len(numeric_values)
                        max_val = max(numeric_values)
                        min_val = min(numeric_values)

                        nodes_result[full_name] = {
                            'p99': round(avg_val, 4),  # p99 value averaged over time range
                            'max': round(max_val, 4),  # Maximum p99 seen
                            'min': round(min_val, 4),  # Minimum p99 seen
                            'unit': 'second'
                        }
                        self.logger.info(f"Collected PLEG relist for {full_name}: p99={nodes_result[full_name]['p99']}s (max={max_val:.4f}s, min={min_val:.4f}s)")

                if not nodes_result:
                    self.logger.warning("No PLEG relist data collected for any nodes")
                    return {
                        'status': 'partial',
                        'metric': 'p99_kubelet_pleg_relist_duration',
                        'description': metric_config.get('description', '99th-percentile PLEG relist latency in seconds'),
                        'nodes': {},
                        'warning': 'No data returned from Prometheus'
                    }

                return {
                    'status': 'success',
                    'metric': 'p99_kubelet_pleg_relist_duration',
                    'description': metric_config.get('description', '99th-percentile PLEG relist latency in seconds'),
                    'nodes': nodes_result
                }

        except Exception as e:
            self.logger.error(f"Error collecting PLEG relist duration: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}


async def main():
    """Main function for testing"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("\n" + "="*80)
    print("PLEG Relist Latency Collector")
    print("="*80)
    print("\nThis collector monitors PLEG (Pod Lifecycle Event Generator) relist latency")
    print("which indicates kubelet performance in managing pod lifecycle events.")
    print("\nMetrics Collected:")
    print("  • p99_kubelet_pleg_relist_duration - 99th percentile PLEG relist latency")
    print("\nInterpretation:")
    print("  • Normal PLEG latency: < 0.1s (100ms)")
    print("  • Warning: 0.1s - 1s")
    print("  • Critical: > 1s")
    print("="*80 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
