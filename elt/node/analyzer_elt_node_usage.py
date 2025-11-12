"""
Extract, Load, Transform module for Node Usage Metrics
Handles node usage data from tools/node/node_usage.py
ONLY contains node_usage specific logic - no generic utilities
"""

import logging
import re
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class nodeUsageELT(utilityELT):
    """Extract, Load, Transform class for node usage metrics data"""
    
    def __init__(self):
        super().__init__()
        self.metric_configs = {
            'cpu_usage': {
                'title': 'CPU Usage',
                'unit': 'percent',
                'thresholds': {'critical': 85.0, 'warning': 70.0}
            },
            'memory_used': {
                'title': 'Memory Used',
                'unit': 'GB',
                'thresholds': {'critical': 85.0, 'warning': 70.0}  # For percentage
            },
            'memory_cache_buffer': {
                'title': 'Memory Cache/Buffer',
                'unit': 'GB',
                'thresholds': {'critical': 85.0, 'warning': 70.0}  # For percentage
            },
            'cgroup_cpu_usage': {
                'title': 'Cgroup CPU Usage',
                'unit': 'percent',
                'thresholds': {'critical': 85.0, 'warning': 70.0}
            },
            'cgroup_rss_usage': {
                'title': 'Cgroup RSS Usage',
                'unit': 'GB',
                'thresholds': {'critical': 85.0, 'warning': 70.0}
            }
        }
    
    def extract_node_usage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node usage information from node_usage.py output
        
        Handles new structure with node_groups containing master/infra/workload/worker groups
        """
        
        # Handle nested data structure
        # Note: GenericELT._extract_actual_data already unwraps 'data' for node_usage,
        # so 'data' here should already be the unwrapped content
        actual_data = data
        # Only unwrap if we still have a 'data' key and no 'node_groups' key
        # (this handles cases where data wasn't pre-unwrapped)
        if 'data' in data and isinstance(data.get('data'), dict) and 'node_groups' not in data:
            actual_data = data['data']
        
        # Log for debugging
        logger.debug(f"Extracting node usage, actual_data keys: {list(actual_data.keys())}")
        logger.debug(f"Has node_groups: {'node_groups' in actual_data}")
        
        structured = {
            'overview': [],
            'timestamp': actual_data.get('timestamp', ''),
            'duration': actual_data.get('duration', ''),
            'time_range': {},
        }
        
        # Initialize role-based tables for each metric
        for role in ['controlplane', 'infra', 'worker', 'workload']:
            structured[f'cpu_usage_{role}'] = []
            structured[f'memory_used_{role}'] = []
            structured[f'memory_cache_buffer_{role}'] = []
            structured[f'cgroup_cpu_usage_{role}'] = []
            structured[f'cgroup_rss_usage_{role}'] = []
        
        # Handle new structure with node_groups
        node_groups = actual_data.get('node_groups', {})
        
        if node_groups:
            # Process each node group (master, infra, workload, worker)
            for group_name, group_data in node_groups.items():
                if group_data.get('status') != 'success':
                    # Skip error groups (e.g., "No infra nodes found")
                    logger.debug(f"Skipping {group_name} group: {group_data.get('error', 'unknown error')}")
                    continue
                
                # Get nodes list with roles for this group
                nodes_list = group_data.get('nodes', [])
                node_role_map = {node['name']: node.get('role', group_name) for node in nodes_list}
                
                # Extract node capacities (CPU cores and RAM size) for this group
                node_capacities = group_data.get('node_capacities', {})
                
                # Update time_range from first successful group
                if not structured['time_range'] and 'time_range' in group_data:
                    structured['time_range'] = group_data.get('time_range', {})
                
                # Extract metrics from this group
                metrics = group_data.get('metrics', {})
                
                # Extract each metric type with node capacities
                if 'cpu_usage' in metrics:
                    self._extract_cpu_usage(metrics['cpu_usage'], structured, node_role_map, node_capacities)
                
                if 'memory_used' in metrics:
                    self._extract_memory_used(metrics['memory_used'], structured, node_role_map, node_capacities)
                
                if 'memory_cache_buffer' in metrics:
                    self._extract_memory_cache_buffer(metrics['memory_cache_buffer'], structured, node_role_map, node_capacities)
                
                if 'cgroup_cpu_usage' in metrics:
                    self._extract_cgroup_cpu_usage(metrics['cgroup_cpu_usage'], structured, node_role_map, node_capacities)
                
                if 'cgroup_rss_usage' in metrics:
                    self._extract_cgroup_rss_usage(metrics['cgroup_rss_usage'], structured, node_role_map, node_capacities)
            
            # Generate overview from all node groups
            self._generate_overview_from_groups(node_groups, structured)
        else:
            # Fallback to old structure (single node group)
            query_params = actual_data.get('query_params', {})
            node_group = query_params.get('node_group', 'unknown')
            
            # Get nodes list with roles
            nodes_list = actual_data.get('nodes', [])
            node_role_map = {node['name']: node.get('role', node_group) for node in nodes_list}
            
            # Extract node capacities (CPU cores and RAM size)
            node_capacities = actual_data.get('node_capacities', {})
            
            structured['time_range'] = actual_data.get('time_range', {})
            
            metrics = actual_data.get('metrics', {})
            
            # Extract each metric type with node capacities
            if 'cpu_usage' in metrics:
                self._extract_cpu_usage(metrics['cpu_usage'], structured, node_role_map, node_capacities)
            
            if 'memory_used' in metrics:
                self._extract_memory_used(metrics['memory_used'], structured, node_role_map, node_capacities)
            
            if 'memory_cache_buffer' in metrics:
                self._extract_memory_cache_buffer(metrics['memory_cache_buffer'], structured, node_role_map, node_capacities)
            
            if 'cgroup_cpu_usage' in metrics:
                self._extract_cgroup_cpu_usage(metrics['cgroup_cpu_usage'], structured, node_role_map, node_capacities)
            
            if 'cgroup_rss_usage' in metrics:
                self._extract_cgroup_rss_usage(metrics['cgroup_rss_usage'], structured, node_role_map, node_capacities)
            
            # Generate overview
            self._generate_overview(actual_data, structured, node_role_map)
        
        return structured
    
    def _extract_cpu_usage(self, metric_data: Dict[str, Any], 
                          structured: Dict[str, Any], 
                          node_role_map: Dict[str, str],
                          node_capacities: Dict[str, Any] = None):
        """Extract CPU usage metrics grouped by role with CPU cores and RAM size"""
        if node_capacities is None:
            node_capacities = {}
        
        nodes = metric_data.get('nodes', {})
        
        # Collect all total CPU values for top identification
        all_totals = []
        for node_name, node_data in nodes.items():
            total = node_data.get('total', {})
            if total:
                role = self._get_node_role(node_name, node_role_map)
                all_totals.append((node_name, role, float(total.get('max', 0))))
        
        # Find top 1 max value
        top_max = max((t[2] for t in all_totals), default=0) if all_totals else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            # Get role using utility method
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'cpu_usage_{role}'
            
            # Get node capacity info
            capacity_info = node_capacities.get(node_name, {})
            cpu_cores = capacity_info.get('cpu_cores') if isinstance(capacity_info, dict) else None
            ram_gb = capacity_info.get('memory_gb') if isinstance(capacity_info, dict) else None
            
            modes = node_data.get('modes', {})
            total = node_data.get('total', {})
            
            # Add mode rows
            for mode, mode_data in modes.items():
                avg_val = float(mode_data.get('avg', 0))
                max_val = float(mode_data.get('max', 0))
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Role': role.title(),
                    'CPU Cores': self.format_cpu_cores(cpu_cores),
                    'RAM Size': self.format_ram_size_gb(ram_gb),
                    'Mode': mode,
                    'Avg': self._format_cpu_value(avg_val, is_top=False),
                    'Max': self._format_cpu_value(max_val, is_top=False)
                })
            
            # Add TOTAL row with highlighting
            if total:
                avg_val = float(total.get('avg', 0))
                max_val = float(total.get('max', 0))
                is_top = (max_val == top_max and max_val > 0)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Role': role.title(),
                    'CPU Cores': self.format_cpu_cores(cpu_cores),
                    'RAM Size': self.format_ram_size_gb(ram_gb),
                    'Mode': '<strong>TOTAL</strong>',
                    'Avg': f'<strong>{self._format_cpu_value(avg_val, is_top=False)}</strong>',
                    'Max': f'<strong>{self._format_cpu_value(max_val, is_top=is_top)}</strong>'
                })
    
    def _extract_memory_used(self, metric_data: Dict[str, Any], 
                            structured: Dict[str, Any], 
                            node_role_map: Dict[str, str],
                            node_capacities: Dict[str, Any] = None):
        """Extract memory used metrics grouped by role with CPU cores, RAM size, and usage percentage"""
        if node_capacities is None:
            node_capacities = {}
        
        nodes = metric_data.get('nodes', {})
        
        # Collect all max percentage values for top identification
        all_percentages = []
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            
            # Calculate RAM usage percentage if we have capacity info
            capacity_info = node_capacities.get(node_name, {})
            ram_gb = capacity_info.get('memory_gb') if isinstance(capacity_info, dict) else None
            max_gb = float(node_data.get('max', 0))
            
            if ram_gb and ram_gb > 0:
                max_pct = self.calculate_ram_usage_percentage(max_gb, ram_gb)
                all_percentages.append((node_name, role, max_pct))
            elif 'max_percent' in node_data:
                max_pct = float(node_data.get('max_percent', 0))
                all_percentages.append((node_name, role, max_pct))
        
        # Find top 1 by percentage
        top_percentage = max((v[2] for v in all_percentages), default=0) if all_percentages else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'memory_used_{role}'
            
            # Get node capacity info
            capacity_info = node_capacities.get(node_name, {})
            cpu_cores = capacity_info.get('cpu_cores') if isinstance(capacity_info, dict) else None
            ram_gb = capacity_info.get('memory_gb') if isinstance(capacity_info, dict) else None
            
            avg_gb = float(node_data.get('avg', 0))
            max_gb = float(node_data.get('max', 0))
            
            # Calculate RAM usage percentages
            avg_pct = self.calculate_ram_usage_percentage(avg_gb, ram_gb) if ram_gb and ram_gb > 0 else 0.0
            max_pct = self.calculate_ram_usage_percentage(max_gb, ram_gb) if ram_gb and ram_gb > 0 else 0.0
            
            is_top = (max_pct == top_percentage and max_pct > 0)
            
            structured[table_key].append({
                'Node': self.truncate_node_name(node_name),
                'Role': role.title(),
                'CPU Cores': self.format_cpu_cores(cpu_cores),
                'RAM Size': self.format_ram_size_gb(ram_gb),
                'Avg (GB)': f'{avg_gb:.2f}',
                'RAM Used (%)': self.format_ram_usage_percentage(avg_pct, is_top=False),
                'Max (GB)': f'{max_gb:.2f}',
                'Max RAM (%)': self.format_ram_usage_percentage(max_pct, is_top=is_top)
            })
    
    def _extract_memory_cache_buffer(self, metric_data: Dict[str, Any], 
                                     structured: Dict[str, Any], 
                                     node_role_map: Dict[str, str],
                                     node_capacities: Dict[str, Any] = None):
        """Extract memory cache/buffer metrics grouped by role with CPU cores and RAM size"""
        if node_capacities is None:
            node_capacities = {}
        
        nodes = metric_data.get('nodes', {})
        
        # Collect all max values for top identification
        all_values = []
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            max_val = float(node_data.get('max', 0))
            all_values.append((node_name, role, max_val))
        
        # Find top 1
        top_value = max((v[2] for v in all_values), default=0) if all_values else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'memory_cache_buffer_{role}'
            
            # Get node capacity info
            capacity_info = node_capacities.get(node_name, {})
            cpu_cores = capacity_info.get('cpu_cores') if isinstance(capacity_info, dict) else None
            ram_gb = capacity_info.get('memory_gb') if isinstance(capacity_info, dict) else None
            
            avg_gb = float(node_data.get('avg', 0))
            max_gb = float(node_data.get('max', 0))
            
            is_top = (max_gb == top_value and max_gb > 0)
            thresholds = self.metric_configs['memory_cache_buffer']['thresholds']
            
            structured[table_key].append({
                'Node': self.truncate_node_name(node_name),
                'Role': role.title(),
                'CPU Cores': self.format_cpu_cores(cpu_cores),
                'RAM Size': self.format_ram_size_gb(ram_gb),
                'Avg': self.highlight_critical_values(avg_gb, thresholds, ' GB', is_top=False),
                'Max': self.highlight_critical_values(max_gb, thresholds, ' GB', is_top=is_top)
            })
    
    def _extract_cgroup_cpu_usage(self, metric_data: Dict[str, Any], 
                                  structured: Dict[str, Any], 
                                  node_role_map: Dict[str, str],
                                  node_capacities: Dict[str, Any] = None):
        """Extract cgroup CPU usage metrics grouped by role with CPU cores and RAM size"""
        if node_capacities is None:
            node_capacities = {}
        
        nodes = metric_data.get('nodes', {})
        
        # Collect all total max values for top identification
        all_totals = []
        for node_name, node_data in nodes.items():
            total = node_data.get('total', {})
            if total:
                role = self._get_node_role(node_name, node_role_map)
                all_totals.append((node_name, role, float(total.get('max', 0))))
        
        # Find top 1
        top_max = max((t[2] for t in all_totals), default=0) if all_totals else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'cgroup_cpu_usage_{role}'
            
            # Get node capacity info
            capacity_info = node_capacities.get(node_name, {})
            cpu_cores = capacity_info.get('cpu_cores') if isinstance(capacity_info, dict) else None
            ram_gb = capacity_info.get('memory_gb') if isinstance(capacity_info, dict) else None
            
            cgroups = node_data.get('cgroups', {})
            total = node_data.get('total', {})
            
            # Add cgroup rows
            for cgroup_name, cgroup_data in cgroups.items():
                avg_val = float(cgroup_data.get('avg', 0))
                max_val = float(cgroup_data.get('max', 0))
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Role': role.title(),
                    'CPU Cores': self.format_cpu_cores(cpu_cores),
                    'RAM Size': self.format_ram_size_gb(ram_gb),
                    'Cgroup': cgroup_name,
                    'Avg': self._format_cpu_value(avg_val, is_top=False),
                    'Max': self._format_cpu_value(max_val, is_top=False)
                })
            
            # Add TOTAL row
            if total:
                avg_val = float(total.get('avg', 0))
                max_val = float(total.get('max', 0))
                is_top = (max_val == top_max and max_val > 0)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Role': role.title(),
                    'CPU Cores': self.format_cpu_cores(cpu_cores),
                    'RAM Size': self.format_ram_size_gb(ram_gb),
                    'Cgroup': '<strong>TOTAL</strong>',
                    'Avg': f'<strong>{self._format_cpu_value(avg_val, is_top=False)}</strong>',
                    'Max': f'<strong>{self._format_cpu_value(max_val, is_top=is_top)}</strong>'
                })
    
    def _extract_cgroup_rss_usage(self, metric_data: Dict[str, Any], 
                                  structured: Dict[str, Any], 
                                  node_role_map: Dict[str, str],
                                  node_capacities: Dict[str, Any] = None):
        """Extract cgroup RSS usage metrics grouped by role with CPU cores and RAM size"""
        if node_capacities is None:
            node_capacities = {}
        
        nodes = metric_data.get('nodes', {})
        
        # Collect all total max values for top identification
        all_totals = []
        for node_name, node_data in nodes.items():
            total = node_data.get('total', {})
            if total:
                role = self._get_node_role(node_name, node_role_map)
                all_totals.append((node_name, role, float(total.get('max', 0))))
        
        # Find top 1
        top_max = max((t[2] for t in all_totals), default=0) if all_totals else 0
        
        # Process each node
        for node_name, node_data in nodes.items():
            role = self._get_node_role(node_name, node_role_map)
            table_key = f'cgroup_rss_usage_{role}'
            
            # Get node capacity info
            capacity_info = node_capacities.get(node_name, {})
            cpu_cores = capacity_info.get('cpu_cores') if isinstance(capacity_info, dict) else None
            ram_gb = capacity_info.get('memory_gb') if isinstance(capacity_info, dict) else None
            
            cgroups = node_data.get('cgroups', {})
            total = node_data.get('total', {})
            
            # Add cgroup rows
            for cgroup_name, cgroup_data in cgroups.items():
                avg_val = float(cgroup_data.get('avg', 0))
                max_val = float(cgroup_data.get('max', 0))
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Role': role.title(),
                    'CPU Cores': self.format_cpu_cores(cpu_cores),
                    'RAM Size': self.format_ram_size_gb(ram_gb),
                    'Cgroup': cgroup_name,
                    'Avg': self._format_memory_gb(avg_val, is_top=False),
                    'Max': self._format_memory_gb(max_val, is_top=False)
                })
            
            # Add TOTAL row
            if total:
                avg_val = float(total.get('avg', 0))
                max_val = float(total.get('max', 0))
                is_top = (max_val == top_max and max_val > 0)
                
                structured[table_key].append({
                    'Node': self.truncate_node_name(node_name),
                    'Role': role.title(),
                    'CPU Cores': self.format_cpu_cores(cpu_cores),
                    'RAM Size': self.format_ram_size_gb(ram_gb),
                    'Cgroup': '<strong>TOTAL</strong>',
                    'Avg': f'<strong>{self._format_memory_gb(avg_val, is_top=False)}</strong>',
                    'Max': f'<strong>{self._format_memory_gb(max_val, is_top=is_top)}</strong>'
                })
    
    def _generate_overview_from_groups(self, node_groups: Dict[str, Any], 
                                      structured: Dict[str, Any]):
        """Generate node usage overview from node_groups structure"""
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
        
        # Add top workers info if available
        worker_group = node_groups.get('worker', {})
        if worker_group.get('status') == 'success' and 'top_workers' in worker_group:
            top_workers = worker_group.get('top_workers', {})
            top_workers_count = top_workers.get('count', 0)
            if top_workers_count > 0:
                structured['overview'].append({
                    'Role': 'Top Workers',
                    'Nodes': f"{top_workers_count} (by CPU)",
                    'Metrics Collected': total_metrics_collected,
                    'Status': self.create_status_badge('success', 'Active')
                })
        
        for role, count in sorted(role_counts.items()):
            structured['overview'].append({
                'Role': role.title(),
                'Nodes': count,
                'Metrics Collected': total_metrics_collected,
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def _generate_overview(self, data: Dict[str, Any], 
                          structured: Dict[str, Any],
                          node_role_map: Dict[str, str]):
        """Generate node usage overview (legacy single group structure)"""
        # Count nodes by role
        role_counts = {}
        for node_name, role in node_role_map.items():
            role_counts[role] = role_counts.get(role, 0) + 1
        
        metrics = data.get('metrics', {})
        metrics_collected = len([k for k in metrics.keys() if metrics[k].get('status') == 'success'])
        
        for role, count in sorted(role_counts.items()):
            structured['overview'].append({
                'Role': role.title(),
                'Nodes': count,
                'Metrics Collected': metrics_collected,
                'Status': self.create_status_badge('success', 'Active')
            })
    
    def _get_node_role(self, node_name: str, node_role_map: Dict[str, str] = None) -> str:
        """Get role for a node using get_node_role_from_labels, with fallback to node_role_map"""
        # First try to get role from labels using utility method
        role = self.get_node_role_from_labels(node_name)
        
        # If utility method returns 'worker' (default) and we have a node_role_map, check it
        if role == 'worker' and node_role_map and node_name in node_role_map:
            role = node_role_map[node_name]
        
        return role
    
    def _format_cpu_value(self, value: float, is_top: bool = False) -> str:
        """Format CPU percentage value with thresholds and top highlighting"""
        thresholds = self.metric_configs['cpu_usage']['thresholds']
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {value:.2f}%</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {value:.2f}%</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{value:.2f}%</span>'
        else:
            return f'{value:.2f}%'
    
    def _format_memory_percent(self, value: float, is_top: bool = False) -> str:
        """Format memory percentage value with thresholds and top highlighting"""
        thresholds = self.metric_configs['memory_used']['thresholds']
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {value:.2f}%</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {value:.2f}%</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{value:.2f}%</span>'
        else:
            return f'{value:.2f}%'
    
    def _format_memory_gb(self, value: float, is_top: bool = False) -> str:
        """Format memory GB value with thresholds and top highlighting"""
        thresholds = self.metric_configs['cgroup_rss_usage']['thresholds']
        
        if is_top:
            return f'<span class="text-primary font-weight-bold bg-light px-1">🏆 {value:.2f} GB</span>'
        elif value >= thresholds['critical']:
            return f'<span class="text-danger font-weight-bold">⚠️ {value:.2f} GB</span>'
        elif value >= thresholds['warning']:
            return f'<span class="text-warning font-weight-bold">{value:.2f} GB</span>'
        else:
            return f'{value:.2f} GB'
    
    def summarize_node_usage(self, data: Dict[str, Any]) -> str:
        """Generate node usage summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('overview', [])
            
            # Calculate total nodes (handle both numeric and string values like "1 (by CPU)")
            total_nodes = 0
            for item in overview_data:
                nodes_value = item.get('Nodes', 0)
                if isinstance(nodes_value, str):
                    # Extract number from strings like "1 (by CPU)"
                    match = re.search(r'(\d+)', str(nodes_value))
                    if match:
                        total_nodes += int(match.group(1))
                else:
                    total_nodes += int(nodes_value)
            
            if total_nodes > 0:
                summary_items.append(f"<li>Total Nodes: {total_nodes}</li>")
            
            # Role breakdown
            for item in overview_data:
                role = item.get('Role', 'Unknown')
                nodes = item.get('Nodes', 0)
                metrics = item.get('Metrics Collected', 0)
                if isinstance(nodes, str) or (isinstance(nodes, (int, float)) and nodes > 0):
                    summary_items.append(f"<li>{role}: {nodes} nodes, {metrics} metrics</li>")
            
            # Time range
            time_range = data.get('time_range', {})
            if time_range:
                duration = data.get('duration', 'unknown')
                summary_items.append(f"<li>Duration: {duration}</li>")
                if 'start' in time_range and 'end' in time_range:
                    summary_items.append(f"<li>Time Range: {time_range.get('start')} to {time_range.get('end')}</li>")
            
            return (
                "<div class=\"node-usage-summary\">"
                "<h4>Node Usage Metrics Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate node usage summary: {e}")
            return "Node usage metrics collected"
    
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
            logger.error(f"Failed to transform node usage data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames grouped by metric and role"""
        html_tables = {}
        
        # Define metric groups
        metric_groups = {
            'CPU Usage': 'cpu_usage',
            'Memory Used': 'memory_used',
            'Memory Cache/Buffer': 'memory_cache_buffer',
            'Cgroup CPU Usage': 'cgroup_cpu_usage',
            'Cgroup RSS Usage': 'cgroup_rss_usage'
        }
        
        try:
            # Overview table first
            if 'overview' in dataframes and not dataframes['overview'].empty:
                html_tables['overview'] = self.create_html_table(
                    dataframes['overview'], 
                    'Node Usage Overview'
                )
            
            # Generate tables for each metric group and role
            for metric_name, metric_prefix in metric_groups.items():
                for role in ['controlplane', 'infra', 'worker', 'workload']:
                    table_key = f'{metric_prefix}_{role}'
                    if table_key in dataframes and not dataframes[table_key].empty:
                        display_name = f"{metric_name} - {role.title()}"
                        html_tables[table_key] = self.create_html_table(
                            dataframes[table_key], 
                            display_name
                        )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for node usage: {e}")
        
        return html_tables