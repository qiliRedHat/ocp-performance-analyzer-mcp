"""
Extract, Load, Transform module for etcd Bottleneck Analysis Metrics
Handles bottleneck analysis data from analysis/etcd/etcd_performance_deepdrive.py
ONLY contains bottleneck_analysis specific logic - no generic utilities
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from ..utils.analyzer_elt_utility import utilityELT

logger = logging.getLogger(__name__)


class bottleneckAnalysisELT(utilityELT):
    """Extract, Load, Transform class for etcd bottleneck analysis metrics data"""
    
    def __init__(self):
        super().__init__()
    
    def extract_bottleneck_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract bottleneck analysis information from bottleneck analysis output"""
        
        # Handle nested data structure - bottleneck_analysis can be at top level or nested
        actual_data = data
        if 'data' in data and isinstance(data.get('data'), dict):
            actual_data = data['data']
        
        structured = {
            'bottleneck_analysis_overview': [],
            'disk_io_bottlenecks': [],
            'network_bottlenecks': [],
            'memory_bottlenecks': [],
            'consensus_bottlenecks': [],
            'root_cause_analysis': [],
            'performance_recommendations': []
        }
        
        # Extract bottleneck_analysis section (can be at top level or nested)
        bottleneck_analysis = actual_data.get('bottleneck_analysis', {})
        if not bottleneck_analysis and 'bottleneck_analysis' in data:
            bottleneck_analysis = data.get('bottleneck_analysis', {})
        
        # Extract each bottleneck category
        disk_io_bottlenecks = bottleneck_analysis.get('disk_io_bottlenecks', [])
        for bottleneck in disk_io_bottlenecks:
            self._extract_bottleneck(bottleneck, structured, 'disk_io_bottlenecks')
        
        network_bottlenecks = bottleneck_analysis.get('network_bottlenecks', [])
        for bottleneck in network_bottlenecks:
            self._extract_bottleneck(bottleneck, structured, 'network_bottlenecks')
        
        memory_bottlenecks = bottleneck_analysis.get('memory_bottlenecks', [])
        for bottleneck in memory_bottlenecks:
            self._extract_bottleneck(bottleneck, structured, 'memory_bottlenecks')
        
        consensus_bottlenecks = bottleneck_analysis.get('consensus_bottlenecks', [])
        for bottleneck in consensus_bottlenecks:
            self._extract_bottleneck(bottleneck, structured, 'consensus_bottlenecks')
        
        # Extract root cause analysis (can be at top level or nested)
        root_cause_analysis = actual_data.get('root_cause_analysis', [])
        if not root_cause_analysis and 'root_cause_analysis' in data:
            root_cause_analysis = data.get('root_cause_analysis', [])
        for root_cause in root_cause_analysis:
            self._extract_root_cause(root_cause, structured)
        
        # Extract performance recommendations (can be at top level or nested)
        performance_recommendations = actual_data.get('performance_recommendations', [])
        if not performance_recommendations and 'performance_recommendations' in data:
            performance_recommendations = data.get('performance_recommendations', [])
        for recommendation in performance_recommendations:
            self._extract_recommendation(recommendation, structured)
        
        # Generate overview - use full data to get test_id, duration, timestamp
        overview_data = {
            'bottleneck_analysis': bottleneck_analysis,
            'root_cause_analysis': root_cause_analysis,
            'performance_recommendations': performance_recommendations,
            'test_id': data.get('test_id', actual_data.get('test_id', 'unknown')),
            'duration': data.get('duration', actual_data.get('duration', '1h')),
            'timestamp': data.get('timestamp', actual_data.get('timestamp', ''))
        }
        self._generate_overview(overview_data, structured)
        
        return structured
    
    def _extract_bottleneck(self, bottleneck: Dict[str, Any], structured: Dict[str, Any], category: str):
        """Extract a single bottleneck entry"""
        bottleneck_type = bottleneck.get('type', 'unknown')
        node = bottleneck.get('node', '')
        pod = bottleneck.get('pod', '')
        value = bottleneck.get('value', 0)
        unit = bottleneck.get('unit', '')
        severity = bottleneck.get('severity', 'unknown')
        description = bottleneck.get('description', '')
        
        # Determine role from node or pod name
        role = 'worker'
        if node:
            role = self.get_node_role_from_labels(node)
        elif pod:
            # Infer from pod name
            pod_lower = pod.lower()
            if 'etcd' in pod_lower or 'master' in pod_lower or 'control' in pod_lower:
                role = 'controlplane'
            elif 'infra' in pod_lower:
                role = 'infra'
            elif 'workload' in pod_lower:
                role = 'workload'
        
        # Format value with unit
        formatted_value = self.format_value_with_unit(value, unit)
        
        # Format severity badge
        severity_badge = self.create_severity_badge(severity)
        
        structured[category].append({
            'Metric Name': bottleneck_type.replace('_', ' ').title(),
            'Role': role.title(),
            'Node/Pod': node if node else self.truncate_node_name(pod, 30),
            'Value': formatted_value,
            'Severity': severity_badge,
            'Description': description
        })
    
    def _extract_root_cause(self, root_cause: Dict[str, Any], structured: Dict[str, Any]):
        """Extract root cause analysis entry"""
        category = root_cause.get('category', 'unknown')
        root_cause_text = root_cause.get('root_cause', '')
        evidence = root_cause.get('evidence', '')
        impact = root_cause.get('impact', '')
        likelihood = root_cause.get('likelihood', 'unknown')
        
        structured['root_cause_analysis'].append({
            'Category': category.replace('_', ' ').title(),
            'Root Cause': root_cause_text,
            'Evidence': evidence,
            'Impact': impact,
            'Likelihood': self.create_severity_badge(likelihood)
        })
    
    def _extract_recommendation(self, recommendation: Dict[str, Any], structured: Dict[str, Any]):
        """Extract performance recommendation entry"""
        category = recommendation.get('category', 'unknown')
        priority = recommendation.get('priority', 'unknown')
        recommendation_text = recommendation.get('recommendation', '')
        rationale = recommendation.get('rationale', '')
        
        structured['performance_recommendations'].append({
            'Category': category.replace('_', ' ').title(),
            'Priority': self.create_severity_badge(priority),
            'Recommendation': recommendation_text,
            'Rationale': rationale
        })
    
    def create_severity_badge(self, severity: str) -> str:
        """Create HTML badge for severity/priority/likelihood"""
        severity_lower = str(severity).lower()
        
        if severity_lower in ['high', 'critical', 'danger']:
            return self.create_status_badge('danger', severity.title())
        elif severity_lower in ['medium', 'warning']:
            return self.create_status_badge('warning', severity.title())
        elif severity_lower in ['low', 'info']:
            return self.create_status_badge('info', severity.title())
        else:
            return self.create_status_badge('secondary', severity.title())
    
    def _generate_overview(self, data: Dict[str, Any], structured: Dict[str, Any]):
        """Generate bottleneck analysis overview"""
        bottleneck_analysis = data.get('bottleneck_analysis', {})
        test_id = data.get('test_id', 'unknown')
        duration = data.get('duration', '1h')
        timestamp = data.get('timestamp', '')
        
        # Count bottlenecks by category
        disk_count = len(bottleneck_analysis.get('disk_io_bottlenecks', []))
        network_count = len(bottleneck_analysis.get('network_bottlenecks', []))
        memory_count = len(bottleneck_analysis.get('memory_bottlenecks', []))
        consensus_count = len(bottleneck_analysis.get('consensus_bottlenecks', []))
        total_count = disk_count + network_count + memory_count + consensus_count
        
        root_cause_count = len(data.get('root_cause_analysis', []))
        recommendation_count = len(data.get('performance_recommendations', []))
        
        structured['bottleneck_analysis_overview'].append({
            'Category': 'Bottleneck Analysis',
            'Total Bottlenecks': total_count,
            'Root Causes': root_cause_count,
            'Recommendations': recommendation_count,
            'Duration': duration,
            'Test ID': test_id
        })
        
        # Add category breakdown
        if disk_count > 0:
            structured['bottleneck_analysis_overview'].append({
                'Category': 'Disk I/O Bottlenecks',
                'Total Bottlenecks': disk_count,
                'Root Causes': '',
                'Recommendations': '',
                'Duration': '',
                'Test ID': ''
            })
        if network_count > 0:
            structured['bottleneck_analysis_overview'].append({
                'Category': 'Network Bottlenecks',
                'Total Bottlenecks': network_count,
                'Root Causes': '',
                'Recommendations': '',
                'Duration': '',
                'Test ID': ''
            })
        if memory_count > 0:
            structured['bottleneck_analysis_overview'].append({
                'Category': 'Memory Bottlenecks',
                'Total Bottlenecks': memory_count,
                'Root Causes': '',
                'Recommendations': '',
                'Duration': '',
                'Test ID': ''
            })
        if consensus_count > 0:
            structured['bottleneck_analysis_overview'].append({
                'Category': 'Consensus Bottlenecks',
                'Total Bottlenecks': consensus_count,
                'Root Causes': '',
                'Recommendations': '',
                'Duration': '',
                'Test ID': ''
            })
    
    def summarize_bottleneck_analysis(self, data: Dict[str, Any]) -> str:
        """Generate bottleneck analysis summary as HTML"""
        try:
            summary_items: List[str] = []
            
            overview_data = data.get('bottleneck_analysis_overview', [])
            
            if overview_data:
                main_overview = overview_data[0] if overview_data else {}
                total_bottlenecks = main_overview.get('Total Bottlenecks', 0)
                root_causes = main_overview.get('Root Causes', 0)
                recommendations = main_overview.get('Recommendations', 0)
                duration = main_overview.get('Duration', '1h')
                test_id = main_overview.get('Test ID', 'unknown')
                
                summary_items.append(f"<li>Total Bottlenecks Identified: {total_bottlenecks}</li>")
                summary_items.append(f"<li>Root Causes: {root_causes}</li>")
                summary_items.append(f"<li>Recommendations: {recommendations}</li>")
                summary_items.append(f"<li>Duration: {duration}</li>")
                summary_items.append(f"<li>Test ID: {test_id}</li>")
                
                # Category breakdown
                for item in overview_data[1:]:
                    category = item.get('Category', '')
                    count = item.get('Total Bottlenecks', 0)
                    if count > 0:
                        summary_items.append(f"<li>{category}: {count} bottlenecks</li>")
            
            return (
                "<div class=\"bottleneck-analysis-summary\">"
                "<h4>etcd Bottleneck Analysis Summary:</h4>"
                "<ul>" + "".join(summary_items) + "</ul>"
                "</div>"
            )
        
        except Exception as e:
            logger.error(f"Failed to generate bottleneck analysis summary: {e}")
            return f"etcd Bottleneck Analysis metrics collected"
    
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
            logger.error(f"Failed to transform bottleneck analysis data to DataFrames: {e}")
        
        return dataframes
    
    def generate_html_tables(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Generate HTML tables from DataFrames"""
        html_tables = {}
        
        try:
            # Overview table first
            if 'bottleneck_analysis_overview' in dataframes and not dataframes['bottleneck_analysis_overview'].empty:
                html_tables['bottleneck_analysis_overview'] = self.create_html_table(
                    dataframes['bottleneck_analysis_overview'], 
                    'Bottleneck Analysis Overview'
                )
            
            # Generate tables for each category
            category_order = [
                'disk_io_bottlenecks',
                'network_bottlenecks',
                'memory_bottlenecks',
                'consensus_bottlenecks',
                'root_cause_analysis',
                'performance_recommendations'
            ]
            
            for category in category_order:
                if category in dataframes and not dataframes[category].empty:
                    display_name = category.replace('_', ' ').title()
                    html_tables[category] = self.create_html_table(
                        dataframes[category], 
                        display_name
                    )
        
        except Exception as e:
            logger.error(f"Failed to generate HTML tables for bottleneck analysis: {e}")
        
        return html_tables

