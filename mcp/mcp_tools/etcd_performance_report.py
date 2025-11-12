"""
etcd Performance Report MCP Tool
Generate comprehensive performance analysis reports
"""

import logging
from datetime import datetime
import pytz

from .models import PerformanceReportInput, ETCDPerformanceReportResponse

logger = logging.getLogger(__name__)


def register_performance_report_tool(mcp, get_components_func):
    """Register the performance report tool with the MCP server"""
    
    @mcp.tool()
    async def generate_etcd_performance_report(
        request: PerformanceReportInput
    ) -> ETCDPerformanceReportResponse:
        """
        Generate comprehensive etcd performance analysis report with detailed metrics evaluation and recommendations.
        
        This tool provides enterprise-grade performance analysis and reporting for etcd clusters by:
        
        **Comprehensive Data Collection:**
        - Critical performance metrics: WAL fsync P99 latency, backend commit P99 latency
        - Supporting metrics: CPU/memory usage, network I/O, disk I/O performance
        - Cluster health indicators: proposal rates, leadership changes, compaction metrics
        - Infrastructure metrics: node resources, network utilization, storage performance
        
        **Advanced Performance Analysis:**
        - Threshold-based analysis using etcd best practices (WAL fsync <10ms, backend commit <25ms)
        - Baseline comparison against industry benchmarks and performance targets
        - Health status determination with severity classification (excellent/good/warning/critical)
        - Cross-metric correlation analysis to identify performance patterns
        
        **Executive Reporting:**
        - Executive summary with overall cluster health assessment and performance grade
        - Critical alerts section highlighting urgent performance issues requiring immediate attention
        - Detailed metrics analysis with formatted tables showing per-pod performance
        - Baseline comparison showing current vs. target performance with pass/fail status
        - Prioritized recommendations categorized by priority (high/medium/low) with specific actions
        
        **Analysis Methodology:**
        - Industry best practice thresholds and performance benchmarks
        - Root cause analysis linking performance symptoms to underlying infrastructure issues
        - Performance impact assessment and optimization recommendations
        - Detailed methodology explanation for audit and compliance purposes
        
        **Report Features:**
        - Professional formatting suitable for stakeholder presentations
        - Unique test ID for tracking and historical comparison
        - Timestamp and duration information for audit trails
        - Actionable recommendations with implementation guidance
        - Analysis rationale and methodology documentation
        
        **Use Cases:**
        - Regular performance health checks and monitoring reports
        - Pre/post-change performance impact analysis
        - Performance troubleshooting and root cause analysis
        - Capacity planning and infrastructure optimization
        - Executive dashboards and stakeholder reporting
        - Compliance documentation and performance auditing
        
        Args:
            request: Request object with duration and optional test_id fields. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            ETCDPerformanceReportResponse: Comprehensive performance analysis results and formatted report 
                                          including critical metrics analysis, performance summary, baseline 
                                          comparison, prioritized recommendations, and executive-ready documentation
        """
        components = get_components_func()
        auth_manager = components.get('auth_manager')
        node_usage_collector = components.get('node_usage_collector')
        
        # Get effective parameters from request
        eff_duration = request.duration if request.duration else "1h"
        test_id = request.test_id if request.test_id else f"perf-report-{datetime.now(pytz.UTC).strftime('%Y%m%d-%H%M%S')}"
        
        try:
            if not auth_manager:
                return ETCDPerformanceReportResponse(
                    status="error",
                    error="OpenShift authentication not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=eff_duration,
                    test_id=test_id
                )
            
            # Lazy import of analysis modules
            try:
                from analysis.etcd.etcd_performance_deepdrive import etcdDeepDriveAnalyzer
                from analysis.etcd.etcd_performance_report import etcdReportAnalyzer
            except ImportError as import_err:
                logger.error(f"Failed to import performance report modules: {import_err}")
                return ETCDPerformanceReportResponse(
                    status="error",
                    error=f"Performance report analysis modules not available: {import_err}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=eff_duration,
                    test_id=test_id
                )
            
            # Initialize the deep drive analyzer to collect metrics
            deep_drive_analyzer = etcdDeepDriveAnalyzer(auth_manager, eff_duration)
            
            # Collect comprehensive metrics for analysis
            metrics_result = await deep_drive_analyzer.analyze_performance_deep_drive()
            
            if metrics_result.get('status') != 'success':
                return ETCDPerformanceReportResponse(
                    status="error",
                    error=f"Failed to collect metrics for performance report: {metrics_result.get('error', 'Unknown error')}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=eff_duration,
                    test_id=test_id
                )
            
            # Initialize the performance report analyzer
            report_analyzer = etcdReportAnalyzer()

            # Collect node usage to enrich the analysis
            node_usage_wrapped = None
            try:
                if not node_usage_collector:
                    # Lazy init
                    from tools.node.node_usage import nodeUsageCollector
                    prometheus_config = {
                        'url': auth_manager.prometheus_url,
                        'token': getattr(auth_manager, 'prometheus_token', None),
                        'verify_ssl': False,
                    }
                    node_usage_collector = nodeUsageCollector(auth_manager, prometheus_config)
                    components['node_usage_collector'] = node_usage_collector
                    
                node_usage_result = await node_usage_collector.collect_all_metrics(
                    node_group='master', duration=eff_duration
                )
                node_usage_wrapped = {
                    'status': node_usage_result.get('status', 'unknown'),
                    'data': node_usage_result,
                    'timestamp': node_usage_result.get('timestamp')
                }
            except Exception as nu_err:
                logger.warning(f"Node usage collection failed for performance report: {nu_err}")
                node_usage_wrapped = None

            # Analyze the collected metrics
            analysis_results = report_analyzer.analyze_performance_metrics(
                metrics_result, test_id, node_usage_wrapped
            )
            
            # Generate the comprehensive performance report
            performance_report = report_analyzer.generate_performance_report(
                analysis_results, test_id, eff_duration
            )
            
            return ETCDPerformanceReportResponse(
                status=analysis_results.get('status', 'success'),
                analysis_results=analysis_results,
                performance_report=performance_report,
                error=analysis_results.get('error'),
                timestamp=analysis_results.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                duration=eff_duration,
                test_id=test_id
            )
            
        except Exception as e:
            logger.error(f"Error generating etcd performance report: {e}")
            return ETCDPerformanceReportResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=eff_duration,
                test_id=test_id
            )

