"""
etcd Bottleneck Analysis MCP Tool
Advanced performance bottleneck detection and root cause analysis
"""

import logging
from datetime import datetime
import pytz

from .models import ETCDBottleneckAnalysisResponse, DurationInput

logger = logging.getLogger(__name__)


def register_bottleneck_analysis_tool(mcp, get_components_func):
    """Register the bottleneck analysis tool with the MCP server"""
    
    @mcp.tool()
    async def get_etcd_bottleneck_analysis(request: DurationInput) -> ETCDBottleneckAnalysisResponse:
        """
        Perform advanced etcd performance bottleneck analysis with root cause identification 
        and optimization recommendations.
        
        This tool performs sophisticated bottleneck analysis by:
        
        **Bottleneck Detection:**
        - Disk I/O bottlenecks: WAL fsync high latency (>100ms P99), backend commit delays (>50ms P99), 
          low disk throughput
        - Network bottlenecks: High peer-to-peer latency (>100ms), network utilization (>80%), packet drops
        - Memory bottlenecks: High memory usage (>80%), memory pressure indicators, potential memory leaks
        - Consensus bottlenecks: Proposal failures, high pending proposals (>10), slow applies, 
          frequent leader changes (>1/hour)
        
        **Analysis Methodology:**
        - Automated threshold-based bottleneck identification with severity classification (high/medium/low)
        - Cross-subsystem correlation analysis to identify cascading performance issues
        - Performance impact assessment for each identified bottleneck
        - Historical pattern analysis to distinguish temporary vs. persistent issues
        
        **Root Cause Analysis:**
        - Evidence-based root cause identification linking symptoms to underlying causes
        - Likelihood assessment for each potential root cause
        - Impact analysis showing how bottlenecks affect cluster performance
        - Categorization by subsystem (disk_io, network, memory, consensus)
        
        **Optimization Recommendations:**
        - Prioritized recommendations based on performance impact and implementation complexity
        - Specific actionable steps for each identified bottleneck
        - Infrastructure optimization suggestions (storage upgrades, network improvements)
        - Configuration tuning recommendations for etcd and OpenShift
        
        **Use Cases:**
        - Performance troubleshooting and problem diagnosis
        - Proactive performance optimization planning
        - Infrastructure capacity planning and upgrades
        - Performance regression analysis after changes
        - Creating performance improvement roadmaps
        
        Args:
            request: Request object with duration field. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            ETCDBottleneckAnalysisResponse: Comprehensive bottleneck analysis including identified performance issues, 
                                           root cause analysis, and prioritized optimization recommendations with 
                                           unique test ID
        """
        # Extract duration from request (defaults to "1h" from DurationInput model)
        duration = request.duration
        
        components = get_components_func()
        auth_manager = components.get('auth_manager')
        
        try:
            if not auth_manager:
                return ETCDBottleneckAnalysisResponse(
                    status="error",
                    error="OpenShift authentication not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
            
            # Lazy import of analysis module
            try:
                from analysis.etcd.etcd_performance_deepdrive import etcdDeepDriveAnalyzer
            except ImportError as import_err:
                logger.error(f"Failed to import etcdDeepDriveAnalyzer: {import_err}")
                return ETCDBottleneckAnalysisResponse(
                    status="error",
                    error=f"Bottleneck analysis module not available: {import_err}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
            
            # Initialize the deep drive analyzer
            deep_drive_analyzer = etcdDeepDriveAnalyzer(auth_manager, duration)
            
            # Perform bottleneck analysis
            result = await deep_drive_analyzer.analyze_bottlenecks()
            
            return ETCDBottleneckAnalysisResponse(
                status=result.get('status', 'success'),
                bottleneck_analysis=result.get('bottleneck_analysis'),
                root_cause_analysis=result.get('root_cause_analysis'),
                performance_recommendations=result.get('performance_recommendations'),
                error=result.get('error'),
                timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                duration=duration,
                test_id=result.get('test_id')
            )
            
        except Exception as e:
            logger.error(f"Error performing etcd bottleneck analysis: {e}")
            return ETCDBottleneckAnalysisResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )