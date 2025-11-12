"""
etcd Performance Deep Drive Analysis MCP Tool
Comprehensive performance analysis across all subsystems
"""

import logging
from datetime import datetime
import pytz

from .models import DeepDriveInput, ETCDPerformanceDeepDriveResponse

logger = logging.getLogger(__name__)


def register_performance_deep_drive_tool(mcp, get_components_func):
    """Register the performance deep drive tool with the MCP server"""
    
    @mcp.tool()
    async def get_etcd_performance_deep_drive(
        request: DeepDriveInput
    ) -> ETCDPerformanceDeepDriveResponse:
        """
        Perform comprehensive etcd performance deep drive analysis across all critical subsystems.
        
        This tool executes an in-depth performance analysis of the etcd cluster by collecting and analyzing 
        metrics from multiple subsystems:
        
        **Collected Metrics:**
        - General cluster metrics: CPU/memory usage, proposal rates, leadership changes, operation rates
        - WAL fsync performance: P99 latency, operation rates, duration statistics (critical for write performance)
        - Disk I/O metrics: Container and node-level disk throughput, IOPS, device statistics
        - Network I/O performance: Container network, peer communication, client gRPC, node utilization
        - Backend commit operations: Database commit latency, operation rates, efficiency analysis
        - Compact/defrag operations: Database maintenance performance, compaction duration, page faults
        
        **Analysis Features:**
        - Latency pattern analysis across all subsystems
        - Performance correlation analysis between different metrics
        - Health scoring and performance benchmarking
        - Automated performance summary with key findings
        - Cross-subsystem performance impact assessment
        
        **Use Cases:**
        - Comprehensive cluster health assessment
        - Performance baseline establishment
        - Pre/post-change performance comparison
        - Identifying performance trends and patterns
        - Generating detailed performance reports for stakeholders
        
        The analysis provides a holistic view of etcd performance, making it easier to identify performance 
        bottlenecks and optimization opportunities across the entire cluster stack.
        
        Args:
            request: Request object with duration field. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            ETCDPerformanceDeepDriveResponse: Comprehensive performance analysis including all subsystem metrics, 
                                             latency analysis, performance summary, and actionable insights with 
                                             unique test ID for tracking
        """
        # Extract duration from request (defaults to "1h" from DeepDriveInput model)
        eff_duration = request.duration if request.duration else "1h"
        
        components = get_components_func()
        auth_manager = components.get('auth_manager')
        
        try:
            if not auth_manager:
                return ETCDPerformanceDeepDriveResponse(
                    status="error",
                    error="OpenShift authentication not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=eff_duration
                )
            
            # Lazy import of analysis module
            try:
                from analysis.etcd.etcd_performance_deepdrive import etcdDeepDriveAnalyzer
            except ImportError as import_err:
                logger.error(f"Failed to import etcdDeepDriveAnalyzer: {import_err}")
                return ETCDPerformanceDeepDriveResponse(
                    status="error",
                    error=f"Performance deep drive analysis module not available: {import_err}",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=eff_duration
                )
            
            deep_drive_analyzer = etcdDeepDriveAnalyzer(auth_manager, eff_duration)
            
            # Perform the comprehensive analysis
            result = await deep_drive_analyzer.analyze_performance_deep_drive()
            
            return ETCDPerformanceDeepDriveResponse(
                status=result.get('status', 'unknown'),
                data=result.get('data'),
                analysis=result.get('analysis'),
                summary=result.get('summary'),
                error=result.get('error'),
                timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                duration=eff_duration,
                test_id=result.get('test_id')
            )
            
        except Exception as e:
            logger.error(f"Error performing etcd performance deep drive analysis: {e}")
            return ETCDPerformanceDeepDriveResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=eff_duration
            )

