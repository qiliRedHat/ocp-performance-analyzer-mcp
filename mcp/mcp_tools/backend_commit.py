"""
etcd Backend Commit MCP Tool
Get backend database commit operation metrics
"""

import logging
from datetime import datetime
import pytz

from .models import ETCDBackendCommitResponse, DurationInput

logger = logging.getLogger(__name__)


def register_backend_commit_tool(mcp, get_components_func):
    """Register the backend commit tool with the MCP server"""
    
    @mcp.tool()
    async def get_etcd_disk_backend_commit(request: DurationInput) -> ETCDBackendCommitResponse:
        """
        Get etcd backend commit operation performance metrics.
        
        Monitors backend database commit operations that handle data persistence:
        - Backend commit duration P99 latency (99th percentile response times)
        - Commit operation rates and counts  
        - Commit duration statistics and efficiency analysis
        - Performance recommendations for write optimization
        
        Backend commit latency affects overall write performance. High latency (>25ms) indicates storage bottlenecks.
        
        Args:
            request: Request object with duration field. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            ETCDBackendCommitResponse: Backend commit performance metrics including P99 latency, 
                                      operation throughput, and storage optimization recommendations
        """
        # Extract duration from request (defaults to "1h" from DurationInput model)
        duration = request.duration
        
        components = get_components_func()
        backend_commit_collector = components.get('backend_commit_collector')
        
        try:
            if not backend_commit_collector:
                return ETCDBackendCommitResponse(
                    status="error",
                    error="Backend commit collector not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
            
            result = await backend_commit_collector.collect_metrics(duration)
            
            return ETCDBackendCommitResponse(
                status=result.get('status', 'unknown'),
                data=result.get('data'),
                error=result.get('error'),
                timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                duration=duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting backend commit metrics: {e}")
            return ETCDBackendCommitResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )