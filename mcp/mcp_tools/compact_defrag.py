"""
etcd Compaction and Defragmentation MCP Tool
Get database maintenance operation metrics
"""

import logging
from datetime import datetime
import pytz

from .models import ETCDCompactDefragResponse, DurationInput

logger = logging.getLogger(__name__)


def register_compact_defrag_tool(mcp, get_components_func):
    """Register the compact/defrag tool with the MCP server"""
    
    @mcp.tool()
    async def get_etcd_disk_compact_defrag(request: DurationInput) -> ETCDCompactDefragResponse:
        """
        Get etcd database compaction and defragmentation performance metrics.
        
        Monitors database maintenance operations that are critical for etcd performance:
        - Compaction duration and rates (time spent compacting old revisions)
        - Defragmentation duration and rates (database defragmentation operations)
        - Page fault metrics (vmstat pgmajfault rates indicating memory pressure)
        - Operation efficiency analysis and performance recommendations
        
        These metrics help identify database maintenance bottlenecks and storage performance issues.
        
        Args:
            request: Request object with duration field. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            ETCDCompactDefragResponse: Compaction and defragmentation metrics with performance analysis 
                                      and recommendations
        """
        # Extract duration from request (defaults to "1h" from DurationInput model)
        duration = request.duration
        
        components = get_components_func()
        compact_defrag_collector = components.get('compact_defrag_collector')
        
        try:
            if not compact_defrag_collector:
                return ETCDCompactDefragResponse(
                    status="error",
                    error="Compact/defrag collector not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
            
            result = await compact_defrag_collector.collect_metrics(duration)
            
            return ETCDCompactDefragResponse(
                status=result.get('status', 'unknown'),
                data=result.get('data'),
                error=result.get('error'),
                timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                duration=duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting compact/defrag metrics: {e}")
            return ETCDCompactDefragResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )