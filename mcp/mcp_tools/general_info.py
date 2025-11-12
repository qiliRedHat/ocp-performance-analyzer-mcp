"""
etcd General Information MCP Tool
Get general cluster metrics including resource usage and operational metrics
"""

import logging
from datetime import datetime
from typing import Optional
import pytz

from .models import ETCDGeneralInfoResponse, DurationInput

logger = logging.getLogger(__name__)


def register_general_info_tool(mcp, get_components_func):
    """Register the general info tool with the MCP server"""
    
    @mcp.tool()
    async def get_etcd_general_info(request: Optional[DurationInput] = None) -> ETCDGeneralInfoResponse:
        """
        Get general etcd cluster information including resource usage and operational metrics.
        
        Collects comprehensive etcd performance and health metrics including:
        - CPU and memory usage patterns
        - Database size metrics (physical and logical sizes, space utilization)
        - Proposal metrics (commit rates, failures, pending proposals)
        - Leadership metrics (leader changes, elections, has_leader status)
        - Performance metrics (slow applies, read indexes, operation rates)
        - Health metrics (heartbeat failures, total keys, compacted keys)
        
        Args:
            request: Optional request object with duration field. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            ETCDGeneralInfoResponse: General cluster information including resource usage, 
                                    operational performance, and health statistics
        """
        # Extract duration from request, default to "1h" if not provided
        duration = request.duration if request and request.duration else "1h"
        
        components = get_components_func()
        general_collector = components.get('general_collector')
        
        try:
            if not general_collector:
                return ETCDGeneralInfoResponse(
                    status="error",
                    error="General info collector not initialized",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    duration=duration
                )
            
            result = await general_collector.collect_metrics(duration)
            
            return ETCDGeneralInfoResponse(
                status=result.get('status', 'unknown'),
                data=result.get('data'),
                error=result.get('error'),
                timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                duration=duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting general info: {e}")
            return ETCDGeneralInfoResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )