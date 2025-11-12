"""
etcd WAL Fsync MCP Tool
Get Write-Ahead Log fsync performance metrics
"""

import logging
from datetime import datetime
import pytz

from .models import ETCDWALFsyncResponse, DurationInput

logger = logging.getLogger(__name__)


def register_wal_fsync_tool(mcp, get_components_func):
    """Register the WAL fsync tool with the MCP server"""
    
    @mcp.tool()
    async def get_etcd_disk_wal_fsync(request: DurationInput) -> ETCDWALFsyncResponse:
        """
        Get etcd Write-Ahead Log (WAL) fsync performance metrics.
        
        Monitors WAL fsync operations that are critical for etcd data durability and write performance:
        - WAL fsync P99 latency (99th percentile fsync duration - target <10ms for good performance)
        - WAL fsync operation rates and counts (operations per second)
        - WAL fsync duration sum statistics (cumulative fsync time)
        - Cluster-wide WAL fsync performance analysis and health scoring
        
        WAL fsync performance directly impacts write latency. High fsync times (>100ms) indicate storage bottlenecks
        that can cause cluster instability and performance degradation.
        
        Args:
            request: Request object with duration field. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            ETCDWALFsyncResponse: WAL fsync performance metrics including P99 latency, operation rates, 
                                 and cluster-wide analysis with storage performance recommendations
        """
        # Extract duration from request (defaults to "1h" from DurationInput model)
        duration = request.duration
        
        components = get_components_func()
        auth_manager = components.get('auth_manager')
        wal_fsync_collector = components.get('wal_fsync_collector')
        config = components.get('config')
        
        try:
            if not wal_fsync_collector:
                # Lazy initialize
                if auth_manager is None:
                    from ocauth.openshift_auth import OpenShiftAuth
                    auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                    try:
                        await auth_manager.initialize()
                    except Exception:
                        return ETCDWALFsyncResponse(
                            status="error",
                            error="Failed to initialize OpenShift auth for WAL fsync",
                            timestamp=datetime.now(pytz.UTC).isoformat(),
                            duration=duration
                        )
                
                try:
                    from tools.etcd.etcd_disk_wal_fsync import DiskWALFsyncCollector
                    wal_fsync_collector = DiskWALFsyncCollector(auth_manager, duration)
                    components['wal_fsync_collector'] = wal_fsync_collector
                except Exception as e:
                    return ETCDWALFsyncResponse(
                        status="error",
                        error=f"Failed to initialize DiskWALFsyncCollector: {e}",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        duration=duration
                    )
            
            # Update duration for this collection
            wal_fsync_collector.duration = duration
            result = await wal_fsync_collector.collect_all_metrics()
            
            return ETCDWALFsyncResponse(
                status=result.get('status', 'unknown'),
                data=result,
                error=result.get('error'),
                timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                duration=duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting WAL fsync metrics: {e}")
            return ETCDWALFsyncResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )