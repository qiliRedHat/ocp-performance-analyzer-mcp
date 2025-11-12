"""
etcd Disk I/O MCP Tool
Get comprehensive disk I/O performance metrics
"""

import logging
import os
from datetime import datetime
import pytz

from .models import NodeDiskIOResponse, DurationInput

logger = logging.getLogger(__name__)


def register_disk_io_tool(mcp, get_components_func):
    """Register the disk I/O tool with the MCP server"""
    
    @mcp.tool()
    async def get_node_disk_io(request: DurationInput) -> NodeDiskIOResponse:
        """
        Get etcd disk I/O performance metrics including throughput and IOPS.
        
        Monitors comprehensive disk I/O performance metrics that directly impact etcd performance:
        - Container disk write metrics (etcd pod disk write throughput and patterns)
        - Node disk read/write throughput (bytes per second for storage devices)  
        - Node disk read/write IOPS (input/output operations per second)
        - Device-level I/O statistics aggregated by master node
        - Storage performance analysis and bottleneck identification
        
        Disk I/O performance is critical for etcd operations including:
        - WAL (Write-Ahead Log) write operations
        - Database snapshot creation and transfers
        - Compaction and defragmentation operations
        - Overall cluster stability and response times
        
        Poor disk I/O performance can cause etcd timeouts, leader elections, and cluster instability.
        
        Args:
            request: Request object with duration field. Default duration is '1h'.
                   Examples: '15m', '30m', '1h', '2h', '6h', '12h', '1d'
        
        Returns:
            NodeDiskIOResponse: Disk I/O performance metrics including container write rates, 
                               node throughput/IOPS, device statistics, and storage optimization recommendations
        """
        # Extract duration from request (defaults to "1h" from DurationInput model)
        duration = request.duration
        
        components = get_components_func()
        auth_manager = components.get('auth_manager')
        disk_io_collector = components.get('disk_io_collector')
        config = components.get('config')
        
        try:
            if not disk_io_collector:
                # Lazy initialize
                if auth_manager is None:
                    from ocauth.openshift_auth import OpenShiftAuth
                    auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                    try:
                        await auth_manager.initialize()
                    except Exception:
                        return NodeDiskIOResponse(
                            status="error",
                            error="Failed to initialize OpenShift auth for disk I/O",
                            timestamp=datetime.now(pytz.UTC).isoformat(),
                            duration=duration
                        )
                
                try:
                    from tools.disk.disk_io import DiskIOCollector
                    
                    # Get project root
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
                    
                    # Use metrics-disk.yml for disk I/O metrics
                    metrics_disk_file = os.path.join(project_root, 'config', 'metrics-disk.yml')
                    disk_io_collector = DiskIOCollector(auth_manager, duration, metrics_file_path=metrics_disk_file)
                    components['disk_io_collector'] = disk_io_collector
                except Exception as e:
                    return NodeDiskIOResponse(
                        status="error",
                        error=f"Failed to initialize DiskIOCollector: {e}",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        duration=duration
                    )
            
            # Update duration for this collection
            disk_io_collector.duration = duration
            result = await disk_io_collector.collect_all_metrics()
            
            return NodeDiskIOResponse(
                status=result.get('status', 'unknown'),
                data=result,
                error=result.get('error'),
                timestamp=result.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                duration=duration
            )
            
        except Exception as e:
            logger.error(f"Error collecting disk I/O metrics: {e}")
            return NodeDiskIOResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                duration=duration
            )