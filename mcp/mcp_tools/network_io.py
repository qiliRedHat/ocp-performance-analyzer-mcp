"""
Network I/O MCP Tool for etcd
Get comprehensive cluster node level network I/O metrics
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional, List
import pytz

from .models import NetworkIORequest, NetworkMetricsResponse
from .utils import duration_from_time_range

logger = logging.getLogger(__name__)


def register_network_io_tool(mcp, get_components_func):
    """Register the network I/O tool with the MCP server"""
    
    @mcp.tool()
    async def get_network_io_node(request: NetworkIORequest) -> NetworkMetricsResponse:
        """
        Get comprehensive cluster node level network I/O metrics and performance statistics.
        
        Monitors network performance metrics including:
        - RX/TX bandwidth utilization on ocp node level (bits per second)
        - RX/TX packet rates on ocp node level (packets per second)
        - Packet drops and errors on ocp node level (indicating congestion or issues)
        - Network saturation metrics on ocp node level (percentage of capacity)
        - Interface status on ocp node level (up/down, carrier detection)
        - Network speed configuration on ocp node level
        - Connection tracking (conntrack entries and limits) on ocp node level
        - ARP table statistics on ocp node level
        - FIFO queue depths on ocp node level
        
        Metrics organized by node role with avg/max statistics.
        Worker nodes show top 3 performers.
        
        USE CASES:
        - Network bottleneck identification and saturation analysis
        - Packet drop troubleshooting and congestion detection
        - Connection tracking table utilization monitoring
        - Network capacity planning for infrastructure upgrades
        - API server network load analysis via gRPC streams
        - Interface health and carrier status monitoring
        - Network performance baseline establishment
        
        Args:
            duration: Time range for metrics (e.g., '5m', '15m', '1h', '6h', '1d'). Default: '5m'
            start_time: Optional start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
            end_time: Optional end time in ISO format (YYYY-MM-DDTHH:MM:SSZ)
            include_metrics: Optional list of specific metrics to include
            node_groups: Optional list of node groups to filter (e.g., ['controlplane', 'worker'])
        
        Returns:
            NetworkMetricsResponse: Network I/O performance metrics grouped by node role
        """
        components = get_components_func()
        auth_manager = components.get('auth_manager')
        network_collector = components.get('network_collector')
        config = components.get('config')
        
        # Compute effective duration
        duration = request.duration
        if request.start_time and request.end_time:
            computed_duration = duration_from_time_range(request.start_time, request.end_time)
            if computed_duration:
                duration = computed_duration
        
        try:
            if not network_collector:
                # Lazy initialize
                if auth_manager is None:
                    from ocauth.openshift_auth import OpenShiftAuth
                    if config is None:
                        from config.metrics_config_reader import Config
                        config = Config()
                    auth_manager = OpenShiftAuth(config.kubeconfig_path if config else None)
                    try:
                        await auth_manager.initialize()
                    except Exception:
                        return NetworkMetricsResponse(
                            status="error",
                            error="Failed to initialize OpenShift auth for network I/O",
                            timestamp=datetime.now(pytz.UTC).isoformat(),
                            category="network_io",
                            duration=duration
                        )
                
                try:
                    from tools.net.network_io import NetworkIOCollector
                    from config.metrics_config_reader import Config
                    
                    # Get project root
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
                    
                    metrics_net_file = os.path.join(project_root, 'config', 'metrics-net.yml')
                    network_config = Config()
                    
                    # Load metrics file explicitly
                    load_result = network_config.load_metrics_file(metrics_net_file)
                    if not load_result.get('success'):
                        error_msg = load_result.get('error', 'Unknown error loading metrics file')
                        logger.error(f"Failed to load metrics file: {error_msg}")
                        return NetworkMetricsResponse(
                            status="error",
                            error=f"Failed to load metrics file: {error_msg}",
                            timestamp=datetime.now(pytz.UTC).isoformat(),
                            category="network_io",
                            duration=duration
                        )
                    logger.info(f"Loaded {load_result.get('metrics_loaded', 0)} metrics from {metrics_net_file}")
                    
                    network_collector = NetworkIOCollector(
                        prometheus_url=auth_manager.prometheus_url,
                        token=getattr(auth_manager, 'prometheus_token', None),
                        config=network_config
                    )
                    await network_collector.initialize()
                    components['network_collector'] = network_collector
                    
                except Exception as e:
                    return NetworkMetricsResponse(
                        status="error",
                        error=f"Failed to initialize NetworkIOCollector: {e}",
                        timestamp=datetime.now(pytz.UTC).isoformat(),
                        category="network_io",
                        duration=duration
                    )
            
            # Ensure collector is initialized
            if not hasattr(network_collector, 'prometheus_client') or network_collector.prometheus_client is None:
                await network_collector.initialize()
            
            # Check if metrics are loaded
            if not hasattr(network_collector, 'metrics') or not network_collector.metrics:
                logger.warning("NetworkIOCollector has no metrics loaded from config")
                return NetworkMetricsResponse(
                    status="error",
                    error="No network metrics configured. Please check metrics-net.yml configuration.",
                    timestamp=datetime.now(pytz.UTC).isoformat(),
                    category="network_io",
                    duration=duration
                )
            
            logger.info(f"Collecting network I/O metrics with {len(network_collector.metrics)} metrics configured")
            
            # Collect metrics
            network_data = await asyncio.wait_for(
                network_collector.collect_all_metrics(duration=duration), timeout=60.0
            )
            
            # Derive status from summary
            status = "success"
            error = None
            if network_data.get('summary'):
                summary = network_data['summary']
                errors = summary.get('errors', 0)
                empty = summary.get('empty', 0)
                with_data = summary.get('with_data', 0)
                
                if errors > 0:
                    status = "error"
                    error = f"{errors} metric(s) failed to collect"
                elif empty == summary.get('total_metrics', 0) and with_data == 0:
                    status = "warning"
                    error = f"All {empty} metrics returned empty results. Check Prometheus connectivity and metric availability."
                    logger.warning(f"All network I/O metrics are empty. Total: {summary.get('total_metrics', 0)}, Empty: {empty}, With Data: {with_data}")
            
            return NetworkMetricsResponse(
                status=status,
                data=network_data,
                error=error,
                timestamp=network_data.get('timestamp', datetime.now(pytz.UTC).isoformat()),
                category="network_io",
                duration=duration
            )
            
        except asyncio.TimeoutError:
            return NetworkMetricsResponse(
                status="error",
                error="Timeout collecting network I/O metrics",
                timestamp=datetime.now(pytz.UTC).isoformat(),
                category="network_io",
                duration=duration
            )
        except Exception as e:
            logger.error(f"Error collecting network I/O: {e}")
            return NetworkMetricsResponse(
                status="error",
                error=str(e),
                timestamp=datetime.now(pytz.UTC).isoformat(),
                category="network_io",
                duration=duration
            )