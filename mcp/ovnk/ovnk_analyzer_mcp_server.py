#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Analyzer MCP Server
Main server implementation using FastMCP with streamable-http transport

File Structure:
- ovnk_analyzer_mcp_server.py (this file) - Main server and initialization
- mcp_tools/
  ├── __init__.py
  ├── models.py - Pydantic models
  ├── utils.py - Common utilities
  ├── health_check.py - Health check tool
  ├── cluster_info.py - Cluster information tool
  ├── node_usage.py - Node resource usage tool
  ├── ovn_database.py - OVN database metrics
  ├── kubelet_cni.py - Kubelet CNI/CRIO metrics
  ├── ovn_latency.py - OVN latency metrics
  ├── ovs_usage.py - OVS usage metrics
  ├── ovnk_pods_usage.py - OVN-K pod metrics
  ├── multus_pods_usage.py - Multus pod metrics
  ├── api_server.py - API server metrics
  └── network_io.py - Network I/O metrics
"""

import os
import sys
import asyncio
import logging
import warnings
import signal
import subprocess
import shutil

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

_server_log_level = os.environ.get("OVNK_LOG_LEVEL", "INFO").upper()
try:
    root_level = getattr(logging, _server_log_level, logging.INFO)
except Exception:
    root_level = logging.INFO
logging.getLogger().setLevel(root_level)
logger.setLevel(root_level)

# Reduce noise from libs
logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

# Set timezone to UTC
os.environ['TZ'] = 'UTC'

# Suppress warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning, module="anyio.streams.memory")

# Ensure project root and mcp directory are on sys.path
try:
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    MCP_DIR = os.path.dirname(CURRENT_DIR)  # mcp/ directory
    PROJECT_ROOT = os.path.dirname(MCP_DIR)  # Project root
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    if MCP_DIR not in sys.path:
        sys.path.insert(0, MCP_DIR)
except Exception:
    pass

# Import required libraries
try:
    from fastmcp import FastMCP
    import uvicorn
except ImportError as e:
    logger.error(f"Required dependencies not installed: {e}")
    logger.error("Please install: pip install fastmcp>=1.12.4 uvicorn")
    sys.exit(1)

# Import our modules
try:
    from ocauth.openshift_auth import OpenShiftAuth
    from config.metrics_config_reader import Config
    from tools.utils.promql_basequery import PrometheusBaseQuery
except ImportError as e:
    logger.error(f"Failed to import local modules: {e}")
    sys.exit(1)

# ==================== Initialize FastMCP ====================

app = FastMCP("ovnk-analyzer")
mcp = app

# Global components
auth_manager = None
config = None
ovn_db_collector = None
kubelet_cni_collector = None
latency_collector = None
ovs_collector = None
pods_collector = None
api_collector = None
cluster_info_collector = None
network_collector = None
node_usage_collector = None

shutdown_event = asyncio.Event()

# ==================== Helper Functions ====================

async def cleanup_resources():
    """Clean up global resources on shutdown"""
    global auth_manager
    
    logger.info("Cleaning up resources...")
    
    try:
        if auth_manager:
            await auth_manager.cleanup()
    except Exception as e:
        logger.error(f"Error cleaning up auth manager: {e}")
    
    logger.info("Resource cleanup completed")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


async def initialize_collectors():
    """Initialize all collectors with proper error handling"""
    global auth_manager, config, ovn_db_collector, kubelet_cni_collector
    global latency_collector, ovs_collector, pods_collector, api_collector
    global cluster_info_collector, network_collector, node_usage_collector
    
    try:
        logger.info("=" * 70)
        logger.info("Initializing OVN-Kubernetes Analyzer components...")
        logger.info("=" * 70)
        
        # Initialize global config
        config = Config()
        loaded_before = config.get_metrics_count()
        
        if loaded_before == 0:
            # Load network metrics configuration
            metrics_net_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-net.yml')
            load_result = config.load_metrics_file(metrics_net_file)
            total_after = config.get_metrics_count()
            
            if total_after > 0:
                file_summary = config.get_file_summary()
                files_descr = ", ".join(
                    f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values()
                ) or os.path.basename(metrics_net_file)
                logger.info(f"Metrics loaded: total={total_after}, files=[{files_descr}]")
            else:
                logger.warning(f"No metrics loaded: {load_result.get('error', 'no metrics found')}")
        else:
            file_summary = config.get_file_summary()
            total = config.get_metrics_count()
            files_descr = ", ".join(
                f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values()
            )
            logger.info(f"Metrics preloaded: total={total}, files=[{files_descr}]")
        
        # Initialize OpenShift authentication
        logger.info("🔗 Initializing OpenShift authentication...")
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()
        logger.info("✅ OpenShift authentication initialized")
        
        # Initialize collectors
        logger.info("📊 Initializing metric collectors...")
        logger.info("-" * 70)
        
        # Cluster Info Collector
        logger.info("Initializing ClusterInfoCollector...")
        from tools.ocp.cluster_info import ClusterInfoCollector
        cluster_info_collector = ClusterInfoCollector()
        await cluster_info_collector.initialize()
        logger.info("✅ ClusterInfoCollector initialized")
        
        # OVN Database Collector
        logger.info("Initializing ovnDBCollector...")
        from tools.ovnk.ovnk_baseinfo import ovnDBCollector
        ovn_db_collector = ovnDBCollector(auth_manager)
        logger.info("✅ ovnDBCollector initialized")
        
        # Latency Collector
        logger.info("Initializing OVNLatencyCollector...")
        from tools.ovnk.ovnk_latency import OVNLatencyCollector
        latency_collector = OVNLatencyCollector(auth_manager)
        logger.info("✅ OVNLatencyCollector initialized")
        
        # OVS Usage Collector
        logger.info("Initializing OVSUsageCollector...")
        from tools.ovnk.ovnk_ovs_usage import OVSUsageCollector
        ovs_collector = OVSUsageCollector(auth_manager)
        logger.info("✅ OVSUsageCollector initialized")
        
        # Pods Usage Collector
        logger.info("Initializing PodsUsageCollector...")
        from tools.pods.pods_usage import PodsUsageCollector
        pods_collector = PodsUsageCollector(auth_manager)
        logger.info("✅ PodsUsageCollector initialized")
        
        # API Usage Collector
        logger.info("Initializing apiUsageCollector...")
        from tools.ocp.cluster_apistats import apiUsageCollector
        api_collector = apiUsageCollector(auth_manager)
        logger.info("✅ apiUsageCollector initialized")
        
        # Network IO Collector
        logger.info("Initializing NetworkIOCollector...")
        from tools.net.network_io import NetworkIOCollector
        network_collector = NetworkIOCollector(
            prometheus_url=auth_manager.prometheus_url,
            token=getattr(auth_manager, 'prometheus_token', None)
        )
        await network_collector.initialize()
        logger.info("✅ NetworkIOCollector initialized")
        
        logger.info("-" * 70)
        logger.info("✅ All collectors initialized successfully!")
        logger.info("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"❌ Failed to initialize collectors: {e}")
        logger.error("=" * 70)
        return False


def get_global_components():
    """Get initialized global components"""
    return {
        'auth_manager': auth_manager,
        'config': config,
        'ovn_db_collector': ovn_db_collector,
        'kubelet_cni_collector': kubelet_cni_collector,
        'latency_collector': latency_collector,
        'ovs_collector': ovs_collector,
        'pods_collector': pods_collector,
        'api_collector': api_collector,
        'cluster_info_collector': cluster_info_collector,
        'network_collector': network_collector,
        'node_usage_collector': node_usage_collector,
        'initialize_components': initialize_collectors
    }


# ==================== Import MCP Tools ====================

from mcp_tools.health_check import register_health_check_tool
from mcp_tools.cluster_info import register_cluster_info_tool
from mcp_tools.node_usage import register_node_usage_tool
from mcp_tools.ovn_database import register_ovn_database_tool
from mcp_tools.kubelet_cni import register_kubelet_cni_tool
from mcp_tools.ovn_latency import register_ovn_latency_tool
from mcp_tools.ovs_usage import register_ovs_usage_tool
from mcp_tools.ovnk_pods_usage import register_ovnk_pods_usage_tool
from mcp_tools.multus_pods_usage import register_multus_pods_usage_tool
from mcp_tools.api_server import register_api_server_tool
from mcp_tools.network_io import register_network_io_tool

# Register all tools
logger.info("Registering MCP tools...")
register_health_check_tool(mcp, get_global_components)
register_cluster_info_tool(mcp, get_global_components)
register_node_usage_tool(mcp, get_global_components)
register_ovn_database_tool(mcp, get_global_components)
register_kubelet_cni_tool(mcp, get_global_components)
register_ovn_latency_tool(mcp, get_global_components)
register_ovs_usage_tool(mcp, get_global_components)
register_ovnk_pods_usage_tool(mcp, get_global_components)
register_multus_pods_usage_tool(mcp, get_global_components)
register_api_server_tool(mcp, get_global_components)
register_network_io_tool(mcp, get_global_components)
logger.info("✅ All MCP tools registered")


# ==================== Startup and Main ====================

async def startup_event():
    """Startup event handler"""
    logger.info("")
    logger.info("🚀 Starting OVN-Kubernetes Analyzer MCP Server...")
    logger.info("")
    
    try:
        init_success = await initialize_collectors()
        
        if not init_success:
            logger.error("❌ Failed to initialize collectors. Server may not function properly.")
            logger.error("")
        else:
            logger.info("✅ OVN-Kubernetes Analyzer MCP Server started successfully!")
            logger.info("")
            logger.info("📋 Available Tools:")
            logger.info("  1. get_mcp_health_status - Health checks and connectivity")
            logger.info("  2. get_cluster_info - Cluster infrastructure (static)")
            logger.info("  3. get_ocp_node_usage - Node CPU/memory/cgroup metrics")
            logger.info("  4. get_ovn_database_info - OVN database sizes")
            logger.info("  5. get_kubelet_cni_stats - Kubelet CNI/CRIO metrics")
            logger.info("  6. get_ovn_latency_stats - OVN operation latencies")
            logger.info("  7. get_ovs_usage - OVS resource usage")
            logger.info("  8. get_ovnk_pods_usage - OVN-K pod resources")
            logger.info("  9. get_multus_pods_usage - Multus pod resources")
            logger.info("  10. get_api_server_stats - API server performance")
            logger.info("  11. get_network_io - Network I/O metrics")
            logger.info("")
    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")
        logger.error("")


async def shutdown_handler():
    """Handle graceful shutdown"""
    logger.info("Shutdown handler called")
    await cleanup_resources()
    logger.info("Shutdown complete")


async def main():
    """Main entry point with improved error handling and graceful shutdown"""
    try:
        # Initialize components
        await startup_event()
        
        # Optional MCP Inspector launch
        enable_inspector = os.environ.get("ENABLE_MCP_INSPECTOR", "").lower() in (
            "1", "true", "yes", "on"
        )
        host = "0.0.0.0"
        port = 8003
        
        if enable_inspector:
            def start_mcp_inspector(url: str):
                try:
                    if shutil.which("npx") is None:
                        logger.warning("MCP Inspector requested but 'npx' not found")
                        return
                    inspector_cmd = [
                        "npx", "--yes", "@modelcontextprotocol/inspector", url
                    ]
                    subprocess.Popen(inspector_cmd)
                    logger.info("Launched MCP Inspector for URL: %s", url)
                except Exception as ie:
                    logger.warning("Failed to launch MCP Inspector: %s", ie)
            
            inspector_url = os.environ.get(
                "MCP_INSPECTOR_URL", f"http://127.0.0.1:{port}/sse"
            )
            start_mcp_inspector(inspector_url)
        
        # Create tasks for server and shutdown handler
        server_task = asyncio.create_task(
            app.run_async(
                transport="streamable-http",
                host=host,
                port=port,
                uvicorn_config={"ws": "none"}
            )
        )
        
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        
        # Wait for either server completion or shutdown signal
        done, pending = await asyncio.wait(
            [server_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # If shutdown was triggered, clean up
        if shutdown_task in done:
            logger.info("Shutdown signal received, cleaning up...")
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass
            await shutdown_handler()
        
        # If server task completed, check for exceptions
        if server_task in done:
            try:
                await server_task
            except Exception as e:
                logger.error(f"Server task failed: {e}")
                raise
    
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
        await shutdown_handler()
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        await shutdown_handler()
        raise
    finally:
        logger.info("Main function exiting")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)