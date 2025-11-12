#!/usr/bin/env python3
"""
OpenShift OVN-Kubernetes Network Analyzer MCP Server
Main server implementation using FastMCP with streamable-http transport

File Structure:
- network_analyzer_mcp_server.py (this file) - Main server and initialization
- mcp_tools/
  ├── __init__.py
  ├── health_check.py - Health check tool
  ├── cluster_info.py - Cluster information tool
  ├── network_l1.py - Layer 1 interface metrics
  ├── network_io.py - Network traffic and I/O
  ├── socket_tcp.py - TCP socket statistics
  ├── socket_udp.py - UDP socket statistics
  ├── socket_memory.py - Socket memory statistics
  ├── socket_softnet.py - Softnet statistics
  ├── socket_ip.py - IP statistics
  ├── netstat_tcp.py - TCP netstat
  └── netstat_udp.py - UDP netstat
"""

import asyncio
import os
import logging
import signal
import sys

# Ensure project root and mcp directory are on sys.path
CURRENT_DIR = os.path.dirname(__file__)
MCP_DIR = os.path.dirname(CURRENT_DIR)  # mcp/ directory
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if MCP_DIR not in sys.path:
    sys.path.insert(0, MCP_DIR)

from ocauth.openshift_auth import OpenShiftAuth
from config.metrics_config_reader import Config
from tools.utils.promql_basequery import PrometheusBaseQuery

import fastmcp
from fastmcp.server import FastMCP

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, message=r"HTTPResponse\.getheaders\(\) is deprecated")
warnings.filterwarnings("ignore", category=UserWarning, module="anyio.streams.memory")

# Configure logging
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

os.environ['TZ'] = 'UTC'
shutdown_event = asyncio.Event()

# ==================== Initialize FastMCP ====================

app = FastMCP("ovnk-network-analyzer")
mcp = app

# Global components
auth_manager = None
config = None
prometheus_client = None
cluster_info_collector = None
network_collector = None

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

async def initialize_components():
    """Initialize global components with proper error handling"""
    global auth_manager, config, prometheus_client, cluster_info_collector, network_collector
    
    try:
        logger.info("="*70)
        logger.info("Initializing OVN-Kubernetes Network Analyzer components...")
        logger.info("="*70)
        
        # Initialize config
        config = Config()
        loaded_before = config.get_metrics_count()
        if loaded_before == 0:
            metrics_file_path = os.path.join(PROJECT_ROOT, 'config', 'metrics-net.yml')
            load_result = config.load_metrics_file(metrics_file_path)
            total_after = config.get_metrics_count()
            if total_after > 0:
                file_summary = config.get_file_summary()
                files_descr = ", ".join(
                    f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values()
                ) or os.path.basename(metrics_file_path)
                logger.info(f"Metrics loaded: total={total_after}, files=[{files_descr}]")
            else:
                logger.warning(f"No metrics loaded: {load_result.get('error', 'no metrics found')}")
        else:
            file_summary = config.get_file_summary()
            total = config.get_metrics_count()
            files_descr = ", ".join(f"{v['file_name']}={v['metrics_count']}" for v in file_summary.values())
            logger.info(f"Metrics preloaded: total={total}, files=[{files_descr}]")
        
        # Initialize OpenShift authentication
        logger.info("🔗 Initializing OpenShift authentication...")
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()
        logger.info("✅ OpenShift authentication initialized")
        
        # Initialize Prometheus client
        prom_cfg = auth_manager.get_prometheus_config() or {}
        base_url = prom_cfg.get('url') or auth_manager.prometheus_url
        token = prom_cfg.get('token') or auth_manager.prometheus_token
        fallbacks = prom_cfg.get('fallback_urls') or []
        
        def _is_route(u: str) -> bool:
            return ('.apps.' in u) and ('svc.cluster.local' not in u)
        
        preferred_url = base_url
        for u in fallbacks:
            if _is_route(u):
                preferred_url = u
                break
        
        prometheus_client = PrometheusBaseQuery(preferred_url, token)
        
        # Initialize collectors
        logger.info("📊 Initializing metric collectors...")
        logger.info("-" * 70)
        
        # Cluster Info Collector
        logger.info("Initializing ClusterInfoCollector...")
        from tools.ocp.cluster_info import ClusterInfoCollector
        cluster_info_collector = ClusterInfoCollector()
        await cluster_info_collector.initialize()
        logger.info("✅ ClusterInfoCollector initialized")
        
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
        logger.info("="*70)
        logger.info("✅ All components initialized successfully!")
        logger.info("="*70)
        
        return True
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"❌ Failed to initialize components: {e}")
        logger.error("=" * 70)
        return False

def get_global_components():
    """Get initialized global components"""
    return {
        'auth_manager': auth_manager,
        'config': config,
        'prometheus_client': prometheus_client,
        'cluster_info_collector': cluster_info_collector,
        'network_collector': network_collector,
        'initialize_components': initialize_components
    }

# ==================== Import MCP Tools ====================

from mcp_tools.health_check import register_health_check_tool
from mcp_tools.cluster_info import register_cluster_info_tool
from mcp_tools.network_l1 import register_network_l1_tool
from mcp_tools.network_io import register_network_io_tool
from mcp_tools.socket_tcp import register_socket_tcp_tool
from mcp_tools.socket_udp import register_socket_udp_tool
from mcp_tools.socket_memory import register_socket_memory_tool
from mcp_tools.socket_softnet import register_socket_softnet_tool
from mcp_tools.socket_ip import register_socket_ip_tool
from mcp_tools.netstat_tcp import register_netstat_tcp_tool
from mcp_tools.netstat_udp import register_netstat_udp_tool

# Register all tools
logger.info("Registering MCP tools...")
register_health_check_tool(mcp, get_global_components)
register_cluster_info_tool(mcp, get_global_components)
register_network_l1_tool(mcp, get_global_components)
register_network_io_tool(mcp, get_global_components)
register_socket_tcp_tool(mcp, get_global_components)
register_socket_udp_tool(mcp, get_global_components)
register_socket_memory_tool(mcp, get_global_components)
register_socket_softnet_tool(mcp, get_global_components)
register_socket_ip_tool(mcp, get_global_components)
register_netstat_tcp_tool(mcp, get_global_components)
register_netstat_udp_tool(mcp, get_global_components)
logger.info("✅ All MCP tools registered")

# ==================== Startup and Main ====================

async def startup_event():
    """Startup event handler"""
    logger.info("")
    logger.info("🚀 Starting OVN-Kubernetes Network Analyzer MCP Server...")
    logger.info("")
    
    try:
        init_success = await initialize_components()
        
        if not init_success:
            logger.error("❌ Failed to initialize components. Server may not function properly.")
            logger.error("")
        else:
            logger.info("✅ OVN-Kubernetes Network Analyzer MCP Server started successfully!")
            logger.info("")
            logger.info("📋 Available Tools:")
            logger.info("  1. get_server_health - Health checks and connectivity")
            logger.info("  2. get_ocp_cluster_info - Cluster infrastructure (static)")
            logger.info("  3. get_network_l1_stats - Layer 1 interface metrics")
            logger.info("  4. get_network_io_node - Network traffic and I/O")
            logger.info("  5. get_network_socket_tcp_stats - TCP socket resources")
            logger.info("  6. get_network_socket_udp_stats - UDP socket resources")
            logger.info("  7. get_network_socket_memory - Socket buffer memory")
            logger.info("  8. get_network_socket_softnet_stats - Kernel packet processing")
            logger.info("  9. get_network_socket_ip_stats - IP traffic and ICMP")
            logger.info("  10. get_network_netstat_tcp_stats - TCP protocol statistics")
            logger.info("  11. get_network_netstat_udp_stats - UDP protocol statistics")
            logger.info("")
            logger.info("⚠️  For node CPU/memory usage, use the node_usage MCP server")
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
        
        # Create tasks for server and shutdown handler
        server_task = asyncio.create_task(
            app.run_async(
                transport="streamable-http",
                host="0.0.0.0",
                port=8002,
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