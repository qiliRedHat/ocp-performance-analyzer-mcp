#!/usr/bin/env python3
"""
OpenShift Node Analyzer MCP Server
Main server implementation using FastMCP

Modular Architecture:
- node_analyzer_mcp_server.py (this file) - Main server and initialization
- mcp_tools/ - Individual tool implementations
  ├── __init__.py
  ├── models.py - Pydantic models
  ├── utils.py - Common utilities
  ├── health_check.py - Health status
  ├── cluster_info.py - OCP cluster info
  ├── node_usage.py - Node resource metrics
  ├── node_pleg_relist.py - Node PLEG relist latency metrics
  └── node_kubelet_runtime_operations_errors.py - Kubelet runtime operations errors
"""

import os
import sys
import asyncio
import logging
import warnings
import subprocess
import shutil
from datetime import datetime
import pytz

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set timezone to UTC
os.environ['TZ'] = 'UTC'

# Suppress deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"^websockets(\..*)?$")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"^uvicorn\.protocols\.websockets(\..*)?$")
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r"HTTPResponse\.getheaders\(\) is deprecated"
)

# Ensure project root and mcp directory are on sys.path
try:
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    MCP_DIR = os.path.dirname(CURRENT_DIR)  # mcp/ directory
    PROJECT_ROOT = os.path.dirname(MCP_DIR)  # Project root
    if PROJECT_ROOT not in sys.path:
        sys.path.append(PROJECT_ROOT)
    if MCP_DIR not in sys.path:
        sys.path.append(MCP_DIR)
except Exception:
    pass

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
except ImportError as e:
    logger.error(f"Failed to import local modules: {e}")
    logger.error("Please ensure all modules are in the correct directory structure")
    sys.exit(1)

# Initialize MCP server
mcp = FastMCP("OpenShift Node Analyzer")

# ==================== Global Components ====================

# Global variables for collectors
auth_manager = None
config = None
cluster_info_collector = None
node_usage_collector = None
pleg_relist_collector = None
kubelet_runtime_operations_errors_collector = None

# ==================== Initialization ====================

async def initialize_collectors():
    """Initialize all collectors with authentication - each loads its own metrics"""
    global auth_manager, config, cluster_info_collector, node_usage_collector, pleg_relist_collector, kubelet_runtime_operations_errors_collector

    try:
        logger.info("="*70)
        logger.info("Initializing OpenShift Node Analyzer components...")
        logger.info("="*70)
        
        # Initialize global config for shared access
        config = Config()
        metrics_node_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-node.yml')
        
        # Initialize OpenShift authentication
        logger.info("🔗 Initializing OpenShift authentication...")
        auth_manager = OpenShiftAuth(config.kubeconfig_path)
        await auth_manager.initialize()
        logger.info("✅ OpenShift authentication initialized successfully")
        logger.info("")

        # Initialize collectors - each will load and log their own category metrics
        logger.info("📊 Initializing metric collectors...")
        logger.info("-" * 70)
        
        # Import collector modules
        from tools.ocp.cluster_info import ClusterInfoCollector
        from tools.node.node_usage import nodeUsageCollector
        from tools.node.node_pleg_relist import plegRelistCollector
        from tools.node.node_kubelet_runtime_operations_errors import kubeletRuntimeOperationsErrorsCollector

        # OCP Cluster Info Collector
        logger.info("Initializing ClusterInfoCollector...")
        cluster_info_collector = ClusterInfoCollector()
        await cluster_info_collector.initialize()
        logger.info("✅ ClusterInfoCollector initialized")

        # Node Usage Collector
        logger.info("Initializing nodeUsageCollector...")
        prometheus_config = {
            'url': auth_manager.prometheus_url,
            'token': getattr(auth_manager, 'prometheus_token', None),
            'verify_ssl': False
        }
        node_usage_collector = nodeUsageCollector(auth_manager, prometheus_config)
        logger.info("✅ nodeUsageCollector initialized (uses node metrics)")

        # PLEG Relist Collector
        logger.info("Initializing plegRelistCollector...")
        pleg_relist_collector = plegRelistCollector(auth_manager, prometheus_config)
        logger.info("✅ plegRelistCollector initialized (uses PLEG metrics)")

        # Runtime Operations Errors Collector
        logger.info("Initializing kubeletRuntimeOperationsErrorsCollector...")
        kubelet_runtime_operations_errors_collector = kubeletRuntimeOperationsErrorsCollector(auth_manager, prometheus_config)
        logger.info("✅ kubeletRuntimeOperationsErrorsCollector initialized (uses runtime error metrics)")
        
        logger.info("-" * 70)
        logger.info("✅ All collectors initialized successfully!")
        logger.info("="*70)
        logger.info("")
        
        return True
        
    except Exception as e:
        logger.error("="*70)
        logger.error(f"❌ Failed to initialize collectors: {e}")
        logger.error("="*70)
        return False

def get_global_components():
    """Get initialized global components"""
    return {
        'auth_manager': auth_manager,
        'config': config,
        'cluster_info_collector': cluster_info_collector,
        'node_usage_collector': node_usage_collector,
        'pleg_relist_collector': pleg_relist_collector,
        'kubelet_runtime_operations_errors_collector': kubelet_runtime_operations_errors_collector
    }

# ==================== Import and Register MCP Tools ====================

from mcp_tools.health_check import register_health_check_tool
from mcp_tools.cluster_info import register_cluster_info_tool
from mcp_tools.node_usage import register_node_usage_tool
from mcp_tools.node_pleg_relist import register_pleg_relist_tool
from mcp_tools.node_kubelet_runtime_operations_errors import register_runtime_errors_tool

# Register all tools
logger.info("Registering MCP tools...")
register_health_check_tool(mcp, get_global_components)
register_cluster_info_tool(mcp, get_global_components)
register_node_usage_tool(mcp, get_global_components)
register_pleg_relist_tool(mcp, get_global_components)
register_runtime_errors_tool(mcp, get_global_components)
logger.info("✅ All MCP tools registered")

# ==================== Startup and Main ====================

async def startup_event():
    """Startup event handler"""
    logger.info("")
    logger.info("🚀 Starting OpenShift Node Analyzer MCP Server...")
    logger.info("")
    
    # Initialize collectors (includes metrics loading with logging)
    init_success = await initialize_collectors()
    if not init_success:
        logger.error("❌ Failed to initialize collectors. Server may not function properly.")
        logger.error("")
    else:
        logger.info("✅ OpenShift Node Analyzer MCP Server started successfully!")
        logger.info("")
        logger.info("📋 Available Tools:")
        logger.info("  1. get_server_health - Health checks and status")
        logger.info("  2. get_ocp_cluster_info - Cluster information")
        logger.info("  3. get_ocp_node_usage - Node resource usage (CPU, memory, cgroup)")
        logger.info("  4. get_ocp_node_pleg_latency - PLEG latency metrics")
        logger.info("  5. get_ocp_node_runtime_errors - Kubelet runtime operations errors")
        logger.info("")


def main():
    """Main function to run the MCP server"""
    async def run_server():
        try:
            # Perform startup initialization
            await startup_event()
            
            # Optional MCP Inspector launch
            enable_inspector = os.environ.get("ENABLE_MCP_INSPECTOR", "").lower() in ("1", "true", "yes", "on")
            host = "0.0.0.0"
            port = 8004

            if enable_inspector:
                def start_mcp_inspector(url: str):
                    try:
                        if shutil.which("npx") is None:
                            logger.warning("MCP Inspector requested but 'npx' not found")
                            return
                        inspector_cmd = ["npx", "--yes", "@modelcontextprotocol/inspector", url]
                        subprocess.Popen(inspector_cmd)
                        logger.info("Launched MCP Inspector for URL: %s", url)
                    except Exception as ie:
                        logger.warning("Failed to launch MCP Inspector: %s", ie)

                inspector_url = os.environ.get("MCP_INSPECTOR_URL", f"http://127.0.0.1:{port}/sse")
                start_mcp_inspector(inspector_url)

            # Run the server
            await mcp.run_async(
                transport="streamable-http",
                port=port,
                host=host,
                uvicorn_config={"ws": "none"}
            )
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            sys.exit(1)
    
    try:
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            logger.warning("Already running in an event loop. Creating new task.")
            return loop.create_task(run_server())
        else:
            new_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(new_loop)
                new_loop.run_until_complete(run_server())
            finally:
                new_loop.close()
                asyncio.set_event_loop(None)
            
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()