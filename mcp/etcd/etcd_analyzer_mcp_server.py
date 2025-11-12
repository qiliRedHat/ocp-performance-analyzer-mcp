#!/usr/bin/env python3
"""
OpenShift etcd Analyzer MCP Server
Main server implementation using FastMCP

Modular Architecture:
- etcd_analyzer_mcp_server.py (this file) - Main server and initialization
- mcp_tools/ - Individual tool implementations
  ├── __init__.py
  ├── models.py - Pydantic models
  ├── utils.py - Common utilities
  ├── health_check.py - Health status
  ├── cluster_info.py - OCP cluster info
  ├── cluster_status.py - etcd cluster status
  ├── node_usage.py - Node resource metrics
  ├── general_info.py - General etcd metrics
  ├── compact_defrag.py - Compaction/defrag metrics
  ├── wal_fsync.py - WAL fsync metrics
  ├── backend_commit.py - Backend commit metrics
  ├── network_io.py - Network I/O metrics
  ├── disk_io.py - Disk I/O metrics
  ├── etcd_performance_deep_drive.py - Deep drive analysis
  ├── bottleneck_analysis.py - Bottleneck detection
  └── etcd_performance_report.py - Performance reporting
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
mcp = FastMCP("OpenShift etcd Analyzer")

# ==================== Global Components ====================

# Global variables for collectors
auth_manager = None
config = None
cluster_collector = None
general_collector = None
compact_defrag_collector = None
wal_fsync_collector = None
backend_commit_collector = None
network_collector = None
disk_io_collector = None
cluster_info_collector = None
node_usage_collector = None

# ==================== Initialization ====================

async def initialize_collectors():
    """Initialize all collectors with authentication - each loads its own metrics"""
    global auth_manager, config, cluster_collector, general_collector, compact_defrag_collector
    global wal_fsync_collector, backend_commit_collector, network_collector, disk_io_collector
    global cluster_info_collector, node_usage_collector

    try:
        logger.info("="*70)
        logger.info("Initializing OpenShift etcd Analyzer components...")
        logger.info("="*70)
        
        # Initialize global config for shared access
        config = Config()
        metrics_etcd_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-etcd.yml')
        metrics_disk_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-disk.yml')
        
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
        from tools.etcd.etcd_cluster_status import ClusterStatCollector
        from tools.etcd.etcd_general_info import GeneralInfoCollector
        from tools.etcd.etcd_disk_compact_defrag import CompactDefragCollector
        from tools.etcd.etcd_disk_wal_fsync import DiskWALFsyncCollector
        from tools.etcd.etcd_disk_backend_commit import DiskBackendCommitCollector
        from tools.net.network_io import NetworkIOCollector
        from tools.disk.disk_io import DiskIOCollector
        from tools.ocp.cluster_info import ClusterInfoCollector
        from tools.node.node_usage import nodeUsageCollector
        
        # Cluster Status Collector
        logger.info("Initializing ClusterStatCollector...")
        cluster_collector = ClusterStatCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # General Info Collector
        logger.info("Initializing GeneralInfoCollector...")
        general_collector = GeneralInfoCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # Compact/Defrag Collector
        logger.info("Initializing CompactDefragCollector...")
        compact_defrag_collector = CompactDefragCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # WAL Fsync Collector
        logger.info("Initializing DiskWALFsyncCollector...")
        wal_fsync_collector = DiskWALFsyncCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # Backend Commit Collector
        logger.info("Initializing DiskBackendCommitCollector...")
        backend_commit_collector = DiskBackendCommitCollector(auth_manager, metrics_file_path=metrics_etcd_file)
        
        # Network IO Collector - uses metrics-net.yml
        logger.info("Initializing NetworkIOCollector...")
        metrics_net_file = os.path.join(PROJECT_ROOT, 'config', 'metrics-net.yml')
        network_config = Config()
        load_result = network_config.load_metrics_file(metrics_net_file)
        if not load_result.get('success'):
            logger.error(f"Failed to load network metrics file: {load_result.get('error', 'Unknown error')}")
            logger.warning("NetworkIOCollector will be initialized but may not have metrics")
        else:
            logger.info(f"Loaded {load_result.get('metrics_loaded', 0)} network metrics from {metrics_net_file}")
        
        network_collector = NetworkIOCollector(
            prometheus_url=auth_manager.prometheus_url,
            token=getattr(auth_manager, 'prometheus_token', None),
            config=network_config
        )
        await network_collector.initialize()
        logger.info("✅ NetworkIOCollector initialized")
        
        # Disk IO Collector - uses metrics-disk.yml
        logger.info("Initializing DiskIOCollector...")
        disk_io_collector = DiskIOCollector(auth_manager, duration="1h", metrics_file_path=metrics_disk_file)
        
        # Node Usage Collector
        logger.info("Initializing nodeUsageCollector...")
        prometheus_config = {
            'url': auth_manager.prometheus_url,
            'token': getattr(auth_manager, 'prometheus_token', None),
            'verify_ssl': False
        }
        node_usage_collector = nodeUsageCollector(auth_manager, prometheus_config)
        logger.info("✅ nodeUsageCollector initialized (uses node metrics)")
        
        # OCP Cluster Info Collector
        logger.info("Initializing ClusterInfoCollector...")
        cluster_info_collector = ClusterInfoCollector()
        await cluster_info_collector.initialize()
        logger.info("✅ ClusterInfoCollector initialized")
        
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
        'cluster_collector': cluster_collector,
        'general_collector': general_collector,
        'compact_defrag_collector': compact_defrag_collector,
        'wal_fsync_collector': wal_fsync_collector,
        'backend_commit_collector': backend_commit_collector,
        'network_collector': network_collector,
        'disk_io_collector': disk_io_collector,
        'cluster_info_collector': cluster_info_collector,
        'node_usage_collector': node_usage_collector
    }

# ==================== Import and Register MCP Tools ====================

from mcp_tools.health_check import register_health_check_tool
from mcp_tools.cluster_info import register_cluster_info_tool
from mcp_tools.cluster_status import register_cluster_status_tool
from mcp_tools.node_usage import register_node_usage_tool
from mcp_tools.general_info import register_general_info_tool
from mcp_tools.compact_defrag import register_compact_defrag_tool
from mcp_tools.wal_fsync import register_wal_fsync_tool
from mcp_tools.backend_commit import register_backend_commit_tool
from mcp_tools.network_io import register_network_io_tool
from mcp_tools.disk_io import register_disk_io_tool
from mcp_tools.etcd_performance_deep_drive import register_performance_deep_drive_tool
from mcp_tools.bottleneck_analysis import register_bottleneck_analysis_tool
from mcp_tools.etcd_performance_report import register_performance_report_tool

# Register all tools
logger.info("Registering MCP tools...")
register_health_check_tool(mcp, get_global_components)
register_cluster_info_tool(mcp, get_global_components)
register_cluster_status_tool(mcp, get_global_components)
register_node_usage_tool(mcp, get_global_components)
register_general_info_tool(mcp, get_global_components)
register_compact_defrag_tool(mcp, get_global_components)
register_wal_fsync_tool(mcp, get_global_components)
register_backend_commit_tool(mcp, get_global_components)
register_network_io_tool(mcp, get_global_components)
register_disk_io_tool(mcp, get_global_components)
register_performance_deep_drive_tool(mcp, get_global_components)
register_bottleneck_analysis_tool(mcp, get_global_components)
register_performance_report_tool(mcp, get_global_components)
logger.info("✅ All MCP tools registered")

# ==================== Startup and Main ====================

async def startup_event():
    """Startup event handler"""
    logger.info("")
    logger.info("🚀 Starting OpenShift etcd Analyzer MCP Server...")
    logger.info("")
    
    # Initialize collectors (includes metrics loading with logging)
    init_success = await initialize_collectors()
    if not init_success:
        logger.error("❌ Failed to initialize collectors. Server may not function properly.")
        logger.error("")
    else:
        logger.info("✅ OpenShift etcd Analyzer MCP Server started successfully!")
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
            port = 8001

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