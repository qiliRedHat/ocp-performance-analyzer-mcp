"""
Common Pydantic models for MCP tools (Network, OVN-Kubernetes, etcd)
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


# ============================================================================
# BASE MODELS
# ============================================================================

class MCPBaseModel(BaseModel):
    """Base model with common configuration"""
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


# ============================================================================
# REQUEST MODELS
# ============================================================================

class DurationInput(MCPBaseModel):
    """Basic duration input for simple queries"""
    duration: str = Field(
        default="1h", 
        description="Time duration for metrics collection (e.g., '5m', '1h', '6h')"
    )


class TimeRangeInput(MCPBaseModel):
    """Time range input with optional start/end times"""
    start_time: Optional[str] = Field(
        default=None, 
        description="Start time in ISO format"
    )
    end_time: Optional[str] = Field(
        default=None, 
        description="End time in ISO format"
    )
    duration: Optional[str] = Field(
        default="1h", 
        description="Duration if start/end not provided"
    )


class NetworkL1Request(MCPBaseModel):
    """Network Layer 1 metrics request"""
    duration: str = Field(default="5m", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    metric_name: Optional[str] = Field(default=None, description="Specific metric name")
    timeout_seconds: Optional[int] = Field(default=120, ge=10, le=600)


class NetworkIORequest(MCPBaseModel):
    """Network I/O metrics request"""
    duration: str = Field(default="5m", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    include_metrics: Optional[List[str]] = Field(default=None, description="Filter specific metrics")
    node_groups: Optional[List[str]] = Field(default=None, description="Filter by node groups")


class NetworkSocketTCPRequest(MCPBaseModel):
    """Network socket TCP metrics request"""
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None)
    end_time: Optional[str] = Field(default=None)
    include_all_workers: bool = Field(default=False)
    metric_filter: Optional[List[str]] = Field(default=None)


class NetworkSocketUDPRequest(MCPBaseModel):
    """Network socket UDP metrics request"""
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None)
    end_time: Optional[str] = Field(default=None)
    include_all_workers: bool = Field(default=False)


class NetworkSocketMemRequest(MCPBaseModel):
    """Network socket memory metrics request"""
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None)
    end_time: Optional[str] = Field(default=None)
    include_statistics: bool = Field(default=True)
    worker_top_n: int = Field(default=3, ge=1, le=10)
    include_all_roles: bool = Field(default=True)
    metric_filter: Optional[List[str]] = Field(default=None)


class NetworkSocketSoftnetRequest(MCPBaseModel):
    """Network socket softnet metrics request"""
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None)
    end_time: Optional[str] = Field(default=None)
    step: str = Field(default="15s")
    include_summary: bool = Field(default=True)


class NetworkSocketIPRequest(MCPBaseModel):
    """Network socket IP metrics request"""
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None)
    end_time: Optional[str] = Field(default=None)
    step: str = Field(default="15s")


class NetStatTCPRequest(MCPBaseModel):
    """Network statistics TCP request"""
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None)
    end_time: Optional[str] = Field(default=None)
    step: str = Field(default="15s")


class NetStatUDPRequest(MCPBaseModel):
    """Network statistics UDP request"""
    duration: str = Field(default="1h", description="Time duration for metrics collection")
    start_time: Optional[str] = Field(default=None)
    end_time: Optional[str] = Field(default=None)


class ClusterInfoRequest(MCPBaseModel):
    """Cluster information request with detailed options"""
    include_node_details: bool = Field(
        default=True, 
        description="Include detailed node information"
    )
    include_resource_counts: bool = Field(
        default=True, 
        description="Include resource counts"
    )
    include_network_policies: bool = Field(
        default=True, 
        description="Include network policy information"
    )
    include_operator_status: bool = Field(
        default=True, 
        description="Include cluster operator status"
    )
    include_mcp_status: bool = Field(
        default=True, 
        description="Include Machine Config Pool status"
    )


class OVNKPodMetricsRequest(MCPBaseModel):
    """OVN-Kubernetes pod metrics request"""
    duration: str = Field(default="1h", description="Query duration")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")
    include_node: bool = Field(default=True, description="Include ovnkube-node metrics")


class MultusPodMetricsRequest(MCPBaseModel):
    """Multus pod metrics request"""
    duration: str = Field(default="1h", description="Query duration")
    start_time: Optional[str] = Field(default=None, description="Start time in ISO format")
    end_time: Optional[str] = Field(default=None, description="End time in ISO format")


class DeepDriveInput(MCPBaseModel):
    """Deep drive analysis input"""
    duration: Optional[str] = Field(
        default="1h", 
        description="Time range for metrics collection"
    )


class PerformanceReportInput(MCPBaseModel):
    """Performance report input"""
    duration: Optional[str] = Field(
        default="1h", 
        description="Time range for metrics collection"
    )
    test_id: Optional[str] = Field(
        default=None, 
        description="Optional test identifier"
    )


class HealthCheckRequest(MCPBaseModel):
    """Health check request (no parameters needed)"""
    pass


class OCPClusterInfoRequest(MCPBaseModel):
    """OpenShift cluster information request (no parameters needed)"""
    pass


# ============================================================================
# RESPONSE MODELS
# ============================================================================

class HealthCheckResponse(MCPBaseModel):
    """Health check response"""
    status: str
    timestamp: str
    mcp_server: Dict[str, Any]
    prometheus: Dict[str, Any]
    kubeapi: Dict[str, Any]


class ServerHealthResponse(MCPBaseModel):
    """Server health response with collector status"""
    status: str
    timestamp: str
    collectors_initialized: bool
    details: Dict[str, bool]


class NetworkMetricsResponse(MCPBaseModel):
    """Standard response for network metrics"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: Optional[str] = None
    duration: Optional[str] = None


class MetricResponse(MCPBaseModel):
    """Standard response for generic metrics"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: Optional[str] = None
    duration: Optional[str] = None


class OCPClusterInfoResponse(MCPBaseModel):
    """OpenShift cluster information response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str


class ETCDClusterStatusResponse(MCPBaseModel):
    """etcd cluster status response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str


class ETCDMetricsResponse(MCPBaseModel):
    """etcd metrics response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: Optional[str] = None
    duration: Optional[str] = None


class ETCDNodeUsageResponse(MCPBaseModel):
    """etcd node usage response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="node_usage")
    duration: str


class ETCDGeneralInfoResponse(MCPBaseModel):
    """etcd general information response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="general_info")
    duration: str


class ETCDCompactDefragResponse(MCPBaseModel):
    """etcd compact and defrag response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="disk_compact_defrag")
    duration: str


class ETCDWALFsyncResponse(MCPBaseModel):
    """etcd WAL fsync response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="disk_wal_fsync")
    duration: str


class ETCDBackendCommitResponse(MCPBaseModel):
    """etcd backend commit response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="disk_backend_commit")
    duration: str


class NodeDiskIOResponse(MCPBaseModel):
    """Node disk I/O response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="disk_io")
    duration: str


class ETCDPerformanceDeepDriveResponse(MCPBaseModel):
    """etcd performance deep drive response"""
    status: str
    data: Optional[Dict[str, Any]] = None
    analysis: Optional[Dict[str, Any]] = None
    summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str
    category: str = Field(default="performance_deep_drive")
    duration: str
    test_id: Optional[str] = None


class ETCDBottleneckAnalysisResponse(MCPBaseModel):
    """etcd bottleneck analysis response"""
    status: str
    bottleneck_analysis: Optional[Dict[str, Any]] = None
    root_cause_analysis: Optional[List[Dict[str, Any]]] = None
    performance_recommendations: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    timestamp: str
    duration: str
    test_id: Optional[str] = None


class ETCDPerformanceReportResponse(MCPBaseModel):
    """etcd performance report response"""
    status: str
    analysis_results: Optional[Dict[str, Any]] = None
    performance_report: Optional[str] = None
    error: Optional[str] = None
    timestamp: str
    duration: str
    test_id: Optional[str] = None