#!/usr/bin/env python3
"""
OpenShift Cluster Information Collector - Optimized Version
Key optimizations:
- Concurrent API calls for all resources
- Streamlined pagination with limits
- Aggressive timeouts
- Minimal retry logic
"""

import json
import logging
import asyncio
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from ocauth.openshift_auth import OpenShiftAuth

logger = logging.getLogger(__name__)

# Performance tuning constants
API_TIMEOUT = 10  # seconds per API call
PAGINATION_LIMIT = 1000  # items per page
MAX_CONCURRENT_CALLS = 10  # max parallel API calls


@dataclass
class NodeInfo:
    """Node information structure"""
    name: str
    node_type: str
    cpu_capacity: str
    memory_capacity: str
    architecture: str
    kernel_version: str
    container_runtime: str
    kubelet_version: str
    os_image: str
    ready_status: str
    schedulable: bool
    creation_timestamp: str


@dataclass
class ClusterInfo:
    """Complete cluster information structure"""
    cluster_name: str
    cluster_version: str
    platform: str
    api_server_url: str
    total_nodes: int
    master_nodes: List[NodeInfo]
    infra_nodes: List[NodeInfo]
    worker_nodes: List[NodeInfo]
    namespaces_count: int
    pods_count: int
    services_count: int
    secrets_count: int
    configmaps_count: int
    networkpolicies_count: int
    adminnetworkpolicies_count: int
    baselineadminnetworkpolicies_count: int
    egressfirewalls_count: int
    egressips_count: int
    clusteruserdefinednetworks_count: int
    userdefinednetworks_count: int
    unavailable_cluster_operators: List[str]
    mcp_status: Dict[str, str]
    collection_timestamp: str


class ClusterInfoCollector:
    """Collects comprehensive OpenShift cluster information and status"""

    def __init__(self, kubeconfig_path: Optional[str] = None):
        self.kubeconfig_path = kubeconfig_path
        self.auth_manager = None
        self.k8s_client = None

    async def initialize(self):
        """Initialize the collector with authentication"""
        self.auth_manager = OpenShiftAuth(self.kubeconfig_path)
        await self.auth_manager.initialize()
        self.k8s_client = self.auth_manager.kube_client

    async def collect_cluster_info(self) -> ClusterInfo:
        """Collect all cluster information with parallel execution"""
        try:
            if not self.k8s_client:
                await self.initialize()

            logger.info("Starting optimized cluster information collection...")
            start_time = asyncio.get_event_loop().time()

            # Collect all data concurrently
            results = await asyncio.gather(
                self._get_cluster_name(),
                self._get_cluster_version(),
                self._get_infrastructure_platform(),
                self._collect_nodes_info(),
                self._collect_resource_counts(),
                self._get_unavailable_cluster_operators(),
                self._get_mcp_status(),
                return_exceptions=True
            )

            # Unpack results with error handling
            cluster_name = results[0] if not isinstance(results[0], Exception) else "unknown"
            cluster_version = results[1] if not isinstance(results[1], Exception) else "unknown"
            platform = results[2] if not isinstance(results[2], Exception) else "unknown"
            nodes_info = results[3] if not isinstance(results[3], Exception) else {
                "all_nodes": [], "master_nodes": [], "infra_nodes": [], "worker_nodes": []
            }
            resource_counts = results[4] if not isinstance(results[4], Exception) else {}
            unavailable_operators = results[5] if not isinstance(results[5], Exception) else []
            mcp_status = results[6] if not isinstance(results[6], Exception) else {}

            # Log any errors
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.warning(f"Task {i} failed: {result}")

            api_server_url = getattr(getattr(self.k8s_client, "configuration", None), "host", "") or ""

            cluster_info = ClusterInfo(
                cluster_name=cluster_name,
                cluster_version=cluster_version,
                platform=platform,
                api_server_url=api_server_url,
                total_nodes=len(nodes_info["all_nodes"]),
                master_nodes=nodes_info["master_nodes"],
                infra_nodes=nodes_info["infra_nodes"],
                worker_nodes=nodes_info["worker_nodes"],
                namespaces_count=resource_counts.get("namespaces", 0),
                pods_count=resource_counts.get("pods", 0),
                services_count=resource_counts.get("services", 0),
                secrets_count=resource_counts.get("secrets", 0),
                configmaps_count=resource_counts.get("configmaps", 0),
                networkpolicies_count=resource_counts.get("networkpolicies", 0),
                adminnetworkpolicies_count=resource_counts.get("adminnetworkpolicies", 0),
                baselineadminnetworkpolicies_count=resource_counts.get("baselineadminnetworkpolicies", 0),
                egressfirewalls_count=resource_counts.get("egressfirewalls", 0),
                egressips_count=resource_counts.get("egressips", 0),
                clusteruserdefinednetworks_count=resource_counts.get("clusteruserdefinednetworks", 0),
                userdefinednetworks_count=resource_counts.get("userdefinednetworks", 0),
                unavailable_cluster_operators=unavailable_operators,
                mcp_status=mcp_status,
                collection_timestamp=datetime.now(timezone.utc).isoformat(),
            )

            elapsed = asyncio.get_event_loop().time() - start_time
            logger.info(f"Cluster information collection completed in {elapsed:.2f}s")
            return cluster_info

        except Exception as e:
            logger.error(f"Failed to collect cluster info: {e}")
            raise

    async def _get_cluster_name(self) -> str:
        """Get cluster name from infrastructure config"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            infrastructure = await asyncio.wait_for(
                asyncio.to_thread(
                    custom_api.get_cluster_custom_object,
                    group="config.openshift.io",
                    version="v1",
                    plural="infrastructures",
                    name="cluster"
                ),
                timeout=API_TIMEOUT
            )
            return infrastructure.get("status", {}).get("infrastructureName", "unknown")
        except Exception as e:
            logger.warning(f"Could not get cluster name: {e}")
            return "unknown"

    async def _get_cluster_version(self) -> str:
        """Get OpenShift cluster version"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            cluster_version = await asyncio.wait_for(
                asyncio.to_thread(
                    custom_api.get_cluster_custom_object,
                    group="config.openshift.io",
                    version="v1",
                    plural="clusterversions",
                    name="version"
                ),
                timeout=API_TIMEOUT
            )
            history = cluster_version.get("status", {}).get("history", [])
            if history:
                return history[0].get("version", "unknown")
        except Exception as e:
            logger.warning(f"Could not get cluster version: {e}")
        return "unknown"

    async def _get_infrastructure_platform(self) -> str:
        """Get infrastructure platform"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            infrastructure = await asyncio.wait_for(
                asyncio.to_thread(
                    custom_api.get_cluster_custom_object,
                    group="config.openshift.io",
                    version="v1",
                    plural="infrastructures",
                    name="cluster"
                ),
                timeout=API_TIMEOUT
            )
            return infrastructure.get("status", {}).get("platform", "unknown")
        except Exception as e:
            logger.warning(f"Could not get infrastructure platform: {e}")
            return "unknown"

    async def _collect_nodes_info(self) -> Dict[str, List[NodeInfo]]:
        """Collect detailed information about all nodes"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            nodes = await asyncio.wait_for(
                asyncio.to_thread(v1.list_node),
                timeout=API_TIMEOUT
            )

            master_nodes = []
            infra_nodes = []
            worker_nodes = []
            all_nodes = []

            for node in nodes.items:
                node_info = self._parse_node_info(node)
                all_nodes.append(node_info)

                labels = node.metadata.labels or {}
                if labels.get("node-role.kubernetes.io/master") == "" or \
                   labels.get("node-role.kubernetes.io/control-plane") == "":
                    node_info.node_type = "master"
                    master_nodes.append(node_info)
                elif labels.get("node-role.kubernetes.io/infra") == "":
                    node_info.node_type = "infra"
                    infra_nodes.append(node_info)
                else:
                    node_info.node_type = "worker"
                    worker_nodes.append(node_info)

            return {
                "all_nodes": all_nodes,
                "master_nodes": master_nodes,
                "infra_nodes": infra_nodes,
                "worker_nodes": worker_nodes,
            }
        except Exception as e:
            logger.error(f"Failed to collect nodes info: {e}")
            return {"all_nodes": [], "master_nodes": [], "infra_nodes": [], "worker_nodes": []}

    def _parse_node_info(self, node) -> NodeInfo:
        """Parse Kubernetes node object into NodeInfo"""
        status = node.status
        spec = node.spec
        metadata = node.metadata

        ready_status = False
        for condition in status.conditions or []:
            if condition.type == "Ready" and condition.status == "True":
                ready_status = True
                break

        schedulable = not spec.unschedulable if spec.unschedulable is not None else True

        if ready_status and schedulable:
            status_str = 'Ready'
        elif ready_status and not schedulable:
            status_str = 'Ready,SchedulingDisabled'
        elif not ready_status and schedulable:
            status_str = 'NotReady'
        elif not ready_status and not schedulable:
            status_str = 'NotReady,SchedulingDisabled'
        else:
            status_str = 'Unknown'

        return NodeInfo(
            name=metadata.name,
            node_type="unknown",
            cpu_capacity=status.capacity.get("cpu", "unknown"),
            memory_capacity=status.capacity.get("memory", "unknown"),
            architecture=status.node_info.architecture,
            kernel_version=status.node_info.kernel_version,
            container_runtime=status.node_info.container_runtime_version,
            kubelet_version=status.node_info.kubelet_version,
            os_image=status.node_info.os_image,
            ready_status=status_str,
            schedulable=schedulable,
            creation_timestamp=metadata.creation_timestamp.isoformat() if metadata.creation_timestamp else "",
        )

    async def _collect_resource_counts(self) -> Dict[str, int]:
        """Collect counts of various cluster resources concurrently"""
        # Create all count tasks
        tasks = [
            self._count_namespaces(),
            self._count_pods(),
            self._count_services(),
            self._count_secrets(),
            self._count_configmaps(),
            self._count_networkpolicies(),
            self._count_custom_resource("policy.networking.k8s.io", "v1alpha1", "adminnetworkpolicies"),
            self._count_custom_resource("policy.networking.k8s.io", "v1alpha1", "baselineadminnetworkpolicies"),
            self._count_custom_resource("k8s.ovn.org", "v1", "egressfirewalls"),
            self._count_custom_resource("k8s.ovn.org", "v1", "egressips"),
            self._count_custom_resource("k8s.ovn.org", "v1", "clusteruserdefinednetworks"),
            self._count_custom_resource("k8s.ovn.org", "v1", "userdefinednetworks"),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        return {
            "namespaces": results[0] if not isinstance(results[0], Exception) else 0,
            "pods": results[1] if not isinstance(results[1], Exception) else 0,
            "services": results[2] if not isinstance(results[2], Exception) else 0,
            "secrets": results[3] if not isinstance(results[3], Exception) else 0,
            "configmaps": results[4] if not isinstance(results[4], Exception) else 0,
            "networkpolicies": results[5] if not isinstance(results[5], Exception) else 0,
            "adminnetworkpolicies": results[6] if not isinstance(results[6], Exception) else 0,
            "baselineadminnetworkpolicies": results[7] if not isinstance(results[7], Exception) else 0,
            "egressfirewalls": results[8] if not isinstance(results[8], Exception) else 0,
            "egressips": results[9] if not isinstance(results[9], Exception) else 0,
            "clusteruserdefinednetworks": results[10] if not isinstance(results[10], Exception) else 0,
            "userdefinednetworks": results[11] if not isinstance(results[11], Exception) else 0,
        }

    async def _count_namespaces(self) -> int:
        """Count namespaces"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            result = await asyncio.wait_for(
                asyncio.to_thread(v1.list_namespace),
                timeout=API_TIMEOUT
            )
            return len(result.items)
        except Exception as e:
            logger.warning(f"Could not count namespaces: {e}")
            return 0

    async def _count_pods(self) -> int:
        """Count pods with optimized pagination"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            total = 0
            continue_token = None
            
            while True:
                result = await asyncio.wait_for(
                    asyncio.to_thread(
                        v1.list_pod_for_all_namespaces,
                        limit=PAGINATION_LIMIT,
                        _continue=continue_token
                    ),
                    timeout=API_TIMEOUT
                )
                total += len(result.items)
                
                if hasattr(result.metadata, '_continue') and result.metadata._continue:
                    continue_token = result.metadata._continue
                else:
                    break
            
            return total
        except Exception as e:
            logger.warning(f"Could not count pods: {e}")
            return 0

    async def _count_services(self) -> int:
        """Count services with optimized pagination"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            total = 0
            continue_token = None
            
            while True:
                result = await asyncio.wait_for(
                    asyncio.to_thread(
                        v1.list_service_for_all_namespaces,
                        limit=PAGINATION_LIMIT,
                        _continue=continue_token
                    ),
                    timeout=API_TIMEOUT
                )
                total += len(result.items)
                
                if hasattr(result.metadata, '_continue') and result.metadata._continue:
                    continue_token = result.metadata._continue
                else:
                    break
            
            return total
        except Exception as e:
            logger.warning(f"Could not count services: {e}")
            return 0

    async def _count_secrets(self) -> int:
        """Count secrets with single-attempt pagination"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            total = 0
            continue_token = None
            
            # Limit iterations to prevent long-running queries
            max_iterations = 10
            iteration = 0
            
            while iteration < max_iterations:
                result = await asyncio.wait_for(
                    asyncio.to_thread(
                        v1.list_secret_for_all_namespaces,
                        limit=PAGINATION_LIMIT,
                        _continue=continue_token
                    ),
                    timeout=API_TIMEOUT
                )
                total += len(result.items)
                iteration += 1
                
                if hasattr(result.metadata, '_continue') and result.metadata._continue:
                    continue_token = result.metadata._continue
                else:
                    break
            
            return total
        except Exception as e:
            logger.warning(f"Could not count secrets: {e}")
            return 0

    async def _count_configmaps(self) -> int:
        """Count configmaps with optimized pagination"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            total = 0
            continue_token = None
            
            while True:
                result = await asyncio.wait_for(
                    asyncio.to_thread(
                        v1.list_config_map_for_all_namespaces,
                        limit=PAGINATION_LIMIT,
                        _continue=continue_token
                    ),
                    timeout=API_TIMEOUT
                )
                total += len(result.items)
                
                if hasattr(result.metadata, '_continue') and result.metadata._continue:
                    continue_token = result.metadata._continue
                else:
                    break
            
            return total
        except Exception as e:
            logger.warning(f"Could not count configmaps: {e}")
            return 0

    async def _count_networkpolicies(self) -> int:
        """Count network policies with optimized pagination"""
        try:
            networking_v1 = client.NetworkingV1Api(self.k8s_client)
            total = 0
            continue_token = None
            
            while True:
                result = await asyncio.wait_for(
                    asyncio.to_thread(
                        networking_v1.list_network_policy_for_all_namespaces,
                        limit=PAGINATION_LIMIT,
                        _continue=continue_token
                    ),
                    timeout=API_TIMEOUT
                )
                total += len(result.items)
                
                if hasattr(result.metadata, '_continue') and result.metadata._continue:
                    continue_token = result.metadata._continue
                else:
                    break
            
            return total
        except Exception as e:
            logger.warning(f"Could not count network policies: {e}")
            return 0

    async def _count_custom_resource(self, group: str, version: str, plural: str) -> int:
        """Count custom resources"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    custom_api.list_cluster_custom_object,
                    group=group,
                    version=version,
                    plural=plural
                ),
                timeout=API_TIMEOUT
            )
            return len(result.get("items", []))
        except Exception as e:
            logger.debug(f"Could not count {plural}: {e}")
            return 0

    async def _get_unavailable_cluster_operators(self) -> List[str]:
        """Get list of unavailable cluster operators"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            operators = await asyncio.wait_for(
                asyncio.to_thread(
                    custom_api.list_cluster_custom_object,
                    group="config.openshift.io",
                    version="v1",
                    plural="clusteroperators"
                ),
                timeout=API_TIMEOUT
            )

            unavailable_operators = []
            for operator in operators.get("items", []):
                name = operator["metadata"]["name"]
                conditions = operator.get("status", {}).get("conditions", [])

                is_available = any(
                    condition["type"] == "Available" and condition["status"] == "True"
                    for condition in conditions
                )

                if not is_available:
                    unavailable_operators.append(name)

            return unavailable_operators
        except Exception as e:
            logger.warning(f"Could not get cluster operators status: {e}")
            return []

    async def _get_mcp_status(self) -> Dict[str, str]:
        """Get machine config pool status"""
        try:
            custom_api = client.CustomObjectsApi(self.k8s_client)
            mcp_response = await asyncio.wait_for(
                asyncio.to_thread(
                    custom_api.list_cluster_custom_object,
                    group="machineconfiguration.openshift.io",
                    version="v1",
                    plural="machineconfigpools"
                ),
                timeout=API_TIMEOUT
            )

            mcp_status = {}
            for mcp in mcp_response.get('items', []):
                pool_name = mcp.get('metadata', {}).get('name', 'unknown')
                conditions = mcp.get('status', {}).get('conditions', [])

                updated = any(c.get('type') == 'Updated' and c.get('status') == 'True' for c in conditions)
                updating = any(c.get('type') == 'Updating' and c.get('status') == 'True' for c in conditions)
                degraded = any(c.get('type') == 'Degraded' and c.get('status') == 'True' for c in conditions)

                if degraded:
                    status = 'Degraded'
                elif updating:
                    status = 'Updating'
                elif updated:
                    status = 'Updated'
                else:
                    status = 'Unknown'

                mcp_status[pool_name] = status

            return mcp_status
        except Exception as e:
            logger.warning(f"Could not get machine config pool status: {e}")
            return {}

    def to_json(self, cluster_info: ClusterInfo) -> str:
        """Convert cluster info to JSON string"""
        return json.dumps(asdict(cluster_info), indent=2, default=str)

    def to_dict(self, cluster_info: ClusterInfo) -> Dict[str, Any]:
        """Convert cluster info to dictionary"""
        return asdict(cluster_info)


# Backward compatibility
OpenShiftGeneralInfo = ClusterInfoCollector


async def collect_cluster_information(kubeconfig_path: Optional[str] = None) -> Dict[str, Any]:
    """Collect and return cluster information as dictionary"""
    collector = ClusterInfoCollector(kubeconfig_path)
    cluster_info = await collector.collect_cluster_info()
    return collector.to_dict(cluster_info)


async def get_cluster_info_json(kubeconfig_path: Optional[str] = None) -> str:
    """Collect and return cluster information as JSON string"""
    collector = ClusterInfoCollector(kubeconfig_path)
    cluster_info = await collector.collect_cluster_info()
    return collector.to_json(cluster_info)


if __name__ == "__main__":
    async def main():
        try:
            collector = ClusterInfoCollector()
            cluster_info = await collector.collect_cluster_info()
            print(collector.to_json(cluster_info))
        except Exception as e:
            print(f"Error: {e}")

    asyncio.run(main())