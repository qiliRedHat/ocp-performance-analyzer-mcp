"""
OpenShift Authentication Module - Optimized Version
Key optimizations:
- Concurrent token attempts
- Fast-fail route discovery
- Service discovery caching
- Reduced timeout values
"""

import os
import logging
import json
from typing import Optional, Dict, Any
import shlex
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import asyncio
import ssl

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.MaxRetryError)
urllib3_logger = logging.getLogger('urllib3.connectionpool')
urllib3_logger.setLevel(logging.ERROR)
import requests

# Performance tuning
DISCOVERY_TIMEOUT = 5  # seconds per discovery attempt
TOKEN_TIMEOUT = 3  # seconds per token creation attempt


class OpenShiftAuth:
    """OpenShift authentication and service discovery - Optimized"""
    
    def __init__(self, kubeconfig_path: Optional[str] = None):
        self.k8s_client = None
        self.prometheus_url = None
        self.token = None
        self.ca_cert_path = None
        self.logger = logging.getLogger(__name__)
        self.kubeconfig_path: Optional[str] = kubeconfig_path
        self.prometheus_info: Optional[Dict[str, Any]] = None
        self.k8s_verify_ssl = None
        self.k8s_ca_cert_path: Optional[str] = None
        self.cluster_info: Dict[str, Any] = {}
        
    async def initialize(self) -> bool:
        """Initialize Kubernetes client and discover services"""
        try:
            # Clear potentially broken CA bundles
            original_ca_bundle = os.environ.pop('REQUESTS_CA_BUNDLE', None)
            original_curl_bundle = os.environ.pop('CURL_CA_BUNDLE', None)

            # Load kubeconfig
            if self.kubeconfig_path:
                self.logger.info(f"Loading kubeconfig from path: {self.kubeconfig_path}")
                config.load_kube_config(config_file=self.kubeconfig_path)
            else:
                config.load_kube_config()

            # Configure SSL verification
            def _env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
                val = os.getenv(name)
                if val is None:
                    return default
                val_l = val.strip().lower()
                if val_l in ("true", "1", "yes"): return True
                if val_l in ("false", "0", "no"): return False
                return default

            self.k8s_verify_ssl = (
                _env_bool('K8S_VERIFY', None)
                if _env_bool('K8S_VERIFY', None) is not None
                else _env_bool('OCP_API_VERIFY', None)
            )

            k8s_conf = client.Configuration.get_default_copy()
            if self.k8s_verify_ssl is not None:
                k8s_conf.verify_ssl = self.k8s_verify_ssl
                if not self.k8s_verify_ssl:
                    k8s_conf.assert_hostname = False
                    k8s_conf.ssl_ca_cert = None
                self.logger.info(f"Kubernetes API TLS verify: {self.k8s_verify_ssl}")

            # Validate CA cert
            if getattr(k8s_conf, 'ssl_ca_cert', None):
                ca_path = k8s_conf.ssl_ca_cert
                if os.path.isfile(ca_path):
                    try:
                        with open(ca_path, 'r') as f:
                            content = f.read()
                            if 'BEGIN CERTIFICATE' in content:
                                self.k8s_ca_cert_path = ca_path
                                self.logger.info(f"Using valid CA cert: {ca_path}")
                    except Exception as e:
                        self.logger.warning(f"CA cert validation failed: {e}")

            env_ca = os.getenv('K8S_CA_CERT')
            if env_ca and os.path.isfile(env_ca):
                self.k8s_ca_cert_path = env_ca

            prom_ca_env = os.getenv('PROMETHEUS_CA_CERT')
            if prom_ca_env and os.path.isfile(prom_ca_env):
                self.ca_cert_path = prom_ca_env
            elif self.k8s_ca_cert_path and not self.ca_cert_path:
                self.ca_cert_path = self.k8s_ca_cert_path

            self.k8s_client = client.ApiClient(configuration=k8s_conf)
            
            # Proactive API connectivity check
            try:
                ca_from_conf = getattr(k8s_conf, 'ssl_ca_cert', None)
                if ca_from_conf and self.k8s_verify_ssl is not True:
                    if not self._is_ca_file_valid(ca_from_conf):
                        self.logger.warning(f"Invalid CA cert detected, disabling TLS verification")
                        k8s_conf_fallback = client.Configuration.get_default_copy()
                        k8s_conf_fallback.verify_ssl = False
                        k8s_conf_fallback.assert_hostname = False
                        k8s_conf_fallback.ssl_ca_cert = None
                        self.k8s_client = client.ApiClient(configuration=k8s_conf_fallback)
                        self.k8s_verify_ssl = False
                
                self._ensure_k8s_api_connectivity()
            except Exception:
                raise
            
            # Get auth and discover Prometheus concurrently
            auth_task = asyncio.create_task(self._get_auth_details())
            prom_task = asyncio.create_task(self._discover_prometheus())
            
            await auth_task
            prometheus_info = await prom_task
            
            if prometheus_info:
                self.prometheus_info = prometheus_info
                self.prometheus_url = prometheus_info['url']
                self.logger.info(f"Discovered Prometheus: {self.prometheus_url}")
                return True
            else:
                self.logger.error("Failed to discover Prometheus service")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to initialize OCP authentication: {e}")
            return False

    @property
    def kube_client(self):
        """Backward compatible alias for k8s_client."""
        return self.k8s_client

    @property
    def prometheus_token(self) -> Optional[str]:
        """Backward compatible alias."""
        return self.token

    def _is_ca_file_valid(self, ca_path: str) -> bool:
        """Validate CA bundle"""
        try:
            if not os.path.isfile(ca_path):
                return False
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.load_verify_locations(cafile=ca_path)
            return True
        except Exception:
            return False

    def _ensure_k8s_api_connectivity(self) -> None:
        """Probe Kubernetes API and auto-recover from SSL issues"""
        try:
            version_api = client.VersionApi(self.k8s_client)
            _ = version_api.get_code()
            self.logger.info("Kubernetes API connectivity verified")
            return
        except Exception as probe_err:
            err_text = str(probe_err)
            ssl_error_indicators = (
                "PEM lib", "CERTIFICATE_VERIFY_FAILED", "SSLError",
                "certificate verify failed", "ssl.c:", "[X509]",
            )
            if not any(ind in err_text for ind in ssl_error_indicators):
                self.logger.error(f"Kubernetes API check failed: {err_text}")
                raise

            self.logger.warning("SSL verification failed, disabling TLS verification")

            k8s_conf_fallback = client.Configuration.get_default_copy()
            k8s_conf_fallback.verify_ssl = False
            k8s_conf_fallback.assert_hostname = False
            k8s_conf_fallback.ssl_ca_cert = None
            self.k8s_client = client.ApiClient(configuration=k8s_conf_fallback)
            self.k8s_verify_ssl = False

            try:
                version_api = client.VersionApi(self.k8s_client)
                _ = version_api.get_code()
                self.logger.info("Kubernetes API verified (TLS disabled)")
            except Exception as fallback_err:
                self.logger.error(f"API connectivity failed: {fallback_err}")
                raise
    
    async def _get_auth_details(self):
        """Extract authentication details with concurrent attempts"""
        # Try all token methods concurrently
        tasks = [
            self._create_prometheus_sa_token(),
            self._try_oc_whoami_token(),
            self._try_service_account_token()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Use first successful token
        for result in results:
            if result and not isinstance(result, Exception) and len(result) > 10:
                self.token = result
                self.logger.info(f"Token acquired (length: {len(self.token)})")
                return
        
        self.logger.warning("No authentication token found")

    async def _try_oc_whoami_token(self) -> Optional[str]:
        """Try to get token via 'oc whoami -t'"""
        try:
            proc = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    'oc', 'whoami', '-t',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                ),
                timeout=TOKEN_TIMEOUT
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=TOKEN_TIMEOUT)
            if proc.returncode == 0:
                token = stdout.decode().strip()
                if token and len(token) > 10:
                    return token
        except Exception:
            pass
        return None

    async def _try_service_account_token(self) -> Optional[str]:
        """Try to read service account token"""
        try:
            sa_token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
            if os.path.exists(sa_token_path):
                with open(sa_token_path, 'r') as f:
                    return f.read().strip()
        except Exception:
            pass
        return None
    
    async def _create_prometheus_sa_token(self) -> Optional[str]:
        """Create service account token with concurrent attempts"""
        sa_configs = [
            {'namespace': 'openshift-monitoring', 'name': 'prometheus-k8s'},
            {'namespace': 'openshift-user-workload-monitoring', 'name': 'prometheus-k8s'},
        ]
        
        tasks = []
        for sa_config in sa_configs:
            namespace = sa_config['namespace']
            sa_name = sa_config['name']
            tasks.append(self._try_oc_create_token(namespace, sa_name))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if result and not isinstance(result, Exception):
                return result
        
        return None
    
    async def _try_oc_create_token(self, namespace: str, sa_name: str) -> Optional[str]:
        """Try to create token using 'oc create token'"""
        try:
            env = os.environ.copy()
            env.pop('REQUESTS_CA_BUNDLE', None)
            
            cmd = ['oc', 'create', 'token', sa_name, '-n', namespace, '--duration', '24h']
            
            proc = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env
                ),
                timeout=TOKEN_TIMEOUT
            )
            
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=TOKEN_TIMEOUT)
            
            if proc.returncode == 0:
                token = stdout.decode().strip()
                if token and len(token) > 20:
                    return token
        except Exception:
            pass
        return None
    
    async def _discover_prometheus(self) -> Optional[Dict[str, Any]]:
        """Discover Prometheus with fast-fail strategy"""
        try:
            monitoring_namespaces = [
                'openshift-monitoring',
                'openshift-user-workload-monitoring',
            ]
            
            # Try route discovery in parallel across namespaces
            route_tasks = [
                self._find_prometheus_route(ns) for ns in monitoring_namespaces
            ]
            
            route_results = await asyncio.gather(*route_tasks, return_exceptions=True)
            
            for route_info in route_results:
                if route_info and not isinstance(route_info, Exception):
                    return {
                        'url': route_info['url'],
                        'namespace': route_info.get('namespace', 'unknown'),
                        'access_method': 'route',
                        'route_host': route_info['host'],
                        'tls_enabled': route_info['tls_enabled']
                    }
            
            # Fall back to service discovery
            self.logger.warning("No routes found, trying service discovery")
            return await self._discover_prometheus_service()
            
        except Exception as e:
            self.logger.error(f"Error discovering Prometheus: {e}")
            return None
    
    async def _find_prometheus_route(self, namespace: str) -> Optional[Dict[str, Any]]:
        """Find route via oc command or API"""
        try:
            # Try oc command first (faster)
            route_info = await asyncio.wait_for(
                self._find_route_via_oc_command(namespace),
                timeout=DISCOVERY_TIMEOUT
            )
            if route_info:
                return route_info
            
            # Fall back to API
            return await asyncio.wait_for(
                asyncio.to_thread(self._discover_route_via_requests, namespace),
                timeout=DISCOVERY_TIMEOUT
            )
        except asyncio.TimeoutError:
            self.logger.debug(f"Route discovery timeout for {namespace}")
            return None
        except Exception:
            return None
    
    async def _find_route_via_oc_command(self, namespace: str) -> Optional[Dict[str, Any]]:
        """Find route using oc command"""
        try:
            env = os.environ.copy()
            env.pop('REQUESTS_CA_BUNDLE', None)
            
            cmd = ['oc', 'get', 'route', '-n', namespace, '-o', 'json']
            
            proc = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env
                ),
                timeout=DISCOVERY_TIMEOUT
            )
            
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=DISCOVERY_TIMEOUT)
            
            if proc.returncode == 0:
                routes_data = json.loads(stdout.decode())
                items = routes_data.get('items', [])
                
                prometheus_patterns = ['prometheus-k8s', 'prometheus']
                
                for route in items:
                    route_name = route.get('metadata', {}).get('name', '').lower()
                    
                    if any(pattern in route_name for pattern in prometheus_patterns):
                        spec = route.get('spec', {})
                        host = spec.get('host')
                        if not host:
                            continue
                        
                        tls_enabled = bool(spec.get('tls'))
                        scheme = 'https' if tls_enabled else 'http'
                        
                        return {
                            'url': f"{scheme}://{host}",
                            'host': host,
                            'tls_enabled': tls_enabled,
                            'route_name': route.get('metadata', {}).get('name'),
                            'namespace': namespace
                        }
        except Exception:
            pass
        return None

    def _discover_route_via_requests(self, namespace: str) -> Optional[Dict[str, Any]]:
        """Query OpenShift Routes API"""
        if not self.k8s_client:
            return None

        base_url = self.k8s_client.configuration.host.rstrip('/')
        headers = {'Accept': 'application/json'}
        if self.token:
            headers['Authorization'] = f"Bearer {self.token}"

        verify: Any
        if self.k8s_ca_cert_path and os.path.isfile(self.k8s_ca_cert_path):
            verify = self.k8s_ca_cert_path
        elif self.k8s_verify_ssl is not None:
            verify = self.k8s_verify_ssl
        else:
            verify = False

        timeout = (2, DISCOVERY_TIMEOUT)
        
        try:
            url = f"{base_url}/apis/route.openshift.io/v1/namespaces/{namespace}/routes"
            resp = requests.get(url, headers=headers, params={"labelSelector": "app=prometheus"}, 
                              verify=verify, timeout=timeout)
            
            if resp.status_code == 200:
                data = resp.json()
                items = data.get('items') or []
                if items:
                    route = items[0]
                    spec = route.get('spec', {})
                    host = spec.get('host')
                    tls_enabled = bool(spec.get('tls'))
                    scheme = 'https' if tls_enabled else 'http'
                    return {
                        'url': f"{scheme}://{host}",
                        'host': host,
                        'tls_enabled': tls_enabled,
                        'route_name': (route.get('metadata') or {}).get('name'),
                        'namespace': namespace
                    }
        except Exception:
            pass
        return None
    
    async def _discover_prometheus_service(self) -> Optional[Dict[str, Any]]:
        """Discover Prometheus through service"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            for namespace in ['openshift-monitoring', 'openshift-user-workload-monitoring']:
                try:
                    services = await asyncio.wait_for(
                        asyncio.to_thread(
                            v1.list_namespaced_service,
                            namespace=namespace,
                            label_selector="app=prometheus"
                        ),
                        timeout=DISCOVERY_TIMEOUT
                    )
                    
                    if services.items:
                        service = services.items[0]
                        service_name = service.metadata.name
                        port = 9090
                        
                        for svc_port in service.spec.ports:
                            if svc_port.name in ['web', 'http', 'prometheus']:
                                port = svc_port.port
                                break
                        
                        prometheus_url = f"http://{service_name}.{namespace}.svc.cluster.local:{port}"
                        
                        return {
                            'url': prometheus_url,
                            'namespace': namespace,
                            'service_name': service_name,
                            'port': port,
                            'access_method': 'cluster_internal'
                        }
                except Exception:
                    continue
        except Exception:
            pass
        return None
    
    async def _discover_prometheus_pods(self) -> Optional[Dict[str, Any]]:
        """Discover via pods (last resort)"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            
            for namespace in ['openshift-monitoring', 'openshift-user-workload-monitoring']:
                try:
                    pods = await asyncio.wait_for(
                        asyncio.to_thread(
                            v1.list_namespaced_pod,
                            namespace=namespace,
                            label_selector="app=prometheus"
                        ),
                        timeout=DISCOVERY_TIMEOUT
                    )
                    
                    if pods.items:
                        pod = pods.items[0]
                        pod_ip = pod.status.pod_ip
                        port = 9090
                        
                        return {
                            'url': f"http://{pod_ip}:{port}",
                            'namespace': namespace,
                            'pod_name': pod.metadata.name,
                            'port': port,
                            'access_method': 'direct_pod'
                        }
                except Exception:
                    continue
        except Exception:
            pass
        return None
    
    async def get_etcd_endpoints(self) -> Dict[str, Any]:
        """Get etcd endpoints"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            pods = await asyncio.wait_for(
                asyncio.to_thread(
                    v1.list_namespaced_pod,
                    namespace="openshift-etcd",
                    label_selector="app=etcd"
                ),
                timeout=DISCOVERY_TIMEOUT
            )
            
            endpoints = []
            for pod in pods.items:
                if pod.status.phase == "Running":
                    endpoints.append({
                        'name': pod.metadata.name,
                        'ip': pod.status.pod_ip,
                        'endpoint': f"{pod.status.pod_ip}:2379"
                    })
            
            return {
                'namespace': "openshift-etcd",
                'endpoints': endpoints,
                'total_members': len(endpoints)
            }
        except Exception as e:
            return {'error': str(e)}
    
    async def execute_etcd_command(self, command: str) -> Dict[str, Any]:
        """Execute etcdctl command"""
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            pods = await asyncio.wait_for(
                asyncio.to_thread(
                    v1.list_namespaced_pod,
                    namespace="openshift-etcd",
                    label_selector="app=etcd"
                ),
                timeout=DISCOVERY_TIMEOUT
            )
            
            if not pods.items:
                return {'error': 'No etcd pods found'}
            
            pod_name = None
            for pod in pods.items:
                if pod.status.phase == "Running":
                    pod_name = pod.metadata.name
                    break
            
            if not pod_name:
                return {'error': 'No running etcd pods'}
            
            container_name = None
            for c in pods.items[0].spec.containers:
                if c.name in ["etcdctl", "etcd"]:
                    container_name = c.name
                    break
            if not container_name:
                container_name = pods.items[0].spec.containers[0].name

            from kubernetes.stream import stream
            
            base_cmd = [
                'etcdctl',
                '--cacert=/etc/etcd/tls/etcd-ca/ca.crt',
                '--cert=/etc/etcd/tls/etcd-peer/peer.crt', 
                '--key=/etc/etcd/tls/etcd-peer/peer.key',
                '--endpoints=https://localhost:2379'
            ] + command.split()

            unsafe_envs = [
                'ETCDCTL_KEY','ETCDCTL_CERT','ETCDCTL_CACERT','ETCDCTL_ENDPOINTS',
                'ETCDCTL_USER','ETCDCTL_PASSWORD','ETCDCTL_TOKEN','ETCDCTL_INSECURE_SKIP_TLS_VERIFY'
            ]
            unset_parts = ' '.join([f"-u {name}" for name in unsafe_envs])
            cmd_str = ' '.join(shlex.quote(p) for p in base_cmd)
            shell_cmd = f"env {unset_parts} ETCDCTL_API=3 {cmd_str}"
            exec_command = ['/bin/sh', '-c', shell_cmd]

            resp = await asyncio.wait_for(
                asyncio.to_thread(
                    stream,
                    v1.connect_get_namespaced_pod_exec,
                    pod_name,
                    "openshift-etcd",
                    command=exec_command,
                    container=container_name,
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False
                ),
                timeout=30
            )
            
            return {
                'pod_name': pod_name,
                'command': ' '.join(exec_command),
                'output': resp
            }
        except Exception as e:
            return {'error': str(e)}
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        headers = {}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return headers
    
    def get_prometheus_config(self) -> Dict[str, Any]:
        """Get Prometheus connection configuration"""
        env_url = os.getenv('PROMETHEUS_URL') or os.getenv('OCP_PROMETHEUS_URL')
        env_token = os.getenv('PROMETHEUS_TOKEN')
        env_verify = os.getenv('PROMETHEUS_VERIFY')
        
        base_url = env_url or self.prometheus_url
        token_value = env_token if env_token else self.token
        verify_value = self.ca_cert_path if self.ca_cert_path else False
        
        if env_verify is not None:
            env_verify_lower = env_verify.lower()
            if env_verify_lower in ('true', '1', 'yes'):
                verify_value = True
            elif env_verify_lower in ('false', '0', 'no'):
                verify_value = False
            elif os.path.isfile(env_verify):
                verify_value = env_verify

        fallback_urls = []
        env_fallbacks = os.getenv('PROMETHEUS_FALLBACK_URLS')
        if env_fallbacks:
            fallback_urls.extend([u.strip() for u in env_fallbacks.split(',') if u.strip()])
        fallback_urls.extend(self._build_fallback_urls())

        return {
            'url': base_url,
            'headers': self.get_auth_headers(),
            'token': token_value,
            'verify': verify_value,
            'fallback_urls': fallback_urls,
        }

    def _build_fallback_urls(self) -> list:
        """Construct fallback Prometheus URLs"""
        fallbacks = []
        try:
            info = self.prometheus_info or {}
            namespace = info.get('namespace')
            service_name = info.get('service_name') or 'prometheus-k8s'
            port = info.get('port') or 9090

            if namespace:
                fallbacks.append(f"http://{service_name}.{namespace}.svc.cluster.local:{port}")
                fallbacks.append(f"http://{service_name}.{namespace}.svc:{port}")

            if not namespace:
                for ns in ['openshift-monitoring', 'openshift-user-workload-monitoring']:
                    fallbacks.append(f"http://prometheus-k8s.{ns}.svc.cluster.local:9090")
        except Exception:
            pass

        seen = set()
        unique = []
        for u in fallbacks:
            if u and u not in seen:
                seen.add(u)
                unique.append(u)
        return unique

    async def test_kubeapi_connection(self) -> bool:
        """Test Kubernetes API connectivity"""
        try:
            if not self.k8s_client:
                k8s_conf = client.Configuration.get_default_copy()
                self.k8s_client = client.ApiClient(configuration=k8s_conf)
            v1 = client.CoreV1Api(self.k8s_client)
            await asyncio.wait_for(
                asyncio.to_thread(v1.list_namespace, limit=1),
                timeout=5
            )
            return True
        except Exception:
            return False