"""
Kubernetes API Client
======================
Handles interactions with the Kubernetes API for:
1. Reading current deployment replica counts
2. Scaling deployments (patching replica count)
3. Listing pods for a deployment (for completeness checking)
"""

import logging
from typing import Dict, List, Optional

try:
    from kubernetes import client, config as k8s_config
    K8S_AVAILABLE = True
except ImportError:
    K8S_AVAILABLE = False

from .config import KubernetesConfig

logger = logging.getLogger("microguardian.k8s_client")


class KubernetesClient:
    """
    Thin wrapper around the Kubernetes Python client for autoscaler operations.
    """

    def __init__(self, config: Optional[KubernetesConfig] = None):
        self.config = config or KubernetesConfig()
        self._initialized = False
        self._apps_v1 = None
        self._core_v1 = None

    def initialize(self):
        """Initialize the Kubernetes client (in-cluster or kubeconfig)."""
        if not K8S_AVAILABLE:
            logger.error(
                "kubernetes Python package not installed. "
                "Install with: pip install kubernetes"
            )
            return False

        try:
            if self.config.in_cluster:
                k8s_config.load_incluster_config()
                logger.info("Loaded in-cluster Kubernetes config")
            else:
                k8s_config.load_kube_config()
                logger.info("Loaded kubeconfig from default location")

            self._apps_v1 = client.AppsV1Api()
            self._core_v1 = client.CoreV1Api()
            self._initialized = True
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}")
            return False

    def get_deployment_replicas(self, deployment_name: str) -> Optional[int]:
        """Get the current replica count for a deployment."""
        if not self._initialized:
            self.initialize()

        try:
            deployment = self._apps_v1.read_namespaced_deployment(
                name=deployment_name,
                namespace=self.config.namespace,
            )
            replicas = deployment.status.ready_replicas or 0
            logger.debug(f"[{deployment_name}] Current replicas: {replicas}")
            return replicas

        except Exception as e:
            logger.error(f"Failed to read deployment '{deployment_name}': {e}")
            return None

    def scale_deployment(self, deployment_name: str, desired_replicas: int) -> bool:
        """
        Scale a deployment to the desired replica count.

        Returns True if the scaling request was successful.
        """
        if not self._initialized:
            self.initialize()

        try:
            # Patch the deployment's spec.replicas
            body = {"spec": {"replicas": desired_replicas}}
            self._apps_v1.patch_namespaced_deployment_scale(
                name=deployment_name,
                namespace=self.config.namespace,
                body=body,
            )
            logger.info(
                f"[{deployment_name}] Scaled to {desired_replicas} replicas"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to scale '{deployment_name}' to {desired_replicas}: {e}"
            )
            return False

    def list_deployment_pods(self, deployment_name: str) -> List[str]:
        """
        List all pod names belonging to a deployment.
        Used by the Completeness Watchdog to know expected scrape targets.
        """
        if not self._initialized:
            self.initialize()

        try:
            # List pods with the deployment's app label
            pods = self._core_v1.list_namespaced_pod(
                namespace=self.config.namespace,
                label_selector=f"app={deployment_name}",
            )
            pod_names = [
                pod.metadata.name
                for pod in pods.items
                if pod.status.phase in ("Running", "Pending")
            ]
            logger.debug(f"[{deployment_name}] Found {len(pod_names)} pods")
            return pod_names

        except Exception as e:
            logger.error(f"Failed to list pods for '{deployment_name}': {e}")
            return []

    def get_deployment_spec_replicas(self, deployment_name: str) -> Optional[int]:
        """Get the desired (spec) replica count for a deployment."""
        if not self._initialized:
            self.initialize()

        try:
            deployment = self._apps_v1.read_namespaced_deployment(
                name=deployment_name,
                namespace=self.config.namespace,
            )
            return deployment.spec.replicas

        except Exception as e:
            logger.error(f"Failed to read deployment spec '{deployment_name}': {e}")
            return None
