"""
Prometheus Metrics Collector
=============================
Queries Prometheus for metric vectors M(t) = [CPU, Memory, RPS, Latency]
at each reconciliation interval. Returns structured metric data with
timestamps for the Telemetry Watchdog Layer to validate.
"""

import time
import logging
import requests
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .config import PrometheusConfig

logger = logging.getLogger("microguardian.metrics_collector")


@dataclass
class MetricSample:
    """A single metric sample from Prometheus."""
    metric_name: str
    pod_name: str
    value: float
    timestamp: float    # Unix timestamp from Prometheus


@dataclass
class MetricVector:
    """
    Complete metric vector M(t) for a deployment at time t.
    Contains per-pod metric values and aggregate statistics.
    """
    deployment: str
    timestamp: float
    pods: Dict[str, Dict[str, float]]    # pod_name → {metric_name: value}
    aggregates: Dict[str, float]          # metric_name → aggregate value
    pod_timestamps: Dict[str, float]      # pod_name → sample timestamp
    query_duration_ms: float              # How long the Prometheus query took
    scrape_success: bool                  # Whether all queries succeeded


class MetricsCollector:
    """
    Collects metrics from Prometheus for target deployments.
    Returns structured MetricVectors suitable for Watchdog validation.
    """

    def __init__(self, config: Optional[PrometheusConfig] = None):
        self.config = config or PrometheusConfig()
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def collect(self, deployment: str) -> Optional[MetricVector]:
        """
        Collect all metric dimensions for a deployment.

        Args:
            deployment: Name of the K8s deployment (e.g., "order-service")

        Returns:
            MetricVector with per-pod and aggregate metrics, or None on total failure
        """
        start_time = time.time()
        pods: Dict[str, Dict[str, float]] = {}
        pod_timestamps: Dict[str, float] = {}
        aggregates: Dict[str, float] = {}
        all_success = True

        # Define metric queries
        queries = {
            "cpu": self.config.cpu_query.format(deployment=deployment),
            "memory": self.config.memory_query.format(deployment=deployment),
            "rps": self.config.rps_query.format(deployment=deployment),
            "latency_p95": self.config.latency_query.format(deployment=deployment),
        }

        for metric_name, query in queries.items():
            try:
                samples = self._query_prometheus(query)
                if samples:
                    values = []
                    for sample in samples:
                        pod = sample.pod_name
                        if pod not in pods:
                            pods[pod] = {}
                        pods[pod][metric_name] = sample.value
                        pod_timestamps[pod] = sample.timestamp
                        values.append(sample.value)

                    # Compute aggregate (average across pods)
                    if values:
                        aggregates[metric_name] = sum(values) / len(values)
                else:
                    logger.warning(
                        f"[{deployment}] No data for metric '{metric_name}'"
                    )
                    aggregates[metric_name] = 0.0
                    all_success = False

            except Exception as e:
                logger.error(
                    f"[{deployment}] Failed to query '{metric_name}': {e}"
                )
                aggregates[metric_name] = 0.0
                all_success = False

        query_duration = (time.time() - start_time) * 1000  # ms

        if not pods:
            logger.error(f"[{deployment}] No metric data collected from Prometheus")
            return None

        vector = MetricVector(
            deployment=deployment,
            timestamp=time.time(),
            pods=pods,
            aggregates=aggregates,
            pod_timestamps=pod_timestamps,
            query_duration_ms=round(query_duration, 2),
            scrape_success=all_success,
        )

        logger.debug(
            f"[{deployment}] Collected metrics for {len(pods)} pods "
            f"in {query_duration:.1f}ms (success={all_success})"
        )

        return vector

    def _query_prometheus(self, query: str) -> List[MetricSample]:
        """
        Execute a PromQL instant query and parse the results.
        """
        try:
            response = self.session.get(
                f"{self.config.url}/api/v1/query",
                params={"query": query},
                timeout=self.config.query_timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "success":
                logger.warning(f"Prometheus query failed: {data.get('error', 'unknown')}")
                return []

            results = data.get("data", {}).get("result", [])
            samples = []

            for result in results:
                pod_name = result.get("metric", {}).get("pod", "unknown")
                value_pair = result.get("value", [0, "0"])

                try:
                    timestamp = float(value_pair[0])
                    value = float(value_pair[1])
                except (ValueError, IndexError):
                    continue

                samples.append(MetricSample(
                    metric_name=query[:30],  # Truncated for logging
                    pod_name=pod_name,
                    value=value,
                    timestamp=timestamp,
                ))

            return samples

        except requests.exceptions.ConnectionError:
            logger.error(f"Cannot connect to Prometheus at {self.config.url}")
            return []
        except requests.exceptions.Timeout:
            logger.error(f"Prometheus query timed out after {self.config.query_timeout_seconds}s")
            return []
        except Exception as e:
            logger.error(f"Prometheus query error: {e}")
            return []

    def check_prometheus_health(self) -> bool:
        """Check if Prometheus is reachable and healthy."""
        try:
            response = self.session.get(
                f"{self.config.url}/-/healthy",
                timeout=5,
            )
            return response.status_code == 200
        except Exception:
            return False
