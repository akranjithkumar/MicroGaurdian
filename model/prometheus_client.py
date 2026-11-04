"""
Prometheus Live Telemetry Ingestion Client
==========================================
Polls live telemetry streams from Prometheus HTTP API (v1) and converts
raw Prometheus time-series into normalized TelemetrySample objects.
"""

import time
import logging
import requests
from typing import Dict, List, Optional, Any

from .base import TelemetrySample

logger = logging.getLogger("microguardian.model.prometheus")


class PrometheusLiveStream:
    """
    Connects to Prometheus instance (local or in-cluster) and extracts
    real-time telemetry and watchdog signals.
    """

    def __init__(
        self,
        prometheus_url: str = "http://localhost:9090",
        deployment: str = "order-service",
        namespace: str = "default",
        timeout: float = 3.0,
    ):
        self.prometheus_url = prometheus_url.rstrip("/")
        self.deployment = deployment
        self.namespace = namespace
        self.timeout = timeout
        
        # State tracking for continuity
        self.prev_cpu: Optional[float] = None
        self.prev_time: Optional[float] = None

    def is_connected(self) -> bool:
        """Test whether Prometheus endpoint is reachable."""
        try:
            resp = requests.get(f"{self.prometheus_url}/api/v1/status/buildinfo", timeout=self.timeout)
            return resp.status_code == 200
        except Exception:
            return False

    def query(self, promql: str) -> List[Dict]:
        """Execute instant query against Prometheus."""
        url = f"{self.prometheus_url}/api/v1/query"
        resp = requests.get(url, params={"query": promql}, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "success":
            return data.get("data", {}).get("result", [])
        return []

    def fetch_live_sample(self, expected_pods: int = 2) -> Optional[TelemetrySample]:
        """
        Polls live metrics and constructs a TelemetrySample.
        Returns None if Prometheus is unreachable.
        """
        now = time.time()
        try:
            # Query 1: CPU Utilization
            cpu_promql = f'rate(process_cpu_seconds_total{{app="{self.deployment}"}}[1m])'
            cpu_res = self.query(cpu_promql)
            
            # Query 2: RPS
            rps_promql = f'sum(rate(http_server_requests_seconds_count{{app="{self.deployment}"}}[1m]))'
            rps_res = self.query(rps_promql)
            
            # Query 3: Target pod health
            up_promql = f'up{{app="{self.deployment}"}}'
            up_res = self.query(up_promql)

            # Compute CPU
            reported_cpus = []
            sample_timestamps = []
            for item in cpu_res:
                val = item.get("value", [0, "0"])
                sample_timestamps.append(float(val[0]))
                reported_cpus.append(float(val[1]))
            
            avg_cpu = (sum(reported_cpus) / len(reported_cpus)) if reported_cpus else 0.0

            # Compute RPS
            rps = 0.0
            if rps_res:
                rps = float(rps_res[0].get("value", [0, "0"])[1])

            # Freshness: (t_curr - t_sample)
            if sample_timestamps:
                avg_staleness = now - (sum(sample_timestamps) / len(sample_timestamps))
                s_fresh = max(0.0, min(1.0, 1.0 - max(0.0, avg_staleness - 15.0) / 45.0))
            else:
                s_fresh = 0.0

            # Continuity: |cpu - prev_cpu| / dt
            s_cont = 1.0
            if self.prev_cpu is not None and self.prev_time is not None:
                dt = max(0.1, now - self.prev_time)
                rate = abs(avg_cpu - self.prev_cpu) / dt
                if rate > 2.0:
                    s_cont = max(0.0, 1.0 - (rate - 2.0) / 5.0)
            
            self.prev_cpu = avg_cpu
            self.prev_time = now

            # Completeness: valid pods / expected pods
            valid_pods = sum(1 for item in up_res if float(item.get("value", [0, 0])[1]) == 1.0)
            s_valid = min(1.0, valid_pods / expected_pods) if expected_pods > 0 else 1.0

            return TelemetrySample(
                timestamp=now,
                deployment=self.deployment,
                s_fresh=round(s_fresh, 4),
                s_cont=round(s_cont, 4),
                s_valid=round(s_valid, 4),
                cpu_utilization=round(avg_cpu, 4),
                rps=round(rps, 2),
                memory_mb=250.0,
                p95_latency_ms=120.0,
                current_replicas=expected_pods,
                ground_truth_fault=(s_fresh < 0.6 or s_cont < 0.6 or s_valid < 0.6),
                fault_type="live_prom",
            )
        except Exception as e:
            logger.warning(f"Failed to fetch live sample from Prometheus: {e}")
            return None
