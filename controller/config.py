"""
MicroGuardian Configuration
===========================
Central configuration for the Telemetry-Integrity-Aware Autoscaler.
Contains all tunable parameters: Trust Score thresholds, watchdog settings,
reconciliation intervals, and Kubernetes target specifications.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class WatchdogConfig:
    """Configuration for the Telemetry Watchdog Layer."""

    # Temporal Freshness (S_fresh) thresholds
    freshness_threshold_seconds: float = 30.0      # τ_threshold: max acceptable staleness
    freshness_max_staleness_seconds: float = 120.0  # τ_max: staleness at which S_fresh = 0

    # Derivative Continuity (S_cont) thresholds
    continuity_max_rate: float = 5.0    # σ_max: max acceptable rate-of-change (normalized)
    continuity_decay_factor: float = 3.0  # k: decay factor for S_cont scoring

    # Scrape Completeness (S_valid) — no additional params needed (ratio-based)


@dataclass
class TrustScoreConfig:
    """Configuration for the Telemetry Trust Score computation."""

    # Weights for T_s = w_f * S_fresh + w_c * S_cont + w_v * S_valid
    weight_freshness: float = 0.4   # w_f
    weight_continuity: float = 0.3  # w_c
    weight_completeness: float = 0.3  # w_v

    # Decision thresholds
    alpha_high: float = 0.8    # T_s >= α: scale normally
    beta_low: float = 0.4      # T_s < β: severe telemetry loss → fallback

    def validate(self):
        """Ensure weights sum to 1.0 and thresholds are ordered."""
        total = self.weight_freshness + self.weight_continuity + self.weight_completeness
        assert abs(total - 1.0) < 1e-6, f"Trust score weights must sum to 1.0, got {total}"
        assert self.beta_low < self.alpha_high, \
            f"beta_low ({self.beta_low}) must be less than alpha_high ({self.alpha_high})"


@dataclass
class ScalingConfig:
    """Configuration for autoscaling behavior."""

    # Target CPU utilization (as fraction, e.g., 0.50 = 50%)
    target_cpu_utilization: float = 0.50

    # Replica bounds
    min_replicas: int = 2
    max_replicas: int = 10

    # Moving average fallback — window size (number of past samples)
    moving_average_window: int = 10

    # Scale-up / scale-down cooldown (seconds)
    scale_up_cooldown_seconds: int = 30
    scale_down_cooldown_seconds: int = 60

    # Tolerance band: don't scale if desired is within ±tolerance of current
    scaling_tolerance: float = 0.1


@dataclass
class PrometheusConfig:
    """Configuration for Prometheus metrics collection."""

    url: str = field(default_factory=lambda: os.getenv(
        "PROMETHEUS_URL", "http://localhost:9090"
    ))
    query_timeout_seconds: int = 10

    # Queries for each metric dimension
    cpu_query: str = (
        'avg(rate(process_cpu_seconds_total{{namespace="microguardian", '
        'pod=~"{deployment}.*"}}[1m])) by (pod)'
    )
    memory_query: str = (
        'avg(container_memory_working_set_bytes{{namespace="microguardian", '
        'pod=~"{deployment}.*"}}) by (pod)'
    )
    rps_query: str = (
        'sum(rate(http_server_requests_seconds_count{{namespace="microguardian", '
        'pod=~"{deployment}.*"}}[1m])) by (pod)'
    )
    latency_query: str = (
        'histogram_quantile(0.95, sum(rate(http_server_requests_seconds_bucket{{'
        'namespace="microguardian", pod=~"{deployment}.*"}}[1m])) by (le, pod))'
    )


@dataclass
class KubernetesConfig:
    """Configuration for Kubernetes API interaction."""

    namespace: str = "microguardian"
    in_cluster: bool = field(default_factory=lambda: os.getenv(
        "IN_CLUSTER", "false"
    ).lower() == "true")

    # Deployments managed by MicroGuardian
    managed_deployments: List[str] = field(default_factory=lambda: [
        "order-service",
        "inventory-service",
    ])


@dataclass
class ReconcilerConfig:
    """Configuration for the main reconciliation loop."""

    # How often to run the reconciliation cycle (seconds)
    reconciliation_interval_seconds: int = 15

    # How many historical metric samples to retain for fallback/analysis
    history_buffer_size: int = 100

    # Logging
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_trust_scores: bool = True


@dataclass
class MicroGuardianConfig:
    """Top-level configuration aggregating all sub-configs."""

    watchdog: WatchdogConfig = field(default_factory=WatchdogConfig)
    trust_score: TrustScoreConfig = field(default_factory=TrustScoreConfig)
    scaling: ScalingConfig = field(default_factory=ScalingConfig)
    prometheus: PrometheusConfig = field(default_factory=PrometheusConfig)
    kubernetes: KubernetesConfig = field(default_factory=KubernetesConfig)
    reconciler: ReconcilerConfig = field(default_factory=ReconcilerConfig)

    def validate(self):
        """Validate all sub-configurations."""
        self.trust_score.validate()
        assert self.scaling.min_replicas > 0, "min_replicas must be positive"
        assert self.scaling.max_replicas >= self.scaling.min_replicas, \
            "max_replicas must be >= min_replicas"
        assert self.reconciler.reconciliation_interval_seconds > 0, \
            "Reconciliation interval must be positive"


# Singleton default configuration
DEFAULT_CONFIG = MicroGuardianConfig()
