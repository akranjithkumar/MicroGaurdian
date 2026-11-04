"""
MicroGuardian Reconciler — Main Control Loop
==============================================
The reconciler is the main entry point for the MicroGuardian autoscaler.
It runs a periodic reconciliation loop that:

1. Collects metrics from Prometheus (MetricsCollector)
2. Validates telemetry through the Watchdog Layer (Freshness, Continuity, Completeness)
3. Computes the Telemetry Trust Score (TrustScoreEngine)
4. Makes scaling decisions (DecisionEngine)
5. Executes scaling actions via the Kubernetes API (KubernetesClient)

This replaces the standard HPA's direct Prometheus → Scale pipeline with
a fault-tolerant, trust-gated control loop.
"""

import sys
import time
import json
import signal
import logging
from typing import Dict, Optional

from .config import MicroGuardianConfig
from .metrics_collector import MetricsCollector, MetricVector
from .watchdog.freshness import evaluate_freshness
from .watchdog.continuity import evaluate_continuity
from .watchdog.completeness import evaluate_completeness
from .trust_score import TrustScoreEngine
from .decision_engine import DecisionEngine
from .fallback import FallbackEngine
from .k8s_client import KubernetesClient

logger = logging.getLogger("microguardian")


class MicroGuardianReconciler:
    """
    Main reconciliation controller for the Telemetry-Integrity-Aware Autoscaler.
    Implements the closed-loop: Collect → Validate → Score → Decide → Actuate.
    """

    def __init__(self, config: Optional[MicroGuardianConfig] = None):
        self.config = config or MicroGuardianConfig()
        self.config.validate()

        # Initialize components
        self.metrics_collector = MetricsCollector(self.config.prometheus)
        self.trust_engine = TrustScoreEngine(
            self.config.trust_score,
            history_size=self.config.reconciler.history_buffer_size,
        )
        self.fallback_engine = FallbackEngine(
            window_size=self.config.scaling.moving_average_window,
            min_replicas=self.config.scaling.min_replicas,
        )
        self.decision_engine = DecisionEngine(
            scaling_config=self.config.scaling,
            trust_config=self.config.trust_score,
            fallback_engine=self.fallback_engine,
        )
        self.k8s_client = KubernetesClient(self.config.kubernetes)

        # State: previous metric values for continuity checking
        self._previous_metrics: Dict[str, Dict[str, float]] = {}
        self._previous_timestamps: Dict[str, float] = {}

        # Event log for analysis
        self._event_log: list = []
        self._running = False

    def run(self):
        """
        Start the main reconciliation loop.
        Runs indefinitely until interrupted (SIGINT/SIGTERM).
        """
        self._running = True

        # Handle graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        logger.info("=" * 60)
        logger.info("MicroGuardian — Telemetry-Integrity-Aware Autoscaler")
        logger.info("=" * 60)
        logger.info(f"  Managed deployments: {self.config.kubernetes.managed_deployments}")
        logger.info(f"  Prometheus URL:      {self.config.prometheus.url}")
        logger.info(f"  Trust Score weights: "
                     f"w_f={self.config.trust_score.weight_freshness}, "
                     f"w_c={self.config.trust_score.weight_continuity}, "
                     f"w_v={self.config.trust_score.weight_completeness}")
        logger.info(f"  Thresholds:          "
                     f"α_high={self.config.trust_score.alpha_high}, "
                     f"β_low={self.config.trust_score.beta_low}")
        logger.info(f"  Reconciliation interval: "
                     f"{self.config.reconciler.reconciliation_interval_seconds}s")
        logger.info("=" * 60)

        # Initialize Kubernetes client
        if not self.k8s_client.initialize():
            logger.error("Failed to initialize Kubernetes client. Exiting.")
            sys.exit(1)

        # Check Prometheus connectivity
        if self.metrics_collector.check_prometheus_health():
            logger.info("✓ Prometheus is healthy and reachable")
        else:
            logger.warning("⚠ Prometheus is not reachable — will retry during reconciliation")

        # Main loop
        cycle = 0
        while self._running:
            cycle += 1
            logger.info(f"\n{'─' * 40} Cycle {cycle} {'─' * 40}")

            for deployment in self.config.kubernetes.managed_deployments:
                try:
                    self._reconcile_deployment(deployment, cycle)
                except Exception as e:
                    logger.error(f"[{deployment}] Reconciliation error: {e}", exc_info=True)

            # Sleep until next interval
            time.sleep(self.config.reconciler.reconciliation_interval_seconds)

        logger.info("MicroGuardian shutdown complete.")

    def _reconcile_deployment(self, deployment: str, cycle: int):
        """
        Execute one reconciliation cycle for a single deployment.
        Follows the pipeline: Collect → Validate → Score → Decide → Actuate
        """
        # ═══════════════════════════════════════════════════
        # Step 1: COLLECT — Query Prometheus for metrics
        # ═══════════════════════════════════════════════════
        metric_vector = self.metrics_collector.collect(deployment)

        # ═══════════════════════════════════════════════════
        # Step 2: VALIDATE — Run Watchdog Layer checks
        # ═══════════════════════════════════════════════════

        # 2a. Freshness Check (S_fresh)
        if metric_vector and metric_vector.pod_timestamps:
            freshness_result = evaluate_freshness(
                sample_timestamps=list(metric_vector.pod_timestamps.values()),
                threshold_seconds=self.config.watchdog.freshness_threshold_seconds,
                max_staleness_seconds=self.config.watchdog.freshness_max_staleness_seconds,
            )
            s_fresh = freshness_result["score"]
        else:
            s_fresh = 0.0
            logger.warning(f"[{deployment}] No metric data — S_fresh = 0.0")

        # 2b. Continuity Check (S_cont)
        if metric_vector and metric_vector.aggregates:
            current_agg = metric_vector.aggregates
            previous_agg = self._previous_metrics.get(deployment, {})
            prev_ts = self._previous_timestamps.get(deployment, time.time())
            delta_t = time.time() - prev_ts

            continuity_result = evaluate_continuity(
                current_values=current_agg,
                previous_values=previous_agg,
                delta_time_seconds=max(delta_t, 1.0),
                max_rate=self.config.watchdog.continuity_max_rate,
                decay_factor=self.config.watchdog.continuity_decay_factor,
            )
            s_cont = continuity_result["score"]

            # Update previous metrics state
            self._previous_metrics[deployment] = current_agg.copy()
            self._previous_timestamps[deployment] = time.time()
        else:
            s_cont = 0.0

        # 2c. Completeness Check (S_valid)
        expected_pods = self.k8s_client.list_deployment_pods(deployment)
        if metric_vector and metric_vector.pods:
            completeness_result = evaluate_completeness(
                expected_pods=expected_pods,
                scraped_metrics=metric_vector.pods,
            )
            s_valid = completeness_result["score"]
        else:
            s_valid = 0.0 if expected_pods else 1.0

        # ═══════════════════════════════════════════════════
        # Step 3: SCORE — Compute Telemetry Trust Score
        # ═══════════════════════════════════════════════════
        trust_snapshot = self.trust_engine.compute(
            deployment=deployment,
            freshness_score=s_fresh,
            continuity_score=s_cont,
            completeness_score=s_valid,
        )

        # ═══════════════════════════════════════════════════
        # Step 4: DECIDE — Make scaling decision
        # ═══════════════════════════════════════════════════
        current_replicas = self.k8s_client.get_deployment_replicas(deployment)
        if current_replicas is None:
            logger.error(f"[{deployment}] Cannot read current replicas — skipping")
            return

        # Get the primary scaling metric (CPU utilization)
        cpu_metric = None
        if metric_vector and "cpu" in metric_vector.aggregates:
            cpu_metric = metric_vector.aggregates["cpu"]

        decision = self.decision_engine.decide(
            deployment=deployment,
            trust_snapshot=trust_snapshot,
            current_replicas=current_replicas,
            current_metric_value=cpu_metric,
        )

        # ═══════════════════════════════════════════════════
        # Step 5: ACTUATE — Execute scaling action
        # ═══════════════════════════════════════════════════
        if decision.desired_replicas != current_replicas:
            success = self.k8s_client.scale_deployment(
                deployment, decision.desired_replicas
            )
            if success:
                logger.info(
                    f"[{deployment}] ✓ Scaled: {current_replicas} → "
                    f"{decision.desired_replicas} ({decision.action})"
                )
            else:
                logger.error(
                    f"[{deployment}] ✗ Scaling FAILED: "
                    f"{current_replicas} → {decision.desired_replicas}"
                )
        else:
            logger.info(
                f"[{deployment}] No scaling change needed "
                f"(replicas={current_replicas}, action={decision.action})"
            )

        # Log event for analysis
        event = {
            "cycle": cycle,
            "timestamp": time.time(),
            "deployment": deployment,
            "trust_score": trust_snapshot.to_dict(),
            "decision": decision.to_dict(),
            "s_fresh": s_fresh,
            "s_cont": s_cont,
            "s_valid": s_valid,
        }
        self._event_log.append(event)

        # Optionally log trust scores for debugging
        if self.config.reconciler.log_trust_scores:
            logger.info(
                f"[{deployment}] Summary: T_s={trust_snapshot.trust_score:.4f} "
                f"({trust_snapshot.confidence_level}), "
                f"action={decision.action}, "
                f"replicas: {current_replicas} → {decision.desired_replicas}"
            )

    def get_event_log(self) -> list:
        """Return the event log for analysis."""
        return self._event_log

    def export_event_log(self, filepath: str):
        """Export event log to JSON file for analysis."""
        with open(filepath, "w") as f:
            json.dump(self._event_log, f, indent=2)
        logger.info(f"Event log exported to {filepath}")

    def _shutdown(self, signum, frame):
        """Handle graceful shutdown."""
        logger.info(f"Received signal {signum} — shutting down MicroGuardian...")
        self._running = False


def setup_logging(level: str = "INFO"):
    """Configure logging for MicroGuardian."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    """Entry point for the MicroGuardian reconciler."""
    config = MicroGuardianConfig()

    setup_logging(config.reconciler.log_level)

    reconciler = MicroGuardianReconciler(config)
    reconciler.run()


if __name__ == "__main__":
    main()
