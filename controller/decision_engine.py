"""
Confidence-Weighted Decision Engine
=====================================
Evaluates the Trust Score against operational thresholds to select the
appropriate autoscaling actuation policy.

Decision Logic:
    T_s >= α_high (0.8)  → HIGH confidence    → Standard ratio-based HPA scaling
    β_low ≤ T_s < α_high → DEGRADED confidence → Freeze current replica count
    T_s < β_low (0.4)    → SEVERE loss         → Moving-average fallback or min-floor

Standard HPA Scaling Formula:
    R_desired = ceil(R_current * (current_metric / target_metric))
"""

import math
import time
import logging
from dataclasses import dataclass
from typing import Optional

from .config import ScalingConfig, TrustScoreConfig
from .trust_score import TrustScoreSnapshot
from .fallback import FallbackEngine

logger = logging.getLogger("microguardian.decision_engine")


@dataclass
class ScalingDecision:
    """Result of the decision engine for a single reconciliation cycle."""
    deployment: str
    action: str                  # "SCALE", "FREEZE", "FALLBACK"
    desired_replicas: int
    current_replicas: int
    trust_score: float
    confidence_level: str
    reason: str
    timestamp: float
    metric_value: Optional[float] = None
    target_value: Optional[float] = None

    def to_dict(self):
        return {
            "deployment": self.deployment,
            "action": self.action,
            "desired_replicas": self.desired_replicas,
            "current_replicas": self.current_replicas,
            "trust_score": self.trust_score,
            "confidence_level": self.confidence_level,
            "reason": self.reason,
            "timestamp": self.timestamp,
            "metric_value": self.metric_value,
            "target_value": self.target_value,
        }


class DecisionEngine:
    """
    Confidence-weighted multi-state autoscaling decision engine.
    Gates scaling actions based on Trust Score thresholds.
    """

    def __init__(
        self,
        scaling_config: Optional[ScalingConfig] = None,
        trust_config: Optional[TrustScoreConfig] = None,
        fallback_engine: Optional[FallbackEngine] = None,
    ):
        self.scaling_config = scaling_config or ScalingConfig()
        self.trust_config = trust_config or TrustScoreConfig()
        self.fallback = fallback_engine or FallbackEngine(
            window_size=self.scaling_config.moving_average_window,
            min_replicas=self.scaling_config.min_replicas,
        )

        # Track last scaling action time per deployment (for cooldown)
        self._last_scale_up: dict[str, float] = {}
        self._last_scale_down: dict[str, float] = {}

    def decide(
        self,
        deployment: str,
        trust_snapshot: TrustScoreSnapshot,
        current_replicas: int,
        current_metric_value: Optional[float],
        target_metric_value: Optional[float] = None,
        now: Optional[float] = None,
    ) -> ScalingDecision:
        """
        Make a scaling decision based on Trust Score and current metrics.

        Args:
            deployment: K8s deployment name
            trust_snapshot: Latest Trust Score computation result
            current_replicas: Current number of running replicas
            current_metric_value: Observed metric (e.g., avg CPU utilization as fraction)
            target_metric_value: Target metric threshold (e.g., 0.50 for 50% CPU)
            now: Optional timestamp override for simulation / testing

        Returns:
            ScalingDecision with the recommended action
        """
        if target_metric_value is None:
            target_metric_value = self.scaling_config.target_cpu_utilization

        ts = trust_snapshot.trust_score
        if now is None:
            now = time.time()

        # ─── HIGH CONFIDENCE: Standard ratio-based scaling ───
        if ts >= self.trust_config.alpha_high:
            return self._decide_scale(
                deployment, current_replicas,
                current_metric_value, target_metric_value,
                trust_snapshot, now,
            )

        # ─── DEGRADED CONFIDENCE: Freeze replicas ───
        elif ts >= self.trust_config.beta_low:
            logger.warning(
                f"[{deployment}] DEGRADED confidence (T_s={ts:.4f}) — "
                f"FREEZING replicas at {current_replicas}"
            )
            return ScalingDecision(
                deployment=deployment,
                action="FREEZE",
                desired_replicas=current_replicas,
                current_replicas=current_replicas,
                trust_score=ts,
                confidence_level="DEGRADED",
                reason=f"Telemetry degraded (T_s={ts:.4f}). "
                       f"Freezing at {current_replicas} replicas to prevent flapping.",
                timestamp=now,
                metric_value=current_metric_value,
                target_value=target_metric_value,
            )

        # ─── SEVERE TELEMETRY LOSS: Fallback strategy ───
        else:
            fallback_replicas = self.fallback.compute_fallback(
                deployment, current_replicas
            )
            logger.error(
                f"[{deployment}] SEVERE telemetry loss (T_s={ts:.4f}) — "
                f"FALLBACK to {fallback_replicas} replicas "
                f"(moving-average or min-floor)"
            )
            return ScalingDecision(
                deployment=deployment,
                action="FALLBACK",
                desired_replicas=fallback_replicas,
                current_replicas=current_replicas,
                trust_score=ts,
                confidence_level="SEVERE",
                reason=f"Severe telemetry loss (T_s={ts:.4f}). "
                       f"Using historical moving-average fallback → "
                       f"{fallback_replicas} replicas.",
                timestamp=now,
                metric_value=current_metric_value,
                target_value=target_metric_value,
            )

    def _decide_scale(
        self,
        deployment: str,
        current_replicas: int,
        metric_value: Optional[float],
        target_value: float,
        trust_snapshot: TrustScoreSnapshot,
        now: float,
    ) -> ScalingDecision:
        """Standard ratio-based HPA scaling under high confidence."""

        if metric_value is None or metric_value <= 0:
            logger.info(
                f"[{deployment}] HIGH confidence but no valid metric — "
                f"maintaining {current_replicas} replicas"
            )
            return ScalingDecision(
                deployment=deployment,
                action="SCALE",
                desired_replicas=current_replicas,
                current_replicas=current_replicas,
                trust_score=trust_snapshot.trust_score,
                confidence_level="HIGH",
                reason="No valid metric value; maintaining current replicas.",
                timestamp=now,
                metric_value=metric_value,
                target_value=target_value,
            )

        # R_desired = ceil(R_current * (current_metric / target_metric))
        ratio = metric_value / target_value
        desired = math.ceil(current_replicas * ratio)

        # Apply replica bounds
        desired = max(self.scaling_config.min_replicas, desired)
        desired = min(self.scaling_config.max_replicas, desired)

        # Apply tolerance band — don't scale if change is minimal
        if abs(desired - current_replicas) / max(current_replicas, 1) <= self.scaling_config.scaling_tolerance:
            desired = current_replicas

        # Apply cooldown
        if desired > current_replicas:
            last_up = self._last_scale_up.get(deployment, 0)
            if (now - last_up) < self.scaling_config.scale_up_cooldown_seconds:
                desired = current_replicas
                logger.info(f"[{deployment}] Scale-up cooldown active, holding at {current_replicas}")
            else:
                self._last_scale_up[deployment] = now
        elif desired < current_replicas:
            last_down = self._last_scale_down.get(deployment, 0)
            if (now - last_down) < self.scaling_config.scale_down_cooldown_seconds:
                desired = current_replicas
                logger.info(f"[{deployment}] Scale-down cooldown active, holding at {current_replicas}")
            else:
                self._last_scale_down[deployment] = now

        # Record for fallback history
        self.fallback.record_replica_count(deployment, desired)

        action_detail = "no change"
        if desired > current_replicas:
            action_detail = f"scale UP {current_replicas} → {desired}"
        elif desired < current_replicas:
            action_detail = f"scale DOWN {current_replicas} → {desired}"

        logger.info(
            f"[{deployment}] HIGH confidence (T_s={trust_snapshot.trust_score:.4f}) — "
            f"ratio={ratio:.3f}, {action_detail}"
        )

        return ScalingDecision(
            deployment=deployment,
            action="SCALE",
            desired_replicas=desired,
            current_replicas=current_replicas,
            trust_score=trust_snapshot.trust_score,
            confidence_level="HIGH",
            reason=f"Standard scaling: metric={metric_value:.4f}, "
                   f"target={target_value:.4f}, ratio={ratio:.3f} → {desired} replicas.",
            timestamp=now,
            metric_value=metric_value,
            target_value=target_value,
        )
