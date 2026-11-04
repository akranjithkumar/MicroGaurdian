"""
Telemetry Trust Score Computation
==================================
Aggregates the three Watchdog validation signals (Freshness, Continuity,
Completeness) into a single normalized Trust Score T_s ∈ [0, 1].

Formula:
    T_s(t) = w_f · S_fresh + w_c · S_cont + w_v · S_valid

Where:
    w_f + w_c + w_v = 1.0
    Default: w_f = 0.4, w_c = 0.3, w_v = 0.3
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from collections import deque

from .config import TrustScoreConfig

logger = logging.getLogger("microguardian.trust_score")


@dataclass
class TrustScoreSnapshot:
    """A single Trust Score computation result at time t."""
    timestamp: float
    trust_score: float           # T_s ∈ [0, 1]
    freshness_score: float       # S_fresh ∈ [0, 1]
    continuity_score: float      # S_cont ∈ [0, 1]
    completeness_score: float    # S_valid ∈ [0, 1]
    confidence_level: str        # "HIGH", "DEGRADED", or "SEVERE"
    deployment: str              # Which deployment this is for

    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp,
            "trust_score": self.trust_score,
            "freshness": self.freshness_score,
            "continuity": self.continuity_score,
            "completeness": self.completeness_score,
            "level": self.confidence_level,
            "deployment": self.deployment,
        }


class TrustScoreEngine:
    """
    Computes and tracks the Telemetry Trust Score over time.
    Maintains a history buffer for trend analysis and logging.
    """

    def __init__(self, config: Optional[TrustScoreConfig] = None, history_size: int = 100):
        self.config = config or TrustScoreConfig()
        self.config.validate()
        self.history: Dict[str, deque] = {}  # deployment → deque of TrustScoreSnapshot
        self.history_size = history_size

    def compute(
        self,
        deployment: str,
        freshness_score: float,
        continuity_score: float,
        completeness_score: float,
    ) -> TrustScoreSnapshot:
        """
        Compute the Trust Score for a deployment at the current time.

        Args:
            deployment: Name of the Kubernetes deployment
            freshness_score: S_fresh from the Freshness Watchdog
            continuity_score: S_cont from the Continuity Watchdog
            completeness_score: S_valid from the Completeness Watchdog

        Returns:
            TrustScoreSnapshot with the computed T_s and confidence level
        """
        # T_s = w_f * S_fresh + w_c * S_cont + w_v * S_valid
        trust_score = (
            self.config.weight_freshness * freshness_score
            + self.config.weight_continuity * continuity_score
            + self.config.weight_completeness * completeness_score
        )
        trust_score = round(max(0.0, min(1.0, trust_score)), 4)

        # Determine confidence level
        if trust_score >= self.config.alpha_high:
            level = "HIGH"
        elif trust_score >= self.config.beta_low:
            level = "DEGRADED"
        else:
            level = "SEVERE"

        snapshot = TrustScoreSnapshot(
            timestamp=time.time(),
            trust_score=trust_score,
            freshness_score=round(freshness_score, 4),
            continuity_score=round(continuity_score, 4),
            completeness_score=round(completeness_score, 4),
            confidence_level=level,
            deployment=deployment,
        )

        # Store in history
        if deployment not in self.history:
            self.history[deployment] = deque(maxlen=self.history_size)
        self.history[deployment].append(snapshot)

        logger.info(
            f"[{deployment}] Trust Score: T_s = {trust_score:.4f} "
            f"(S_fresh={freshness_score:.4f}, S_cont={continuity_score:.4f}, "
            f"S_valid={completeness_score:.4f}) → {level}"
        )

        return snapshot

    def get_history(self, deployment: str) -> List[TrustScoreSnapshot]:
        """Get Trust Score history for a deployment."""
        return list(self.history.get(deployment, []))

    def get_latest(self, deployment: str) -> Optional[TrustScoreSnapshot]:
        """Get the most recent Trust Score for a deployment."""
        history = self.history.get(deployment)
        if history:
            return history[-1]
        return None

    def get_trend(self, deployment: str, window: int = 5) -> Optional[float]:
        """
        Compute the trend of Trust Score over the last N snapshots.
        Returns positive if trust is improving, negative if degrading.
        """
        history = self.history.get(deployment)
        if not history or len(history) < 2:
            return None

        recent = list(history)[-window:]
        if len(recent) < 2:
            return None

        # Simple linear trend: last - first
        return recent[-1].trust_score - recent[0].trust_score
