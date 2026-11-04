"""
Fallback Engine
================
Implements fallback strategies for severe telemetry loss scenarios:
1. Moving-Average Fallback: Uses a sliding window of historical replica counts
2. Minimum Replica Floor: Ensures at least N replicas during outages

These strategies prevent the autoscaler from making destructive scaling
decisions when telemetry data is unreliable.
"""

import logging
from collections import deque
from typing import Dict, Optional

logger = logging.getLogger("microguardian.fallback")


class FallbackEngine:
    """
    Manages fallback strategies when telemetry trust is severely degraded.
    Maintains per-deployment history of replica counts for moving-average computation.
    """

    def __init__(self, window_size: int = 10, min_replicas: int = 2):
        """
        Args:
            window_size: Number of past replica observations for moving average
            min_replicas: Absolute minimum replica floor during severe loss
        """
        self.window_size = window_size
        self.min_replicas = min_replicas
        self._history: Dict[str, deque] = {}

    def record_replica_count(self, deployment: str, replica_count: int):
        """
        Record a replica count observation for future fallback computation.
        Called during HIGH confidence periods to build historical baseline.
        """
        if deployment not in self._history:
            self._history[deployment] = deque(maxlen=self.window_size)
        self._history[deployment].append(replica_count)

    def compute_fallback(self, deployment: str, current_replicas: int) -> int:
        """
        Compute the fallback replica count during severe telemetry loss.

        Strategy:
            1. If historical data available → use moving average of past observations
            2. If no history → use max(current_replicas, min_replicas)

        This prevents:
            - Scaling down during monitoring outages (protects SLOs)
            - Scaling up wildly based on garbage data (prevents over-provisioning)
        """
        history = self._history.get(deployment)

        if history and len(history) >= 2:
            # Moving average of historical replica counts
            avg = sum(history) / len(history)
            fallback_replicas = max(round(avg), self.min_replicas)
            logger.info(
                f"[{deployment}] Fallback: moving average over {len(history)} samples "
                f"= {avg:.1f} → {fallback_replicas} replicas"
            )
        else:
            # No history — use conservative approach
            fallback_replicas = max(current_replicas, self.min_replicas)
            logger.info(
                f"[{deployment}] Fallback: no history available, "
                f"using max(current={current_replicas}, min={self.min_replicas}) "
                f"= {fallback_replicas} replicas"
            )

        return fallback_replicas

    def get_history(self, deployment: str) -> list:
        """Get replica count history for a deployment."""
        return list(self._history.get(deployment, []))

    def clear_history(self, deployment: str):
        """Clear history for a deployment (e.g., after config change)."""
        if deployment in self._history:
            self._history[deployment].clear()
            logger.info(f"[{deployment}] Fallback history cleared")
