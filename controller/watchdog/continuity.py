"""
Derivative Continuity Checker (S_cont)
=======================================
Enforces rate-of-change constraints to filter out impossible metric jumps
caused by counter resets, partially initialized pod scrapes, or
Prometheus exporter restarts.

Formula:
    rate = |M(t) - M(t-1)| / Δt
    S_cont = 1.0                                   if rate <= σ_max
    S_cont = max(0, 1 - (rate - σ_max) / (k * σ_max))  if rate > σ_max

Where:
    M(t)    = current metric value
    M(t-1)  = previous metric value
    Δt      = time interval between samples
    σ_max   = maximum acceptable rate of change (normalized)
    k       = decay factor controlling how quickly score drops
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("microguardian.watchdog.continuity")


@dataclass
class ContinuityResult:
    """Result of a continuity evaluation for a single metric pair."""
    score: float           # S_cont ∈ [0, 1]
    rate_of_change: float  # Computed |dM/dt|
    is_anomalous: bool     # Whether rate exceeds σ_max
    metric_name: str       # Which metric this evaluates
    current_value: float
    previous_value: float
    delta_time: float      # Time interval between samples


def evaluate_continuity(
    current_values: Dict[str, float],
    previous_values: Dict[str, float],
    delta_time_seconds: float,
    max_rate: float = 5.0,
    decay_factor: float = 3.0,
) -> Dict:
    """
    Evaluate derivative continuity for a set of metric dimensions.

    Args:
        current_values: Dict mapping metric name → current value
        previous_values: Dict mapping metric name → previous value
        delta_time_seconds: Time between current and previous sample (Δt)
        max_rate: σ_max — maximum acceptable rate of change
        decay_factor: k — how rapidly S_cont decays beyond σ_max

    Returns:
        Dictionary with:
            - 'score': Aggregate S_cont score [0, 1]
            - 'per_metric': List of ContinuityResult for each metric
            - 'anomaly_count': Number of metrics with anomalous jumps
    """
    if delta_time_seconds <= 0:
        logger.warning("Invalid delta_time (≤ 0) — returning S_cont = 0.0")
        return {"score": 0.0, "per_metric": [], "anomaly_count": 0}

    if not current_values:
        logger.warning("No current values provided — returning S_cont = 0.0")
        return {"score": 0.0, "per_metric": [], "anomaly_count": 0}

    results = []
    for metric_name, current_val in current_values.items():
        previous_val = previous_values.get(metric_name)

        if previous_val is None:
            # First observation — no derivative to compute; assume OK
            results.append(ContinuityResult(
                score=1.0,
                rate_of_change=0.0,
                is_anomalous=False,
                metric_name=metric_name,
                current_value=current_val,
                previous_value=0.0,
                delta_time=delta_time_seconds,
            ))
            continue

        # Normalize rate of change
        # For percentage-based metrics (CPU), use absolute change
        # For counter-based metrics (RPS), use rate
        absolute_change = abs(current_val - previous_val)
        rate = absolute_change / delta_time_seconds

        if rate <= max_rate:
            score = 1.0
            is_anomalous = False
        else:
            # Decay from 1.0 to 0.0 as rate goes from σ_max to σ_max * (1 + k)
            decay_range = decay_factor * max_rate
            if decay_range <= 0:
                score = 0.0
            else:
                score = max(0.0, 1.0 - (rate - max_rate) / decay_range)
            is_anomalous = True

        results.append(ContinuityResult(
            score=round(score, 4),
            rate_of_change=round(rate, 4),
            is_anomalous=is_anomalous,
            metric_name=metric_name,
            current_value=current_val,
            previous_value=previous_val,
            delta_time=delta_time_seconds,
        ))

    if not results:
        return {"score": 0.0, "per_metric": [], "anomaly_count": 0}

    # Aggregate: use the minimum score across all metrics
    # (conservative approach — one bad metric degrades trust)
    aggregate_score = min(r.score for r in results)
    anomaly_count = sum(1 for r in results if r.is_anomalous)

    if anomaly_count > 0:
        anomalous_metrics = [r.metric_name for r in results if r.is_anomalous]
        logger.warning(
            f"Continuity check: {anomaly_count} anomalous metric(s) detected "
            f"({', '.join(anomalous_metrics)}), aggregate S_cont = {aggregate_score:.4f}"
        )

    return {
        "score": round(aggregate_score, 4),
        "per_metric": results,
        "anomaly_count": anomaly_count,
    }
