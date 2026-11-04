"""
Temporal Freshness Checker (S_fresh)
====================================
Evaluates whether metric samples are temporally fresh — i.e., whether the
time elapsed since the sample was collected is within acceptable bounds.

Formula:
    S_fresh = 1.0                           if (t_curr - t_sample) <= τ_threshold
    S_fresh = max(0, 1 - (skew - τ) / τ_max) if (t_curr - t_sample) > τ_threshold

Where:
    t_curr      = controller's current monotonic clock
    t_sample    = timestamp of the metric sample
    τ_threshold = maximum acceptable staleness (default: 30s)
    τ_max       = staleness at which score reaches 0 (default: 120s)
"""

import time
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger("microguardian.watchdog.freshness")


@dataclass
class FreshnessResult:
    """Result of a freshness evaluation for a single metric sample."""
    score: float               # S_fresh ∈ [0, 1]
    skew_seconds: float        # Actual staleness in seconds
    is_stale: bool             # Whether sample exceeds threshold
    sample_timestamp: float    # Original sample timestamp
    evaluation_timestamp: float  # When evaluation occurred


def evaluate_freshness(
    sample_timestamps: List[float],
    threshold_seconds: float = 30.0,
    max_staleness_seconds: float = 120.0,
    current_time: Optional[float] = None,
) -> Dict:
    """
    Evaluate temporal freshness for a set of metric samples.

    Args:
        sample_timestamps: List of Unix timestamps for each metric sample
        threshold_seconds: τ_threshold — max acceptable staleness
        max_staleness_seconds: τ_max — staleness range for score decay
        current_time: Override for current time (for testing)

    Returns:
        Dictionary with:
            - 'score': Aggregate S_fresh score [0, 1]
            - 'per_sample': List of FreshnessResult for each sample
            - 'stale_count': Number of stale samples
            - 'total_count': Total number of samples evaluated
    """
    if current_time is None:
        current_time = time.time()

    if not sample_timestamps:
        logger.warning("No sample timestamps provided — returning S_fresh = 0.0")
        return {
            "score": 0.0,
            "per_sample": [],
            "stale_count": 0,
            "total_count": 0,
        }

    results = []
    for ts in sample_timestamps:
        skew = current_time - ts

        if skew <= threshold_seconds:
            score = 1.0
            is_stale = False
        else:
            # Linear decay from 1.0 to 0.0 over the range [τ, τ + τ_max]
            decay_range = max_staleness_seconds - threshold_seconds
            if decay_range <= 0:
                score = 0.0
            else:
                score = max(0.0, 1.0 - (skew - threshold_seconds) / decay_range)
            is_stale = True

        results.append(FreshnessResult(
            score=round(score, 4),
            skew_seconds=round(skew, 2),
            is_stale=is_stale,
            sample_timestamp=ts,
            evaluation_timestamp=current_time,
        ))

    # Aggregate: average freshness across all samples
    aggregate_score = sum(r.score for r in results) / len(results)
    stale_count = sum(1 for r in results if r.is_stale)

    if stale_count > 0:
        logger.info(
            f"Freshness check: {stale_count}/{len(results)} samples stale, "
            f"aggregate S_fresh = {aggregate_score:.4f}"
        )

    return {
        "score": round(aggregate_score, 4),
        "per_sample": results,
        "stale_count": stale_count,
        "total_count": len(results),
    }
