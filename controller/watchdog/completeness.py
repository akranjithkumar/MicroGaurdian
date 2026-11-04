"""
Scrape Completeness Checker (S_valid)
======================================
Verifies the absence of null values, timeout drops, or missing scrape targets
across expected pod endpoints.

Formula:
    S_valid = valid_targets / expected_targets

Where:
    valid_targets   = number of pods that returned non-null, non-zero metrics
    expected_targets = number of pods expected to report metrics (from K8s API)
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

logger = logging.getLogger("microguardian.watchdog.completeness")


@dataclass
class CompletenessResult:
    """Result of a completeness evaluation."""
    score: float                    # S_valid ∈ [0, 1]
    valid_targets: int             # Pods with valid metrics
    expected_targets: int          # Total expected pods
    missing_targets: List[str]     # Pod names that are missing
    null_value_targets: List[str]  # Pods with null/zero-fill values


def evaluate_completeness(
    expected_pods: List[str],
    scraped_metrics: Dict[str, Optional[Dict[str, float]]],
    null_threshold: float = 0.0,
) -> Dict:
    """
    Evaluate scrape completeness across expected pod targets.

    Args:
        expected_pods: List of expected pod names/identifiers
        scraped_metrics: Dict mapping pod_name → metric_dict (or None if scrape failed)
            Example: {
                "order-service-abc": {"cpu": 0.35, "memory": 128000000},
                "order-service-def": None,  # scrape failed
            }
        null_threshold: Values at or below this threshold are considered "null"
            (handles zero-filling by monitoring agents)

    Returns:
        Dictionary with:
            - 'score': S_valid [0, 1]
            - 'details': CompletenessResult with breakdown
    """
    if not expected_pods:
        logger.warning("No expected pods specified — returning S_valid = 0.0")
        return {
            "score": 0.0,
            "details": CompletenessResult(
                score=0.0,
                valid_targets=0,
                expected_targets=0,
                missing_targets=[],
                null_value_targets=[],
            ),
        }

    expected_set: Set[str] = set(expected_pods)
    scraped_set: Set[str] = set(scraped_metrics.keys())

    # Find completely missing targets (not in scraped results at all)
    missing_targets = list(expected_set - scraped_set)

    # Find targets that returned null/None or zero-filled values
    null_value_targets = []
    valid_count = 0

    for pod_name in expected_pods:
        metrics = scraped_metrics.get(pod_name)

        if metrics is None:
            # Scrape returned nothing — already counted as missing
            continue

        # Check if all metric values are null/zero-filled
        has_valid_values = False
        for metric_name, value in metrics.items():
            if value is not None and value > null_threshold:
                has_valid_values = True
                break

        if has_valid_values:
            valid_count += 1
        else:
            null_value_targets.append(pod_name)

    # Calculate score
    total_expected = len(expected_pods)
    score = valid_count / total_expected if total_expected > 0 else 0.0

    if missing_targets or null_value_targets:
        logger.info(
            f"Completeness check: {valid_count}/{total_expected} valid targets, "
            f"{len(missing_targets)} missing, {len(null_value_targets)} null-filled"
        )

    result = CompletenessResult(
        score=round(score, 4),
        valid_targets=valid_count,
        expected_targets=total_expected,
        missing_targets=missing_targets,
        null_value_targets=null_value_targets,
    )

    return {
        "score": result.score,
        "details": result,
    }
