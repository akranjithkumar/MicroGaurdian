"""
Watchdog package — Telemetry Watchdog Layer for MicroGuardian.
Provides freshness, continuity, and completeness validation modules.
"""

from .freshness import evaluate_freshness
from .continuity import evaluate_continuity
from .completeness import evaluate_completeness

__all__ = [
    "evaluate_freshness",
    "evaluate_continuity",
    "evaluate_completeness",
]
