"""
Base Interfaces and Data Structures for Trust Score Models
==========================================================
Defines the standard abstraction for telemetry trust scoring,
allowing seamless pluggability, side-by-side evaluation, and
standardized metric reporting across both baseline and advanced models.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import time


@dataclass
class TelemetrySample:
    """
    Standardized telemetry sample captured at a single reconciliation tick.
    """
    timestamp: float
    deployment: str
    
    # Watchdog validation dimensions (each in [0, 1])
    s_fresh: float
    s_cont: float
    s_valid: float
    
    # Raw operational telemetry metrics
    cpu_utilization: float = 0.0          # e.g., 0.45 = 45%
    rps: float = 0.0                      # requests per second
    memory_mb: float = 0.0                # memory usage in MB
    p95_latency_ms: float = 0.0           # 95th percentile latency
    error_rate_pct: float = 0.0           # error rate percentage
    current_replicas: int = 2             # current pod count
    
    # Ground truth (used during benchmarking/realtime testing if known)
    ground_truth_fault: bool = False      # True if sensor fault is actually active
    fault_type: str = "none"              # e.g., "scrape-delay", "counter-spike", "zero-fill"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "deployment": self.deployment,
            "s_fresh": self.s_fresh,
            "s_cont": self.s_cont,
            "s_valid": self.s_valid,
            "cpu_utilization": self.cpu_utilization,
            "rps": self.rps,
            "memory_mb": self.memory_mb,
            "p95_latency_ms": self.p95_latency_ms,
            "error_rate_pct": self.error_rate_pct,
            "current_replicas": self.current_replicas,
            "ground_truth_fault": self.ground_truth_fault,
            "fault_type": self.fault_type,
        }


@dataclass
class ModelPrediction:
    """
    Unified prediction output from any Trust Score model.
    """
    model_name: str
    timestamp: float
    deployment: str
    
    # Primary Trust Score in [0, 1]
    trust_score: float
    
    # Actionable Gating Decision
    # HIGH (T_s >= 0.8): Safe to scale
    # DEGRADED (0.4 <= T_s < 0.8): Freeze replicas
    # SEVERE (T_s < 0.4): Fallback (Moving average / Safe floor)
    confidence_level: str
    action: str                           # "SCALE", "FREEZE", "FALLBACK"
    
    # Advanced Statistical & Explainability Signals
    uncertainty: float = 0.0              # Epistemic variance / confidence spread
    bottleneck_signal: Optional[str] = None # Which signal pulled the score down (e.g., "freshness")
    anomaly_detected: bool = False        # Whether model detected an anomaly
    weights_used: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "timestamp": self.timestamp,
            "deployment": self.deployment,
            "trust_score": round(self.trust_score, 4),
            "confidence_level": self.confidence_level,
            "action": self.action,
            "uncertainty": round(self.uncertainty, 4),
            "bottleneck_signal": self.bottleneck_signal,
            "anomaly_detected": self.anomaly_detected,
            "weights_used": {k: round(v, 4) for k, v in self.weights_used.items()},
            "metadata": self.metadata,
        }


class BaseTrustModel(ABC):
    """
    Abstract base class for all telemetry trust score models.
    """
    
    def __init__(self, name: str, alpha_high: float = 0.8, beta_low: float = 0.4):
        self.name = name
        self.alpha_high = alpha_high
        self.beta_low = beta_low
    
    @abstractmethod
    def evaluate(self, sample: TelemetrySample) -> ModelPrediction:
        """
        Evaluate a telemetry sample and compute the trust score prediction.
        """
        pass
    
    def classify_action(self, trust_score: float) -> tuple[str, str]:
        """
        Map a trust score to confidence level and autoscaling action.
        """
        if trust_score >= self.alpha_high:
            return "HIGH", "SCALE"
        elif trust_score >= self.beta_low:
            return "DEGRADED", "FREEZE"
        else:
            return "SEVERE", "FALLBACK"
    
    @abstractmethod
    def reset(self, deployment: Optional[str] = None):
        """
        Reset model internal history/state.
        """
        pass
