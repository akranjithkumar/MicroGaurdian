"""
Baseline Linear Trust Score Model (Existing System)
===================================================
Implements the static weighted-linear heuristic model currently used in
the initial MicroGuardian implementation:
    T_s = w_f * S_fresh + w_c * S_cont + w_v * S_valid

Default weights: w_f = 0.4, w_c = 0.3, w_v = 0.3
"""

from typing import Optional, Dict
from .base import BaseTrustModel, TelemetrySample, ModelPrediction


class BaselineLinearModel(BaseTrustModel):
    """
    Standard Linear Weighted Model (Existing baseline).
    
    Known limitations:
    1. Additive compensation: A total collapse in freshness (S_fresh = 0.0)
       is masked by high continuity (1.0) and completeness (1.0), yielding
       T_s = 0.60 (DEGRADED instead of SEVERE).
    2. Static rigid weights regardless of stream variance or noise.
    3. Zero temporal memory / lack of uncertainty estimation.
    4. Ignores multi-metric cross-validation (e.g. RPS vs CPU decoupling).
    """

    def __init__(
        self,
        name: str = "Existing_Linear_Model",
        w_freshness: float = 0.4,
        w_continuity: float = 0.3,
        w_completeness: float = 0.3,
        alpha_high: float = 0.8,
        beta_low: float = 0.4,
    ):
        super().__init__(name=name, alpha_high=alpha_high, beta_low=beta_low)
        self.w_freshness = w_freshness
        self.w_continuity = w_continuity
        self.w_completeness = w_completeness

    def evaluate(self, sample: TelemetrySample) -> ModelPrediction:
        # Linear weighted sum
        ts_raw = (
            self.w_freshness * sample.s_fresh
            + self.w_continuity * sample.s_cont
            + self.w_completeness * sample.s_valid
        )
        trust_score = max(0.0, min(1.0, ts_raw))
        
        confidence, action = self.classify_action(trust_score)
        
        # Identify minimum signal for basic diagnostics
        signals = {
            "freshness": sample.s_fresh,
            "continuity": sample.s_cont,
            "completeness": sample.s_valid,
        }
        bottleneck = min(signals, key=signals.get)
        anomaly_detected = trust_score < self.alpha_high
        
        return ModelPrediction(
            model_name=self.name,
            timestamp=sample.timestamp,
            deployment=sample.deployment,
            trust_score=trust_score,
            confidence_level=confidence,
            action=action,
            uncertainty=0.0,  # Baseline model has no uncertainty representation
            bottleneck_signal=bottleneck,
            anomaly_detected=anomaly_detected,
            weights_used={
                "freshness": self.w_freshness,
                "continuity": self.w_continuity,
                "completeness": self.w_completeness,
            },
            metadata={
                "computation": "linear_sum",
                "raw_score": round(ts_raw, 4),
            },
        )

    def reset(self, deployment: Optional[str] = None):
        # Baseline model is memoryless
        pass
