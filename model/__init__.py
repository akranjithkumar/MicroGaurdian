"""
MicroGuardian Telemetry Trust Score Model Package
=================================================
Exports:
  - BaseTrustModel, TelemetrySample, ModelPrediction
  - BaselineLinearModel (Existing heuristic model)
  - NonlinearAdaptiveBayesianTrustModel (Distinct NAB model)
  - RealtimeEvaluator, SyntheticRealtimeStreamGenerator
  - PrometheusLiveStream
"""

from .base import BaseTrustModel, TelemetrySample, ModelPrediction
from .baseline_linear import BaselineLinearModel
from .distinct_model import NonlinearAdaptiveBayesianTrustModel
from .realtime_evaluator import RealtimeEvaluator, SyntheticRealtimeStreamGenerator
from .prometheus_client import PrometheusLiveStream

__all__ = [
    "BaseTrustModel",
    "TelemetrySample",
    "ModelPrediction",
    "BaselineLinearModel",
    "NonlinearAdaptiveBayesianTrustModel",
    "RealtimeEvaluator",
    "SyntheticRealtimeStreamGenerator",
    "PrometheusLiveStream",
]
