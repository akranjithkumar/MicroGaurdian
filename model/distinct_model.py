"""
Distinct Telemetry Trust Score Model: Non-Linear Adaptive Bayesian-Entropy Model (NAB-Trust)
=============================================================================================
A novel, high-accuracy telemetry trust model designed to overcome the core vulnerabilities
of traditional static linear weighted models in Kubernetes autoscaling.

Core Innovations:
-----------------
1. Non-Linear Bottleneck T-Norm Gating (Anti-Compensation Barrier):
   Prevents compensatory masking. If a single critical signal (e.g., Freshness) collapses,
   the trust score plummets immediately rather than floating at a deceptively high 0.60.

2. Dynamic Shannon Information-Entropy & Volatility Weighting:
   Dynamically recalibrates weights based on the rolling stability (inverse-variance) of each
   telemetry stream instead of clinging to rigid static weights.

3. Recursive Bayesian Belief Updating with Epistemic Uncertainty:
   Maintains a conjugate Beta-distribution state (alpha, beta) with exponential memory decay,
   providing both a robust expected trust estimate and a rigorous uncertainty metric (variance).

4. Multivariate Semantic Coherence Verification:
   Validates cross-metric causality between RPS, CPU utilization, and P95 latency to catch
   silent sensor freezing, zero-filling, and exporter resets before simple thresholds fire.
"""

import math
from collections import deque
from typing import Dict, List, Optional, Tuple, Any

from .base import BaseTrustModel, TelemetrySample, ModelPrediction


class DeploymentState:
    """Tracks state history and Bayesian priors for a single deployment."""

    def __init__(self, window_size: int = 12):
        self.window_size = window_size
        
        # Sliding windows for watchdog signals
        self.fresh_history: deque[float] = deque(maxlen=window_size)
        self.cont_history: deque[float] = deque(maxlen=window_size)
        self.valid_history: deque[float] = deque(maxlen=window_size)
        
        # Sliding windows for cross-validation metrics
        self.cpu_history: deque[float] = deque(maxlen=window_size)
        self.rps_history: deque[float] = deque(maxlen=window_size)
        self.latency_history: deque[float] = deque(maxlen=window_size)
        
        # Bayesian Beta distribution parameters (Prior: Beta(10, 1) -> Healthy telemetry default)
        self.alpha: float = 10.0
        self.beta: float = 1.0


class NonlinearAdaptiveBayesianTrustModel(BaseTrustModel):
    """
    Distinct High-Accuracy Telemetry Trust Model.
    """

    def __init__(
        self,
        name: str = "Distinct_NAB_Model",
        alpha_high: float = 0.8,
        beta_low: float = 0.4,
        bottleneck_threshold: float = 0.45,
        bottleneck_gamma: float = 1.8,
        bayesian_decay: float = 0.82,
        prior_weight: float = 0.35,
        window_size: int = 12,
    ):
        super().__init__(name=name, alpha_high=alpha_high, beta_low=beta_low)
        self.bottleneck_threshold = bottleneck_threshold
        self.bottleneck_gamma = bottleneck_gamma
        self.bayesian_decay = bayesian_decay
        self.prior_weight = prior_weight
        self.window_size = window_size
        
        # Deployment states
        self.states: Dict[str, DeploymentState] = {}
        
        # Static baseline reference weights (used as prior)
        self.w0_fresh = 0.40
        self.w0_cont = 0.30
        self.w0_valid = 0.30

    def _get_state(self, deployment: str) -> DeploymentState:
        if deployment not in self.states:
            self.states[deployment] = DeploymentState(window_size=self.window_size)
        return self.states[deployment]

    @staticmethod
    def _variance(series: deque[float]) -> float:
        """Compute sample variance with safety against division by zero."""
        n = len(series)
        if n < 2:
            return 0.0
        mean = sum(series) / n
        return sum((x - mean) ** 2 for x in series) / (n - 1)

    def _compute_dynamic_weights(self, state: DeploymentState) -> Dict[str, float]:
        """
        Dynamically calculate weights using inverse volatility / stability.
        More stable streams receive higher confidence; noisy channels are damp-weighted.
        """
        if len(state.fresh_history) < 3:
            return {
                "freshness": self.w0_fresh,
                "continuity": self.w0_cont,
                "completeness": self.w0_valid,
            }

        var_f = self._variance(state.fresh_history)
        var_c = self._variance(state.cont_history)
        var_v = self._variance(state.valid_history)

        # Stability index: u = exp(-10 * std_dev)
        u_f = math.exp(-10.0 * math.sqrt(var_f))
        u_c = math.exp(-10.0 * math.sqrt(var_c))
        u_v = math.exp(-10.0 * math.sqrt(var_v))
        total_u = u_f + u_c + u_v

        if total_u < 1e-6:
            return {
                "freshness": self.w0_fresh,
                "continuity": self.w0_cont,
                "completeness": self.w0_valid,
            }

        dyn_f = u_f / total_u
        dyn_c = u_c / total_u
        dyn_v = u_v / total_u

        # Blend prior with data-driven dynamic weights
        alpha = self.prior_weight
        w_f = alpha * self.w0_fresh + (1.0 - alpha) * dyn_f
        w_c = alpha * self.w0_cont + (1.0 - alpha) * dyn_c
        w_v = alpha * self.w0_valid + (1.0 - alpha) * dyn_v

        # Normalize
        w_sum = w_f + w_c + w_v
        return {
            "freshness": w_f / w_sum,
            "continuity": w_c / w_sum,
            "completeness": w_v / w_sum,
        }

    def _compute_bottleneck_gate(self, s_fresh: float, s_cont: float, s_valid: float) -> Tuple[float, Optional[str]]:
        """
        Anti-Compensation Barrier:
        If any individual dimension falls below bottleneck_threshold, penalize non-linearly.
        """
        signals = {"freshness": s_fresh, "continuity": s_cont, "completeness": s_valid}
        min_signal_name = min(signals, key=signals.get)
        min_val = signals[min_signal_name]

        if min_val >= self.bottleneck_threshold:
            return 1.0, min_signal_name

        # Non-linear penalty factor: (min_val / threshold) ^ gamma
        ratio = max(0.0, min_val / self.bottleneck_threshold)
        gate = math.pow(ratio, self.bottleneck_gamma)
        return max(0.0, min(1.0, gate)), min_signal_name

    def _check_semantic_coherence(self, sample: TelemetrySample, state: DeploymentState) -> float:
        """
        Evaluates multi-variate consistency between CPU, RPS, and Latency.
        Detects decoupled anomalies (e.g. sensor zero-filling or exporter freeze).
        """
        coherence = 1.0

        # Anomaly 1: Sensor Zero-filling under load
        # Traffic is actively flowing (RPS > 35 req/s), but reported CPU is zero
        if sample.rps > 35.0 and sample.cpu_utilization <= 0.01:
            coherence *= 0.20  # Severe penalty for false zero reporting

        # Anomaly 2: Extreme Latency Congestion Decoupled from CPU
        # Latency is critically high (> 800ms), but CPU is reported as idle (< 10%)
        if sample.p95_latency_ms > 800.0 and sample.cpu_utilization < 0.10:
            coherence *= 0.35

        # Anomaly 3: Sudden derivative spike in CPU with zero change in external RPS
        if len(state.cpu_history) >= 2 and len(state.rps_history) >= 2:
            prev_cpu = state.cpu_history[-1]
            prev_rps = state.rps_history[-1]
            cpu_jump = abs(sample.cpu_utilization - prev_cpu)
            rps_jump = abs(sample.rps - prev_rps)
            
            # CPU spiked by > 50% in one tick while RPS remained unchanged (+/- 5 req/s)
            if cpu_jump > 0.50 and rps_jump < 5.0:
                coherence *= 0.50

        return max(0.0, min(1.0, coherence))

    def evaluate(self, sample: TelemetrySample) -> ModelPrediction:
        state = self._get_state(sample.deployment)

        # 1. Update sliding histories
        state.fresh_history.append(sample.s_fresh)
        state.cont_history.append(sample.s_cont)
        state.valid_history.append(sample.s_valid)
        state.cpu_history.append(sample.cpu_utilization)
        state.rps_history.append(sample.rps)
        state.latency_history.append(sample.p95_latency_ms)

        # 2. Dynamic weights via Information Entropy / Stability
        weights = self._compute_dynamic_weights(state)

        # 3. Geometric weighted base score (Geometric mean handles zero-drops naturally)
        # S_geom = S_fresh^w_f * S_cont^w_c * S_valid^w_v
        # Add tiny epsilon to avoid log(0)
        eps = 1e-4
        s_f_clamped = max(eps, min(1.0, sample.s_fresh))
        s_c_clamped = max(eps, min(1.0, sample.s_cont))
        s_v_clamped = max(eps, min(1.0, sample.s_valid))
        
        instant_score = (
            math.pow(s_f_clamped, weights["freshness"])
            * math.pow(s_c_clamped, weights["continuity"])
            * math.pow(s_v_clamped, weights["completeness"])
        )

        # 4. Anti-compensation bottleneck gate
        gate, bottleneck_signal = self._compute_bottleneck_gate(
            sample.s_fresh, sample.s_cont, sample.s_valid
        )

        # 5. Semantic cross-metric coherence
        coherence = self._check_semantic_coherence(sample, state)

        # 6. Instantaneous synthesized evidence
        # Combine geometric core with bottleneck gate and semantic coherence
        evidence = gate * (0.85 * instant_score + 0.15 * coherence)
        evidence = max(0.0, min(1.0, evidence))

        # 7. Recursive Bayesian Belief Updating
        # If strong healthy evidence arrives consecutively, accelerate beta decay for nimble recovery
        decay = self.bayesian_decay
        if evidence >= 0.90:
            state.beta *= 0.65  # Quick recovery dampening
            state.alpha = decay * state.alpha + 3.0 * evidence
        else:
            state.alpha = decay * state.alpha + 2.5 * evidence
            state.beta = decay * state.beta + 2.5 * (1.0 - evidence)

        # Bayesian Posterior Mean and Variance (Uncertainty)
        total_mass = state.alpha + state.beta
        posterior_mean = state.alpha / total_mass
        posterior_var = (state.alpha * state.beta) / (total_mass * total_mass * (total_mass + 1.0))
        uncertainty = math.sqrt(posterior_var)

        # Conservative Credible Lower Bound
        # When evidence is low, enforce lower bound; when evidence is high, trust posterior mean
        if evidence < self.alpha_high:
            distinct_trust_score = max(0.0, min(1.0, posterior_mean - 0.75 * uncertainty))
        else:
            distinct_trust_score = max(0.0, min(1.0, posterior_mean))

        # If bottleneck is severe, force rapid clamping to prevent any delay in emergency freezing
        if gate < 0.25:
            distinct_trust_score = min(distinct_trust_score, evidence)

        confidence, action = self.classify_action(distinct_trust_score)
        anomaly_detected = (distinct_trust_score < self.alpha_high) or (coherence < 0.8) or (gate < 0.8)

        return ModelPrediction(
            model_name=self.name,
            timestamp=sample.timestamp,
            deployment=sample.deployment,
            trust_score=distinct_trust_score,
            confidence_level=confidence,
            action=action,
            uncertainty=uncertainty,
            bottleneck_signal=bottleneck_signal,
            anomaly_detected=anomaly_detected,
            weights_used=weights,
            metadata={
                "bottleneck_gate": round(gate, 4),
                "semantic_coherence": round(coherence, 4),
                "bayesian_alpha": round(state.alpha, 3),
                "bayesian_beta": round(state.beta, 3),
                "posterior_mean": round(posterior_mean, 4),
                "posterior_var": round(posterior_var, 6),
                "instant_evidence": round(evidence, 4),
            },
        )

    def reset(self, deployment: Optional[str] = None):
        if deployment:
            self.states.pop(deployment, None)
        else:
            self.states.clear()
