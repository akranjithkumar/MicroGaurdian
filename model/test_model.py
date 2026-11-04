"""
Unit Tests for Trust Score Models (Baseline and Distinct NAB Model)
===================================================================
Tests mathematical properties, edge cases, bounds, and detection accuracy.
"""

import sys
import os
import unittest
import math

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from model.base import TelemetrySample, ModelPrediction
from model.baseline_linear import BaselineLinearModel
from model.distinct_model import NonlinearAdaptiveBayesianTrustModel


class TestTrustScoreModels(unittest.TestCase):

    def setUp(self):
        self.baseline = BaselineLinearModel(name="Test_Linear")
        self.distinct = NonlinearAdaptiveBayesianTrustModel(name="Test_NAB")

    def test_healthy_telemetry_high_confidence(self):
        """When all signals are 1.0, both models should produce High Confidence (SCALE)."""
        sample = TelemetrySample(
            timestamp=1000.0,
            deployment="order-service",
            s_fresh=1.0,
            s_cont=1.0,
            s_valid=1.0,
            cpu_utilization=0.45,
            rps=80.0,
        )
        pred_b = self.baseline.evaluate(sample)
        pred_d = self.distinct.evaluate(sample)

        self.assertAlmostEqual(pred_b.trust_score, 1.0, places=2)
        self.assertEqual(pred_b.action, "SCALE")

        self.assertGreaterEqual(pred_d.trust_score, 0.80)
        self.assertEqual(pred_d.action, "SCALE")
        self.assertFalse(pred_d.anomaly_detected)

    def test_anti_compensation_bottleneck(self):
        """
        Critical vulnerability test:
        When Freshness drops to 0.0 (data is completely stale), but continuity
        and completeness are 1.0:
        - Baseline linear model fails: gives 0.60 (DEGRADED)
        - Distinct NAB model succeeds: non-linear gate drops score to < 0.25 (SEVERE/FALLBACK)
        """
        sample = TelemetrySample(
            timestamp=1000.0,
            deployment="order-service",
            s_fresh=0.0,
            s_cont=1.0,
            s_valid=1.0,
            cpu_utilization=0.45,
            rps=80.0,
            ground_truth_fault=True,
        )
        pred_b = self.baseline.evaluate(sample)
        pred_d = self.distinct.evaluate(sample)

        # Baseline linear model compensation flaw
        self.assertAlmostEqual(pred_b.trust_score, 0.60, places=2)
        self.assertEqual(pred_b.action, "FREEZE")  # Does not trigger emergency fallback!

        # Distinct NAB model catches single-point failure via bottleneck gating
        self.assertLess(pred_d.trust_score, 0.35)
        self.assertEqual(pred_d.action, "FALLBACK")
        self.assertTrue(pred_d.anomaly_detected)
        self.assertEqual(pred_d.bottleneck_signal, "freshness")

    def test_bounds_and_zero_division(self):
        """Ensure score strictly lies within [0.0, 1.0] under extreme values."""
        extreme_samples = [
            (0.0, 0.0, 0.0),
            (1.0, 1.0, 1.0),
            (0.0001, 0.9999, 0.5),
            (1e-9, 1e-9, 1e-9),
        ]
        for f, c, v in extreme_samples:
            sample = TelemetrySample(
                timestamp=1000.0,
                deployment="order-service",
                s_fresh=f,
                s_cont=c,
                s_valid=v,
            )
            pred_b = self.baseline.evaluate(sample)
            pred_d = self.distinct.evaluate(sample)

            self.assertGreaterEqual(pred_b.trust_score, 0.0)
            self.assertLessEqual(pred_b.trust_score, 1.0)
            self.assertGreaterEqual(pred_d.trust_score, 0.0)
            self.assertLessEqual(pred_d.trust_score, 1.0)
            self.assertGreaterEqual(pred_d.uncertainty, 0.0)

    def test_semantic_coherence_zero_filling(self):
        """
        When traffic is high (80 RPS) but reported CPU is 0.0 (sensor zero-filling),
        the distinct model should flag semantic decouple anomaly.
        """
        sample = TelemetrySample(
            timestamp=1000.0,
            deployment="order-service",
            s_fresh=1.0,
            s_cont=1.0,
            s_valid=1.0,
            cpu_utilization=0.0,   # Rotten zero
            rps=85.0,              # High traffic
        )
        pred_d = self.distinct.evaluate(sample)
        self.assertLess(pred_d.metadata["semantic_coherence"], 0.5)
        self.assertTrue(pred_d.anomaly_detected)

    def test_dynamic_weight_adaptation(self):
        """
        When one signal fluctuates with high variance while others are rock solid,
        dynamic entropy weighting should penalize the unstable signal.
        """
        # Feed 6 samples with fluctuating continuity
        for i in range(6):
            cont_val = 0.2 if i % 2 == 0 else 0.9
            sample = TelemetrySample(
                timestamp=1000.0 + i,
                deployment="order-service",
                s_fresh=1.0,
                s_cont=cont_val,
                s_valid=1.0,
            )
            pred_d = self.distinct.evaluate(sample)

        weights = pred_d.weights_used
        # Continuity should receive lower weight than fresh/valid due to high instability
        self.assertLess(weights["continuity"], weights["freshness"])
        self.assertAlmostEqual(sum(weights.values()), 1.0, places=4)

    def test_reset_clears_state(self):
        """Reset should clear deployment history."""
        sample = TelemetrySample(timestamp=1000.0, deployment="order-service", s_fresh=0.1, s_cont=0.1, s_valid=0.1)
        self.distinct.evaluate(sample)
        self.assertIn("order-service", self.distinct.states)

        self.distinct.reset("order-service")
        self.assertNotIn("order-service", self.distinct.states)


if __name__ == "__main__":
    unittest.main()
