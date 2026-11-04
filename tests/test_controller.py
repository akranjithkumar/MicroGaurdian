"""
Unit Tests for MicroGuardian Controller Components
===================================================
Validates the mathematical properties and decision logic of:
  - Temporal Freshness (S_fresh)
  - Derivative Continuity (S_cont)
  - Scrape Completeness (S_valid)
  - Telemetry Trust Score (T_s)
  - Multi-state Decision Engine (Scale, Freeze, Fallback)
"""

import sys
import os
import unittest
import time

# Ensure project root is on PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from controller.config import MicroGuardianConfig, TrustScoreConfig, WatchdogConfig
from controller.watchdog.freshness import evaluate_freshness
from controller.watchdog.continuity import evaluate_continuity
from controller.watchdog.completeness import evaluate_completeness
from controller.trust_score import TrustScoreEngine
from controller.decision_engine import DecisionEngine
from controller.fallback import FallbackEngine


class TestWatchdogLayer(unittest.TestCase):

    def test_freshness_fresh_samples(self):
        now = 1000.0
        # Samples collected 10s and 20s ago (within threshold 30s)
        samples = [now - 10.0, now - 20.0]
        res = evaluate_freshness(samples, threshold_seconds=30.0, max_staleness_seconds=120.0, current_time=now)
        self.assertEqual(res["score"], 1.0)
        self.assertEqual(res["stale_count"], 0)

    def test_freshness_decay_on_stale(self):
        now = 1000.0
        # Sample collected 75s ago (exceeds 30s threshold, halfway to 120s max)
        samples = [now - 75.0]
        res = evaluate_freshness(samples, threshold_seconds=30.0, max_staleness_seconds=120.0, current_time=now)
        # Expected: 1 - (75 - 30)/(120 - 30) = 1 - 45/90 = 0.5
        self.assertAlmostEqual(res["score"], 0.5, places=2)
        self.assertEqual(res["stale_count"], 1)

    def test_continuity_smooth_change(self):
        # Change in CPU is 0.1 over 10s => rate = 0.01 <= 5.0 max
        current = {"cpu": 0.5}
        prev = {"cpu": 0.4}
        res = evaluate_continuity(current, prev, delta_time_seconds=10.0, max_rate=5.0)
        self.assertEqual(res["score"], 1.0)
        self.assertEqual(res["anomaly_count"], 0)

    def test_continuity_spike_decay(self):
        # Instant spike: 50.0 jump over 1s => rate = 50.0 >> 5.0
        current = {"cpu": 55.0}
        prev = {"cpu": 5.0}
        res = evaluate_continuity(current, prev, delta_time_seconds=1.0, max_rate=5.0, decay_factor=3.0)
        self.assertLess(res["score"], 0.1)
        self.assertEqual(res["anomaly_count"], 1)

    def test_completeness_all_present(self):
        expected = ["pod-1", "pod-2"]
        scraped = {
            "pod-1": {"cpu": 0.4},
            "pod-2": {"cpu": 0.6},
        }
        res = evaluate_completeness(expected, scraped)
        self.assertEqual(res["score"], 1.0)

    def test_completeness_missing_or_null(self):
        expected = ["pod-1", "pod-2"]
        # pod-2 is zero-filled / missing
        scraped = {
            "pod-1": {"cpu": 0.4},
            "pod-2": {"cpu": 0.0},
        }
        res = evaluate_completeness(expected, scraped)
        self.assertEqual(res["score"], 0.5)


class TestTrustScoreAndDecisions(unittest.TestCase):

    def setUp(self):
        self.cfg = MicroGuardianConfig()
        self.trust_engine = TrustScoreEngine(self.cfg.trust_score)
        self.fallback = FallbackEngine(window_size=5, min_replicas=2)
        self.decision_engine = DecisionEngine(
            scaling_config=self.cfg.scaling,
            trust_config=self.cfg.trust_score,
            fallback_engine=self.fallback,
        )

    def test_high_confidence_scaling(self):
        # Perfect scores -> Ts = 1.0 -> High confidence -> Scale
        snap = self.trust_engine.compute("order-service", 1.0, 1.0, 1.0)
        self.assertEqual(snap.trust_score, 1.0)
        self.assertEqual(snap.confidence_level, "HIGH")

        # Observed 0.8 CPU, target 0.5, current replicas 2 -> desired = ceil(2 * 0.8 / 0.5) = 4
        dec = self.decision_engine.decide(
            deployment="order-service",
            trust_snapshot=snap,
            current_replicas=2,
            current_metric_value=0.8,
            target_metric_value=0.5,
        )
        self.assertEqual(dec.action, "SCALE")
        self.assertEqual(dec.desired_replicas, 4)

    def test_degraded_confidence_freezes_replicas(self):
        # Degraded trust: Ts around 0.6 (between 0.4 and 0.8)
        # S_fresh=0.5, S_cont=0.8, S_valid=0.5 => 0.4*0.5 + 0.3*0.8 + 0.3*0.5 = 0.2 + 0.24 + 0.15 = 0.59
        snap = self.trust_engine.compute("order-service", 0.5, 0.8, 0.5)
        self.assertEqual(snap.confidence_level, "DEGRADED")

        dec = self.decision_engine.decide(
            deployment="order-service",
            trust_snapshot=snap,
            current_replicas=4,
            current_metric_value=0.1,  # Metric falsely drops
        )
        self.assertEqual(dec.action, "FREEZE")
        self.assertEqual(dec.desired_replicas, 4)  # Maintained at 4 despite metric drop!

    def test_severe_telemetry_loss_uses_fallback(self):
        # Record some healthy history first
        self.fallback.record_replica_count("order-service", 5)
        self.fallback.record_replica_count("order-service", 5)

        # Severe loss: Ts < 0.4
        snap = self.trust_engine.compute("order-service", 0.1, 0.2, 0.1)
        self.assertEqual(snap.confidence_level, "SEVERE")

        dec = self.decision_engine.decide(
            deployment="order-service",
            trust_snapshot=snap,
            current_replicas=2,
            current_metric_value=0.0,
        )
        self.assertEqual(dec.action, "FALLBACK")
        self.assertEqual(dec.desired_replicas, 5)  # Historical moving average preserved!


if __name__ == "__main__":
    unittest.main()
