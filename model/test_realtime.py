"""
Interactive Realtime Testing Tool for Trust Score Models
========================================================
Streams realtime telemetry data (either live from Prometheus or from the
high-fidelity synthetic generator) and compares the Existing Linear Model
against the Distinct NAB Model side-by-side with live terminal metrics.

Usage:
------
# Run 30 seconds of live simulated stream:
python model/test_realtime.py --seconds 30

# Run with an automated fault injected midway:
python model/test_realtime.py --seconds 40 --inject-fault scrape-delay --fault-at 10

# Test against live Prometheus cluster if running:
python model/test_realtime.py --prometheus-url http://localhost:9090

# Fast evaluation mode (no sleep delay):
python model/test_realtime.py --fast --seconds 100
"""

import sys
import os
import time
import argparse
from typing import Optional

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from model.base import TelemetrySample
from model.baseline_linear import BaselineLinearModel
from model.distinct_model import NonlinearAdaptiveBayesianTrustModel
from model.realtime_evaluator import RealtimeEvaluator, SyntheticRealtimeStreamGenerator
from model.prometheus_client import PrometheusLiveStream


def format_bar(val: float, width: int = 12) -> str:
    """ASCII progress bar [====    ]."""
    filled = int(round(val * width))
    filled = max(0, min(width, filled))
    return f"[{'=' * filled}{' ' * (width - filled)}]"


def run_realtime_test(
    duration_seconds: int = 30,
    interval_seconds: float = 1.0,
    fault_type: Optional[str] = None,
    fault_at_tick: int = 10,
    fault_duration: int = 10,
    prometheus_url: Optional[str] = None,
    fast_mode: bool = False,
):
    print("=" * 96)
    print("   MICROGUARDIAN REAL-TIME TRUST SCORE MODEL EVALUATION & STREAMING MONITOR")
    print("=" * 96)

    # Initialize models and evaluator
    baseline = BaselineLinearModel(name="Existing_Linear")
    distinct = NonlinearAdaptiveBayesianTrustModel(name="Distinct_NAB")
    evaluator = RealtimeEvaluator(baseline_model=baseline, distinct_model=distinct)

    # Determine stream source
    prom_stream: Optional[PrometheusLiveStream] = None
    if prometheus_url:
        prom_stream = PrometheusLiveStream(prometheus_url=prometheus_url)
        if prom_stream.is_connected():
            print(f"[INFO] Successfully connected to live Prometheus at: {prometheus_url}")
        else:
            print(f"[WARN] Prometheus at {prometheus_url} unreachable. Falling back to High-Fidelity Synthetic Stream.")
            prom_stream = None

    generator = SyntheticRealtimeStreamGenerator(deployment="order-service")

    print(f"Mode: {'LIVE PROMETHEUS' if prom_stream else 'SYNTHETIC REALTIME STREAM'}")
    print(f"Duration: {duration_seconds} ticks | Interval: {0 if fast_mode else interval_seconds}s")
    if fault_type and not prom_stream:
        print(f"Scheduled Fault: [{fault_type}] at tick {fault_at_tick} for {fault_duration} ticks")
    print("-" * 96)
    print(f"{'Tick':<5} | {'Fault':<12} | {'Signals (F/C/V)':<17} | {'Existing Model':<23} | {'Distinct NAB Model':<28}")
    print("-" * 96)

    for tick in range(1, duration_seconds + 1):
        # Trigger fault if scheduled
        if fault_type and tick == fault_at_tick and not prom_stream:
            generator.trigger_fault(fault_type, duration_ticks=fault_duration)

        # Get next sample
        if prom_stream:
            sample = prom_stream.fetch_live_sample()
            if sample is None:
                sample = generator.next_sample()
        else:
            sample = generator.next_sample()

        pred_base, pred_dist = evaluator.evaluate_sample(sample)

        # Format display strings
        fault_str = sample.fault_type if sample.ground_truth_fault else "healthy"
        signals_str = f"{sample.s_fresh:.2f} / {sample.s_cont:.2f} / {sample.s_valid:.2f}"
        
        base_str = f"Ts={pred_base.trust_score:.2f} ({pred_base.action:<8})"
        dist_str = f"Ts={pred_dist.trust_score:.2f} ({pred_dist.action:<8}) +/-{pred_dist.uncertainty:.2f}"
        
        # Colorize or mark significant discrepancies
        marker = ""
        if pred_base.action != pred_dist.action:
            if sample.ground_truth_fault and pred_base.action == "SCALE":
                marker = " <-- [ALERT: Base blindly trusts corrupted telemetry!]"
            elif sample.ground_truth_fault and pred_dist.action in ("FREEZE", "FALLBACK"):
                marker = " <-- [SAFE: Distinct model gated unsafe scaling!]"

        print(f"{tick:<5} | {fault_str:<12} | {signals_str:<17} | {base_str:<23} | {dist_str:<28}{marker}")

        if not fast_mode:
            time.sleep(interval_seconds)

    # Print summary metrics
    summary = evaluator.get_summary_comparison()
    print("=" * 96)
    print("   REAL-TIME STREAMING TEST COMPLETED -- PERFORMANCE SUMMARY")
    print("=" * 96)
    print(f"Total Ticks Evaluated:     {summary['total_samples']}")
    print("-" * 96)
    print(f"{'Metric':<28} | {'Existing Linear Model':<22} | {'Distinct NAB Model':<22}")
    print("-" * 96)
    print(f"{'Autoscaling Accuracy':<28} | {summary['baseline']['accuracy']:<22}% | {summary['distinct']['accuracy']:<22}%")
    print(f"{'Fault Detection Recall':<28} | {summary['baseline']['recall']:<22}% | {summary['distinct']['recall']:<22}%")
    print(f"{'Precision (Trustworthiness)':<28} | {summary['baseline']['precision']:<22}% | {summary['distinct']['precision']:<22}%")
    print(f"{'F1-Score':<28} | {summary['baseline']['f1_score']:<22}% | {summary['distinct']['f1_score']:<22}%")
    print(f"{'Root Mean Square Error':<28} | {summary['baseline']['rmse']:<22} | {summary['distinct']['rmse']:<22}")
    print(f"{'Unsafe Blind Scales (FN)':<28} | {summary['baseline']['false_negatives']:<22} | {summary['distinct']['false_negatives']:<22}")
    print(f"{'Actuation Flapping Events':<28} | {summary['baseline']['flapping_count']:<22} | {summary['distinct']['flapping_count']:<22}")
    print("=" * 96)
    print(f"  * Accuracy Gain:          +{summary['improvement']['accuracy_gain_pct']}%")
    print(f"  * F1-Score Gain:          +{summary['improvement']['f1_gain_pct']}%")
    print(f"  * Error (RMSE) Reduction:  {summary['improvement']['rmse_reduction_pct']}%")
    print("=" * 96)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Realtime Trust Score Model Testing Suite")
    parser.add_argument("--seconds", type=int, default=25, help="Total duration in seconds / ticks")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds per tick interval")
    parser.add_argument("--inject-fault", type=str, default="scrape-delay",
                        choices=["scrape-delay", "counter-spike", "endpoint-failure", "zero-fill", "multi-fault", "none"],
                        help="Fault scenario to inject")
    parser.add_argument("--fault-at", type=int, default=8, help="Tick at which to inject fault")
    parser.add_argument("--fault-duration", type=int, default=8, help="How many ticks fault remains active")
    parser.add_argument("--prometheus-url", type=str, default=None, help="Live Prometheus URL (optional)")
    parser.add_argument("--fast", action="store_true", help="Run without sleep delay for instant evaluation")

    args = parser.parse_args()
    fault_arg = None if args.inject_fault == "none" else args.inject_fault

    run_realtime_test(
        duration_seconds=args.seconds,
        interval_seconds=args.interval,
        fault_type=fault_arg,
        fault_at_tick=args.fault_at,
        fault_duration=args.fault_duration,
        prometheus_url=args.prometheus_url,
        fast_mode=args.fast,
    )
