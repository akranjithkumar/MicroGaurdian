"""
Chaos Experiment Runner & Evaluator
====================================
Orchestrates comparative experiments between:
  1. Standard Kubernetes HPA (Control)
  2. MicroGuardian Telemetry-Integrity-Aware Autoscaler (Treatment)

Under 4 distinct telemetry fault scenarios:
  - E1: Scrape Delay (stale metrics)
  - E2: Packet Loss / Drops (erratic metrics)
  - E3: Telemetry Endpoint Failure / Zero-Filling (missing data)
  - E4: Metric Counter Spike (instantaneous derivative explosion)

Supports both:
  - Live Kubernetes cluster mode (Minikube / k6)
  - Synthetic simulation mode (for self-contained demos and regression tests)
"""

import os
import sys
import time
import math
import json
import random
import argparse
import logging
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple

# Add parent directory to path so controller package can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from controller.config import MicroGuardianConfig
from controller.watchdog.freshness import evaluate_freshness
from controller.watchdog.continuity import evaluate_continuity
from controller.watchdog.completeness import evaluate_completeness
from controller.trust_score import TrustScoreEngine
from controller.decision_engine import DecisionEngine
from controller.fallback import FallbackEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("microguardian.experiment")


@dataclass
class CycleRecord:
    timestamp: float
    cycle: int
    true_load_cpu: float
    observed_cpu_hpa: float
    observed_cpu_mg: float
    hpa_replicas: int
    mg_replicas: int
    mg_trust_score: float
    mg_confidence: str
    mg_action: str
    hpa_action: str
    s_fresh: float
    s_cont: float
    s_valid: float
    fault_active: bool
    fault_type: str
    hpa_slo_violated: bool
    mg_slo_violated: bool


class ExperimentSimulator:
    """
    Simulates a microservice workload, telemetry faults, and side-by-side
    actuation of Standard HPA vs MicroGuardian over a timeline.
    """

    def __init__(self, config: MicroGuardianConfig = None):
        self.config = config or MicroGuardianConfig()
        self.trust_engine = TrustScoreEngine(self.config.trust_score)
        self.fallback_engine = FallbackEngine(
            window_size=self.config.scaling.moving_average_window,
            min_replicas=self.config.scaling.min_replicas,
        )
        self.decision_engine = DecisionEngine(
            scaling_config=self.config.scaling,
            trust_config=self.config.trust_score,
            fallback_engine=self.fallback_engine,
        )

    def run(
        self,
        scenario: str = "scrape-delay",
        cycles: int = 40,
        interval_seconds: float = 15.0,
        output_file: str = "experiment_results.json",
    ) -> List[CycleRecord]:
        logger.info(f"Starting Experiment: {scenario.upper()}")
        logger.info(f"Total cycles: {cycles}, interval: {interval_seconds}s")

        records: List[CycleRecord] = []

        # Initial state
        hpa_replicas = 2
        mg_replicas = 2
        prev_mg_metrics = {"cpu": 0.3}
        start_sim_time = 100000.0
        prev_time = start_sim_time

        hpa_prev_direction = 0  # +1 up, -1 down
        mg_prev_direction = 0
        hpa_flaps = 0
        mg_flaps = 0

        # Define fault injection window (e.g., cycles 12 to 26)
        fault_start_cycle = 12
        fault_end_cycle = 26

        for c in range(1, cycles + 1):
            now = start_sim_time + (c * interval_seconds)
            fault_active = (fault_start_cycle <= c <= fault_end_cycle)

            # Ground truth demand: high traffic surge between cycle 10 and 32
            if 10 <= c <= 32:
                # Real load is HIGH (e.g. 85% CPU needed per replica baseline)
                base_demand = 0.85
            else:
                base_demand = 0.35

            # True workload CPU per replica depending on replica count
            true_cpu = min(1.0, base_demand * 2.0 / max(1, mg_replicas))

            # Simulate telemetry anomalies based on scenario
            sample_time = now
            observed_cpu_hpa = true_cpu
            observed_cpu_mg = true_cpu
            s_valid_override = 1.0

            if fault_active:
                if scenario == "scrape-delay":
                    # Scraping lag: sample is 65 seconds old (stale)
                    sample_time = now - 65.0
                    # Standard HPA receives old baseline reading (0.25) during high traffic
                    observed_cpu_hpa = 0.25
                    observed_cpu_mg = 0.25
                elif scenario == "packet-loss":
                    # 50% packet drop / jitter causes erratic spikes and drops
                    jitter = random.choice([0.15, 0.95, 0.10, 0.90])
                    observed_cpu_hpa = jitter
                    observed_cpu_mg = jitter
                elif scenario == "endpoint-failure":
                    # Prometheus times out, zero-fills CPU metric
                    observed_cpu_hpa = 0.0
                    observed_cpu_mg = 0.0
                    s_valid_override = 0.2  # 80% targets missing
                elif scenario == "counter-spike":
                    # Instantaneous impossible jump (counter reset)
                    observed_cpu_hpa = 4.80
                    observed_cpu_mg = 4.80

            # ── 1. STANDARD HPA ACTUATION (Naive Ratio) ──
            target_cpu = self.config.scaling.target_cpu_utilization
            if observed_cpu_hpa <= 0.05:
                # Zero filling causes HPA to drop to minReplicas!
                desired_hpa = self.config.scaling.min_replicas
            else:
                ratio = observed_cpu_hpa / target_cpu
                desired_hpa = math.ceil(hpa_replicas * ratio)
                desired_hpa = max(self.config.scaling.min_replicas, min(self.config.scaling.max_replicas, desired_hpa))

            hpa_action = "MAINTAIN"
            if desired_hpa > hpa_replicas:
                hpa_action = "SCALE_UP"
                if hpa_prev_direction == -1:
                    hpa_flaps += 1
                hpa_prev_direction = 1
                hpa_replicas = desired_hpa
            elif desired_hpa < hpa_replicas:
                hpa_action = "SCALE_DOWN"
                if hpa_prev_direction == 1:
                    hpa_flaps += 1
                hpa_prev_direction = -1
                hpa_replicas = desired_hpa

            # ── 2. MICROGUARDIAN ACTUATION (Watchdog + Trust Score) ──
            # Freshness
            fresh_res = evaluate_freshness(
                sample_timestamps=[sample_time],
                threshold_seconds=self.config.watchdog.freshness_threshold_seconds,
                max_staleness_seconds=self.config.watchdog.freshness_max_staleness_seconds,
                current_time=now,
            )
            s_fresh = fresh_res["score"]

            # Continuity
            delta_t = max(now - prev_time, 1.0)
            cont_res = evaluate_continuity(
                current_values={"cpu": observed_cpu_mg},
                previous_values=prev_mg_metrics,
                delta_time_seconds=delta_t,
                max_rate=self.config.watchdog.continuity_max_rate,
            )
            s_cont = cont_res["score"]
            prev_mg_metrics = {"cpu": observed_cpu_mg}
            prev_time = now

            # Completeness
            if s_valid_override < 1.0:
                s_valid = s_valid_override
            else:
                s_valid = 1.0

            # Trust Score computation
            trust_snap = self.trust_engine.compute(
                deployment="order-service",
                freshness_score=s_fresh,
                continuity_score=s_cont,
                completeness_score=s_valid,
            )

            # Decision Engine
            mg_decision = self.decision_engine.decide(
                deployment="order-service",
                trust_snapshot=trust_snap,
                current_replicas=mg_replicas,
                current_metric_value=observed_cpu_mg,
                target_metric_value=target_cpu,
                now=now,
            )

            desired_mg = mg_decision.desired_replicas
            mg_action = mg_decision.action

            if desired_mg > mg_replicas:
                if mg_prev_direction == -1:
                    mg_flaps += 1
                mg_prev_direction = 1
                mg_replicas = desired_mg
            elif desired_mg < mg_replicas:
                if mg_prev_direction == 1:
                    mg_flaps += 1
                mg_prev_direction = -1
                mg_replicas = desired_mg

            # SLO Violations: If load is high (base_demand = 0.85) but replicas < 4, SLO violated
            hpa_slo_violated = (base_demand > 0.5 and hpa_replicas < 4)
            mg_slo_violated = (base_demand > 0.5 and mg_replicas < 4)

            record = CycleRecord(
                timestamp=now,
                cycle=c,
                true_load_cpu=round(true_cpu, 3),
                observed_cpu_hpa=round(observed_cpu_hpa, 3),
                observed_cpu_mg=round(observed_cpu_mg, 3),
                hpa_replicas=hpa_replicas,
                mg_replicas=mg_replicas,
                mg_trust_score=round(trust_snap.trust_score, 3),
                mg_confidence=trust_snap.confidence_level,
                mg_action=mg_action,
                hpa_action=hpa_action,
                s_fresh=round(s_fresh, 3),
                s_cont=round(s_cont, 3),
                s_valid=round(s_valid, 3),
                fault_active=fault_active,
                fault_type=scenario if fault_active else "none",
                hpa_slo_violated=hpa_slo_violated,
                mg_slo_violated=mg_slo_violated,
            )
            records.append(record)

            status_indicator = "🔴 FAULT" if fault_active else "🟢 NORMAL"
            logger.info(
                f"[Cycle {c:02d}] {status_indicator} | "
                f"Observed: {observed_cpu_mg:4.2f} | "
                f"Ts: {trust_snap.trust_score:4.2f} ({trust_snap.confidence_level:8s}) | "
                f"HPA: {hpa_replicas} pods ({hpa_action}) | "
                f"MicroGuardian: {mg_replicas} pods ({mg_action})"
            )

        # Write results
        dict_records = [asdict(r) for r in records]
        summary = {
            "scenario": scenario,
            "total_cycles": cycles,
            "hpa_flapping_count": hpa_flaps,
            "mg_flapping_count": mg_flaps,
            "hpa_slo_violation_cycles": sum(1 for r in records if r.hpa_slo_violated),
            "mg_slo_violation_cycles": sum(1 for r in records if r.mg_slo_violated),
            "records": dict_records,
        }

        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(summary, f, indent=2)

        logger.info("\n" + "=" * 55)
        logger.info(f"EXPERIMENT SUMMARY: {scenario.upper()}")
        logger.info("=" * 55)
        logger.info(f"Standard HPA Flapping Events:        {hpa_flaps}")
        logger.info(f"MicroGuardian Flapping Events:       {mg_flaps}")
        logger.info(f"Standard HPA SLO Violation Cycles:   {summary['hpa_slo_violation_cycles']}")
        logger.info(f"MicroGuardian SLO Violation Cycles:  {summary['mg_slo_violation_cycles']}")
        logger.info(f"Results saved to: {output_file}")
        logger.info("=" * 55)

        return records


def main():
    parser = argparse.ArgumentParser(description="MicroGuardian Chaos Experiment Runner")
    parser.add_argument(
        "--scenario",
        choices=["scrape-delay", "packet-loss", "endpoint-failure", "counter-spike", "all"],
        default="scrape-delay",
        help="Telemetry fault scenario to simulate/inject",
    )
    parser.add_argument("--cycles", type=int, default=36, help="Number of reconciliation cycles")
    parser.add_argument("--output-dir", default="analysis/results", help="Directory to save experiment json")

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    sim = ExperimentSimulator()

    if args.scenario == "all":
        scenarios = ["scrape-delay", "packet-loss", "endpoint-failure", "counter-spike"]
    else:
        scenarios = [args.scenario]

    for sc in scenarios:
        out_path = os.path.join(args.output_dir, f"{sc}_results.json")
        sim.run(scenario=sc, cycles=args.cycles, output_file=out_path)


if __name__ == "__main__":
    main()
