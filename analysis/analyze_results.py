"""
Comparative Statistical Analysis
================================
Parses experiment result JSON files and computes the 4 primary evaluation metrics:
  1. SLO Violation Duration (seconds / cycles)
  2. Replica Flapping Index (directional reversals)
  3. Over-Provisioning Ratio (allocated vs utilized replica-cycles)
  4. Telemetry Resilience Score
"""

import os
import sys
import json
import glob
import argparse
from typing import Dict, Any


def analyze_file(filepath: str) -> Dict[str, Any]:
    with open(filepath, "r") as f:
        data = json.load(f)

    scenario = data.get("scenario", os.path.basename(filepath))
    records = data.get("records", [])

    if not records:
        return {}

    total_cycles = len(records)
    hpa_replicas = [r["hpa_replicas"] for r in records]
    mg_replicas = [r["mg_replicas"] for r in records]
    trust_scores = [r["mg_trust_score"] for r in records]

    # Calculate flapping
    def count_flaps(replicas_seq):
        flaps = 0
        last_dir = 0
        for i in range(1, len(replicas_seq)):
            diff = replicas_seq[i] - replicas_seq[i - 1]
            if diff > 0:
                current_dir = 1
            elif diff < 0:
                current_dir = -1
            else:
                continue

            if last_dir != 0 and current_dir != last_dir:
                flaps += 1
            last_dir = current_dir
        return flaps

    hpa_flaps = count_flaps(hpa_replicas)
    mg_flaps = count_flaps(mg_replicas)

    # SLO violation cycles
    hpa_slo = sum(1 for r in records if r.get("hpa_slo_violated", False))
    mg_slo = sum(1 for r in records if r.get("mg_slo_violated", False))

    # Over-provisioning during fault
    # Measure sum of replicas allocated during false spikes
    fault_records = [r for r in records if r.get("fault_active", False)]
    if fault_records:
        hpa_fault_alloc = sum(r["hpa_replicas"] for r in fault_records)
        mg_fault_alloc = sum(r["mg_replicas"] for r in fault_records)
    else:
        hpa_fault_alloc = sum(hpa_replicas)
        mg_fault_alloc = sum(mg_replicas)

    avg_trust = sum(trust_scores) / total_cycles

    return {
        "scenario": scenario,
        "total_cycles": total_cycles,
        "hpa_flaps": hpa_flaps,
        "mg_flaps": mg_flaps,
        "hpa_slo_cycles": hpa_slo,
        "mg_slo_cycles": mg_slo,
        "hpa_fault_alloc": hpa_fault_alloc,
        "mg_fault_alloc": mg_fault_alloc,
        "avg_trust": round(avg_trust, 3),
    }


def main():
    parser = argparse.ArgumentParser(description="Analyze MicroGuardian experiment data")
    parser.add_argument(
        "--results-dir",
        default="analysis/results",
        help="Path to results directory containing json files",
    )
    args = parser.parse_args()

    files = glob.glob(os.path.join(args.results_dir, "*_results.json"))

    if not files:
        print(f"No result JSON files found in {args.results_dir}.")
        print("Run experiments first using: python chaos/run_experiment.py --scenario all")
        return

    print("\n" + "=" * 90)
    print("MICROGUARDIAN EMPIRICAL EVALUATION SUMMARY (Phase 3 Results)")
    print("=" * 90)
    print(
        f"{'Scenario':<20} | {'Metric':<18} | {'Standard HPA':<14} | "
        f"{'MicroGuardian':<14} | {'Improvement':<12}"
    )
    print("-" * 90)

    for f in sorted(files):
        metrics = analyze_file(f)
        if not metrics:
            continue

        sc = metrics["scenario"]
        # Flapping
        flap_red = (
            f"{-100 if metrics['hpa_flaps'] == 0 else round((metrics['hpa_flaps'] - metrics['mg_flaps']) / max(1, metrics['hpa_flaps']) * 100, 1)}%"
            if metrics["hpa_flaps"] > 0
            else "0%"
        )
        print(
            f"{sc:<20} | {'Flapping Events':<18} | {metrics['hpa_flaps']:<14} | "
            f"{metrics['mg_flaps']:<14} | {flap_red:<12}"
        )

        # SLO Violations
        slo_red = (
            f"{-100 if metrics['hpa_slo_cycles'] == 0 else round((metrics['hpa_slo_cycles'] - metrics['mg_slo_cycles']) / max(1, metrics['hpa_slo_cycles']) * 100, 1)}%"
            if metrics["hpa_slo_cycles"] > 0
            else "0%"
        )
        print(
            f"{'':<20} | {'SLO Violations':<18} | {metrics['hpa_slo_cycles']:<14} | "
            f"{metrics['mg_slo_cycles']:<14} | {slo_red:<12}"
        )

        # Allocated Replicas during fault
        print(
            f"{'':<20} | {'Fault Alloc Pod-c':<18} | {metrics['hpa_fault_alloc']:<14} | "
            f"{metrics['mg_fault_alloc']:<14} | Avg Ts: {metrics['avg_trust']}"
        )
        print("-" * 90)

    print("=" * 90)


if __name__ == "__main__":
    main()
