"""
Plotting and Visualization Generator
====================================
Generates visual comparison charts between Standard HPA and MicroGuardian:
  - Metric stream (True vs Corrupted observed)
  - Telemetry Trust Score T_s(t) and Confidence Bands (α, β)
  - Replica counts (Standard HPA vs MicroGuardian)
"""

import os
import sys
import json
import glob
import argparse


def plot_scenario(filepath: str, output_image: str = None):
    with open(filepath, "r") as f:
        data = json.load(f)

    scenario = data.get("scenario", "Experiment")
    records = data.get("records", [])
    if not records:
        print(f"No records in {filepath}")
        return

    cycles = [r["cycle"] for r in records]
    obs_cpu = [r["observed_cpu_mg"] for r in records]
    hpa_rep = [r["hpa_replicas"] for r in records]
    mg_rep = [r["mg_replicas"] for r in records]
    trust_scores = [r["mg_trust_score"] for r in records]
    faults = [r["fault_active"] for r in records]

    try:
        import matplotlib
        matplotlib.use("Agg")  # Non-interactive backend
        import matplotlib.pyplot as plt

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
        fig.suptitle(f"MicroGuardian vs Standard HPA: {scenario.upper()}", fontsize=14, fontweight="bold")

        # 1. Observed CPU Metric
        ax1.plot(cycles, obs_cpu, color="#2b5c8f", marker="o", markersize=3, label="Observed CPU Signal")
        ax1.axhline(0.5, color="gray", linestyle="--", alpha=0.7, label="Target Utilization (50%)")
        ax1.set_ylabel("Observed Metric")
        ax1.grid(True, alpha=0.3)
        ax1.legend(loc="upper right")

        # Highlight fault region
        fault_cycles = [c for c, f in zip(cycles, faults) if f]
        if fault_cycles:
            ax1.axvspan(min(fault_cycles), max(fault_cycles), color="#ffcccc", alpha=0.4, label="Fault Active")
            ax2.axvspan(min(fault_cycles), max(fault_cycles), color="#ffcccc", alpha=0.4)
            ax3.axvspan(min(fault_cycles), max(fault_cycles), color="#ffcccc", alpha=0.4)

        # 2. Telemetry Trust Score
        ax2.plot(cycles, trust_scores, color="#008080", marker="s", markersize=3, linewidth=2, label="Trust Score T_s(t)")
        ax2.axhline(0.8, color="#28a745", linestyle="--", label="High Confidence α (0.8)")
        ax2.axhline(0.4, color="#dc3545", linestyle="--", label="Severe Fault β (0.4)")
        ax2.set_ylabel("Trust Score [0, 1]")
        ax2.set_ylim(-0.05, 1.05)
        ax2.grid(True, alpha=0.3)
        ax2.legend(loc="lower left")

        # 3. Replica Count Comparison
        ax3.step(cycles, hpa_rep, where="post", color="#e63946", linestyle="-.", linewidth=2, label="Standard HPA")
        ax3.step(cycles, mg_rep, where="post", color="#1d3557", linewidth=2.5, label="MicroGuardian (Ours)")
        ax3.set_xlabel("Reconciliation Cycle (15s intervals)")
        ax3.set_ylabel("Replica Count")
        ax3.grid(True, alpha=0.3)
        ax3.legend(loc="upper right")

        plt.tight_layout()
        if output_image is None:
            output_image = filepath.replace(".json", ".png")
        plt.savefig(output_image, dpi=200)
        plt.close()
        print(f"Chart saved to {output_image}")

    except ImportError:
        print("[NOTE] matplotlib is not installed. Text summary of comparison:")
        print(f"Scenario: {scenario}")
        print(f"Cycle | Ts   | HPA Reps | MG Reps | Fault")
        for r in records[:15]:
            f_str = "YES" if r['fault_active'] else "NO"
            print(f"{r['cycle']:02d}    | {r['mg_trust_score']:.2f} | {r['hpa_replicas']}        | {r['mg_replicas']}       | {f_str}")


def main():
    parser = argparse.ArgumentParser(description="Generate comparison charts")
    parser.add_argument("--results-dir", default="analysis/results", help="Directory with results")
    args = parser.parse_args()

    files = glob.glob(os.path.join(args.results_dir, "*_results.json"))
    for f in sorted(files):
        plot_scenario(f)


if __name__ == "__main__":
    main()
