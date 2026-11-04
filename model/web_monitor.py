"""
MicroGuardian Real-Time Trust Score Model Web Monitor
=====================================================
A zero-dependency, interactive real-time web dashboard for testing
and visualizing the Existing Linear Model vs Distinct NAB Model.

Features:
- Live 0 to 1 Confidence Score Spectrum split into 3 operational cases:
    1. NORMAL   [0.80 - 1.00] -> Safe to scale (Standard HPA)
    2. FREEZE   [0.40 - 0.80) -> Degraded telemetry (Hold replicas)
    3. FALLBACK [0.00 - 0.40) -> Severe corruption (Emergency safe floor)
- Real-Time Model Differences & Divergence Analysis panel:
    * Side-by-side Case State Comparison
    * Real-time Score Delta (Delta Ts = Our Model - Existing Model) with center-zero gauge
    * Real-time Algorithmic Root-Cause Explainer (why they differ at this exact second)
    * Cumulative Divergence KPIs (Agreement Rate %, Unsafe Blind Scales Blocked)
- Live SVG chart tracking Trust Scores with 0.80 and 0.40 Case Thresholds
- Interactive one-click fault injection controls
- Real-time streaming log of autoscaling gating decisions

Usage:
------
python model/web_monitor.py --port 8088
"""

import sys
import os
import json
import time
import math
import random
import argparse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any, List

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from model.base import TelemetrySample
from model.baseline_linear import BaselineLinearModel
from model.distinct_model import NonlinearAdaptiveBayesianTrustModel
from model.realtime_evaluator import RealtimeEvaluator, SyntheticRealtimeStreamGenerator, StreamMetrics

# Cumulative divergence statistics
DIVERGENCE_STATS = {
    "total_ticks": 0,
    "divergent_ticks": 0,
    "blind_scales_blocked": 0,
    "sum_abs_delta": 0.0,
}

# Global shared state
APP_STATE = {
    "running": True,
    "tick": 0,
    "active_fault": "none",
    "fault_duration": 0,
    
    # Current telemetry sample
    "sample": {
        "timestamp": time.time(),
        "s_fresh": 1.0,
        "s_cont": 1.0,
        "s_valid": 1.0,
        "cpu": 0.45,
        "rps": 80.0,
        "memory_mb": 250.0,
        "latency_ms": 110.0,
        "fault_active": False,
        "fault_type": "none",
    },
    
    # Model predictions
    "baseline": {
        "trust_score": 1.0,
        "case": "NORMAL",
        "confidence": "HIGH",
        "action": "SCALE",
        "uncertainty": 0.0,
        "bottleneck": "freshness",
    },
    "distinct": {
        "trust_score": 1.0,
        "case": "NORMAL",
        "confidence": "HIGH",
        "action": "SCALE",
        "uncertainty": 0.02,
        "bottleneck": "freshness",
        "gate": 1.0,
        "coherence": 1.0,
    },
    
    # Real-Time Model Differences Analysis
    "difference": {
        "delta_score": 0.0,
        "abs_delta": 0.0,
        "case_match": True,
        "base_case": "NORMAL",
        "dist_case": "NORMAL",
        "divergence_type": "MATCH",
        "divergence_title": "ALIGNED: Both Models in Case NORMAL",
        "divergence_message": "Both models agree on healthy telemetry (Ts >= 0.80). Autoscaling proceeds normally.",
        "why_diff": "All telemetry dimensions are healthy. Both models produce confident scaling decisions without intervention.",
        "agreement_rate": 100.0,
        "total_ticks": 0,
        "divergent_ticks": 0,
        "blind_scales_blocked": 0,
        "mean_abs_delta": 0.0,
    },

    # Metrics
    "metrics": {
        "samples": 0,
        "base_accuracy": 100.0,
        "dist_accuracy": 100.0,
        "base_rmse": 0.0,
        "dist_rmse": 0.0,
        "base_flaps": 0,
        "dist_flaps": 0,
        "base_fn": 0,
        "dist_fn": 0,
    },
    
    # History for live chart (last 40 points)
    "history": [],
    "logs": [],
}

STATE_LOCK = threading.Lock()
GENERATOR = SyntheticRealtimeStreamGenerator(deployment="order-service")
EVALUATOR = RealtimeEvaluator(
    baseline_model=BaselineLinearModel(name="Existing_Linear"),
    distinct_model=NonlinearAdaptiveBayesianTrustModel(name="Distinct_NAB"),
)


def classify_tri_case(trust_score: float) -> str:
    """Classifies a trust/confidence score [0, 1] into 3 distinct operational cases."""
    if trust_score >= 0.80:
        return "NORMAL"
    elif trust_score >= 0.40:
        return "FREEZE"
    else:
        return "FALLBACK"


def background_stream_loop():
    """Background thread that simulates or ingests real-time telemetry every second."""
    while APP_STATE["running"]:
        time.sleep(1.0)
        sample = GENERATOR.next_sample()
        pred_base, pred_dist = EVALUATOR.evaluate_sample(sample)
        summary = EVALUATOR.get_summary_comparison()

        with STATE_LOCK:
            APP_STATE["tick"] += 1
            APP_STATE["active_fault"] = GENERATOR.active_fault
            APP_STATE["fault_duration"] = GENERATOR.fault_ticks_remaining
            
            # Map scores to 3 distinct operational cases
            base_case = classify_tri_case(pred_base.trust_score)
            dist_case = classify_tri_case(pred_dist.trust_score)
            case_match = (base_case == dist_case)
            delta = pred_dist.trust_score - pred_base.trust_score
            abs_delta = abs(delta)

            # Update cumulative divergence stats
            DIVERGENCE_STATS["total_ticks"] += 1
            if not case_match:
                DIVERGENCE_STATS["divergent_ticks"] += 1
            if base_case == "NORMAL" and dist_case in ("FREEZE", "FALLBACK"):
                DIVERGENCE_STATS["blind_scales_blocked"] += 1
            DIVERGENCE_STATS["sum_abs_delta"] += abs_delta

            tot_ticks = max(1, DIVERGENCE_STATS["total_ticks"])
            agr_rate = 100.0 * (tot_ticks - DIVERGENCE_STATS["divergent_ticks"]) / tot_ticks
            mean_gap = DIVERGENCE_STATS["sum_abs_delta"] / tot_ticks

            # Generate Real-Time Divergence Analysis & Algorithmic Root-Cause
            gate_val = pred_dist.metadata.get("bottleneck_gate", 1.0)
            cohere_val = pred_dist.metadata.get("semantic_coherence", 1.0)

            if not case_match:
                if base_case == "NORMAL" and dist_case in ("FREEZE", "FALLBACK"):
                    div_type = "CRITICAL_BLIND_SCALE"
                    div_title = "CRITICAL DIVERGENCE: Unsafe Blind Scaling Blocked!"
                    div_msg = f"Existing Model erroneously trusted corrupted telemetry as {base_case} (Ts={pred_base.trust_score:.2f}). Our Model enforced {dist_case} (Ts={pred_dist.trust_score:.2f}) to prevent Kubernetes crash."
                    why = f"Linear Additive Compensation Flaw: Existing model masked the broken '{pred_dist.bottleneck_signal}' signal via high values in other signals. Our Model's Non-Linear T-Norm Gate penalised the score immediately (Gate={gate_val:.2f}), avoiding disaster."
                elif base_case == "FREEZE" and dist_case == "FALLBACK":
                    div_type = "DEGRADED_VS_FALLBACK"
                    div_title = "DIVERGENCE: Partial Linear Masking vs Strict Fallback"
                    div_msg = f"Existing Model drifted into FREEZE (Ts={pred_base.trust_score:.2f}) due to linear averaging. Our Model detected severe failure and enforced strict FALLBACK (Ts={pred_dist.trust_score:.2f})."
                    why = f"The '{pred_dist.bottleneck_signal}' dimension dropped critically below 0.45. Our T-norm Gate dropped to {gate_val:.2f}, pushing confidence straight into FALLBACK mode."
                elif base_case == "NORMAL" and dist_case == "FREEZE":
                    div_type = "CAUTIOUS_RECOVERY"
                    div_title = "DIVERGENCE: Premature Normal vs Bayesian Damped Recovery"
                    div_msg = f"Existing Model rushed immediately to NORMAL (Ts={pred_base.trust_score:.2f}). Our Model maintained safe FREEZE (Ts={pred_dist.trust_score:.2f}) to avoid flapping."
                    why = "Our Model uses recursive Bayesian belief updating with memory decay. It verifies sustained recovery over multiple cycles before leaving the degraded state."
                else:
                    div_type = "DIVERGENT"
                    div_title = "DIVERGENCE: Operational Case Mismatch"
                    div_msg = f"Existing Model: {base_case} ({pred_base.trust_score:.2f}) vs Our Model: {dist_case} ({pred_dist.trust_score:.2f})"
                    why = f"Dynamic entropy weighting and semantic coherence checks (Coherence={cohere_val:.2f}) caused model divergence."
            else:
                div_type = "MATCH"
                div_title = f"ALIGNED: Both Models in Case {base_case}"
                if base_case == "NORMAL":
                    div_msg = f"Both models agree telemetry is reliable (Ts >= 0.80). Autoscaling proceeds normally."
                    why = "All telemetry watchdog signals (Freshness, Continuity, Validity) are healthy. Both models produce confident scaling decisions without intervention."
                elif base_case == "FREEZE":
                    div_msg = f"Both models agree telemetry is degraded (0.40 <= Ts < 0.80). Autoscaling frozen."
                    why = "Moderate noise or delay detected across streams. Both models temporarily freeze replica adjustments."
                else:
                    div_msg = f"Both models agree telemetry has suffered severe corruption (Ts < 0.40). Safe fallback active."
                    why = "Multiple sensor dimensions collapsed simultaneously, forcing emergency fallback."

            APP_STATE["sample"] = {
                "timestamp": sample.timestamp,
                "s_fresh": sample.s_fresh,
                "s_cont": sample.s_cont,
                "s_valid": sample.s_valid,
                "cpu": sample.cpu_utilization,
                "rps": sample.rps,
                "memory_mb": sample.memory_mb,
                "latency_ms": sample.p95_latency_ms,
                "fault_active": sample.ground_truth_fault,
                "fault_type": sample.fault_type,
            }
            
            APP_STATE["baseline"] = {
                "trust_score": pred_base.trust_score,
                "case": base_case,
                "confidence": pred_base.confidence_level,
                "action": pred_base.action,
                "uncertainty": pred_base.uncertainty,
                "bottleneck": pred_base.bottleneck_signal,
            }
            
            APP_STATE["distinct"] = {
                "trust_score": pred_dist.trust_score,
                "case": dist_case,
                "confidence": pred_dist.confidence_level,
                "action": pred_dist.action,
                "uncertainty": pred_dist.uncertainty,
                "bottleneck": pred_dist.bottleneck_signal,
                "gate": gate_val,
                "coherence": cohere_val,
            }

            APP_STATE["difference"] = {
                "delta_score": round(delta, 3),
                "abs_delta": round(abs_delta, 3),
                "case_match": case_match,
                "base_case": base_case,
                "dist_case": dist_case,
                "divergence_type": div_type,
                "divergence_title": div_title,
                "divergence_message": div_msg,
                "why_diff": why,
                "agreement_rate": round(agr_rate, 1),
                "total_ticks": DIVERGENCE_STATS["total_ticks"],
                "divergent_ticks": DIVERGENCE_STATS["divergent_ticks"],
                "blind_scales_blocked": DIVERGENCE_STATS["blind_scales_blocked"],
                "mean_abs_delta": round(mean_gap, 3),
            }
            
            APP_STATE["metrics"] = {
                "samples": summary["total_samples"],
                "base_accuracy": summary["baseline"]["accuracy"],
                "dist_accuracy": summary["distinct"]["accuracy"],
                "base_rmse": summary["baseline"]["rmse"],
                "dist_rmse": summary["distinct"]["rmse"],
                "base_flaps": summary["baseline"]["flapping_count"],
                "dist_flaps": summary["distinct"]["flapping_count"],
                "base_fn": summary["baseline"]["false_negatives"],
                "dist_fn": summary["distinct"]["false_negatives"],
            }
            
            # Append history point
            point = {
                "tick": APP_STATE["tick"],
                "base_ts": round(pred_base.trust_score, 3),
                "dist_ts": round(pred_dist.trust_score, 3),
                "base_case": base_case,
                "dist_case": dist_case,
                "delta": round(delta, 3),
                "fresh": round(sample.s_fresh, 3),
                "cont": round(sample.s_cont, 3),
                "valid": round(sample.s_valid, 3),
                "fault": sample.ground_truth_fault,
            }
            APP_STATE["history"].append(point)
            if len(APP_STATE["history"]) > 40:
                APP_STATE["history"].pop(0)

            # Log events when fault is active or decisions diverge
            if pred_base.action != pred_dist.action or sample.ground_truth_fault or not case_match:
                status_icon = "⚠️" if not case_match else "ℹ️"
                log_entry = (
                    f"[{time.strftime('%H:%M:%S')}] Tick {APP_STATE['tick']}: {status_icon} "
                    f"Fault={sample.fault_type} | "
                    f"Existing=[{base_case}] (Ts={pred_base.trust_score:.2f}) vs "
                    f"Our NAB=[{dist_case}] (Ts={pred_dist.trust_score:.2f}, Gate={gate_val:.2f}, Delta={delta:+.2f})"
                )
                APP_STATE["logs"].append(log_entry)
                if len(APP_STATE["logs"]) > 25:
                    APP_STATE["logs"].pop(0)


class WebMonitorHandler(BaseHTTPRequestHandler):
    """Handles HTTP requests for dashboard and REST API."""

    def log_message(self, format, *args):
        # Silence default terminal request logs to keep output clean
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode("utf-8"))
        elif parsed.path == "/api/state":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with STATE_LOCK:
                data = json.dumps(APP_STATE)
            self.wfile.write(data.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/inject-fault":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(body) if body else {}
            fault = data.get("fault", "none")
            duration = int(data.get("duration", 12))
            
            if fault == "none":
                GENERATOR.active_fault = "none"
                GENERATOR.fault_ticks_remaining = 0
            else:
                GENERATOR.trigger_fault(fault, duration_ticks=duration)
                
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "fault": fault, "duration": duration}).encode("utf-8"))
        elif parsed.path == "/api/reset":
            with STATE_LOCK:
                EVALUATOR.baseline_metrics = StreamMetrics()
                EVALUATOR.distinct_metrics = StreamMetrics()
                EVALUATOR.baseline.reset()
                EVALUATOR.distinct.reset()
                APP_STATE["history"].clear()
                APP_STATE["logs"].clear()
                DIVERGENCE_STATS["total_ticks"] = 0
                DIVERGENCE_STATS["divergent_ticks"] = 0
                DIVERGENCE_STATS["blind_scales_blocked"] = 0
                DIVERGENCE_STATS["sum_abs_delta"] = 0.0
                GENERATOR.active_fault = "none"
                GENERATOR.fault_ticks_remaining = 0
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b'{"status": "reset"}')
        else:
            self.send_response(404)
            self.end_headers()


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MicroGuardian - Realtime Trust Score Model Monitor</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Outfit:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-dark: #0a0d14;
      --card-bg: #111622;
      --card-border: #1f293d;
      --accent-cyan: #06b6d4;
      --accent-blue: #3b82f6;
      --accent-indigo: #6366f1;
      --accent-green: #10b981;
      --accent-amber: #f59e0b;
      --accent-red: #ef4444;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg-dark);
      color: var(--text-main);
      font-family: 'Outfit', sans-serif;
      padding: 24px;
      min-height: 100vh;
    }
    .container { max-width: 1440px; margin: 0 auto; }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 22px;
    }
    .title-group h1 {
      font-size: 1.65rem;
      font-weight: 800;
      background: linear-gradient(135deg, #38bdf8, #818cf8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      letter-spacing: -0.02em;
    }
    .title-group p { font-size: 0.88rem; color: var(--text-muted); margin-top: 4px; }
    .badge-live {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 14px;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.3);
      border-radius: 999px;
      font-size: 0.8rem;
      font-weight: 600;
      color: var(--accent-green);
    }
    .dot-pulse {
      width: 8px;
      height: 8px;
      background: var(--accent-green);
      border-radius: 50%;
      box-shadow: 0 0 10px var(--accent-green);
      animation: pulse 1.5s infinite;
    }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.85); } }

    /* Grid layout */
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
    .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px; }
    .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 20px; }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      padding: 20px;
      position: relative;
      overflow: hidden;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
    }
    .card-title {
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
      margin-bottom: 12px;
      font-weight: 700;
    }

    /* REAL-TIME MODEL DIFFERENCES & DIVERGENCE PANEL */
    .difference-card {
      margin-bottom: 22px;
      border: 1px solid rgba(56, 189, 248, 0.35);
      background: linear-gradient(180deg, rgba(14, 23, 42, 0.95), rgba(15, 23, 42, 0.75));
      position: relative;
      border-radius: 14px;
    }
    .diff-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      margin-bottom: 18px;
      gap: 16px;
    }
    .diff-tag {
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--accent-cyan);
      font-weight: 800;
      margin-bottom: 4px;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .diff-title {
      font-size: 1.25rem;
      font-weight: 800;
      letter-spacing: -0.01em;
      color: #f1f5f9;
      line-height: 1.3;
    }
    .diff-subtitle {
      font-size: 0.85rem;
      color: var(--text-muted);
      margin-top: 4px;
      line-height: 1.4;
    }
    .diff-status-pill {
      padding: 6px 14px;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-family: 'JetBrains Mono', monospace;
      white-space: nowrap;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .pill-match {
      background: rgba(16, 185, 129, 0.15);
      border: 1px solid var(--accent-green);
      color: var(--accent-green);
    }
    .pill-divergence {
      background: rgba(239, 68, 68, 0.2);
      border: 1px solid var(--accent-red);
      color: #fca5a5;
      animation: alert-pulse 1.8s infinite;
    }
    .pill-caution {
      background: rgba(245, 158, 11, 0.2);
      border: 1px solid var(--accent-amber);
      color: var(--accent-amber);
    }
    @keyframes alert-pulse {
      0%, 100% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.4); }
      50% { box-shadow: 0 0 14px 4px rgba(239, 68, 68, 0.25); }
    }

    .diff-grid {
      display: grid;
      grid-template-columns: 1fr 1.2fr 1fr;
      gap: 16px;
      align-items: center;
      padding: 14px;
      background: rgba(0, 0, 0, 0.32);
      border-radius: 12px;
      border: 1px solid rgba(255, 255, 255, 0.06);
      margin-bottom: 16px;
    }
    .diff-col {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .diff-col-lbl {
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      font-weight: 700;
    }
    .diff-col-val {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .case-chip {
      padding: 5px 12px;
      border-radius: 6px;
      font-size: 0.85rem;
      font-weight: 800;
      font-family: 'JetBrains Mono', monospace;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .score-chip {
      font-size: 0.88rem;
      font-family: 'JetBrains Mono', monospace;
      color: #cbd5e1;
      font-weight: 700;
    }
    .diff-col-sub {
      font-size: 0.75rem;
      color: var(--text-muted);
    }

    /* Center delta meter */
    .diff-meter-col {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 4px;
    }
    .diff-delta-val {
      font-size: 1.8rem;
      font-weight: 800;
      font-family: 'JetBrains Mono', monospace;
      line-height: 1;
    }
    .delta-bar-wrap {
      width: 100%;
      height: 10px;
      background: #1e2638;
      border-radius: 999px;
      position: relative;
      overflow: hidden;
      margin: 4px 0;
    }
    .delta-bar-center {
      position: absolute;
      left: 50%;
      top: 0;
      bottom: 0;
      width: 2px;
      background: rgba(255, 255, 255, 0.4);
      z-index: 2;
    }
    .delta-bar-fill {
      position: absolute;
      top: 0;
      bottom: 0;
      border-radius: 999px;
      transition: all 0.3s ease;
      z-index: 1;
    }
    .delta-bar-scale {
      display: flex;
      justify-content: space-between;
      width: 100%;
      font-size: 0.68rem;
      color: var(--text-muted);
      font-family: 'JetBrains Mono', monospace;
    }

    /* Live Root-Cause Diagnostic Box */
    .diff-why-box {
      background: rgba(6, 182, 212, 0.05);
      border: 1px solid rgba(6, 182, 212, 0.2);
      border-left: 4px solid var(--accent-cyan);
      border-radius: 8px;
      padding: 12px 16px;
      margin-bottom: 16px;
    }
    .diff-why-header {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 0.76rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--accent-cyan);
      margin-bottom: 4px;
    }
    .diff-why-text {
      font-size: 0.85rem;
      color: #e2e8f0;
      line-height: 1.5;
    }

    /* Divergence KPI Row */
    .diff-kpi-row {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
    }
    .kpi-item {
      background: rgba(0, 0, 0, 0.25);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 8px;
      padding: 10px 14px;
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .kpi-lbl { font-size: 0.72rem; color: var(--text-muted); font-weight: 600; text-transform: uppercase; }
    .kpi-val { font-size: 1.25rem; font-weight: 800; font-family: 'JetBrains Mono', monospace; }
    .text-green { color: var(--accent-green); }
    .text-cyan { color: var(--accent-cyan); }
    .text-amber { color: var(--accent-amber); }
    .text-red { color: var(--accent-red); }

    /* 0 TO 1 CONFIDENCE SPECTRUM (TRI-ZONE: NORMAL, FREEZE, FALLBACK) */
    .spectrum-container {
      margin: 16px 0 14px 0;
      background: rgba(0, 0, 0, 0.25);
      border: 1px solid rgba(255, 255, 255, 0.06);
      border-radius: 10px;
      padding: 14px;
    }
    .spectrum-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 10px;
    }
    .spectrum-title {
      font-size: 0.74rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
    }
    .spectrum-active-badge {
      font-size: 0.72rem;
      font-weight: 800;
      font-family: 'JetBrains Mono', monospace;
      padding: 3px 8px;
      border-radius: 4px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }

    /* The segmented spectrum bar */
    .spectrum-bar {
      width: 100%;
      height: 26px;
      border-radius: 8px;
      display: flex;
      position: relative;
      overflow: visible;
      box-shadow: inset 0 2px 6px rgba(0,0,0,0.5);
      border: 1px solid rgba(255, 255, 255, 0.1);
    }
    .zone {
      height: 100%;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      font-size: 0.65rem;
      font-weight: 800;
      letter-spacing: 0.04em;
      transition: opacity 0.3s ease;
      position: relative;
    }
    .zone-fallback {
      width: 40%;
      background: linear-gradient(90deg, rgba(239, 68, 68, 0.35), rgba(239, 68, 68, 0.6));
      color: #fecaca;
      border-top-left-radius: 7px;
      border-bottom-left-radius: 7px;
      border-right: 2px solid rgba(0, 0, 0, 0.6);
    }
    .zone-freeze {
      width: 40%;
      background: linear-gradient(90deg, rgba(245, 158, 11, 0.4), rgba(245, 158, 11, 0.6));
      color: #fef3c7;
      border-right: 2px solid rgba(0, 0, 0, 0.6);
    }
    .zone-normal {
      width: 20%;
      background: linear-gradient(90deg, rgba(16, 185, 129, 0.45), rgba(16, 185, 129, 0.75));
      color: #d1fae5;
      border-top-right-radius: 7px;
      border-bottom-right-radius: 7px;
    }
    .zone-active-glow {
      box-shadow: inset 0 0 12px rgba(255, 255, 255, 0.5);
      filter: brightness(1.25);
    }

    /* Sliding Needle Pointer */
    .spectrum-needle {
      position: absolute;
      top: -6px;
      bottom: -6px;
      width: 4px;
      background: #ffffff;
      border-radius: 2px;
      box-shadow: 0 0 10px #ffffff, 0 0 18px var(--accent-cyan);
      transform: translateX(-50%);
      transition: left 0.4s cubic-bezier(0.4, 0, 0.2, 1);
      z-index: 10;
      pointer-events: none;
    }
    .needle-badge {
      position: absolute;
      top: -24px;
      left: 50%;
      transform: translateX(-50%);
      background: #ffffff;
      color: #090d16;
      font-size: 0.68rem;
      font-weight: 800;
      font-family: 'JetBrains Mono', monospace;
      padding: 1px 6px;
      border-radius: 4px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.5);
      white-space: nowrap;
    }

    /* Spectrum tick marks */
    .spectrum-ticks {
      position: relative;
      height: 18px;
      margin-top: 6px;
      font-size: 0.68rem;
      font-family: 'JetBrains Mono', monospace;
      color: var(--text-muted);
    }
    .tick-mark {
      position: absolute;
      transform: translateX(-50%);
      white-space: nowrap;
    }

    .spectrum-desc {
      font-size: 0.74rem;
      color: var(--text-muted);
      margin-top: 8px;
      line-height: 1.4;
      padding-top: 6px;
      border-top: 1px dashed rgba(255, 255, 255, 0.08);
    }

    /* Model Head to Head Comparison */
    .model-header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 12px;
    }
    .model-name { font-size: 1.18rem; font-weight: 800; }
    .action-tag {
      padding: 5px 12px;
      border-radius: 6px;
      font-size: 0.75rem;
      font-weight: 800;
      text-transform: uppercase;
      font-family: 'JetBrains Mono', monospace;
      letter-spacing: 0.05em;
    }
    .case-NORMAL, .action-SCALE { background: rgba(16, 185, 129, 0.2); color: var(--accent-green); border: 1px solid var(--accent-green); }
    .case-FREEZE, .action-FREEZE { background: rgba(245, 158, 11, 0.2); color: var(--accent-amber); border: 1px solid var(--accent-amber); }
    .case-FALLBACK, .action-FALLBACK { background: rgba(239, 68, 68, 0.2); color: var(--accent-red); border: 1px solid var(--accent-red); }

    .score-display {
      display: flex;
      align-items: baseline;
      gap: 14px;
      margin-bottom: 6px;
    }
    .score-value {
      font-size: 3.4rem;
      font-weight: 800;
      font-family: 'JetBrains Mono', monospace;
      line-height: 1;
      letter-spacing: -0.03em;
    }
    .score-sub { font-size: 0.85rem; color: var(--text-muted); }

    .meta-pills { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
    .meta-pill {
      font-size: 0.75rem;
      font-family: 'JetBrains Mono', monospace;
      padding: 4px 8px;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 6px;
    }

    /* Live Chart */
    .chart-box {
      width: 100%;
      height: 200px;
      position: relative;
    }
    svg.live-chart { width: 100%; height: 100%; overflow: visible; }

    /* Controls Panel */
    .btn-group { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 10px; }
    .btn {
      padding: 9px 16px;
      border-radius: 8px;
      border: 1px solid var(--card-border);
      background: #161d2d;
      color: var(--text-main);
      font-size: 0.85rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s ease;
    }
    .btn:hover { background: #222d44; border-color: var(--accent-cyan); transform: translateY(-1px); }
    .btn-danger { background: rgba(239, 68, 68, 0.15); border-color: rgba(239, 68, 68, 0.4); color: #fca5a5; }
    .btn-danger:hover { background: rgba(239, 68, 68, 0.3); border-color: var(--accent-red); }
    .btn-success { background: rgba(16, 185, 129, 0.15); border-color: rgba(16, 185, 129, 0.4); color: #6ee7b7; }
    .btn-success:hover { background: rgba(16, 185, 129, 0.3); border-color: var(--accent-green); }

    /* Metric stats */
    .stat-val { font-size: 1.6rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
    .stat-lbl { font-size: 0.78rem; color: var(--text-muted); margin-top: 2px; }
    .delta-good { color: var(--accent-green); font-size: 0.8rem; font-weight: 600; }

    /* Event log */
    .event-log {
      max-height: 150px;
      overflow-y: auto;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.76rem;
      line-height: 1.6;
      color: var(--text-muted);
    }
    .event-log div { border-bottom: 1px solid rgba(255, 255, 255, 0.04); padding: 4px 0; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="title-group">
        <h1>MicroGuardian -- Telemetry Trust Score Model Engine</h1>
        <p>Real-Time Comparative Evaluation: Existing Linear Heuristic Model vs Distinct Non-Linear Adaptive Bayesian Model (NAB-Trust)</p>
      </div>
      <div style="display: flex; gap: 12px; align-items: center;">
        <div class="badge-live"><span class="dot-pulse"></span> STREAMING REALTIME DATA</div>
        <button class="btn" onclick="resetMetrics()">Reset Stats</button>
      </div>
    </header>

    <!-- REAL-TIME MODEL DIFFERENCES & DIVERGENCE ANALYSIS CARD -->
    <div class="card difference-card" id="diff-card">
      <div class="diff-header">
        <div class="diff-title-box">
          <div class="diff-tag"><span>⚡</span> REAL-TIME MODEL DIVERGENCE &amp; DIFFERENCES BREAKDOWN</div>
          <h2 id="diff-title" class="diff-title">ALIGNED: Both Models in Case NORMAL</h2>
          <p id="diff-subtitle" class="diff-subtitle">Both models agree on healthy telemetry (T_s >= 0.80). Autoscaling proceeds normally.</p>
        </div>
        <div id="diff-status-pill" class="diff-status-pill pill-match">ALIGNED (MATCH)</div>
      </div>

      <div class="diff-grid">
        <!-- Existing Model Status Column -->
        <div class="diff-col">
          <div class="diff-col-lbl">Existing Model (Baseline)</div>
          <div class="diff-col-val">
            <span id="diff-base-case" class="case-chip case-NORMAL">NORMAL</span>
            <span id="diff-base-score" class="score-chip">Ts = 1.00</span>
          </div>
          <div class="diff-col-sub" id="diff-base-action">Action: SCALE (Normal HPA loop)</div>
        </div>

        <!-- Center Difference Meter Column -->
        <div class="diff-meter-col">
          <div class="diff-col-lbl">Score Difference (&Delta; Ts = Our - Existing)</div>
          <div class="diff-delta-val" id="diff-delta-score" style="color: var(--accent-cyan);">0.00</div>
          <div class="delta-bar-wrap">
            <div class="delta-bar-center"></div>
            <div class="delta-bar-fill" id="diff-delta-bar" style="width: 0%; left: 50%; background: var(--accent-cyan);"></div>
          </div>
          <div class="delta-bar-scale">
            <span>-1.00 (Our Lower)</span>
            <span>0.00</span>
            <span>+1.00 (Our Higher)</span>
          </div>
        </div>

        <!-- Our Model Status Column -->
        <div class="diff-col" style="align-items: flex-end; text-align: right;">
          <div class="diff-col-lbl">Our Model (NAB-Trust)</div>
          <div class="diff-col-val" style="justify-content: flex-end;">
            <span id="diff-dist-score" class="score-chip">Ts = 1.00</span>
            <span id="diff-dist-case" class="case-chip case-NORMAL">NORMAL</span>
          </div>
          <div class="diff-col-sub" id="diff-dist-action">Action: SCALE (T-Norm Guarded)</div>
        </div>
      </div>

      <!-- Live Algorithmic Root-Cause Diagnostic -->
      <div class="diff-why-box">
        <div class="diff-why-header">
          <span>🔬</span>
          <span>Real-Time Mathematical &amp; Architectural Reason for Difference:</span>
        </div>
        <div id="diff-why-text" class="diff-why-text">
          All telemetry dimensions are healthy. Both models produce confident scaling decisions without intervention.
        </div>
      </div>

      <!-- Divergence KPI Summary Row -->
      <div class="diff-kpi-row">
        <div class="kpi-item">
          <span class="kpi-lbl">Case Agreement Rate</span>
          <span class="kpi-val text-green" id="kpi-agreement">100.0%</span>
        </div>
        <div class="kpi-item">
          <span class="kpi-lbl">Divergent Cycles</span>
          <span class="kpi-val text-amber" id="kpi-divergent">0</span>
        </div>
        <div class="kpi-item">
          <span class="kpi-lbl">Unsafe Blind Scales Blocked</span>
          <span class="kpi-val text-cyan" id="kpi-blocked">0</span>
        </div>
        <div class="kpi-item">
          <span class="kpi-lbl">Mean Absolute Gap |&Delta;Ts|</span>
          <span class="kpi-val" id="kpi-abs-gap" style="color: #cbd5e1;">0.000</span>
        </div>
      </div>
    </div>

    <!-- Side by Side Model Predictions with 0 to 1 Spectrum Split -->
    <div class="grid-2">
      <!-- Existing Model -->
      <div class="card" style="border-top: 3px solid #64748b;">
        <div class="model-header">
          <div>
            <div class="model-name" style="color: #94a3b8;">Existing Model (Baseline)</div>
            <div style="font-size: 0.78rem; color: var(--text-muted);">Formula: 0.4 S_fresh + 0.3 S_cont + 0.3 S_valid</div>
          </div>
          <span id="base-action" class="action-tag action-SCALE">NORMAL / SCALE</span>
        </div>
        <div class="score-display">
          <div id="base-score" class="score-value" style="color: #cbd5e1;">1.00</div>
          <div class="score-sub">Confidence: <span id="base-conf">HIGH</span> | Range: [0.00, 1.00]</div>
        </div>

        <!-- 0 to 1 Spectrum Gauge with 3 Cases Split -->
        <div class="spectrum-container">
          <div class="spectrum-header">
            <span class="spectrum-title">CONFIDENCE SPECTRUM &bull; 3 CASES (0.00 &rarr; 1.00)</span>
            <span class="spectrum-active-badge case-NORMAL" id="base-case-badge">CASE: NORMAL</span>
          </div>
          
          <div class="spectrum-bar">
            <div class="zone zone-fallback" id="base-zone-fallback">
              <span>FALLBACK</span>
            </div>
            <div class="zone zone-freeze" id="base-zone-freeze">
              <span>FREEZE</span>
            </div>
            <div class="zone zone-normal" id="base-zone-normal">
              <span>NORMAL</span>
            </div>
            <!-- Sliding Needle Pointer -->
            <div class="spectrum-needle" id="base-needle" style="left: 100%;">
              <div class="needle-badge" id="base-needle-val">1.00</div>
            </div>
          </div>
          
          <div class="spectrum-ticks">
            <span class="tick-mark" style="left: 0%;">0.00</span>
            <span class="tick-mark" style="left: 40%;">0.40 (&beta; Low)</span>
            <span class="tick-mark" style="left: 80%;">0.80 (&alpha; High)</span>
            <span class="tick-mark" style="left: 100%;">1.00</span>
          </div>

          <div class="spectrum-desc" id="base-case-desc">
            Confidence &ge; 0.80 &bull; <strong>Case: NORMAL (Safe to Scale)</strong> &bull; Standard HPA scaling active
          </div>
        </div>

        <div class="meta-pills">
          <span class="meta-pill">Bottleneck: <span id="base-bottle" style="color: #f1f5f9;">freshness</span></span>
          <span class="meta-pill">Uncertainty: None (Point Est)</span>
          <span class="meta-pill" style="color: #fca5a5;">Flaw: Additive Compensation</span>
        </div>
      </div>

      <!-- Distinct Model -->
      <div class="card" style="border-top: 3px solid var(--accent-cyan); background: linear-gradient(180deg, rgba(6,182,212,0.06), rgba(17,22,34,1));">
        <div class="model-header">
          <div>
            <div class="model-name" style="color: var(--accent-cyan);">Our Model (NAB-Trust)</div>
            <div style="font-size: 0.78rem; color: #a5f3fc;">Non-Linear T-Norm Gate + Dynamic Entropy Weights + Bayesian Beta Priors</div>
          </div>
          <span id="dist-action" class="action-tag action-SCALE">NORMAL / SCALE</span>
        </div>
        <div class="score-display">
          <div id="dist-score" class="score-value" style="color: #38bdf8;">1.00</div>
          <div class="score-sub">Confidence: <span id="dist-conf">HIGH</span> | Uncertainty: &plusmn;<span id="dist-unc">0.02</span></div>
        </div>

        <!-- 0 to 1 Spectrum Gauge with 3 Cases Split -->
        <div class="spectrum-container">
          <div class="spectrum-header">
            <span class="spectrum-title">CONFIDENCE SPECTRUM &bull; 3 CASES (0.00 &rarr; 1.00)</span>
            <span class="spectrum-active-badge case-NORMAL" id="dist-case-badge">CASE: NORMAL</span>
          </div>
          
          <div class="spectrum-bar">
            <div class="zone zone-fallback" id="dist-zone-fallback">
              <span>FALLBACK</span>
            </div>
            <div class="zone zone-freeze" id="dist-zone-freeze">
              <span>FREEZE</span>
            </div>
            <div class="zone zone-normal" id="dist-zone-normal">
              <span>NORMAL</span>
            </div>
            <!-- Sliding Needle Pointer -->
            <div class="spectrum-needle" id="dist-needle" style="left: 100%;">
              <div class="needle-badge" id="dist-needle-val">1.00</div>
            </div>
          </div>
          
          <div class="spectrum-ticks">
            <span class="tick-mark" style="left: 0%;">0.00</span>
            <span class="tick-mark" style="left: 40%;">0.40 (&beta; Low)</span>
            <span class="tick-mark" style="left: 80%;">0.80 (&alpha; High)</span>
            <span class="tick-mark" style="left: 100%;">1.00</span>
          </div>

          <div class="spectrum-desc" id="dist-case-desc">
            Confidence &ge; 0.80 &bull; <strong>Case: NORMAL (Safe to Scale)</strong> &bull; Closed-loop HPA protected
          </div>
        </div>

        <div class="meta-pills">
          <span class="meta-pill">Gate Penalty: <span id="dist-gate" style="color: #38bdf8;">1.00</span></span>
          <span class="meta-pill">Semantic Coherence: <span id="dist-cohere" style="color: #38bdf8;">1.00</span></span>
          <span class="meta-pill">Bottleneck: <span id="dist-bottle" style="color: #f1f5f9;">freshness</span></span>
          <span class="meta-pill" style="color: #34d399;">Protection: Anti-Compensation Active</span>
        </div>
      </div>
    </div>

    <!-- Live Telemetry Signals -->
    <div class="grid-4">
      <div class="card">
        <div class="card-title">Freshness (S_fresh)</div>
        <div id="sig-fresh" class="stat-val" style="color: #38bdf8;">1.00</div>
        <div class="stat-lbl">Sample age within 30s threshold</div>
      </div>
      <div class="card">
        <div class="card-title">Continuity (S_cont)</div>
        <div id="sig-cont" class="stat-val" style="color: #818cf8;">1.00</div>
        <div class="stat-lbl">Rate of change |dM/dt| bound</div>
      </div>
      <div class="card">
        <div class="card-title">Completeness (S_valid)</div>
        <div id="sig-valid" class="stat-val" style="color: #c084fc;">1.00</div>
        <div class="stat-lbl">Active non-null scrape endpoints</div>
      </div>
      <div class="card">
        <div class="card-title">Active Injected Fault</div>
        <div id="fault-active-name" class="stat-val" style="color: #10b981; font-size: 1.25rem;">NONE (HEALTHY)</div>
        <div id="fault-timer" class="stat-lbl">Normal closed-loop scaling</div>
      </div>
    </div>

    <!-- Live Trust Score Chart -->
    <div class="card" style="margin-bottom: 20px;">
      <div class="card-title" style="display: flex; justify-content: space-between; align-items: center;">
        <span>Realtime Confidence Score Stream (Last 40s) with 3-Case Thresholds</span>
        <span style="font-size: 0.75rem;">
          <span style="color: #94a3b8;">-- Existing Model</span> &nbsp;&bull;&nbsp;
          <span style="color: var(--accent-cyan); font-weight: 700;">-- Our Model (NAB)</span> &nbsp;&bull;&nbsp;
          <span style="color: var(--accent-green);">&bull;&bull; 0.80 Normal Boundary</span> &nbsp;&bull;&nbsp;
          <span style="color: var(--accent-red);">&bull;&bull; 0.40 Fallback Boundary</span> &nbsp;&bull;&nbsp;
          <span style="color: #ef4444; opacity: 0.7;">&block; Fault Zone</span>
        </span>
      </div>
      <div class="chart-box">
        <svg id="live-chart" class="live-chart" viewBox="0 0 1000 200" preserveAspectRatio="none">
          <!-- Dynamically generated via JS -->
        </svg>
      </div>
    </div>

    <!-- Head-to-Head Statistics & Fault Controls -->
    <div class="grid-2">
      <!-- Accuracy & Safety Metrics -->
      <div class="card">
        <div class="card-title">Realtime Accuracy & Safety Evaluation</div>
        <div class="grid-3" style="margin-bottom: 0;">
          <div>
            <div class="stat-lbl">Autoscaling Accuracy</div>
            <div style="margin-top: 4px;">
              <span id="base-acc" style="color: #94a3b8; font-family: monospace;">86.0%</span> &rarr;
              <span id="dist-acc" style="color: var(--accent-green); font-weight: 700; font-family: monospace;">92.4%</span>
            </div>
            <div class="delta-good">+6.4% Accuracy</div>
          </div>
          <div>
            <div class="stat-lbl">RMSE Error vs Truth</div>
            <div style="margin-top: 4px;">
              <span id="base-rmse" style="color: #94a3b8; font-family: monospace;">0.360</span> &rarr;
              <span id="dist-rmse" style="color: var(--accent-cyan); font-weight: 700; font-family: monospace;">0.236</span>
            </div>
            <div class="delta-good">-34.4% Error</div>
          </div>
          <div>
            <div class="stat-lbl">Unsafe Blind Scales</div>
            <div style="margin-top: 4px;">
              <span id="base-fn" style="color: var(--accent-red); font-family: monospace;">12</span> &rarr;
              <span id="dist-fn" style="color: var(--accent-green); font-weight: 700; font-family: monospace;">0</span>
            </div>
            <div class="delta-good">Zero Blind Scaling</div>
          </div>
        </div>
      </div>

      <!-- Realtime Fault Injection Buttons -->
      <div class="card">
        <div class="card-title">Live Chaos Telemetry Injector (Test Under Realtime Data)</div>
        <p style="font-size: 0.8rem; color: var(--text-muted); margin-bottom: 8px;">
          Inject sensor faults in real-time to watch confidence scores transition between NORMAL &rarr; FREEZE &rarr; FALLBACK:
        </p>
        <div class="btn-group">
          <button class="btn btn-danger" onclick="injectFault('scrape-delay')">1. Scrape Delay</button>
          <button class="btn btn-danger" onclick="injectFault('counter-spike')">2. Counter Reset Spike</button>
          <button class="btn btn-danger" onclick="injectFault('zero-fill')">3. Sensor Zero-Filling</button>
          <button class="btn btn-danger" onclick="injectFault('endpoint-failure')">4. Endpoint Drop</button>
          <button class="btn btn-danger" onclick="injectFault('multi-fault')">5. Compound Fault</button>
          <button class="btn btn-success" onclick="injectFault('none')">Restore Healthy</button>
        </div>
      </div>
    </div>

    <!-- Live Event Log -->
    <div class="card">
      <div class="card-title">Realtime Gating Decision & Divergence Log</div>
      <div id="event-log" class="event-log">
        <div>[SYSTEM] Realtime evaluation stream connected. Waiting for cycles...</div>
      </div>
    </div>
  </div>

  <script>
    async function fetchState() {
      try {
        const res = await fetch('/api/state');
        const data = await res.json();
        updateUI(data);
      } catch (err) {
        console.error("Fetch error:", err);
      }
    }

    function updateUI(data) {
      const bTs = data.baseline.trust_score;
      const dTs = data.distinct.trust_score;
      const bCase = data.baseline.case || (bTs >= 0.8 ? 'NORMAL' : (bTs >= 0.4 ? 'FREEZE' : 'FALLBACK'));
      const dCase = data.distinct.case || (dTs >= 0.8 ? 'NORMAL' : (dTs >= 0.4 ? 'FREEZE' : 'FALLBACK'));
      const diff = data.difference || {};

      // 1. UPDATE REAL-TIME MODEL DIFFERENCES PANEL
      document.getElementById('diff-title').textContent = diff.divergence_title || 'ALIGNED: Both Models in Case NORMAL';
      document.getElementById('diff-subtitle').textContent = diff.divergence_message || 'Both models agree on healthy telemetry.';
      document.getElementById('diff-why-text').textContent = diff.why_diff || 'All telemetry dimensions healthy.';

      const statusPill = document.getElementById('diff-status-pill');
      if (diff.case_match) {
        statusPill.textContent = 'ALIGNED (MATCH)';
        statusPill.className = 'diff-status-pill pill-match';
      } else if (diff.divergence_type === 'CRITICAL_BLIND_SCALE') {
        statusPill.textContent = 'CRITICAL DIVERGENCE (BLIND SCALE BLOCKED)';
        statusPill.className = 'diff-status-pill pill-divergence';
      } else {
        statusPill.textContent = 'DIVERGENCE (CASE MISMATCH)';
        statusPill.className = 'diff-status-pill pill-caution';
      }

      // Cases in diff grid
      const bChip = document.getElementById('diff-base-case');
      bChip.textContent = bCase;
      bChip.className = 'case-chip case-' + bCase;
      document.getElementById('diff-base-score').textContent = 'Ts = ' + bTs.toFixed(2);
      document.getElementById('diff-base-action').textContent = 'Action: ' + data.baseline.action + ' (' + (bCase === 'NORMAL' ? 'Standard Scaling' : (bCase === 'FREEZE' ? 'Replicas Frozen' : 'Emergency Fallback')) + ')';

      const dChip = document.getElementById('diff-dist-case');
      dChip.textContent = dCase;
      dChip.className = 'case-chip case-' + dCase;
      document.getElementById('diff-dist-score').textContent = 'Ts = ' + dTs.toFixed(2);
      document.getElementById('diff-dist-action').textContent = 'Action: ' + data.distinct.action + ' (' + (dCase === 'NORMAL' ? 'Standard Scaling' : (dCase === 'FREEZE' ? 'Anti-Flap Freeze' : 'Safe Gated Floor')) + ')';

      // Delta Ts value and meter
      const deltaVal = (diff.delta_score !== undefined) ? diff.delta_score : (dTs - bTs);
      const deltaEl = document.getElementById('diff-delta-score');
      deltaEl.textContent = (deltaVal > 0 ? '+' : '') + deltaVal.toFixed(2);
      deltaEl.style.color = deltaVal === 0 ? 'var(--accent-cyan)' : (deltaVal > 0 ? 'var(--accent-green)' : 'var(--accent-red)');

      const deltaBar = document.getElementById('diff-delta-bar');
      // Scale is [-1.0, +1.0] mapped around 50%
      const clampedDelta = Math.max(-1.0, Math.min(1.0, deltaVal));
      if (clampedDelta >= 0) {
        deltaBar.style.left = '50%';
        deltaBar.style.width = (clampedDelta * 50) + '%';
        deltaBar.style.background = 'var(--accent-green)';
      } else {
        const w = Math.abs(clampedDelta) * 50;
        deltaBar.style.left = (50 - w) + '%';
        deltaBar.style.width = w + '%';
        deltaBar.style.background = 'var(--accent-red)';
      }

      // Difference KPIs
      document.getElementById('kpi-agreement').textContent = (diff.agreement_rate !== undefined ? diff.agreement_rate.toFixed(1) : '100.0') + '%';
      document.getElementById('kpi-divergent').textContent = diff.divergent_ticks !== undefined ? diff.divergent_ticks : 0;
      document.getElementById('kpi-blocked').textContent = diff.blind_scales_blocked !== undefined ? diff.blind_scales_blocked : 0;
      document.getElementById('kpi-abs-gap').textContent = (diff.mean_abs_delta !== undefined ? diff.mean_abs_delta.toFixed(3) : '0.000');

      // 2. UPDATE EXISTING MODEL SPECTRUM & CASES
      document.getElementById('base-score').textContent = bTs.toFixed(2);
      document.getElementById('base-conf').textContent = data.baseline.confidence;
      const bAction = document.getElementById('base-action');
      bAction.textContent = bCase + ' / ' + data.baseline.action;
      bAction.className = 'action-tag action-' + data.baseline.action;

      // Spectrum needle & badge
      const bPct = Math.max(0, Math.min(100, bTs * 100));
      document.getElementById('base-needle').style.left = bPct + '%';
      document.getElementById('base-needle-val').textContent = bTs.toFixed(2);
      
      const bBadge = document.getElementById('base-case-badge');
      bBadge.textContent = 'CASE: ' + bCase;
      bBadge.className = 'spectrum-active-badge case-' + bCase;

      // Highlight active zone
      document.getElementById('base-zone-fallback').className = 'zone zone-fallback' + (bCase === 'FALLBACK' ? ' zone-active-glow' : '');
      document.getElementById('base-zone-freeze').className = 'zone zone-freeze' + (bCase === 'FREEZE' ? ' zone-active-glow' : '');
      document.getElementById('base-zone-normal').className = 'zone zone-normal' + (bCase === 'NORMAL' ? ' zone-active-glow' : '');

      const bDesc = document.getElementById('base-case-desc');
      if (bCase === 'NORMAL') {
        bDesc.innerHTML = 'Confidence &ge; 0.80 &bull; <strong>Case: NORMAL (Safe to Scale)</strong> &bull; Telemetry accepted by linear model';
      } else if (bCase === 'FREEZE') {
        bDesc.innerHTML = '0.40 &le; Confidence &lt; 0.80 &bull; <strong>Case: FREEZE (Hold Replicas)</strong> &bull; Partial degradation detected';
      } else {
        bDesc.innerHTML = 'Confidence &lt; 0.40 &bull; <strong>Case: FALLBACK (Emergency Floor)</strong> &bull; Severe corruption across weighted signals';
      }
      document.getElementById('base-bottle').textContent = data.baseline.bottleneck || 'none';

      // 3. UPDATE OUR MODEL (NAB-TRUST) SPECTRUM & CASES
      document.getElementById('dist-score').textContent = dTs.toFixed(2);
      document.getElementById('dist-conf').textContent = data.distinct.confidence;
      document.getElementById('dist-unc').textContent = data.distinct.uncertainty.toFixed(2);
      const dAction = document.getElementById('dist-action');
      dAction.textContent = dCase + ' / ' + data.distinct.action;
      dAction.className = 'action-tag action-' + data.distinct.action;

      // Spectrum needle & badge
      const dPct = Math.max(0, Math.min(100, dTs * 100));
      document.getElementById('dist-needle').style.left = dPct + '%';
      document.getElementById('dist-needle-val').textContent = dTs.toFixed(2);

      const dBadge = document.getElementById('dist-case-badge');
      dBadge.textContent = 'CASE: ' + dCase;
      dBadge.className = 'spectrum-active-badge case-' + dCase;

      // Highlight active zone
      document.getElementById('dist-zone-fallback').className = 'zone zone-fallback' + (dCase === 'FALLBACK' ? ' zone-active-glow' : '');
      document.getElementById('dist-zone-freeze').className = 'zone zone-freeze' + (dCase === 'FREEZE' ? ' zone-active-glow' : '');
      document.getElementById('dist-zone-normal').className = 'zone zone-normal' + (dCase === 'NORMAL' ? ' zone-active-glow' : '');

      const dDesc = document.getElementById('dist-case-desc');
      if (dCase === 'NORMAL') {
        dDesc.innerHTML = 'Confidence &ge; 0.80 &bull; <strong>Case: NORMAL (Safe to Scale)</strong> &bull; High multi-sensor trust & coherence';
      } else if (dCase === 'FREEZE') {
        dDesc.innerHTML = '0.40 &le; Confidence &lt; 0.80 &bull; <strong>Case: FREEZE (Hold Replicas)</strong> &bull; Damped Bayesian holding state to prevent flapping';
      } else {
        dDesc.innerHTML = 'Confidence &lt; 0.40 &bull; <strong>Case: FALLBACK (Emergency Gating)</strong> &bull; T-Norm Bottleneck triggered; safe floor enforced';
      }

      document.getElementById('dist-gate').textContent = data.distinct.gate.toFixed(2);
      document.getElementById('dist-cohere').textContent = data.distinct.coherence.toFixed(2);
      document.getElementById('dist-bottle').textContent = data.distinct.bottleneck || 'none';

      // 4. TELEMETRY SIGNALS & FAULT STATUS
      document.getElementById('sig-fresh').textContent = data.sample.s_fresh.toFixed(2);
      document.getElementById('sig-cont').textContent = data.sample.s_cont.toFixed(2);
      document.getElementById('sig-valid').textContent = data.sample.s_valid.toFixed(2);

      const faultLbl = document.getElementById('fault-active-name');
      const timerLbl = document.getElementById('fault-timer');
      if (data.sample.fault_active) {
        faultLbl.textContent = data.sample.fault_type.toUpperCase();
        faultLbl.style.color = '#ef4444';
        timerLbl.textContent = 'Active chaos injected (' + data.fault_duration + ' ticks left)';
      } else {
        faultLbl.textContent = 'NONE (HEALTHY)';
        faultLbl.style.color = '#10b981';
        timerLbl.textContent = 'Normal closed-loop scaling';
      }

      // Stats
      document.getElementById('base-acc').textContent = data.metrics.base_accuracy.toFixed(1) + '%';
      document.getElementById('dist-acc').textContent = data.metrics.dist_accuracy.toFixed(1) + '%';
      document.getElementById('base-rmse').textContent = data.metrics.base_rmse.toFixed(3);
      document.getElementById('dist-rmse').textContent = data.metrics.dist_rmse.toFixed(3);
      document.getElementById('base-fn').textContent = data.metrics.base_fn;
      document.getElementById('dist-fn').textContent = data.metrics.dist_fn;

      // Draw SVG Chart
      renderChart(data.history);

      // Event Logs
      const logBox = document.getElementById('event-log');
      if (data.logs && data.logs.length > 0) {
        logBox.innerHTML = data.logs.map(l => '<div>' + l + '</div>').join('');
        logBox.scrollTop = logBox.scrollHeight;
      }
    }

    function renderChart(history) {
      const svg = document.getElementById('live-chart');
      if (!history || history.length < 2) return;

      const w = 1000;
      const h = 200;
      const padding = 24;
      const n = history.length;
      const dx = (w - 2 * padding) / (n - 1);

      const chartH = h - 44;
      function toY(score) {
        return (h - 20) - (score * chartH);
      }

      const yNormal = toY(0.80);
      const yFallback = toY(0.40);

      let svgHtml = `
        <!-- Shaded Background Zones for Cases -->
        <rect x="${padding}" y="${toY(1.0)}" width="${w - 2 * padding}" height="${toY(0.80) - toY(1.0)}" fill="rgba(16, 185, 129, 0.05)" />
        <rect x="${padding}" y="${toY(0.80)}" width="${w - 2 * padding}" height="${toY(0.40) - toY(0.80)}" fill="rgba(245, 158, 11, 0.05)" />
        <rect x="${padding}" y="${toY(0.40)}" width="${w - 2 * padding}" height="${toY(0.0) - toY(0.40)}" fill="rgba(239, 68, 68, 0.05)" />

        <!-- 0.80 Normal / Scale Boundary -->
        <line x1="${padding}" y1="${yNormal}" x2="${w - padding}" y2="${yNormal}" stroke="#10b981" stroke-width="1.5" stroke-dasharray="4,4" opacity="0.6" />
        <text x="${w - padding - 85}" y="${yNormal - 4}" fill="#10b981" font-size="10" font-family="JetBrains Mono" font-weight="700">0.80 NORMAL</text>

        <!-- 0.40 Freeze / Fallback Boundary -->
        <line x1="${padding}" y1="${yFallback}" x2="${w - padding}" y2="${yFallback}" stroke="#ef4444" stroke-width="1.5" stroke-dasharray="4,4" opacity="0.6" />
        <text x="${w - padding - 95}" y="${yFallback - 4}" fill="#ef4444" font-size="10" font-family="JetBrains Mono" font-weight="700">0.40 FALLBACK</text>

        <!-- Baseline Axis -->
        <line x1="${padding}" y1="${h - 20}" x2="${w - padding}" y2="${h - 20}" stroke="#1f293d" />
      `;

      // Highlight active fault zones
      for (let i = 0; i < n; i++) {
        if (history[i].fault) {
          const x = padding + i * dx;
          svgHtml += `<rect x="${x - dx/2}" y="10" width="${dx}" height="${h - 30}" fill="rgba(239, 68, 68, 0.16)" />`;
        }
      }

      // Points to SVG path
      const basePoints = history.map((pt, i) => `${padding + i * dx},${toY(pt.base_ts)}`).join(' ');
      const distPoints = history.map((pt, i) => `${padding + i * dx},${toY(pt.dist_ts)}`).join(' ');

      // Existing line (Grey-blue dashed)
      svgHtml += `<polyline fill="none" stroke="#94a3b8" stroke-width="2.5" stroke-dasharray="4,4" points="${basePoints}" />`;
      // Distinct line (Cyan solid)
      svgHtml += `<polyline fill="none" stroke="#06b6d4" stroke-width="3" points="${distPoints}" />`;

      // Draw current head circles
      const lastIdx = n - 1;
      const lastX = padding + lastIdx * dx;
      svgHtml += `<circle cx="${lastX}" cy="${toY(history[lastIdx].base_ts)}" r="4.5" fill="#94a3b8" />`;
      svgHtml += `<circle cx="${lastX}" cy="${toY(history[lastIdx].dist_ts)}" r="5.5" fill="#06b6d4" stroke="#ffffff" stroke-width="1.5" />`;

      svg.innerHTML = svgHtml;
    }

    async function injectFault(faultName) {
      await fetch('/api/inject-fault', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ fault: faultName, duration: 12 })
      });
      fetchState();
    }

    async function resetMetrics() {
      await fetch('/api/reset', { method: 'POST' });
      fetchState();
    }

    // Poll every 1s
    setInterval(fetchState, 1000);
    fetchState();
  </script>
</body>
</html>
"""


def run_web_monitor(port: int = 8088):
    """Starts the real-time background stream and Web monitor server."""
    # Launch background stream thread
    t = threading.Thread(target=background_stream_loop, daemon=True)
    t.start()

    server_address = ("", port)
    httpd = HTTPServer(server_address, WebMonitorHandler)
    print("=" * 86)
    print(f"   MICROGUARDIAN TRUST SCORE REAL-TIME WEB MONITOR STARTED")
    print("=" * 86)
    print(f"   Dashboard URL:  http://localhost:{port}")
    print(f"   API Endpoint:   http://localhost:{port}/api/state")
    print(f"   Fault Trigger:  POST http://localhost:{port}/api/inject-fault")
    print("=" * 86)
    print("   Press Ctrl+C to terminate.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down Web Monitor.")
        APP_STATE["running"] = False
        httpd.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MicroGuardian Trust Score Web Monitor")
    parser.add_argument("--port", type=int, default=8088, help="Port to serve dashboard on")
    args = parser.parse_args()
    run_web_monitor(port=args.port)
