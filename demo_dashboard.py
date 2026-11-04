"""
MicroGuardian Live Interactive Demonstration Server
===================================================
A standalone, zero-dependency interactive web dashboard for Review 1.
Provides real-time visualization of:
  - Telemetry parameters: Requests Per Second (RPS), Error Rate (%), CPU (%), Memory (MB), P95 Latency (ms)
  - Pod Creation & Deletion Triggers & Thresholds
  - Real-time gauge for Telemetry Trust Score T_s(t)
  - Visual breakdown of S_fresh, S_cont, S_valid
  - Side-by-side replica counts: Standard HPA vs MicroGuardian
  - Live buttons to trigger faults: Scrape Delay, Packet Loss, Endpoint Drop, Counter Reset
  - Real-time event log
"""

import os
import sys
import json
import time
import math
import random
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Import controller logic
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from controller.config import MicroGuardianConfig
from controller.watchdog.freshness import evaluate_freshness
from controller.watchdog.continuity import evaluate_continuity
from controller.watchdog.completeness import evaluate_completeness
from controller.trust_score import TrustScoreEngine
from controller.decision_engine import DecisionEngine
from controller.fallback import FallbackEngine

# Shared state between background simulation and HTTP API
STATE = {
    "cycle": 0,
    "running": True,
    "active_fault": "none",
    "fault_duration_remaining": 0,

    # Core Telemetry Parameters that drive Pod Creation & Deletion
    "rps": 120,                  # Total Requests Per Second
    "target_rps_per_pod": 80,    # Target threshold: 80 req/s per pod
    "cpu_pct": 45.0,             # CPU utilization percentage
    "target_cpu_pct": 50.0,      # Target CPU threshold: 50%
    "memory_mb": 240,            # RAM usage per pod (MB)
    "target_memory_mb": 400,     # Memory ceiling limit (MB)
    "p95_latency_ms": 110,       # P95 response latency (ms)
    "sla_latency_ms": 300,       # SLO Latency bound (ms)
    "error_rate_pct": 0.4,       # HTTP 5xx error rate (%)
    "error_threshold_pct": 2.0,  # Warning threshold (%)

    # Scaling State & Triggers
    "hpa_replicas": 2,
    "mg_replicas": 2,
    "scale_trigger_reason": "Steady-state: Load within target limits.",
    "pod_creation_triggered": False,
    "pod_deletion_triggered": False,
    "hpa_action": "MAINTAIN",
    "mg_action": "SCALE",
    "hpa_flaps": 0,
    "mg_flaps": 0,

    # Watchdog & Trust Score
    "trust_score": 1.0,
    "confidence_level": "HIGH",
    "s_fresh": 1.0,
    "s_cont": 1.0,
    "s_valid": 1.0,

    "events": [],
}

config = MicroGuardianConfig()
trust_engine = TrustScoreEngine(config.trust_score)
fallback_engine = FallbackEngine(window_size=8, min_replicas=2)
decision_engine = DecisionEngine(
    scaling_config=config.scaling,
    trust_config=config.trust_score,
    fallback_engine=fallback_engine,
)

hpa_prev_dir = 0
mg_prev_dir = 0
prev_mg_metrics = {"cpu": 0.45}
prev_time = time.time()


def simulation_worker():
    """Background simulation thread running the 2-second tick loop."""
    global hpa_prev_dir, mg_prev_dir, prev_mg_metrics, prev_time

    while STATE["running"]:
        time.sleep(2.0)
        STATE["cycle"] += 1
        now = time.time()

        # Handle active fault countdown
        if STATE["fault_duration_remaining"] > 0:
            STATE["fault_duration_remaining"] -= 1
            if STATE["fault_duration_remaining"] <= 0:
                STATE["active_fault"] = "none"

        fault = STATE["active_fault"]

        # Base workload oscillation (simulating dynamic traffic pattern)
        cycle_phase = STATE["cycle"] * 0.18
        traffic_factor = 0.50 + 0.42 * math.sin(cycle_phase) # range: 0.10 to 0.95
        traffic_factor = max(0.12, min(0.96, traffic_factor))

        # 1. Compute realistic operational telemetry parameters based on traffic
        base_rps = int(60 + traffic_factor * 320)     # e.g., 60 to 380 RPS
        current_pods = STATE["mg_replicas"]
        effective_load_per_pod = (base_rps / max(1, current_pods)) / 80.0

        cpu_val = min(98.0, max(15.0, effective_load_per_pod * 50.0 + random.uniform(-3, 3)))
        mem_val = min(480, max(160, int(180 + (cpu_val / 100.0) * 220 + random.randint(-10, 10))))

        # Latency increases non-linearly if pods are heavily overloaded
        if effective_load_per_pod > 1.2:
            p95_lat = int(250 + (effective_load_per_pod - 1.0) * 450 + random.randint(10, 80))
            err_rate = round(min(15.0, 1.2 + (effective_load_per_pod - 1.0) * 8.5), 1)
        else:
            p95_lat = int(50 + effective_load_per_pod * 90 + random.randint(5, 25))
            err_rate = round(max(0.1, random.uniform(0.1, 0.6)), 1)

        # 2. Inject Telemetry Faults onto the observed signal
        sample_time = now
        obs_cpu_fraction = cpu_val / 100.0
        obs_rps = base_rps
        s_valid_override = 1.0

        if fault == "scrape-delay":
            sample_time = now - 75.0      # 75-second scraping delay
            obs_cpu_fraction = 0.22      # Stale low reading
            obs_rps = 40
        elif fault == "packet-loss":
            jitter = random.choice([0.15, 0.92, 0.10, 0.88])
            obs_cpu_fraction = jitter
            obs_rps = int(jitter * 400)
        elif fault == "endpoint-failure":
            obs_cpu_fraction = 0.0       # Complete zero-fill
            obs_rps = 0
            err_rate = 9.8
            s_valid_override = 0.25      # Missing scrape targets
        elif fault == "counter-spike":
            obs_cpu_fraction = 4.80      # Impossible derivative jump
            obs_rps = 1850

        # Store published values for dashboard
        STATE["rps"] = obs_rps
        STATE["cpu_pct"] = round(obs_cpu_fraction * 100.0, 1) if obs_cpu_fraction <= 1.0 else 480.0
        STATE["memory_mb"] = mem_val
        STATE["p95_latency_ms"] = p95_lat
        STATE["error_rate_pct"] = err_rate

        # 3. Standard Kubernetes HPA Decision Logic (Ratio Formula)
        target_cpu_frac = 0.50
        target_rps_ratio = obs_rps / (STATE["target_rps_per_pod"] * max(1, STATE["hpa_replicas"]))

        if obs_cpu_fraction <= 0.05:
            # Zero-filling trick: standard HPA drops to minReplicas (2 pods)!
            desired_hpa = 2
        else:
            desired_hpa = max(2, min(10, math.ceil(STATE["hpa_replicas"] * (obs_cpu_fraction / target_cpu_frac))))

        if desired_hpa > STATE["hpa_replicas"]:
            if hpa_prev_dir == -1:
                STATE["hpa_flaps"] += 1
            hpa_prev_dir = 1
            STATE["hpa_action"] = "SCALE_UP (Pod Creation)"
            STATE["hpa_replicas"] = desired_hpa
        elif desired_hpa < STATE["hpa_replicas"]:
            if hpa_prev_dir == 1:
                STATE["hpa_flaps"] += 1
            hpa_prev_dir = -1
            STATE["hpa_action"] = "SCALE_DOWN (Pod Deletion)"
            STATE["hpa_replicas"] = desired_hpa
        else:
            STATE["hpa_action"] = "MAINTAIN"

        # 4. MicroGuardian Telemetry Watchdog Checks
        fresh_res = evaluate_freshness([sample_time], threshold_seconds=30.0, max_staleness_seconds=120.0, current_time=now)
        s_fresh = fresh_res["score"]

        delta_t = max(now - prev_time, 1.0)
        cont_res = evaluate_continuity({"cpu": obs_cpu_fraction}, prev_mg_metrics, delta_time_seconds=delta_t, max_rate=5.0)
        s_cont = cont_res["score"]
        prev_mg_metrics = {"cpu": obs_cpu_fraction}
        prev_time = now

        s_valid = s_valid_override

        # 5. Trust Score Computation
        trust_snap = trust_engine.compute("order-service", s_fresh, s_cont, s_valid)
        STATE["trust_score"] = round(trust_snap.trust_score, 2)
        STATE["confidence_level"] = trust_snap.confidence_level
        STATE["s_fresh"] = round(s_fresh, 2)
        STATE["s_cont"] = round(s_cont, 2)
        STATE["s_valid"] = round(s_valid, 2)

        # 6. Confidence-Weighted Decision Engine
        mg_dec = decision_engine.decide(
            deployment="order-service",
            trust_snapshot=trust_snap,
            current_replicas=STATE["mg_replicas"],
            current_metric_value=obs_cpu_fraction,
            target_metric_value=target_cpu_frac,
            now=now,
        )

        desired_mg = mg_dec.desired_replicas
        STATE["mg_action"] = mg_dec.action

        # Determine Pod Creation or Deletion Trigger explanation
        if mg_dec.action == "FREEZE":
            STATE["pod_creation_triggered"] = False
            STATE["pod_deletion_triggered"] = False
            STATE["scale_trigger_reason"] = (
                f"🔒 SCALE FROZEN: Telemetry Trust Score Ts={trust_snap.trust_score:.2f} < 0.80. "
                f"Pod creation/deletion suspended to prevent flapping under {fault.upper()}."
            )
        elif mg_dec.action == "FALLBACK":
            STATE["pod_creation_triggered"] = False
            STATE["pod_deletion_triggered"] = False
            STATE["scale_trigger_reason"] = (
                f"🛡️ HISTORICAL FALLBACK: Severe Telemetry Loss (Ts={trust_snap.trust_score:.2f} < 0.40). "
                f"Maintaining safety floor at {desired_mg} pods."
            )
        else: # Normal Scaling
            if desired_mg > STATE["mg_replicas"]:
                STATE["pod_creation_triggered"] = True
                STATE["pod_deletion_triggered"] = False
                STATE["scale_trigger_reason"] = (
                    f"🟢 POD CREATION (Scale-Up): CPU ({STATE['cpu_pct']}%) > 50% target "
                    f"OR RPS ({STATE['rps']}) > capacity ({STATE['mg_replicas'] * STATE['target_rps_per_pod']} req/s). "
                    f"Scaling {STATE['mg_replicas']} → {desired_mg} pods."
                )
            elif desired_mg < STATE["mg_replicas"]:
                STATE["pod_creation_triggered"] = False
                STATE["pod_deletion_triggered"] = True
                STATE["scale_trigger_reason"] = (
                    f"🟡 POD DELETION (Scale-Down): CPU ({STATE['cpu_pct']}%) < 50% target. "
                    f"Releasing idle capacity: {STATE['mg_replicas']} → {desired_mg} pods."
                )
            else:
                STATE["pod_creation_triggered"] = False
                STATE["pod_deletion_triggered"] = False
                STATE["scale_trigger_reason"] = (
                    f"⚪ EQUILIBRIUM: Workload demand is balanced across {STATE['mg_replicas']} pods "
                    f"(CPU {STATE['cpu_pct']}%, RPS {STATE['rps']})."
                )

        if desired_mg > STATE["mg_replicas"]:
            if mg_prev_dir == -1:
                STATE["mg_flaps"] += 1
            mg_prev_dir = 1
            STATE["mg_replicas"] = desired_mg
        elif desired_mg < STATE["mg_replicas"]:
            if mg_prev_dir == 1:
                STATE["mg_flaps"] += 1
            mg_prev_dir = -1
            STATE["mg_replicas"] = desired_mg

        # Append to live event log
        log_entry = {
            "time": time.strftime("%H:%M:%S"),
            "cycle": STATE["cycle"],
            "fault": fault,
            "rps": STATE["rps"],
            "cpu": STATE["cpu_pct"],
            "err": STATE["error_rate_pct"],
            "lat": STATE["p95_latency_ms"],
            "ts": STATE["trust_score"],
            "level": STATE["confidence_level"],
            "action": STATE["mg_action"],
            "hpa_rep": STATE["hpa_replicas"],
            "mg_rep": STATE["mg_replicas"],
        }
        STATE["events"].insert(0, log_entry)
        if len(STATE["events"]) > 20:
            STATE["events"].pop()


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>MicroGuardian — Telemetry-Integrity-Aware K8s Autoscaling</title>
  <style>
    :root {
      --bg: #0f172a;
      --card: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --accent: #38bdf8;
      --success: #22c55e;
      --warning: #f59e0b;
      --danger: #ef4444;
      --purple: #a855f7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 22px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid var(--border); padding-bottom: 16px; }
    .title h1 { font-size: 24px; font-weight: 700; color: var(--accent); }
    .title p { color: var(--text-muted); font-size: 13px; margin-top: 4px; }
    .status-badge { padding: 6px 14px; border-radius: 999px; font-weight: 600; font-size: 13px; text-transform: uppercase; }
    
    /* Metrics Row */
    .section-title { font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--text-muted); margin-bottom: 10px; font-weight: 700; }
    .metrics-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 14px; margin-bottom: 20px; }
    .metric-card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px; position: relative; }
    .metric-label { font-size: 11px; text-transform: uppercase; color: var(--text-muted); font-weight: 600; }
    .metric-val { font-size: 26px; font-weight: 800; margin: 4px 0; }
    .metric-target { font-size: 11px; color: var(--text-muted); }

    /* Core Control Grids */
    .grid { display: grid; grid-template-columns: 1.4fr 1.1fr 1fr; gap: 16px; margin-bottom: 20px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 18px; }
    .card h3 { font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted); margin-bottom: 12px; }
    .big-value { font-size: 34px; font-weight: 800; }
    .progress-bar { width: 100%; height: 7px; background: rgba(255,255,255,0.1); border-radius: 4px; overflow: hidden; margin-top: 8px; }
    .progress-fill { height: 100%; transition: width 0.3s; }
    
    /* Trigger Banner */
    .trigger-banner { background: #1e293b; border: 1px solid var(--border); border-left: 6px solid var(--accent); border-radius: 8px; padding: 14px 18px; margin-bottom: 20px; }
    .trigger-banner h4 { font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; color: var(--text-muted); }
    .trigger-banner p { font-size: 14px; font-weight: 600; }

    /* Buttons */
    .btn-group { display: flex; flex-wrap: wrap; gap: 10px; }
    button { background: #334155; color: #fff; border: 1px solid #475569; padding: 9px 15px; border-radius: 8px; font-weight: 600; cursor: pointer; transition: all 0.2s; font-size: 12px; }
    button:hover { background: #475569; border-color: var(--accent); }
    button.danger { background: rgba(239, 68, 68, 0.15); border-color: var(--danger); color: #fca5a5; }
    button.danger:hover { background: var(--danger); color: white; }
    
    /* Table */
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 12px; }
    th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); }
    th { color: var(--text-muted); font-weight: 600; }
    .tag { padding: 2px 7px; border-radius: 4px; font-size: 10px; font-weight: 700; }

    @media (max-width: 1024px) {
      .metrics-grid { grid-template-columns: repeat(2, 1fr); }
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="header">
    <div class="title">
      <h1>🛡️ MicroGuardian Control Plane</h1>
      <p>Telemetry-Integrity-Aware Kubernetes Autoscaling — Pod Creation & Deletion Parameters</p>
    </div>
    <div id="system-badge" class="status-badge" style="background: rgba(34,197,94,0.2); color: #86efac;">
      STATUS: HIGH CONFIDENCE
    </div>
  </div>

  <!-- Telemetry Parameters Row (RPS, Errors, CPU, Latency, Memory) -->
  <div class="section-title">📊 Live Telemetry Stream Driving Pod Creation & Deletion</div>
  <div class="metrics-grid">
    <!-- 1. RPS -->
    <div class="metric-card">
      <div class="metric-label">Requests / Sec (RPS)</div>
      <div id="val-rps" class="metric-val" style="color: #38bdf8;">120</div>
      <div class="metric-target">Target: <strong id="target-rps">80 req/s</strong> per pod</div>
    </div>
    <!-- 2. CPU % -->
    <div class="metric-card">
      <div class="metric-label">CPU Utilization %</div>
      <div id="val-cpu" class="metric-val" style="color: #f59e0b;">45.0%</div>
      <div class="metric-target">Autoscale Target: <strong>50.0%</strong></div>
    </div>
    <!-- 3. Error Rate -->
    <div class="metric-card">
      <div class="metric-label">HTTP 5xx Error Rate</div>
      <div id="val-err" class="metric-val" style="color: #22c55e;">0.4%</div>
      <div class="metric-target">SLA Threshold: <strong>&lt; 2.0%</strong></div>
    </div>
    <!-- 4. P95 Latency -->
    <div class="metric-card">
      <div class="metric-label">P95 Response Latency</div>
      <div id="val-lat" class="metric-val" style="color: #a855f7;">110 ms</div>
      <div class="metric-target">SLO Limit: <strong>&lt; 300 ms</strong></div>
    </div>
    <!-- 5. Memory -->
    <div class="metric-card">
      <div class="metric-label">Memory Working Set</div>
      <div id="val-mem" class="metric-val" style="color: #cbd5e1;">240 MB</div>
      <div class="metric-target">Pod Limit: <strong>400 MB</strong></div>
    </div>
  </div>

  <!-- Pod Creation / Deletion Trigger Explanation Banner -->
  <div class="trigger-banner" id="trigger-banner">
    <h4>⚡ Pod Scaling Decision Engine Trigger</h4>
    <p id="trigger-text">Evaluating real-time resource utilization against target capacity...</p>
  </div>

  <div class="grid">
    <!-- Trust Score Card -->
    <div class="card">
      <h3>Telemetry Trust Score T_s(t)</h3>
      <div id="trust-val" class="big-value" style="color: var(--accent)">1.00</div>
      <div class="progress-bar">
        <div id="trust-bar" class="progress-fill" style="width: 100%; background: var(--accent);"></div>
      </div>
      <p style="margin-top: 10px; font-size: 12px; color: var(--text-muted); line-height: 1.5;">
        <strong>Actuation Gates:</strong><br>
        • <span style="color:#86efac;">T_s ≥ 0.80</span>: Normal Pod Creation / Deletion<br>
        • <span style="color:#fde68a;">0.40 ≤ T_s &lt; 0.80</span>: <strong>FREEZE</strong> (Block scaling to stop flapping)<br>
        • <span style="color:#fca5a5;">T_s &lt; 0.40</span>: <strong>FALLBACK</strong> to Moving-Average Floor
      </p>
    </div>

    <!-- Watchdog Integrity Decomposition -->
    <div class="card">
      <h3>Watchdog Validation Layer</h3>
      <div style="font-size: 13px; display: flex; flex-direction: column; gap: 8px;">
        <div>Freshness S_fresh: <strong id="val-fresh" style="color:#38bdf8">1.00</strong> (Max delay: 30s)</div>
        <div class="progress-bar"><div id="bar-fresh" class="progress-fill" style="width:100%; background:#38bdf8"></div></div>
        
        <div>Continuity S_cont: <strong id="val-cont" style="color:#a855f7">1.00</strong> (Max rate: σ=5.0)</div>
        <div class="progress-bar"><div id="bar-cont" class="progress-fill" style="width:100%; background:#a855f7"></div></div>
        
        <div>Completeness S_valid: <strong id="val-valid" style="color:#22c55e">1.00</strong> (Missing pods check)</div>
        <div class="progress-bar"><div id="bar-valid" class="progress-fill" style="width:100%; background:#22c55e"></div></div>
      </div>
    </div>

    <!-- Replicas Comparison -->
    <div class="card">
      <h3>Pods: HPA vs MicroGuardian</h3>
      <div style="display: flex; justify-content: space-around; text-align: center; margin-top: 6px;">
        <div>
          <div style="font-size: 11px; color: var(--danger); font-weight:700;">STANDARD HPA</div>
          <div id="hpa-pods" style="font-size: 32px; font-weight: 800; color: #f87171">2</div>
          <div id="hpa-action" style="font-size: 11px; color: var(--text-muted)">MAINTAIN</div>
          <div style="font-size: 11px; color: #fca5a5; margin-top: 4px;">Flaps: <strong id="hpa-flaps">0</strong></div>
        </div>
        <div style="width: 1px; background: var(--border);"></div>
        <div>
          <div style="font-size: 11px; color: var(--accent); font-weight:700;">MICROGUARDIAN</div>
          <div id="mg-pods" style="font-size: 32px; font-weight: 800; color: #38bdf8">2</div>
          <div id="mg-action" style="font-size: 11px; color: var(--text-muted)">SCALE</div>
          <div style="font-size: 11px; color: #7dd3fc; margin-top: 4px;">Flaps: <strong id="mg-flaps">0</strong></div>
        </div>
      </div>
    </div>
  </div>

  <!-- Chaos Fault Injector Bar -->
  <div class="card" style="margin-bottom: 20px;">
    <h3>💥 Fault Injection Control (Corrupt Live Observability Parameters)</h3>
    <p style="font-size: 12px; color: var(--text-muted); margin-bottom: 10px;">
      Simulate monitoring plane failures to see how Standard HPA erroneously creates/deletes pods vs how MicroGuardian intervenes:
    </p>
    <div class="btn-group">
      <button class="danger" onclick="injectFault('scrape-delay')">⏱️ Inject Scrape Delay (Stale Metrics)</button>
      <button class="danger" onclick="injectFault('packet-loss')">📡 Inject Packet Loss (Jitter & Flapping)</button>
      <button class="danger" onclick="injectFault('endpoint-failure')">❌ Inject Endpoint Failure (Zero-fill Drop)</button>
      <button class="danger" onclick="injectFault('counter-spike')">⚡ Inject Counter Spike (Artificial Burst)</button>
      <button onclick="injectFault('none')">🟢 Restore Normal Scrapes</button>
    </div>
  </div>

  <!-- Realtime Event Log -->
  <div class="card">
    <h3>Live Reconciliation Decision Log</h3>
    <table>
      <thead>
        <tr>
          <th>Time</th>
          <th>Cycle</th>
          <th>Fault Mode</th>
          <th>RPS</th>
          <th>CPU %</th>
          <th>P95 Lat</th>
          <th>Errors</th>
          <th>Trust Score (Ts)</th>
          <th>Watchdog Gating</th>
          <th>HPA Pods</th>
          <th>MicroGuardian Pods</th>
        </tr>
      </thead>
      <tbody id="log-body">
      </tbody>
    </table>
  </div>

  <script>
    async function fetchState() {
      try {
        const res = await fetch('/api/state');
        const d = await res.json();

        // Parameters
        document.getElementById('val-rps').innerText = d.rps + ' req/s';
        document.getElementById('val-cpu').innerText = d.cpu_pct.toFixed(1) + '%';
        document.getElementById('val-err').innerText = d.error_rate_pct.toFixed(1) + '%';
        document.getElementById('val-lat').innerText = d.p95_latency_ms + ' ms';
        document.getElementById('val-mem').innerText = d.memory_mb + ' MB';

        // Error color warning
        document.getElementById('val-err').style.color = d.error_rate_pct > 2.0 ? '#ef4444' : '#22c55e';
        document.getElementById('val-lat').style.color = d.p95_latency_ms > 300 ? '#ef4444' : '#a855f7';

        // Trigger banner
        document.getElementById('trigger-text').innerText = d.scale_trigger_reason;
        const banner = document.getElementById('trigger-banner');
        if (d.pod_creation_triggered) {
          banner.style.borderLeftColor = '#22c55e';
        } else if (d.pod_deletion_triggered) {
          banner.style.borderLeftColor = '#f59e0b';
        } else if (d.confidence_level !== 'HIGH') {
          banner.style.borderLeftColor = '#ef4444';
        } else {
          banner.style.borderLeftColor = '#38bdf8';
        }

        // Trust score
        document.getElementById('trust-val').innerText = d.trust_score.toFixed(2);
        document.getElementById('trust-bar').style.width = (d.trust_score * 100) + '%';

        // Decomposition
        document.getElementById('val-fresh').innerText = d.s_fresh.toFixed(2);
        document.getElementById('bar-fresh').style.width = (d.s_fresh * 100) + '%';
        document.getElementById('val-cont').innerText = d.s_cont.toFixed(2);
        document.getElementById('bar-cont').style.width = (d.s_cont * 100) + '%';
        document.getElementById('val-valid').innerText = d.s_valid.toFixed(2);
        document.getElementById('bar-valid').style.width = (d.s_valid * 100) + '%';

        // Pods
        document.getElementById('hpa-pods').innerText = d.hpa_replicas;
        document.getElementById('hpa-action').innerText = d.hpa_action;
        document.getElementById('hpa-flaps').innerText = d.hpa_flaps;
        document.getElementById('mg-pods').innerText = d.mg_replicas;
        document.getElementById('mg-action').innerText = d.mg_action;
        document.getElementById('mg-flaps').innerText = d.mg_flaps;

        // Badge
        const badge = document.getElementById('system-badge');
        if (d.confidence_level === 'HIGH') {
          badge.style.background = 'rgba(34,197,94,0.2)';
          badge.style.color = '#86efac';
          badge.innerText = 'STATUS: HIGH CONFIDENCE (NORMAL SCALE)';
        } else if (d.confidence_level === 'DEGRADED') {
          badge.style.background = 'rgba(245,158,11,0.2)';
          badge.style.color = '#fde68a';
          badge.innerText = 'STATUS: DEGRADED (POD SCALING FROZEN)';
        } else {
          badge.style.background = 'rgba(239,68,68,0.2)';
          badge.style.color = '#fca5a5';
          badge.innerText = 'STATUS: SEVERE LOSS (HISTORICAL FALLBACK)';
        }

        // Event logs
        const tbody = document.getElementById('log-body');
        tbody.innerHTML = d.events.map(e => `
          <tr>
            <td>${e.time}</td>
            <td>#${e.cycle}</td>
            <td><span class="tag" style="background:${e.fault !== 'none' ? '#ef4444' : '#22c55e'}">${e.fault.toUpperCase()}</span></td>
            <td><strong>${e.rps}</strong></td>
            <td>${e.cpu.toFixed(1)}%</td>
            <td>${e.lat}ms</td>
            <td style="color:${e.err > 2 ? '#ef4444' : '#86efac'}">${e.err}%</td>
            <td><strong>${e.ts.toFixed(2)}</strong></td>
            <td><code>${e.level} (${e.action})</code></td>
            <td style="color:#f87171">${e.hpa_rep}</td>
            <td style="color:#38bdf8"><strong>${e.mg_rep}</strong></td>
          </tr>
        `).join('');

      } catch (err) {
        console.error(err);
      }
    }

    async function injectFault(faultName) {
      await fetch('/api/inject?fault=' + encodeURIComponent(faultName));
      fetchState();
    }

    setInterval(fetchState, 1500);
    fetchState();
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
        elif parsed.path == "/api/state":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(STATE).encode("utf-8"))
        elif parsed.path == "/api/inject":
            qs = parse_qs(parsed.query)
            fault = qs.get("fault", ["none"])[0]
            STATE["active_fault"] = fault
            STATE["fault_duration_remaining"] = 14 if fault != "none" else 0
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "fault": fault}).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def main():
    port = int(os.getenv("PORT", 8090))
    print("=" * 60)
    print(f" MicroGuardian Live Interactive Dashboard")
    print(f" Review 1 Demonstration Server")
    print("=" * 60)
    print(f" Open your browser to: http://localhost:{port}")
    print("=" * 60)

    sim_thread = threading.Thread(target=simulation_worker, daemon=True)
    sim_thread.start()

    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping MicroGuardian Dashboard...")
        STATE["running"] = False
        server.server_close()


if __name__ == "__main__":
    main()
