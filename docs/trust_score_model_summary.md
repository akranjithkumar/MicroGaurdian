# MicroGuardian: Telemetry Trust Score Model Engine
## Technical Summary, Architectural Specification & Empirical Evaluation Report

---

## 1. Executive Summary

In Kubernetes autoscaling environments (Horizontal Pod Autoscaler - HPA, KEDA), controllers make scaling decisions under the blind assumption that telemetry streams (Prometheus, cAdvisor, custom metrics) are consistently pristine, timely, and semantically valid.

**MicroGuardian** introduces an inline **Telemetry Watchdog Layer** that continuously computes a real-time **Telemetry Trust Score** $T_s(t) \in [0, 1]$ across three formal validation dimensions:
1. **Temporal Freshness ($S_{\text{fresh}}$):** Ensures samples are not delayed or stale.
2. **Derivative Continuity ($S_{\text{cont}}$):** Filters impossible metric rate jumps caused by counter resets or exporter restarts.
3. **Scrape Completeness ($S_{\text{valid}}$):** Verifies active pod endpoint reporting without null or zero-filled data.

This report summarizes the design, mathematical formulation, and comparative empirical evaluation of two trust models:
- **Existing Baseline Model:** Static Linear Weighted Combination.
- **Distinct Novel Model:** Non-Linear Adaptive Bayesian-Entropy Trust Model (**NAB-Trust**), implemented in the dedicated module [`d:\Final year project\Code\model\`](file:///d:/Final%20year%20project/Code/model/).

---

## 2. Mathematical System Model & Flaws in Existing Models

### 2.1. The Existing Baseline Model (Linear Weighted Sum)
The initial implementation computed trust via a linear scalar combination:
$$T_s^{\text{linear}}(t) = w_f \cdot S_{\text{fresh}} + w_c \cdot S_{\text{cont}} + w_v \cdot S_{\text{valid}}$$
$$\text{with static weights } w_f = 0.40,\ w_c = 0.30,\ w_v = 0.30$$

#### The Additive Compensation Vulnerability:
Telemetry integrity is an all-or-nothing chain. If Prometheus stops scraping for 2 minutes, $S_{\text{fresh}} = 0.0$. However, because the old metric counter stayed flat ($S_{\text{cont}} = 1.0$) and pods remained running ($S_{\text{valid}} = 1.0$):
$$T_s = 0.4(0.0) + 0.3(1.0) + 0.3(1.0) = \mathbf{0.60}$$
The autoscaler erroneously categorized this as **DEGRADED (0.60)** and froze replicas, rather than immediately executing an emergency **SEVERE fallback ($T_s < 0.40$)**.

---

### 2.2. The Distinct NAB-Trust Model Architecture

The distinct model ([`model/distinct_model.py`](file:///d:/Final%20year%20project/Code/model/distinct_model.py)) integrates four synergistic innovations:

```mermaid
graph TD
    subgraph Watchdog Validation Signals
        A1["Freshness (S_fresh)"]
        A2["Continuity (S_cont)"]
        A3["Completeness (S_valid)"]
    end

    subgraph Dynamic Entropy Weighting
        B["Sliding Window Variance: sigma_i^2"] --> C["Stability Weights: w_i(t)"]
    end

    subgraph Core Non-Linear Fusion
        A1 & A2 & A3 & C --> D["Geometric Power Core: S_geom"]
        A1 & A2 & A3 --> E["Anti-Compensation Gate: g(S_min)"]
        D & E --> F["Instantaneous Evidence: x(t)"]
    end

    subgraph Semantic Coherence
        G["RPS vs CPU Coupling"] --> H["S_cohere"]
    end

    subgraph Bayesian Temporal Belief
        F & H --> I["Beta Prior Updating: Beta(alpha_t, beta_t)"]
        I --> J["Posterior Mean: mu_t"]
        I --> K["Epistemic Uncertainty: sigma_t"]
        J & K --> L["Risk-Adjusted Credible Trust: T_s(t)"]
    end
```

#### 1. Anti-Compensation Bottleneck T-Norm Gate
$$g(S) = \min\left(1.0, \left(\frac{\min(S_{\text{fresh}}, S_{\text{cont}}, S_{\text{valid}})}{\theta_{\text{crit}}}\right)^{\gamma}\right)$$
$$\text{where } \theta_{\text{crit}} = 0.45,\ \gamma = 1.8$$
If any individual telemetry signal drops below $\theta_{\text{crit}}$, the gate collapses non-linearly, preventing compensation.

#### 2. Dynamic Shannon Information-Entropy Weighting
For each signal $i \in \{\text{fresh}, \text{cont}, \text{valid}\}$, sample variance $\sigma_i^2$ is tracked over a rolling window ($W = 12$):
$$u_i = \exp(-10.0 \cdot \sigma_i)$$
$$w_i(t) = \alpha_{\text{prior}} w_{i,0} + (1 - \alpha_{\text{prior}}) \frac{u_i}{\sum_j u_j}$$
Weights dynamically shift away from noisy, unstable channels towards reliable channels.

#### 3. Recursive Bayesian Belief Updating with Epistemic Uncertainty
State is modeled as a conjugate Beta distribution $\text{Beta}(\alpha_t, \beta_t)$ with exponential decay ($\lambda = 0.82$):
$$\alpha_t = \lambda \cdot \alpha_{t-1} + 2.5 \cdot S_{\text{evidence}}$$
$$\beta_t = \lambda \cdot \beta_{t-1} + 2.5 \cdot (1.0 - S_{\text{evidence}})$$
Yields posterior mean $\mu_t$ and epistemic uncertainty variance $\sigma_t^2 = \frac{\alpha \beta}{(\alpha+\beta)^2(\alpha+\beta+1)}$. Actuation uses the conservative credible lower bound:
$$T_s(t) = \max\left(0.0, \mu_t - 0.75 \cdot \sigma_t\right)$$

#### 4. Multivariate Semantic Coherence Verification
Cross-validates operational consistency between coupled metrics:
- **Zero-Fill Detection:** External $\text{RPS} > 35\text{ req/s}$ with reported $\text{CPU} \le 0.01 \implies S_{\text{cohere}} \le 0.20$.
- **Latency Decoupling:** P95 Latency $> 800\text{ ms}$ with $\text{CPU} < 10\% \implies S_{\text{cohere}} \le 0.35$.
- **Counter Spikes:** Derivative $\Delta\text{CPU} > 50\%$ without external RPS movement $\implies S_{\text{cohere}} \le 0.50$.

---

## 3. Empirical Results & Benchmark Comparison

Controlled evaluations were conducted across 5 distinct failure domains ([`model/benchmark.py`](file:///d:/Final%20year%20project/Code/model/benchmark.py)):

| Scenario | Fault Characterization | Existing Model Acc | Distinct NAB Acc | False Negative Reduction | Actuation Status |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **S1: Steady-State** | Pristine telemetry, healthy load | 100.0% | **100.0%** | 0.0% | Normal Scaling |
| **S2: Scrape Delay** | Telemetry lag ($> 60$s staleness) | 90.0% | **86.7%** | -5.9% | Fallback Enforced |
| **S3: Counter Spike** | Counter reset instant jump | 100.0% | **91.4%** | 0.0% | Zero Flapping |
| **S4: Zero-Filling** | Monitoring emits 0.0 under load | 40.0% | **84.0%** | **-73.3%** | **Protected from crash** |
| **S5: Multi-Fault** | Scrape delay + packet loss | 100.0% | **100.0%** | 0.0% | Fallback Enforced |

### Summary Performance Metrics:
- **Classification Accuracy:** Existing = $86.00\% \longrightarrow$ Distinct = $\mathbf{92.42\%}$ (**$+6.42\%$ improvement**)
- **Fault Detection F1-Score:** Existing = $58.06\% \longrightarrow$ Distinct = $\mathbf{72.44\%}$ (**$+14.38\%$ improvement**)
- **Root Mean Square Error (RMSE):** Existing = $0.3596 \longrightarrow$ Distinct = $\mathbf{0.2361}$ (**$34.4\%$ error reduction**)
- **Execution Overhead:** Baseline = $3.1\ \mu\text{s}$, Distinct = $\mathbf{14.5\ \mu\text{s}}$ ($< 0.02\text{ ms}$, negligible CPU cost)

---

## 4. Real-Time Streaming Output Log Analysis

The streaming test was executed using the real-time evaluation suite ([`model/evaluation_output.log`](file:///d:/Final%20year%20project/Code/model/evaluation_output.log)).

### 4.1. Incident 1: Scrape Delay Degradation (Ticks 10–17)
```text
Tick  | Fault        | Signals (F/C/V) | Existing Model          | Distinct NAB-Trust          | Safety Verdict
-----------------------------------------------------------------------------------------------------------------
10    | scrape-delay | 0.20/1.00/1.00  | Ts=0.68 [FREEZE  ]      | Ts=0.19 [FALLBACK] +/-0.08  | Clamped via Bottleneck
11    | scrape-delay | 0.10/1.00/1.00  | Ts=0.64 [FREEZE  ]      | Ts=0.05 [FALLBACK] +/-0.11  | Clamped via Bottleneck
12    | scrape-delay | 0.00/1.00/1.00  | Ts=0.60 [FREEZE  ]      | Ts=0.00 [FALLBACK] +/-0.12  | Clamped via Bottleneck
18    | healthy      | 1.00/1.00/1.00  | Ts=1.00 [SCALE   ]      | Ts=0.45 [FREEZE  ] +/-0.13  | Stabilization Hysteresis
19    | healthy      | 1.00/1.00/1.00  | Ts=1.00 [SCALE   ]      | Ts=0.63 [FREEZE  ] +/-0.13  | Stabilization Hysteresis
20    | healthy      | 1.00/1.00/1.00  | Ts=1.00 [SCALE   ]      | Ts=0.76 [FREEZE  ] +/-0.12  | Stabilization Hysteresis
21    | healthy      | 1.00/1.00/1.00  | Ts=1.00 [SCALE   ]      | Ts=0.85 [SCALE   ] +/-0.10  | Normal Operation Resumed
```

**Observation:**
1. **Immediate Clamping:** At Tick 10–12, while the Existing Model lingered at $T_s = 0.60$ (refusing fallback), the Distinct Model plummeted to $T_s = 0.00$, immediately engaging safe moving-average fallback.
2. **Elimination of Flapping via Hysteresis:** At Tick 18, when clean data returned, the Existing Model abruptly jumped to $1.00$ in a single cycle. The Distinct Model maintained a controlled 3-cycle stabilization cooldown ($0.45 \to 0.63 \to 0.76 \to 0.85$), preventing pod thrashing under transient network flutter.

### 4.2. Incident 2: Counter Reset Spike (Ticks 26–31)
```text
Tick  | Fault         | Signals (F/C/V) | Existing Model          | Distinct NAB-Trust          | Safety Verdict
-----------------------------------------------------------------------------------------------------------------
26    | counter-spike | 1.00/0.07/1.00  | Ts=0.72 [FREEZE  ]      | Ts=0.02 [FALLBACK] +/-0.10  | Clamped via Bottleneck
27    | counter-spike | 1.00/0.03/1.00  | Ts=0.71 [FREEZE  ]      | Ts=0.01 [FALLBACK] +/-0.12  | Clamped via Bottleneck
30    | counter-spike | 1.00/0.03/1.00  | Ts=0.71 [FREEZE  ]      | Ts=0.00 [FALLBACK] +/-0.13  | Clamped via Bottleneck
32    | healthy       | 1.00/1.00/1.00  | Ts=1.00 [SCALE   ]      | Ts=0.52 [FREEZE  ] +/-0.13  | Stabilization Hysteresis
```

**Observation:**
The Existing Model kept $T_s \approx 0.72$, failing to treat an anomalous counter spike as severe. The Distinct Model completely isolated the anomaly, scoring $T_s = 0.00$.

---

## 5. Defense & Viva Review Guide

### Q1: Why is the Distinct Model superior to existing linear combinations?
> **Defense:** Linear combinations suffer from additive compensatory masking ($0.4 \times 0.0 + 0.3 \times 1.0 + 0.3 \times 1.0 = 0.60$). Telemetry integrity requires all primary dimensions to hold. The Distinct Model uses a non-linear bottleneck gate and geometric power mean, guaranteeing that if any single telemetry channel fails, the trust score collapses immediately, preventing false scaling.

### Q2: Does the Bayesian calculation add significant CPU latency to Kubernetes?
> **Defense:** No. The entire recursive Bayesian update uses closed-form conjugate Beta distribution arithmetic ($\alpha_t = \lambda \alpha + c \cdot x$). The measured execution time is **$14.5$ microseconds** per pod cycle. Standard Kubernetes reconciliation happens every 15,000 milliseconds ($15\text{s}$), meaning our model consumes less than **$0.0001\%$** of the control loop time budget.

### Q3: How does the model prevent replica flapping when networks fluctuate?
> **Defense:** Existing models have zero memory and oscillate between $0$ and $1$ on every transient jitter. The Distinct Model maintains temporal memory through the forgetting factor $\lambda = 0.82$, introducing natural hysteresis (2–3 cycle stabilization) before resuming scaling, guaranteeing smooth cluster stability.

---

## 6. Artifact Index

- **Formatted Output Log:** [`model/evaluation_output.log`](file:///d:/Final%20year%20project/Code/model/evaluation_output.log)
- **Structured JSON Dataset:** [`model/evaluation_results.json`](file:///d:/Final%20year%20project/Code/model/evaluation_results.json)
- **Model Codebase:** [`model/distinct_model.py`](file:///d:/Final%20year%20project/Code/model/distinct_model.py)
- **Live Visual Monitor:** [`model/web_monitor.py`](file:///d:/Final%20year%20project/Code/model/web_monitor.py)
- **Automated Generator Script:** [`model/generate_evaluation_report.py`](file:///d:/Final%20year%20project/Code/model/generate_evaluation_report.py)
