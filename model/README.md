# MicroGuardian: Telemetry Trust Score Models & Realtime Evaluation Suite

> Dedicated module containing the baseline trust score model, the novel **Distinct Non-Linear Adaptive Bayesian-Entropy Trust Model (NAB-Trust)**, and a real-time streaming test harness for validation under live telemetry and chaos conditions.

---

## 1. Directory Structure

```
d:\Final year project\Code\model\
├── __init__.py                # Package exports
├── base.py                    # BaseTrustModel, TelemetrySample, ModelPrediction abstractions
├── baseline_linear.py         # Existing linear weighted sum model (Reference baseline)
├── distinct_model.py          # Novel Nonlinear Adaptive Bayesian-Entropy Model (NAB-Trust)
├── realtime_evaluator.py      # Realtime stream evaluator & synthetic microservice generator
├── prometheus_client.py       # Live Prometheus HTTP API client for live Kubernetes clusters
├── benchmark.py               # Rigorous 5-scenario comparative benchmark suite
├── test_realtime.py           # Interactive CLI real-time streaming test runner
├── web_monitor.py             # Live browser-based visual monitoring dashboard (Port 8088)
├── test_model.py              # Comprehensive unit tests for model invariants
└── README.md                  # This technical documentation
```

---

## 2. Research Problem & Flaws in Existing Models

In the initial implementation of MicroGuardian (`controller/trust_score.py`), the Telemetry Trust Score was calculated using a simple linear weighted sum:

$$T_s(t) = w_f \cdot S_{\text{fresh}} + w_c \cdot S_{\text{cont}} + w_v \cdot S_{\text{valid}}$$

$$\text{with fixed static weights } w_f = 0.40, w_c = 0.30, w_v = 0.30$$

### Critical Vulnerabilities in the Existing Model:

1. **Additive Compensation Masking (The "Rotten Apple" Flaw):**
   - Telemetry integrity is an all-or-nothing chain: *if metrics are stale or zero-filled, the entire stream is unsafe to scale upon*.
   - In a linear model, if Prometheus scraping completely halts ($S_{\text{fresh}} = 0.0$), but the stale counter stays flat ($S_{\text{cont}} = 1.0$) and pods are running ($S_{\text{valid}} = 1.0$):
     $$T_s = 0.4(0.0) + 0.3(1.0) + 0.3(1.0) = 0.60$$
   - The controller classifies this as **DEGRADED (0.60)** and freezes pods, instead of executing an immediate **SEVERE (T_s < 0.40) emergency fallback**.
2. **Static Rigid Weights:**
   - Real-world networks experience variable jitter and volatility. The existing model assigns static $0.4 / 0.3 / 0.3$ regardless of which telemetry channel is noisy or reliable.
3. **Zero Epistemic Uncertainty Estimation:**
   - Provides only a single deterministic point estimate without confidence intervals ($\pm \sigma$). It cannot quantify whether trust is supported by consistent history or transient luck.
4. **Semantic Decoupling Obliviousness:**
   - Single-variable isolation: If an exporter crashes and emits `CPU = 0.0` while actual HTTP traffic is 120 req/s, the existing model fails to detect the mismatch and erroneously scales down workloads.

---

## 3. The Distinct Model: Non-Linear Adaptive Bayesian-Entropy (NAB-Trust)

To solve these flaws, we designed and implemented `NonlinearAdaptiveBayesianTrustModel` (`model/distinct_model.py`), integrating four distinct mathematical innovations:

### 3.1. Anti-Compensation Bottleneck T-Norm Gate
Instead of letting high continuity mask dead freshness, the model calculates a non-linear bottleneck penalty:

$$g(S) = \min\left(1.0, \left(\frac{\min(S_{\text{fresh}}, S_{\text{cont}}, S_{\text{valid}})}{\theta_{\text{crit}}}\right)^{\gamma}\right)$$

*Parameters:* $\theta_{\text{crit}} = 0.45, \gamma = 1.8$.

Furthermore, the core combination uses a **geometric power mean** ($S_{\text{geom}} = \prod S_i^{w_i}$). If any single signal approaches zero, the score plummets immediately to zero, completely eliminating false compensation.

### 3.2. Dynamic Shannon Information-Entropy Weighting
Instead of static weights, the model computes sample variance $\sigma_i^2$ for each signal over a rolling window ($W = 12$).
The signal stability index is derived via:

$$u_i = \exp(-\eta \cdot \sigma_i)$$

Dynamic weights adapt in real-time, shifting trust toward stable, uncorrupted observability channels while penalizing erratic channels:

$$w_i(t) = \alpha_{\text{prior}} w_{i,0} + (1 - \alpha_{\text{prior}}) \frac{u_i}{\sum_j u_j}$$

### 3.3. Recursive Bayesian Belief Updating with Epistemic Uncertainty
Maintains a conjugate Beta-distribution state $(\alpha_t, \beta_t)$ with exponential memory decay ($\lambda = 0.82$):

$$\alpha_t = \lambda \cdot \alpha_{t-1} + 2.5 \cdot S_{\text{evidence}}$$
$$\beta_t = \lambda \cdot \beta_{t-1} + 2.5 \cdot (1.0 - S_{\text{evidence}})$$

This yields both the **Posterior Expected Trust** $\mu_t$ and the **Epistemic Uncertainty Variance**:

$$\sigma_t^2 = \frac{\alpha_t \beta_t}{(\alpha_t + \beta_t)^2 (\alpha_t + \beta_t + 1)}$$

To prevent over-optimistic scaling during transient faults, actuation decisions use the **Risk-Adjusted Credible Lower Bound**:

$$T_{\text{credible}} = \max(0.0, \mu_t - 0.75 \cdot \sigma_t)$$

### 3.4. Multi-Variate Semantic Coherence Verification
Cross-validates operational telemetry consistency:
- **Zero-Fill Detection:** Flags when external RPS $> 35 \text{ req/s}$ but reported $\text{CPU} \le 0.01$.
- **Latency Decoupling:** Flags when P95 Latency $> 800\text{ ms}$ while CPU is reported idle ($< 10\%$).
- **Counter Reset Anomaly:** Detects sudden $> 50\%$ single-tick jumps in CPU when external RPS exhibits zero change.

---

## 4. Benchmark Results: Head-to-Head Comparison

Running the benchmark across 5 controlled failure domains (`python model/benchmark.py`):

| Evaluation Metric | Existing Linear Model | Distinct NAB Model | Improvement / Gain |
| :--- | :---: | :---: | :---: |
| **Overall Classification Accuracy** | 86.00% | **92.42%** | **+6.42% gain** |
| **Fault Detection F1-Score** | 58.06% | **72.44%** | **+14.38% gain** |
| **Root Mean Square Error (RMSE)** | 0.3596 | **0.2361** | **34.4% error reduction** |
| **Sensor Zero-Filling Accuracy** | 40.0% | **84.0%** | **+44.0% accuracy gain** |
| **False Negative Rate (Blind Scaling)** | High (Compensatory) | **Near Zero** | **-73.3% blind scales** |
| **Inference Overhead (Latency)** | 2.9 μs | **14.5 μs** | **Ultra-fast (< 0.02 ms)** |

---

## 5. How to Test with Realtime Data

### 5.1. Live Terminal Stream Monitor (CLI)
Stream real-time telemetry tick-by-tick and observe decisions side-by-side:

```bash
# Run 30 seconds of live simulated stream with automated Scrape Delay fault:
python model/test_realtime.py --seconds 30 --inject-fault scrape-delay --fault-at 8 --interval 0.5

# Test sensor zero-filling detection:
python model/test_realtime.py --seconds 30 --inject-fault zero-fill --fault-at 8 --interval 0.5

# Fast batch evaluation mode:
python model/test_realtime.py --fast --seconds 60
```

### 5.2. Testing Against Live Prometheus in Kubernetes
If you have Prometheus running locally or in Minikube (`http://localhost:9090`):

```bash
python model/test_realtime.py --prometheus-url http://localhost:9090 --seconds 60
```

### 5.3. Interactive Real-Time Web Monitor (Browser Dashboard)
Launch the interactive web monitor:

```bash
python model/web_monitor.py --port 8088
```
Open **`http://localhost:8088`** in your browser to:
- View live side-by-side gauges and real-time SVG charts.
- Inject faults with 1 click (**Scrape Delay**, **Counter Reset**, **Sensor Zero-Filling**, **Endpoint Drop**, **Compound Multi-Fault**).
- Inspect live accuracy, RMSE, and autoscaling logs in real time.

### 5.4. Running the Benchmark Suite
```bash
python model/benchmark.py
```

### 5.5. Running Unit Tests
```bash
python -m unittest model/test_model.py
```
