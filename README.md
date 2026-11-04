# MicroGuardian — Telemetry-Integrity-Aware Kubernetes Autoscaler

> Resilient Kubernetes Autoscaling Under Telemetry Drift and Sensor Faults

## Overview

MicroGuardian is a novel Kubernetes autoscaling controller that introduces a **Telemetry Watchdog Layer** into the closed-loop reconciliation cycle of the Horizontal Pod Autoscaler (HPA). Instead of blindly trusting observability pipelines, it computes a real-time **Telemetry Trust Score** to gate scaling decisions and enforce safe fallbacks under degraded monitoring conditions.

### Key Innovation

Traditional HPA assumes telemetry is always accurate. When Prometheus scraping fails, metrics go stale, or counters reset — standard HPA causes **replica flapping**, **SLO violations**, and **resource waste**. MicroGuardian solves this by:

1. **Validating** every metric sample for freshness, continuity, and completeness
2. **Computing** a Trust Score T_s ∈ [0, 1] from weighted validation signals
3. **Gating** scaling actions: scale normally, freeze replicas, or use historical fallbacks

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         MicroGuardian                           │
│                                                                 │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │  Prometheus   │──▶│ Telemetry        │──▶│ Decision       │  │
│  │  Metrics      │   │ Watchdog Layer   │   │ Engine         │  │
│  │  Collector    │   │                  │   │                │  │
│  │              │   │ • Freshness      │   │ Ts ≥ 0.8: Scale│  │
│  │              │   │ • Continuity     │   │ 0.4≤Ts<0.8:    │  │
│  │              │   │ • Completeness   │   │   Freeze       │  │
│  │              │   │        │         │   │ Ts<0.4: Fallbk │  │
│  └──────────────┘   │        ▼         │   └───────┬────────┘  │
│                     │  Trust Score Ts   │           │           │
│                     └──────────────────┘           ▼           │
│                                            ┌──────────────┐    │
│                                            │ K8s API      │    │
│                                            │ Scale Action │    │
│                                            └──────────────┘    │
└─────────────────────────────────────────────────────────────────┘
         ▲                                          │
         │              Scrape                      │ Scale
         │                                          ▼
┌────────────────┐                      ┌───────────────────┐
│ Spring Boot    │◀─── HTTP Traffic ────│ k6 Load Generator │
│ Microservices  │                      └───────────────────┘
│ (order,        │
│  inventory,    │
│  gateway)      │
└────────────────┘
```

## Project Structure

```
├── microservices/           # Spring Boot target workloads
│   ├── gateway-service/     # API Gateway
│   ├── order-service/       # CPU-intensive order processing
│   └── inventory-service/   # Memory-intensive inventory lookups
├── controller/              # Python MicroGuardian controller
│   ├── watchdog/            # Telemetry Watchdog Layer
│   ├── trust_score.py       # Trust Score computation
│   ├── decision_engine.py   # Confidence-weighted decision engine
│   └── reconciler.py        # Main reconciliation loop
├── k8s-manifests/           # Kubernetes deployment manifests
├── chaos/                   # Fault injection scripts
├── loadtest/                # k6 load testing scripts
└── analysis/                # Results analysis & visualization
```

## Prerequisites

- **Docker Desktop** (with Kubernetes enabled) or **Minikube**
- **Java 17+** and **Maven 3.8+**
- **Python 3.10+**
- **kubectl** CLI
- **k6** load testing tool
- **Helm** (optional, for Prometheus)

## Quick Start

### 1. Start Minikube
```bash
minikube start --cpus=4 --memory=8192 --driver=docker
minikube addons enable metrics-server
```

### 2. Build and Deploy Microservices
```bash
# Build Docker images (using Minikube's Docker daemon)
eval $(minikube docker-env)

cd microservices/order-service
mvn clean package -DskipTests
docker build -t order-service:latest .

cd ../inventory-service
mvn clean package -DskipTests
docker build -t inventory-service:latest .

cd ../gateway-service
mvn clean package -DskipTests
docker build -t gateway-service:latest .
```

### 3. Deploy to Kubernetes
```bash
kubectl apply -f k8s-manifests/namespace.yaml
kubectl apply -f k8s-manifests/prometheus/
kubectl apply -f k8s-manifests/microservices/
```

### 4. Install Python Dependencies
```bash
cd controller
pip install -r requirements.txt
```

### 5. Run MicroGuardian Controller
```bash
python -m controller.reconciler
```

### 6. Run Load Tests
```bash
k6 run loadtest/baseline_load.js
```

### 7. Run Chaos Experiments
```bash
python chaos/run_experiment.py --scenario scrape-delay
```

## Evaluation Metrics

| Metric | Description |
|--------|-------------|
| **SLO Violation Duration** | Time (s) where p95 latency exceeds target |
| **Replica Flapping Index** | Scale direction reversals per 5-min window |
| **Over-Provisioning Ratio** | Allocated vs utilized core-hours |
| **HTTP 5xx Error Rate** | % of server errors during faults |

## License

Academic research project — Final Year Project 2026-2027.
