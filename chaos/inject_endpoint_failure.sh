#!/usr/bin/env bash
# ==============================================================================
# Inject Exporter Endpoint Failure (Simulate Scrape Target Timeout / Drop)
# Can modify ConfigMap or block access to the actuator endpoint
# ==============================================================================

NAMESPACE=${1:-"microguardian"}
DURATION_SEC=${2:-"120"}

echo "[CHAOS] Simulating Scrape Target Endpoint Failure..."
echo "[CHAOS] Breaking scrape target address in Prometheus ConfigMap..."

# Temporarily point scrape target to an unreachable IP/port
kubectl patch configmap prometheus-config -n "${NAMESPACE}" --type merge -p '{"data":{"prometheus.yml":"global:\n  scrape_interval: 15s\nscrape_configs:\n  - job_name: spring-boot-services\n    static_configs:\n      - targets: [\"192.0.2.1:8081\"]"}}'

# Reload Prometheus config
kubectl exec -n "${NAMESPACE}" $(kubectl get pods -n "${NAMESPACE}" -l app=prometheus -o jsonpath='{.items[0].metadata.name}') -- wget -qO- --post-data='' http://localhost:9090/-/reload || true

echo "[CHAOS] Scrape failure active for ${DURATION_SEC} seconds..."
sleep "${DURATION_SEC}"

echo "[CHAOS] Restoring valid Prometheus configuration..."
kubectl apply -f k8s-manifests/prometheus/prometheus.yaml
kubectl exec -n "${NAMESPACE}" $(kubectl get pods -n "${NAMESPACE}" -l app=prometheus -o jsonpath='{.items[0].metadata.name}') -- wget -qO- --post-data='' http://localhost:9090/-/reload || true

echo "[CHAOS] Prometheus scrape endpoint restored."
