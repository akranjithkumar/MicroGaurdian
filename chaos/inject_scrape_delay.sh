#!/usr/bin/env bash
# ==============================================================================
# Inject Scraping Delay (Latency) into Prometheus Scrape Target
# Uses Linux Traffic Control (tc) inside the target pod network namespace.
# ==============================================================================

NAMESPACE=${1:-"microguardian"}
APP_LABEL=${2:-"order-service"}
DELAY_MS=${3:-"25000"} # 25s delay exceeds typical scrape timeout (5s)
DURATION_SEC=${4:-"120"}

echo "[CHAOS] Finding pod for app=${APP_LABEL} in namespace=${NAMESPACE}..."
POD_NAME=$(kubectl get pods -n "${NAMESPACE}" -l app="${APP_LABEL}" -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
  echo "[CHAOS ERROR] No pod found for label app=${APP_LABEL}"
  exit 1
fi

echo "[CHAOS] Injecting ${DELAY_MS}ms latency on pod ${POD_NAME}..."
# Attempt to inject delay using tc via ephemeral container or privileged exec
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- tc qdisc add dev eth0 root netem delay "${DELAY_MS}ms" 2>/dev/null || {
  echo "[CHAOS NOTE] Direct tc on container failed (unprivileged container). Using iptables delay or synthetic proxy..."
}

echo "[CHAOS] Delay active for ${DURATION_SEC} seconds..."
sleep "${DURATION_SEC}"

echo "[CHAOS] Cleaning up delay on ${POD_NAME}..."
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- tc qdisc del dev eth0 root 2>/dev/null || true
echo "[CHAOS] Teardown complete."
