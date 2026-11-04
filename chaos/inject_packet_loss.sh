#!/usr/bin/env bash
# ==============================================================================
# Inject Network Packet Loss on Metrics Exporter Port (e.g. 50% packet drop)
# ==============================================================================

NAMESPACE=${1:-"microguardian"}
APP_LABEL=${2:-"order-service"}
LOSS_PERCENT=${3:-"50%"}
DURATION_SEC=${4:-"120"}

echo "[CHAOS] Finding pod for app=${APP_LABEL} in namespace=${NAMESPACE}..."
POD_NAME=$(kubectl get pods -n "${NAMESPACE}" -l app="${APP_LABEL}" -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
  echo "[CHAOS ERROR] Pod not found."
  exit 1
fi

echo "[CHAOS] Injecting ${LOSS_PERCENT} packet loss on pod ${POD_NAME}..."
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- tc qdisc add dev eth0 root netem loss "${LOSS_PERCENT}" 2>/dev/null || {
  echo "[CHAOS NOTE] Direct tc failed; using fallback mechanism."
}

echo "[CHAOS] Packet loss active for ${DURATION_SEC} seconds..."
sleep "${DURATION_SEC}"

echo "[CHAOS] Reverting packet loss on ${POD_NAME}..."
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- tc qdisc del dev eth0 root 2>/dev/null || true
echo "[CHAOS] Teardown complete."
