#!/usr/bin/env bash
# Bring up a local kind cluster for live-testing the K8sAdapter.
# Requires: kind, kubectl, helm on PATH.
set -euo pipefail

CLUSTER="${1:-mb-dev}"

if ! command -v kind >/dev/null;    then echo "需要 kind：https://kind.sigs.k8s.io/"; exit 1; fi
if ! command -v kubectl >/dev/null; then echo "需要 kubectl"; exit 1; fi
if ! command -v helm >/dev/null;    then echo "需要 helm"; exit 1; fi

kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
kubectl cluster-info --context "kind-${CLUSTER}"

# chart repo used by the etcd profile (chart: bitnami/etcd)
helm repo add bitnami https://charts.bitnami.com/bitnami >/dev/null 2>&1 || true
helm repo update >/dev/null

cat <<EOF

kind 集群 'kind-${CLUSTER}' 就绪。试：
  MB_ADAPTER=k8s ./.venv/bin/mb discover
  MB_ADAPTER=k8s ./.venv/bin/mb install etcd -n etcd-dev            # dry-run，看真实 helm 命令
  MB_ADAPTER=k8s ./.venv/bin/mb install etcd -n etcd-dev --apply    # 真装
  kubectl get pods -l app.kubernetes.io/instance=etcd-dev

拆除： kind delete cluster --name ${CLUSTER}
EOF
