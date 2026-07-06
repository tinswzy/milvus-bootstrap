#!/usr/bin/env bash
# Source this before any live mb/kubectl/helm command on THIS machine (local minikube host).
#   source milvus-bootstrap/scripts/mb-live-env.sh
#
# Why each var (see prototype/phase1-setup.html + project memory):
# - HTTP(S)_PROXY: this shell sits behind mihomo at 127.0.0.1:7890; helm needs it to pull charts from the internet.
# - no_proxy/NO_PROXY (BOTH cases): requests/urllib3 (k8s python client) prefers lowercase no_proxy; it MUST include
#   the minikube apiserver IP 192.168.49.2 or every cluster call fails with SSL EOF (proxy intercepts internal IP).
# - MB_ADAPTER=k8s: default is fake (dry-run only). KUBECONFIG: default kubeconfig (current-context=minikube).
# The mb daemon is started AFTER these are exported so it inherits them; change any => mb core stop && mb core start.

export HTTP_PROXY="http://127.0.0.1:7890"
export HTTPS_PROXY="http://127.0.0.1:7890"
_MK_IP="$(minikube ip 2>/dev/null || echo 192.168.49.2)"
export NO_PROXY="${_MK_IP},127.0.0.1,localhost,10.96.0.0/12,192.168.49.0/24"
export no_proxy="$NO_PROXY"
export MB_ADAPTER="k8s"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"

# Activate the mb venv if present and not already active.
if [ -z "${VIRTUAL_ENV:-}" ] && [ -f "$(dirname "${BASH_SOURCE[0]}")/../.venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source "$(dirname "${BASH_SOURCE[0]}")/../.venv/bin/activate"
fi

echo "mb-live-env: adapter=$MB_ADAPTER kubeconfig=$KUBECONFIG no_proxy=$NO_PROXY venv=${VIRTUAL_ENV:-none}"
