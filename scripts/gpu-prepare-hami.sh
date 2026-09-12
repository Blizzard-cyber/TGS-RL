#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. "$ROOT_DIR/scripts/lib/network-profile.sh"
tgsrl_load_network_profile

ACTION=${1:-up}
PROFILE=${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}
EXPECTED_CONTEXT=${TGSRL_KUBE_CONTEXT:-$PROFILE}
NAMESPACE=${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}
HAMI_VERSION=${TGSRL_HAMI_VERSION:-2.10.0}
HAMI_CHART_SHA256=${TGSRL_HAMI_CHART_SHA256:-e1d8429b2270da1a5c26343d6d6fd2a0099ba7b944cf0f52eb20d6b9f14e5099}
HAMI_CHART_URL=${TGSRL_HAMI_CHART_URL:-https://github.com/Project-HAMi/HAMi/releases/download/v${HAMI_VERSION}/hami-${HAMI_VERSION}.tgz}
CACHE_DIR=${TGSRL_HAMI_CACHE_DIR:-.cache/tgsrl/hami}
CHART=${TGSRL_HAMI_CHART:-$CACHE_DIR/hami-${HAMI_VERSION}.tgz}
STATE_PREFIX=${TGSRL_HAMI_STATE_PREFIX:-$CACHE_DIR/state}
HAMI_REGISTER_ANNOTATION=hami.io/node-nvidia-register
SWITCH_STARTED=0
INSTALL_SUCCEEDED=0

require_context() {
  local current
  current=$(kubectl config current-context)
  [[ $current == "$EXPECTED_CONTEXT" ]] ||
    { echo "refusing to modify context $current; expected $EXPECTED_CONTEXT" >&2; exit 1; }
}

require_idle_namespace() {
  local active="" output resource
  for resource in \
    jobrunbundles.tgsrl.io \
    workloads.kueue.x-k8s.io \
    jobs.batch \
    pods; do
    output=$(kubectl -n "$NAMESPACE" get "$resource" -o name) || {
      echo "cannot verify that $NAMESPACE is idle: failed to list $resource" >&2
      exit 1
    }
    if [[ -n $output ]]; then
      active+="${output}"$'\n'
    fi
  done
  [[ -z $active ]] || {
    echo "refusing to switch GPU managers while TGS-RL workloads exist in $NAMESPACE" >&2
    printf '%s' "$active" >&2
    exit 1
  }
}

restore_previous_gpu_manager() {
  kubectl delete -f deploy/kubernetes/hami-smoke-queue.yaml --ignore-not-found=true >/dev/null ||
    return 1
  if helm status hami --namespace kube-system >/dev/null 2>&1; then
    helm uninstall hami --namespace kube-system --wait >/dev/null || return 1
  fi
  while read -r node; do
    [[ -n $node ]] || continue
    kubectl label node "$node" gpu=on --overwrite >/dev/null || return 1
  done <"$STATE_PREFIX.preexisting-labels" 2>/dev/null || true
  while read -r node; do
    [[ -n $node ]] || continue
    if ! grep -Fxq "$node" "$STATE_PREFIX.preexisting-labels" 2>/dev/null; then
      kubectl label node "$node" gpu- >/dev/null || return 1
    fi
  done < <(
    kubectl get nodes -l gpu=on -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
      2>/dev/null
  )
  restore_hami_annotations || return 1
  if [[ $(cat "$STATE_PREFIX.nvidia-addon" 2>/dev/null || true) == enabled ]]; then
    minikube addons enable nvidia-device-plugin -p "$PROFILE" >/dev/null || return 1
    for _ in $(seq 1 60); do
      kubectl -n kube-system get daemonset nvidia-device-plugin-daemonset >/dev/null 2>&1 &&
        break
      sleep 2
    done
    kubectl -n kube-system get daemonset nvidia-device-plugin-daemonset >/dev/null 2>&1 ||
      return 1
    kubectl -n kube-system rollout status daemonset/nvidia-device-plugin-daemonset \
      --timeout=5m >/dev/null || return 1
    wait_for_gpu_capacity_restore || return 1
  fi
  rm -f \
    "$STATE_PREFIX.installed" \
    "$STATE_PREFIX.preexisting-labels" \
    "$STATE_PREFIX.preexisting-hami-annotations.json" \
    "$STATE_PREFIX.preexisting-gpu-capacity.json" \
    "$STATE_PREFIX.nvidia-addon"
}

restore_hami_annotations() {
  local nodes node current desired patch
  [[ -f $STATE_PREFIX.preexisting-hami-annotations.json ]] || return 0
  nodes=$(kubectl get nodes -o json | jq -r '.items[].metadata.name') || return 1
  while read -r node; do
    [[ -n $node ]] || continue
    current=$(kubectl get node "$node" -o json | jq -c '
      (.metadata.annotations // {})
      | with_entries(select(.key | startswith("hami.io/")))
    ') || return 1
    desired=$(jq -c --arg node "$node" '
      map(select(.name == $node))[0].annotations // {}
    ' "$STATE_PREFIX.preexisting-hami-annotations.json") || return 1
    patch=$(jq -cn --argjson current "$current" --argjson desired "$desired" '
      {metadata: {annotations: (($current | with_entries(.value = null)) + $desired)}}
    ') || return 1
    kubectl patch node "$node" --type=merge --patch "$patch" >/dev/null || return 1
  done <<<"$nodes"
}

wait_for_gpu_capacity_restore() {
  local expected current
  [[ -f $STATE_PREFIX.preexisting-gpu-capacity.json ]] || return 1
  expected=$(cat "$STATE_PREFIX.preexisting-gpu-capacity.json")
  for _ in $(seq 1 120); do
    current=$(kubectl get nodes -o json | jq -c '
      [.items[] | {
        name: .metadata.name,
        gpu: (.status.allocatable["nvidia.com/gpu"] // "0")
      }] | sort_by(.name)
    ') || return 1
    if [[ $current == "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  echo 'previous NVIDIA GPU capacity was not restored' >&2
  return 1
}

rollback_failed_install() {
  local status=$?
  if (( SWITCH_STARTED == 1 && INSTALL_SUCCEEDED == 0 )); then
    echo 'HAMi preparation failed; restoring the previous NVIDIA device-plugin state' >&2
    trap - ERR
    restore_previous_gpu_manager || {
      echo "automatic GPU-manager restoration failed; state remains under $CACHE_DIR" >&2
    }
  fi
  exit "$status"
}

install_hami() {
  require_context
  require_idle_namespace
  mkdir -p "$CACHE_DIR"
  if [[ ! -f $CHART || $(sha256sum "$CHART" | awk '{print $1}') != "$HAMI_CHART_SHA256" ]]; then
    rm -f "$CHART"
    tgsrl_download "$HAMI_CHART_URL" "$CHART"
  fi
  [[ $(sha256sum "$CHART" | awk '{print $1}') == "$HAMI_CHART_SHA256" ]] ||
    { echo "HAMi chart checksum mismatch" >&2; exit 1; }

  if helm status hami --namespace kube-system >/dev/null 2>&1; then
    [[ -f $STATE_PREFIX.installed ]] || {
      echo 'HAMi already exists but no TGS-RL switch state was found; refusing to overwrite it' >&2
      exit 1
    }
  else
    kubectl get nodes -l gpu=on -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
      >"$STATE_PREFIX.preexisting-labels"
    kubectl get nodes -o json | jq '
      [.items[] | {
        name: .metadata.name,
        annotations: (
          (.metadata.annotations // {})
          | with_entries(select(.key | startswith("hami.io/")))
        )
      }] | sort_by(.name)
    ' >"$STATE_PREFIX.preexisting-hami-annotations.json"
    kubectl get nodes -o json | jq -c '
      [.items[] | {
        name: .metadata.name,
        gpu: (.status.allocatable["nvidia.com/gpu"] // "0")
      }] | sort_by(.name)
    ' >"$STATE_PREFIX.preexisting-gpu-capacity.json"
    if minikube addons list -p "$PROFILE" | grep -E 'nvidia-device-plugin.*enabled' >/dev/null; then
      printf 'enabled\n' >"$STATE_PREFIX.nvidia-addon"
    else
      printf 'disabled\n' >"$STATE_PREFIX.nvidia-addon"
    fi
    SWITCH_STARTED=1
    trap rollback_failed_install ERR
    if [[ $(cat "$STATE_PREFIX.nvidia-addon") == enabled ]]; then
      minikube addons disable nvidia-device-plugin -p "$PROFILE"
    fi
    : >"$STATE_PREFIX.installed"
  fi
  SWITCH_STARTED=1
  trap rollback_failed_install ERR
  kubectl label nodes --all gpu=on --overwrite

  local registry=docker.io
  local repository=projecthami/hami
  local scheduler_registry=registry.k8s.io
  local scheduler_repository=kube-scheduler
  local patch_registry=docker.io
  local patch_repository=liangjw/kube-webhook-certgen
  if [[ $TGSRL_NETWORK_PROFILE == cn ]]; then
    registry=m.daocloud.io
    repository=docker.io/projecthami/hami
    scheduler_registry=m.daocloud.io
    scheduler_repository=registry.k8s.io/kube-scheduler
    patch_registry=m.daocloud.io
    patch_repository=docker.io/liangjw/kube-webhook-certgen
  fi
  helm upgrade --install hami "$CHART" \
    --namespace kube-system \
    --set-string global.imageTag="v${HAMI_VERSION}" \
    --set-string scheduler.kubeScheduler.image.registry="$scheduler_registry" \
    --set-string scheduler.kubeScheduler.image.repository="$scheduler_repository" \
    --set-string scheduler.kubeScheduler.image.tag="$(kubectl version -o json | jq -r .serverVersion.gitVersion)" \
    --set-string scheduler.extender.image.registry="$registry" \
    --set-string scheduler.extender.image.repository="$repository" \
    --set-string devicePlugin.image.registry="$registry" \
    --set-string devicePlugin.image.repository="$repository" \
    --set-string devicePlugin.monitor.image.registry="$registry" \
    --set-string devicePlugin.monitor.image.repository="$repository" \
    --set-string scheduler.patch.imageNew.registry="$patch_registry" \
    --set-string scheduler.patch.imageNew.repository="$patch_repository" \
    --set scheduler.forceOverwriteDefaultScheduler=false \
    --set devicePlugin.deviceSplitCount=10 \
    --set devicePlugin.migStrategy=none \
    --wait --timeout 15m
  kubectl apply -f deploy/kubernetes/hami-smoke-queue.yaml
  kubectl -n kube-system rollout status deployment/hami-scheduler --timeout=5m
  kubectl -n kube-system rollout status daemonset/hami-device-plugin --timeout=5m
  for _ in $(seq 1 120); do
    if kubectl get nodes -o json | jq -e --arg annotation "$HAMI_REGISTER_ANNOTATION" '
      any(.items[]; ((.status.allocatable["nvidia.com/gpu"] // "0") | tonumber? // 0) > 0
        and ((.metadata.annotations[$annotation] // "") | length) > 0)
    ' >/dev/null; then
      printf 'HAMi %s is ready with NVIDIA inventory\n' "$HAMI_VERSION"
      INSTALL_SUCCEEDED=1
      trap - ERR
      return
    fi
    sleep 2
  done
  echo 'HAMi did not publish a usable NVIDIA node inventory' >&2
  return 1
}

uninstall_hami() {
  require_context
  require_idle_namespace
  if [[ ! -f $STATE_PREFIX.installed ]]; then
    if helm status hami --namespace kube-system >/dev/null 2>&1; then
      echo 'HAMi exists but was not installed by this TGS-RL switch; refusing to remove it' >&2
      exit 1
    fi
    printf 'HAMi is already absent; no recorded GPU-manager state needs restoration\n'
    return
  fi
  restore_previous_gpu_manager || {
    echo "failed to restore the previous GPU manager; recovery state remains under $CACHE_DIR" >&2
    exit 1
  }
  printf 'HAMi removed and previous NVIDIA device-plugin state restored\n'
}

status_hami() {
  require_context
  helm status hami --namespace kube-system
  kubectl -n kube-system get deployment/hami-scheduler daemonset/hami-device-plugin
  kubectl get nodes -o custom-columns='NAME:.metadata.name,HAMI_GPU:.status.allocatable.nvidia\.com/gpu,HAMI_CORE:.status.allocatable.nvidia\.com/gpucores,HAMI_MEMORY:.status.allocatable.nvidia\.com/gpumem-percentage'
}

case "$ACTION" in
  up) install_hami ;;
  down) uninstall_hami ;;
  status) status_hami ;;
  *) echo 'usage: scripts/gpu-prepare-hami.sh up|down|status' >&2; exit 2 ;;
esac
