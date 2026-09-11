#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. "$ROOT_DIR/scripts/lib/network-profile.sh"
tgsrl_load_network_profile

[[ $(uname -s) == Linux ]] || { echo 'GPU setup supports Linux hosts only' >&2; exit 1; }
SUDO=()
if (( EUID != 0 )); then
  command -v sudo >/dev/null 2>&1 || { echo 'sudo is required for host package installation' >&2; exit 1; }
  SUDO=(sudo)
fi
command -v nvidia-smi >/dev/null 2>&1 || {
  echo 'NVIDIA kernel driver is missing; install a supported 580.95+ driver before running this script' >&2
  exit 1
}
nvidia-smi -L >/dev/null 2>&1 || { echo 'the NVIDIA driver cannot enumerate any GPU' >&2; exit 1; }
MIN_DRIVER_VERSION=${TGSRL_MIN_NVIDIA_DRIVER_VERSION:-580.95.05}
driver_version=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -n 1 | tr -d '[:space:]')
if [[ ! $driver_version =~ ^[0-9]+([.][0-9]+){1,2}$ ]] ||
  [[ $(printf '%s\n%s\n' "$MIN_DRIVER_VERSION" "$driver_version" | sort -V | head -n 1) != "$MIN_DRIVER_VERSION" ]]; then
  echo "NVIDIA driver $driver_version is older than required $MIN_DRIVER_VERSION; upgrade and reboot before continuing" >&2
  exit 1
fi

# shellcheck source=/dev/null
. /etc/os-release
[[ ${ID:-} == ubuntu && ${VERSION_ID:-} =~ ^(22.04|24.04)$ ]] || {
  echo "automatic install supports Ubuntu 22.04/24.04 only; found ${ID:-unknown} ${VERSION_ID:-unknown}" >&2
  exit 1
}
[[ $(uname -m) == x86_64 ]] || { echo 'the locked GPU workload currently supports x86_64 only' >&2; exit 1; }

KUBECTL_VERSION=${TGSRL_KUBECTL_VERSION:-v1.35.1}
HELM_VERSION=${TGSRL_HELM_VERSION:-v4.2.4}
MINIKUBE_VERSION=${TGSRL_MINIKUBE_VERSION:-v1.38.1}
UV_VERSION=${TGSRL_UV_VERSION:-0.12.7}
PYTHON_VERSION=${TGSRL_PYTHON_VERSION:-3.12.14}
INSTALL_DOCKER=${TGSRL_INSTALL_DOCKER:-0}
INSTALL_TOOLKIT=${TGSRL_INSTALL_NVIDIA_TOOLKIT:-0}
DOWNLOAD_DIR=${TGSRL_DOWNLOAD_DIR:-.cache/tgsrl/downloads}

DOCKER_NEEDS_INSTALL=0
if ! command -v docker >/dev/null 2>&1 || ! docker buildx version >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  DOCKER_NEEDS_INSTALL=1
  [[ "$INSTALL_DOCKER" == 1 ]] || {
    echo 'Docker Engine with Buildx and Compose is missing; rerun with TGSRL_INSTALL_DOCKER=1' >&2
    exit 1
  }
fi
toolkit_version=""
if command -v nvidia-ctk >/dev/null 2>&1; then
  toolkit_version=$(nvidia-ctk --version | sed -nE 's/.*version:?[[:space:]]+v?([0-9]+([.][0-9]+){1,2}).*/\1/p' | head -n 1)
fi
if [[ -z "$toolkit_version" || "$(printf '%s\n%s\n' 1.18.0 "$toolkit_version" | sort -V | head -n 1)" != 1.18.0 ]]; then
  [[ "$INSTALL_TOOLKIT" == 1 ]] || {
    echo "NVIDIA Container Toolkit 1.18.0+ is required; found ${toolkit_version:-missing}; rerun with TGSRL_INSTALL_NVIDIA_TOOLKIT=1" >&2
    exit 1
  }
fi

"${SUDO[@]}" apt-get update
"${SUDO[@]}" apt-get install -y \
  ca-certificates conntrack curl ebtables ethtool git gnupg ipset iptables jq make openssl \
  python3 python3-pip python3-venv socat
"${SUDO[@]}" modprobe overlay
"${SUDO[@]}" modprobe br_netfilter
printf '%s\n' overlay br_netfilter |
  "${SUDO[@]}" tee /etc/modules-load.d/tgsrl-kubernetes.conf >/dev/null
printf '%s\n' \
  'net.bridge.bridge-nf-call-iptables = 1' \
  'net.bridge.bridge-nf-call-ip6tables = 1' \
  'net.ipv4.ip_forward = 1' |
  "${SUDO[@]}" tee /etc/sysctl.d/99-tgsrl-kubernetes.conf >/dev/null
"${SUDO[@]}" sysctl -w net.bridge.bridge-nf-call-iptables=1 >/dev/null
"${SUDO[@]}" sysctl -w net.bridge.bridge-nf-call-ip6tables=1 >/dev/null
"${SUDO[@]}" sysctl -w net.ipv4.ip_forward=1 >/dev/null

configure_docker_repository() {
  "${SUDO[@]}" install -m 0755 -d /etc/apt/keyrings
  local keyring
  keyring=$(mktemp "${TMPDIR:-/tmp}/tgsrl-docker-key.XXXXXX")
  tgsrl_curl --silent --show-error https://download.docker.com/linux/ubuntu/gpg -o "$keyring"
  "${SUDO[@]}" install -m 0644 "$keyring" /etc/apt/keyrings/docker.asc
  rm -f "$keyring"
  printf '%s\n' \
    "Types: deb" \
    "URIs: https://download.docker.com/linux/ubuntu" \
    "Suites: ${UBUNTU_CODENAME:-$VERSION_CODENAME}" \
    "Components: stable" \
    "Architectures: amd64" \
    "Signed-By: /etc/apt/keyrings/docker.asc" | \
    "${SUDO[@]}" tee /etc/apt/sources.list.d/docker.sources >/dev/null
  "${SUDO[@]}" apt-get update
}

configure_docker_registry_mirrors() {
  [[ -n ${TGSRL_DOCKER_REGISTRY_MIRRORS:-} ]] || return 0
  local config=/etc/docker/daemon.json temp
  temp=$(mktemp "${TMPDIR:-/tmp}/tgsrl-docker-config.XXXXXX")
  "${SUDO[@]}" install -m 0755 -d /etc/docker
  if [[ -s $config ]]; then
    "${SUDO[@]}" cp "$config" "$temp"
  else
    printf '{}\n' >"$temp"
  fi
  python3 - "$temp" "$TGSRL_DOCKER_REGISTRY_MIRRORS" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
mirrors = [item.strip() for item in sys.argv[2].split(",") if item.strip()]
if not mirrors or any(not item.startswith("https://") for item in mirrors):
    raise SystemExit("TGSRL_DOCKER_REGISTRY_MIRRORS must contain comma-separated HTTPS URLs")
payload = json.loads(path.read_text(encoding="utf-8"))
payload["registry-mirrors"] = mirrors
path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
  "${SUDO[@]}" install -m 0644 "$temp" "$config"
  rm -f "$temp"
}

if [[ "$DOCKER_NEEDS_INSTALL" == 1 ]] && ! command -v docker >/dev/null 2>&1; then
  configure_docker_repository
  "${SUDO[@]}" apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
elif [[ "$DOCKER_NEEDS_INSTALL" == 1 ]]; then
  configure_docker_repository
  "${SUDO[@]}" apt-get install -y docker-buildx-plugin docker-compose-plugin
fi
"${SUDO[@]}" systemctl enable --now docker
install_user=${SUDO_USER:-$(id -un)}
if [[ "$install_user" != root ]]; then
  "${SUDO[@]}" usermod -aG docker "$install_user"
fi

if [[ -z "$toolkit_version" || "$(printf '%s\n%s\n' 1.18.0 "$toolkit_version" | sort -V | head -n 1)" != 1.18.0 ]]; then
  "${SUDO[@]}" install -m 0755 -d /usr/share/keyrings
  keyring=$(mktemp "${TMPDIR:-/tmp}/tgsrl-nvidia-key.XXXXXX")
  tgsrl_curl --silent --show-error https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor >"$keyring"
  "${SUDO[@]}" install -m 0644 "$keyring" /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
  rm -f "$keyring"
  tgsrl_curl --silent --show-error https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#' | \
    "${SUDO[@]}" tee /etc/apt/sources.list.d/nvidia-container-toolkit.list >/dev/null
  "${SUDO[@]}" apt-get update
  "${SUDO[@]}" apt-get install -y nvidia-container-toolkit
fi
toolkit_version=$(nvidia-ctk --version | sed -nE 's/.*version:?[[:space:]]+v?([0-9]+([.][0-9]+){1,2}).*/\1/p' | head -n 1)
[[ -n "$toolkit_version" && "$(printf '%s\n%s\n' 1.18.0 "$toolkit_version" | sort -V | head -n 1)" == 1.18.0 ]] || {
  echo "NVIDIA Container Toolkit 1.18.0 or newer is required; found ${toolkit_version:-unknown}" >&2
  exit 1
}
"${SUDO[@]}" nvidia-ctk runtime configure --runtime=docker
configure_docker_registry_mirrors
"${SUDO[@]}" install -m 0755 -d /etc/cdi
"${SUDO[@]}" nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml
"${SUDO[@]}" systemctl restart docker

install_binary() {
  local name=$1 version=$2 url=$3 checksum_url=$4
  local temp="$DOWNLOAD_DIR/${name}-${version}-linux-amd64"
  tgsrl_download_verified "$name" "$version" "$url" "$checksum_url" "$temp"
  "${SUDO[@]}" install -m 0755 "$temp" "/usr/local/bin/$name"
  printf 'installed %s %s\n' "$name" "$version"
}

install_binary kubectl "$KUBECTL_VERSION" \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256"
install_binary minikube "$MINIKUBE_VERSION" \
  "https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-linux-amd64" \
  "https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-linux-amd64.sha256"

helm_archive="$DOWNLOAD_DIR/helm-${HELM_VERSION}-linux-amd64.tar.gz"
tgsrl_download_verified helm "$HELM_VERSION" \
  "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" \
  "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz.sha256sum" \
  "$helm_archive"
helm_dir=$(mktemp -d "${TMPDIR:-/tmp}/tgsrl-helm.XXXXXX")
tar -xzf "$helm_archive" -C "$helm_dir"
"${SUDO[@]}" install -m 0755 "$helm_dir/linux-amd64/helm" /usr/local/bin/helm
rm -rf "$helm_dir"

uv_root=/opt/tgsrl/tools/uv-${UV_VERSION}
if ! command -v uv >/dev/null 2>&1 || [[ $(uv --version 2>/dev/null | awk '{print $2}') != "$UV_VERSION" ]]; then
  "${SUDO[@]}" python3 -m venv "$uv_root"
  pip_args=(--disable-pip-version-check --no-cache-dir)
  [[ -z ${TGSRL_PYPI_INDEX_URL:-} ]] || pip_args+=(--index-url "$TGSRL_PYPI_INDEX_URL")
  "${SUDO[@]}" "$uv_root/bin/python" -m pip install "${pip_args[@]}" "uv==${UV_VERSION}"
  "${SUDO[@]}" ln -sfn "$uv_root/bin/uv" /usr/local/bin/uv
fi
uv_python_args=(python install "$PYTHON_VERSION")
[[ -z ${TGSRL_UV_PYTHON_INSTALL_MIRROR:-} ]] ||
  uv_python_args+=(--mirror "$TGSRL_UV_PYTHON_INSTALL_MIRROR")
uv "${uv_python_args[@]}"
uv sync --python "$PYTHON_VERSION" --frozen

[[ $(kubectl version --client -o json | jq -r '.clientVersion.gitVersion') == "$KUBECTL_VERSION" ]] || { echo 'kubectl version verification failed' >&2; exit 1; }
[[ $(minikube version --short) == "$MINIKUBE_VERSION" ]] || { echo 'minikube version verification failed' >&2; exit 1; }
[[ $(helm version --short | sed 's/+.*//') == "$HELM_VERSION" ]] || { echo 'helm version verification failed' >&2; exit 1; }
[[ $(uv --version | awk '{print $2}') == "$UV_VERSION" ]] || { echo 'uv version verification failed' >&2; exit 1; }

printf '%s\n' 'host tooling and locked Python dependencies are ready.'
printf 'network profile: %s\n' "$TGSRL_NETWORK_PROFILE"
printf '%s\n' 'Log out and back in if Docker still requires sudo, then run make gpu-create-cluster.'
