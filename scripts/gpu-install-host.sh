#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

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
"${SUDO[@]}" apt-get install -y ca-certificates curl git gnupg jq make openssl python3 python3-pip python3-venv

configure_docker_repository() {
  "${SUDO[@]}" install -m 0755 -d /etc/apt/keyrings
  local keyring
  keyring=$(mktemp "${TMPDIR:-/tmp}/tgsrl-docker-key.XXXXXX")
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o "$keyring"
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
  curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor >"$keyring"
  "${SUDO[@]}" install -m 0644 "$keyring" /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
  rm -f "$keyring"
  curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
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
"${SUDO[@]}" install -m 0755 -d /etc/cdi
"${SUDO[@]}" nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml
"${SUDO[@]}" systemctl restart docker

install_binary() {
  local name=$1 version=$2 url=$3 checksum_url=$4
  local temp expected actual
  temp=$(mktemp "${TMPDIR:-/tmp}/tgsrl-${name}.XXXXXX")
  curl -fsSL "$url" -o "$temp"
  expected=$(curl -fsSL "$checksum_url" | awk '{print $1}')
  actual=$(sha256sum "$temp" | awk '{print $1}')
  [[ "$expected" =~ ^[a-f0-9]{64}$ && "$actual" == "$expected" ]] || {
    rm -f "$temp"
    echo "checksum verification failed for $name $version" >&2
    exit 1
  }
  "${SUDO[@]}" install -m 0755 "$temp" "/usr/local/bin/$name"
  rm -f "$temp"
  printf 'installed %s %s\n' "$name" "$version"
}

install_binary kubectl "$KUBECTL_VERSION" \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256"
install_binary minikube "$MINIKUBE_VERSION" \
  "https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-linux-amd64" \
  "https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-linux-amd64.sha256"

helm_archive=$(mktemp "${TMPDIR:-/tmp}/tgsrl-helm.XXXXXX.tar.gz")
helm_checksum=$(curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz.sha256sum" | awk '{print $1}')
curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" -o "$helm_archive"
[[ "$helm_checksum" =~ ^[a-f0-9]{64}$ && $(sha256sum "$helm_archive" | awk '{print $1}') == "$helm_checksum" ]] || {
  rm -f "$helm_archive"
  echo "checksum verification failed for helm $HELM_VERSION" >&2
  exit 1
}
helm_dir=$(mktemp -d "${TMPDIR:-/tmp}/tgsrl-helm.XXXXXX")
tar -xzf "$helm_archive" -C "$helm_dir"
"${SUDO[@]}" install -m 0755 "$helm_dir/linux-amd64/helm" /usr/local/bin/helm
rm -rf "$helm_archive" "$helm_dir"

uv_root=/opt/tgsrl/tools/uv-${UV_VERSION}
if ! command -v uv >/dev/null 2>&1 || [[ $(uv --version 2>/dev/null | awk '{print $2}') != "$UV_VERSION" ]]; then
  "${SUDO[@]}" python3 -m venv "$uv_root"
  "${SUDO[@]}" "$uv_root/bin/python" -m pip install --disable-pip-version-check --no-cache-dir "uv==${UV_VERSION}"
  "${SUDO[@]}" ln -sfn "$uv_root/bin/uv" /usr/local/bin/uv
fi
uv python install "$PYTHON_VERSION"
uv sync --python "$PYTHON_VERSION" --frozen

[[ $(kubectl version --client -o json | jq -r '.clientVersion.gitVersion') == "$KUBECTL_VERSION" ]] || { echo 'kubectl version verification failed' >&2; exit 1; }
[[ $(minikube version --short) == "$MINIKUBE_VERSION" ]] || { echo 'minikube version verification failed' >&2; exit 1; }
[[ $(helm version --short | sed 's/+.*//') == "$HELM_VERSION" ]] || { echo 'helm version verification failed' >&2; exit 1; }
[[ $(uv --version | awk '{print $2}') == "$UV_VERSION" ]] || { echo 'uv version verification failed' >&2; exit 1; }

printf '%s\n' 'host tooling and locked Python dependencies are ready.'
printf '%s\n' 'Log out and back in if Docker still requires sudo, then run make gpu-create-cluster.'
