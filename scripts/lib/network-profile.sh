#!/usr/bin/env bash

# This file is sourced by setup scripts. Call tgsrl_load_network_profile after
# ROOT_DIR has been resolved by the caller.
tgsrl_load_network_profile() {
  local profile=${TGSRL_NETWORK_PROFILE:-official}
  case "$profile" in
    official)
      ;;
    cn)
      # shellcheck disable=SC1091
      . "$ROOT_DIR/configs/network/cn.env"
      ;;
    *)
      echo "unsupported TGSRL_NETWORK_PROFILE=$profile (expected official or cn)" >&2
      return 2
      ;;
  esac

  export TGSRL_NETWORK_PROFILE="$profile"
  export TGSRL_CURL_RETRIES=${TGSRL_CURL_RETRIES:-12}
  export TGSRL_CURL_RETRY_DELAY=${TGSRL_CURL_RETRY_DELAY:-2}
  export TGSRL_CURL_CONNECT_TIMEOUT=${TGSRL_CURL_CONNECT_TIMEOUT:-15}
  [[ -z ${TGSRL_PYPI_INDEX_URL:-} ]] || export PIP_INDEX_URL="$TGSRL_PYPI_INDEX_URL"
  [[ -z ${UV_DEFAULT_INDEX:-} ]] || export UV_DEFAULT_INDEX
  export TGSRL_DOWNLOAD_MIRROR_PREFIX TGSRL_PYPI_INDEX_URL
  export TGSRL_UV_PYTHON_INSTALL_MIRROR TGSRL_DOCKER_REGISTRY_MIRRORS
  export TGSRL_MINIKUBE_IMAGE_MIRROR_COUNTRY TGSRL_MINIKUBE_IMAGE_REPOSITORY
  export TGSRL_MINIKUBE_BASE_IMAGE
  export TGSRL_K8S_OCI_REGISTRY TGSRL_K8S_IMAGE_REGISTRY
  export TGSRL_GO_BASE_IMAGE TGSRL_PYTHON_BASE_IMAGE TGSRL_NODE_BASE_IMAGE
  export TGSRL_DISTROLESS_BASE_IMAGE TGSRL_VERL_BASE_IMAGE TGSRL_GPU_RUNTIME_TEST_IMAGE
  export TGSRL_GOPROXY TGSRL_NPM_REGISTRY
}

tgsrl_mirror_url() {
  local canonical=$1
  if [[ -n ${TGSRL_DOWNLOAD_MIRROR_PREFIX:-} ]]; then
    printf '%s/%s\n' "${TGSRL_DOWNLOAD_MIRROR_PREFIX%/}" "${canonical#*://}"
  else
    printf '%s\n' "$canonical"
  fi
}

tgsrl_curl() {
  curl --http1.1 --fail --location \
    --retry "$TGSRL_CURL_RETRIES" \
    --retry-all-errors \
    --retry-delay "$TGSRL_CURL_RETRY_DELAY" \
    --connect-timeout "$TGSRL_CURL_CONNECT_TIMEOUT" \
    "$@"
}

tgsrl_fetch_text() {
  local canonical=$1
  tgsrl_curl --silent --show-error "$canonical"
}

tgsrl_download() {
  local canonical=$1 output=$2
  local download_url
  download_url=$(tgsrl_mirror_url "$canonical")
  mkdir -p "$(dirname "$output")"
  touch "$output"
  if ! tgsrl_curl --continue-at - "$download_url" --output "$output"; then
    rm -f "$output"
    echo "resume failed; retrying from byte zero: $download_url" >&2
    if ! tgsrl_curl "$download_url" --output "$output"; then
      rm -f "$output"
      if [[ $download_url != "$canonical" ]]; then
        echo "mirror download failed; retrying canonical source: $canonical" >&2
        tgsrl_curl "$canonical" --output "$output" || {
          echo "download failed; partial file kept for resume: $output" >&2
          return 1
        }
      else
        echo "download failed; partial file kept for resume: $output" >&2
        return 1
      fi
    fi
  fi
}

tgsrl_download_verified() {
  local name=$1 version=$2 canonical=$3 checksum_url=$4 output=$5
  local expected actual
  expected=$(tgsrl_fetch_text "$checksum_url" | awk '{print $1}')
  if [[ -f $output ]]; then
    actual=$(sha256sum "$output" | awk '{print $1}')
    if [[ $expected =~ ^[a-f0-9]{64}$ && $actual == "$expected" ]]; then
      printf 'using verified cached download: %s\n' "$output"
      return 0
    fi
  fi
  tgsrl_download "$canonical" "$output"
  actual=$(sha256sum "$output" | awk '{print $1}')
  if [[ ! $expected =~ ^[a-f0-9]{64}$ || $actual != "$expected" ]]; then
    rm -f "$output"
    if [[ -n ${TGSRL_DOWNLOAD_MIRROR_PREFIX:-} ]]; then
      echo "mirror checksum mismatch for $name $version; retrying mirror from byte zero" >&2
      tgsrl_curl "$(tgsrl_mirror_url "$canonical")" --output "$output"
      actual=$(sha256sum "$output" | awk '{print $1}')
    fi
    if [[ ! $expected =~ ^[a-f0-9]{64}$ || $actual != "$expected" ]]; then
      rm -f "$output"
      echo "checksum verification failed for $name $version; removed cached download" >&2
      return 1
    fi
  fi
}
