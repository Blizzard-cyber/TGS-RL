#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT_DIR
readonly BUF_VERSION="1.72.0"
STAGING_DIR="$(mktemp -d "${ROOT_DIR}/.proto-gen.XXXXXX")"
readonly STAGING_DIR
readonly CANDIDATE_DIR="${STAGING_DIR}/gen"
readonly BACKUP_DIR="${STAGING_DIR}/previous-gen"
swapped=0

cleanup() {
  local exit_code=$?
  if [[ ${swapped} -eq 1 && ! -d "${ROOT_DIR}/gen" && -d "${BACKUP_DIR}" ]]; then
    mv "${BACKUP_DIR}" "${ROOT_DIR}/gen"
  fi
  rm -rf "${STAGING_DIR}"
  return "${exit_code}"
}
trap cleanup EXIT INT TERM

resolve_buf() {
  if [[ -n "${BUF_BIN:-}" ]]; then
    printf '%s\n' "${BUF_BIN}"
    return
  fi

  if command -v buf >/dev/null 2>&1; then
    command -v buf
    return
  fi

  local os arch archive_name tool_dir archive_url
  case "$(uname -s)" in
    Darwin) os=Darwin ;;
    Linux) os=Linux ;;
    *)
      printf 'unsupported operating system: %s\n' "$(uname -s)" >&2
      return 1
      ;;
  esac
  case "$(uname -m)" in
    arm64 | aarch64) arch=arm64 ;;
    x86_64 | amd64) arch=x86_64 ;;
    *)
      printf 'unsupported architecture: %s\n' "$(uname -m)" >&2
      return 1
      ;;
  esac

  archive_name="buf-${os}-${arch}.tar.gz"
  tool_dir="${TMPDIR:-/tmp}/tgsrl-tools/buf-v${BUF_VERSION}"
  archive_url="https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/${archive_name}"

  if [[ ! -x "${tool_dir}/buf/bin/buf" ]]; then
    mkdir -p "${tool_dir}"
    curl --fail --location --silent --show-error "${archive_url}" \
      | tar -xz -C "${tool_dir}"
  fi

  printf '%s\n' "${tool_dir}/buf/bin/buf"
}

BUF="$(resolve_buf)"
actual_buf_version="$("${BUF}" --version)"
if [[ "${actual_buf_version}" != "${BUF_VERSION}" ]]; then
  printf 'buf %s is required, found %s at %s\n' \
    "${BUF_VERSION}" "${actual_buf_version}" "${BUF}" >&2
  exit 1
fi

# Generate into an isolated checkout. buf.gen.yaml uses clean: true, so running
# it in the workspace can erase the last known-good generated clients before a
# remote plugin or network failure is reported. Only replace gen/ after every
# plugin has completed successfully.
cp "${ROOT_DIR}/buf.yaml" "${ROOT_DIR}/buf.gen.yaml" "${STAGING_DIR}/"
cp -R "${ROOT_DIR}/proto" "${STAGING_DIR}/proto"
(cd "${STAGING_DIR}" && "${BUF}" generate)
mkdir -p "${CANDIDATE_DIR}/python/tgsrl/v1"
touch "${CANDIDATE_DIR}/python/__init__.py"
touch "${CANDIDATE_DIR}/python/tgsrl/__init__.py"
touch "${CANDIDATE_DIR}/python/tgsrl/v1/__init__.py"

# Some remote generators append an extra blank line. Normalize text endings so
# generated-file checks and Git whitespace checks remain stable across runs.
find "${CANDIDATE_DIR}" -type f -exec perl -0pi -e 's/\n+\z/\n/' {} +

# Candidate and destination share a parent filesystem, so both renames are
# atomic. The EXIT trap restores the backup if interruption happens between
# them. Once the candidate is installed, removing the backup only affects the
# private staging tree.
if [[ -d "${ROOT_DIR}/gen" ]]; then
  mv "${ROOT_DIR}/gen" "${BACKUP_DIR}"
  swapped=1
fi
mv "${CANDIDATE_DIR}" "${ROOT_DIR}/gen"
swapped=0
rm -rf "${BACKUP_DIR}"
