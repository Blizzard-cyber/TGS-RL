#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

failures=0
fail() { printf 'error: %s\n' "$1" >&2; failures=$((failures + 1)); }

required_files=(
  go.mod
  go.sum
  pyproject.toml
  uv.lock
  console/package.json
  console/package-lock.json
  api/openapi.json
  compatibility/sbom/lockfiles.spdx.json
  configs/hardware/environment.example.json
  configs/hardware/e5-interference-job.example.json
  configs/hardware/e6-action-cost-job.example.json
  configs/hardware/verl-job.example.json
  configs/hardware/verl-lifecycle-job.example.json
  configs/gates/e6-action-cost-calibration.json
  configs/gates/e5-static-interference-calibration.json
  configs/gates/e5-static-interference.json
  configs/gates/a10-readiness.json
  configs/gates/gate-a10-readiness.json
  configs/scenarios/e5-static-interference.yaml
  configs/scenarios/a10-full-lifecycle.yaml
  configs/hardware/gpu-requirements.in
  configs/hardware/gpu-requirements.lock
  configs/network/cn.env
  compose.gpu.yaml
  Dockerfile.gpu-smoke
  scripts/check-docs.py
  scripts/gpu-configure-registry.sh
  scripts/gpu-a10-readiness.sh
  scripts/gpu-experiment-workload.py
  scripts/engineering-fault-readiness.py
  scripts/gpu-render-helm-values.sh
  scripts/gpu-helm-smoke.sh
  scripts/lib/network-profile.sh
  README.md
  LICENSE
  docs/README.md
  docs/getting-started.md
  docs/reference/current-capabilities.md
  docs/validation/README.md
  docs/maintainers/repository-hygiene.md
  .gitattributes
  CONTRIBUTING.md
  SECURITY.md
)

for path in "${required_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    fail "required repository input is missing: $path"
  elif git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
    :
  elif git status --porcelain=v1 --untracked-files=all -- "$path" | grep -q '^?? '; then
    printf 'warning: required repository input is new and must be staged before commit: %s\n' "$path" >&2
  else
    fail "required repository input is neither tracked nor visible as a new file: $path"
  fi
done

grep -Fqx '                           Version 2.0, January 2004' LICENSE ||
  fail "LICENSE is not the Apache License 2.0 text"
grep -Fqx 'license = "Apache-2.0"' pyproject.toml ||
  fail "pyproject.toml must declare the Apache-2.0 SPDX expression"
grep -Fqx 'license-files = ["LICENSE"]' pyproject.toml ||
  fail "the Python distribution must include LICENSE"
grep -Fq '"license": "Apache-2.0"' console/package.json ||
  fail "console/package.json must declare Apache-2.0"
grep -Fq '"license": "Apache-2.0"' console/package-lock.json ||
  fail "console/package-lock.json must preserve the root package license"
grep -Fqx '!LICENSE' .dockerignore ||
  fail "LICENSE must be included in Docker build contexts"

distributable_stages=(
  "Dockerfile.local:3"
  "Dockerfile.operator:1"
  "Dockerfile.services:7"
  "Dockerfile.worker-bootstrap:1"
  "Dockerfile.gpu-smoke:1"
)
for stage_spec in "${distributable_stages[@]}"; do
  dockerfile=${stage_spec%:*}
  expected=${stage_spec##*:}
  label_count=$(grep -Fc 'org.opencontainers.image.licenses="Apache-2.0"' "$dockerfile" || true)
  license_count=$(grep -Fc 'COPY LICENSE /licenses/TGS-RL/LICENSE' "$dockerfile" || true)
  [[ "$label_count" -eq "$expected" ]] ||
    fail "expected $expected Apache-2.0 OCI labels in $dockerfile, found $label_count"
  [[ "$license_count" -eq "$expected" ]] ||
    fail "expected $expected LICENSE copies in $dockerfile, found $license_count"
done

for directory in proto/tgsrl/v1 gen/go/tgsrl/v1 gen/python/tgsrl/v1; do
  if ! git ls-files "$directory" | grep -q .; then
    fail "required contract or generated-code directory is empty: $directory"
  fi
done

forbidden_path_pattern='(^|/)(node_modules|dist|build|bin|__pycache__|\.pytest_cache|\.mypy_cache|\.ruff_cache|\.hypothesis|\.cache|playwright-report|test-results|coverage|htmlcov|artifacts|evidence|reports)(/|$)'
forbidden_file_pattern='(^|/)(\.env($|\.)|kubeconfig($|\.)|credentials\.json$|values\.production\.yaml$)|\.(db|sqlite|sqlite3|db-shm|db-wal|journal|checkpoint|log|pid|sock|pem|key|crt|p12|pfx|jks|swp|tmp)$'

while IFS= read -r path; do
  if [[ "$path" =~ $forbidden_path_pattern ]] || [[ "$path" =~ $forbidden_file_pattern ]]; then
    fail "local, generated, or secret-bearing file must not be tracked: $path"
  fi
done < <(git ls-files)

# An overly broad ignore rule can hide source code from `git status`, causing
# a checkout to work only on the author's machine. Inspect ignored files under
# source/configuration roots and reject source-like files outside known caches.
source_roots=(
  adapters gateway-python runtime-python scheduler-go job-controller-go
  operator-go internal cmd console/src scripts tests docs deploy proto configs
  compatibility storage upstream
)
while IFS= read -r path; do
  case "$path" in
    */__pycache__/*|*/.pytest_cache/*|*/.mypy_cache/*|*/.ruff_cache/*|*/.hypothesis/*)
      continue
      ;;
  esac
  case "$path" in
    *.go|*.py|*.ts|*.tsx|*.proto|*.sql|*.sh|*.rb|*.yaml|*.yml|*.md)
      fail "source-like file is hidden by ignore rules: $path"
      ;;
  esac
done < <(git ls-files --others --ignored --exclude-standard -- "${source_roots[@]}")

if (( failures != 0 )); then
  printf 'repository hygiene failed: %d error(s)\n' "$failures" >&2
  exit 1
fi

printf 'repository-hygiene-ok\n'
