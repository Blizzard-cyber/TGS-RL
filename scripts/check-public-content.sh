#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

# Keep public content free of private document links, local workstation paths,
# internal email domains, and credential formats. Build all deny patterns from
# fragments so this checker can validate its own source too.
mode="${CHECK_PUBLIC_CONTENT_MODE:-history}"

company_labels=(bytedance byted)
company_mail_domains=(com net org)
doc_labels=(larkoffice feishu)
doc_suffixes=(com cn)

blocked_parts=()
blocked_parts+=("/Us""ers/[^/[:space:]]+/")
blocked_parts+=("/pri""vate/tmp/")
blocked_parts+=("ghp_""[A-Za-z0-9]{20,}")
blocked_parts+=("git""hub_pat_""[A-Za-z0-9_]{20,}")
blocked_parts+=("sk-""[A-Za-z0-9_-]{20,}")
blocked_parts+=("AK""IA[0-9A-Z]{16}")
blocked_parts+=("AS""IA[0-9A-Z]{16}")
blocked_parts+=("xox""[baprs]-[A-Za-z0-9-]{10,}")
blocked_parts+=("BEGIN ([A-Z0-9]+ )?PRI""VATE KEY")
# Match private addresses where prose, JSON/YAML, or a URL normally starts a
# value. Dotted dependency versions are filtered separately below.
private_ip_prefix='(^|[^0-9])'
private_ip_suffix='([^0-9]|$)'
blocked_parts+=("${private_ip_prefix}10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}${private_ip_suffix}")
blocked_parts+=("${private_ip_prefix}192\.168\.[0-9]{1,3}\.[0-9]{1,3}${private_ip_suffix}")
blocked_parts+=("${private_ip_prefix}172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}${private_ip_suffix}")
blocked_parts+=("Co-[Aa]uthored-[Bb]y:[[:space:]]+TR""AE CLI")

for label in "${company_labels[@]}"; do
  for suffix in "${company_mail_domains[@]}"; do
    blocked_parts+=("[A-Za-z0-9._%+-]+@${label//./\\.}\\.${suffix//./\\.}")
    blocked_parts+=("${label//./\\.}\\.${suffix//./\\.}")
  done
done
for label in "${company_labels[@]}"; do
  for doc_label in "${doc_labels[@]}"; do
    for suffix in "${doc_suffixes[@]}"; do
      blocked_parts+=("${label//./\\.}\\.${doc_label//./\\.}\\.${suffix//./\\.}")
    done
  done
done
for doc_label in "${doc_labels[@]}"; do
  for suffix in "${doc_suffixes[@]}"; do
    blocked_parts+=("${doc_label//./\\.}\\.${suffix//./\\.}")
  done
done

blocked=$(IFS='|'; printf '(%s)' "${blocked_parts[*]}")

failed=0
reported=""

report_hit() {
  local location="$1"
  if [[ " $reported " != *" $location "* ]]; then
    printf '%s\n' "$location" >&2
    reported+=" $location "
  fi
  failed=1
}

contains_blocked_content() {
  local source_path=$1
  # SPDX can contain four-part dotted numeric package versions. They are not
  # network addresses; exclude only the generated versionInfo field while
  # preserving private-IP checks everywhere else, including URLs.
  if [[ "$source_path" == compatibility/sbom/lockfiles.spdx.json || "$source_path" == configs/hardware/gpu-requirements.lock ]]; then
    LC_ALL=C grep -IEiv '^[[:space:]]*"versionInfo":[[:space:]]*"[0-9]+(\.[0-9]+){3}([^"]*)",?[[:space:]]*$' | \
      LC_ALL=C grep -IEiq "$blocked"
  else
    LC_ALL=C grep -IEiq "$blocked"
  fi
}

contains_blocked_content_text() {
  local source_path=$1 content=$2
  printf '%s\n' "$content" | contains_blocked_content "$source_path"
}

self_check() {
  contains_blocked_content_text README.md 'connect to 10.'"23.45.67" || {
    printf '%s\n' 'error: private IPv4 detector rejected its own positive fixture' >&2
    exit 1
  }
  if contains_blocked_content_text compatibility/sbom/lockfiles.spdx.json '  "versionInfo": "10.'"23.45.67"'",'; then
    printf '%s\n' 'error: dependency-version exclusion is not working' >&2
    exit 1
  fi
}

self_check

scan_worktree() {
  local path
  while IFS= read -r -d '' path; do
    if [[ ! -f "$path" ]]; then
      continue
    fi
    # shellcheck disable=SC2094
    if contains_blocked_content "$path" <"$path"; then
      report_hit "WORKTREE:$path"
    fi
  done < <(git ls-files -z --cached --others --exclude-standard)
}

scan_history() {
  local commit
  while IFS= read -r commit; do
    [[ -n "$commit" ]] || continue
    if LC_ALL=C git show --no-patch --format='%an%n%ae%n%cn%n%ce%n%s%n%b' "$commit" | grep -IEiq "$blocked"; then
      report_hit "$commit:COMMIT_METADATA"
    fi
  done < <(git rev-list --all)

  local object_id object_type object_name
  while IFS=' ' read -r object_id object_type object_name; do
    [[ -n "$object_id" ]] || continue
    if [[ "$object_type" != "blob" || -z "$object_name" ]]; then
      continue
    fi
    if LC_ALL=C git cat-file -p "$object_id" | contains_blocked_content "$object_name"; then
      report_hit "$object_id:$object_name"
    fi
  done < <(git rev-list --objects --all | git cat-file --batch-check='%(objectname) %(objecttype) %(rest)')
}

case "$mode" in
  current-tree-only)
    scan_worktree
    ;;
  history)
    scan_worktree
    scan_history
    ;;
  *)
    printf 'error: unsupported CHECK_PUBLIC_CONTENT_MODE=%s\n' "$mode" >&2
    exit 1
    ;;
esac

if (( failed != 0 )); then
  printf 'error: private link, local path, or credential-like content found\n' >&2
  exit 1
fi

printf 'public-content-ok\n'
