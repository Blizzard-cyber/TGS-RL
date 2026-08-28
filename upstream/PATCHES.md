# Upstream patch ledger

Status: **no registered upstream patches**.

The machine-readable source of truth is `upstream/patches.json`; this page
documents the policy and entry shape. `scripts/check-upstream-patches.py` gates
registration, content digests, test evidence, and replay with `git apply --check`.

This means the repository does not currently carry an approved patch entry. It
does not claim that every future dependency or local checkout is patch-free. A
patch must be registered here before it is referenced by the BOM or by a
`FrameworkRuntimeSpec.patch_set`.

## Gate

Before carrying a patch, try the least invasive option in this order:

1. configuration or a public upstream API;
2. an adapter or wrapper;
3. a sidecar or controller;
4. an upstream contribution;
5. a temporary downstream patch;
6. a maintained fork, only with an owner, maintenance budget, and exit plan.

Unregistered patches, floating baselines, and patches without tests or an exit
condition must not be merged.

## Registered patches

| Patch ID | Component | Baseline version | Baseline commit | Status | Owner | Exit condition |
|---|---|---|---|---|---|---|
| _None_ | — | — | — | — | — | — |

## Entry template

Copy this section for each proposed patch. Do not replace unknown values with
guesses. The content digest is the digest of the patch file, not an upstream
artifact digest.

```yaml
patch_id: PATCH-0001
status: proposed
component: null
purpose: null
upstream_repository: null
upstream_baseline:
  version: null
  commit: null
affected_files: []
patch_file: null
content_sha256: null
alternatives_attempted:
  - configuration-or-public-api
  - adapter-or-wrapper
  - sidecar-or-controller
upstream_issue_or_pr: null
tests: []
owner: null
introduced_at: null
review_by: null
exit_condition: null
fork:
  maintained: false
  maintenance_budget_owner: null
```
