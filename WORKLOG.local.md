# TGS-RL handoff and status

Updated: 2026-09-11

This file is a shared handoff record between the development machine and the GPU test machine.
It is committed on purpose so both sides can pull it and stay in sync. It is temporary
coordination scaffolding, not durable product documentation: durable capability claims live in
`README.md` and `docs/` (`docs/reference/current-capabilities.md`,
`docs/project-design-and-code-review.md`, `docs/guides/gpu-smoke.md`). Once the project is ready to
release, this file and the `handoff/` directory should be deleted.

## Machine roles

- **This machine (macOS, no GPU): development only.** It writes and reviews code, runs CPU/Mock,
  process E2E, unit/contract, static and governance checks. It cannot produce real GPU/Kubernetes/
  veRL evidence and must never mark hardware gates as PASS from local checks.
- **The other machine (Linux + NVIDIA GPU): test only.** It pulls this repo and runs the GPU smoke
  and E1-E8 campaign, then pushes results back so the dev machine can pull and fix.

Communication happens through this GitHub repo: dev pushes code, test pushes evidence, both pull.

## Repository state

- Branch: `main`; working tree clean.
- Local `HEAD` == `origin/main` == `0af2e25` (`docs: align project guidance with current implementation`).
- Recent commits:
  - `0af2e25 docs: align project guidance with current implementation`
  - `8dbafc3 feat(gpu): add reproducible full-stack smoke workflow`
  - `4df3d5e fix(runtime): separate workload and control dependencies`
  - `abd26c5 chore(repo): adopt Apache-2.0 license`
- All reachable history was checked for automated assistant `Co-authored-by` trailers; none remain.
- Do not push unless the user explicitly requests it.

## Verified state (this checkpoint, dev machine, 2026-09-11)

Re-ran real checks on this machine (not from memory):

- `go build ./...` and `go vet` pass.
- `go test` across `scheduler-go/ operator-go/ job-controller-go/ internal/ cmd/ storage/` all `ok`.
- `pytest tests/python tests/storage tests/governance tests/api` -> 493 passed.
- No Docker image was pulled or built.

Prior full-suite runs also covered Full-stack CPU Gate, Go race, staticcheck, Ruff, mypy,
generated-proto/migration checks, Helm/deploy contracts, SBOM, repository-hygiene, public-content,
`make check-docs`, and P95 performance budgets.

## Completion snapshot

Architecture (Proto-first + Dual-plane + VUG) is landed and stable. Control plane is essentially
closed; the remaining gap is real hardware evidence, not code structure.

| Area | State |
|---|---|
| Proto / domain contracts | Closed (Buf breaking + cross-language round-trip) |
| Job control plane | Closed (Job/Run/Operation/Timeline/Trace, CLI/SDK/HTTP/Console, restart recovery) |
| Runtime / Trace / Replay | Closed on single-node path (desired/observed split, typed observation, SQLite recovery) |
| Scheduler + transactions | Closed (constraints, scoring, budgets, reservation, receipt, compensation, recovery) |
| CPU Mock + process E2E | Verified (real subprocess, bootstrap, Unix socket, service restart) |
| Console (8 workspaces) | Closed (8 pages + tests, responsive layout) |
| NVIDIA Provider/helper | Code complete, hardware verification pending |
| Kubernetes / DRA | Main chain complete, cluster verification pending |
| veRL adapter | Implemented, real veRL/Ray/PyTorch/vLLM combination pending |
| Hardware Campaign E1-E8 | Runner ready; E1/E2 runnable, E3-E8 have 9 thresholds to calibrate |
| Production release | Not admitted (no real GPU/MIG/veRL evidence) |

Rough progress: control plane ~90%, execution/hardware face ~60-65%, overall ~78%.
`compatibility/bom/runtime.yaml` keeps `gpu_stack_integrated: false`; `.cache/tgsrl/gpu-smoke` is
empty on the dev machine (no real GPU evidence yet), which is expected.

## E1-E8 gate status (verified from configs/gates/e1-e8.json)

```text
E1 Full GPU   | GPU_SINGLE_NODE | 1 rule  | no calibration        <- runnable now
E2 MIG        | GPU_SINGLE_NODE | 1 rule  | no calibration        <- runnable now
E3 throughput | GPU_SINGLE_NODE | 2 rules | 1 threshold pending
E4 staleness  | GPU_SINGLE_NODE | 2 rules | 2 thresholds pending
E5 interfere  | GPU_SINGLE_NODE | 1 rule  | 1 threshold pending
E6 lifecycle  | GPU_SINGLE_NODE | 3 rules | 3 thresholds pending
E7 recovery   | GPU_SINGLE_NODE | 2 rules | 1 threshold pending
E8 multi-node | GPU_MULTI_NODE  | 2 rules | 1 threshold pending
```

E1/E2 have no calibration-required thresholds and can produce a verdict immediately. E3-E8 hold
9 thresholds that stay `BLOCKED` until calibrated from real baseline data.

---

# Instructions for the GPU test machine

This machine has **no GPU**, so the checks below could not be run here. Run them on the NVIDIA host.

## What to run

Follow `docs/guides/gpu-smoke.md` exactly; it is the source of truth. Summary:

1. Clean clone / `git pull` to the exact commit under test, then confirm a clean tree
   (`make gpu-build-images` rejects a dirty tree).
2. Host + cluster prep (each step only pulls/builds when explicitly invoked):
   ```bash
   make gpu-install-host        # pinned host tooling (needs sudo)
   make gpu-create-cluster      # GPU-enabled minikube
   make gpu-prepare-cluster     # Kueue / NFD / NVIDIA DRA / smoke queue
   make gpu-configure-access    # scoped external-Operator kubeconfig
   make gpu-configure-registry  # push immutable bootstrap/workload images
   make gpu-build-images
   make gpu-render-config
   make gpu-preflight           # host GPU + Docker + K8s + DRA + Kueue
   make gpu-up
   ```
3. Run E1 first (single Full GPU on one node):
   ```bash
   make gpu-smoke
   ```
   `make gpu-smoke` exits non-zero on any E1 failure/invalid-evidence/rule failure. A pass only means
   E1; it is not E2-E8 and not release admission.
4. After E1 passes, run E2 (MIG) and, once hooks/thresholds are ready, E3-E8 via
   `make gate-campaign-run` (see `docs/design/gate-e1-e8.md`).

## Where results go (so the dev machine can pull and fix)

The raw run output under `.cache/tgsrl/gpu-smoke/` is git-ignored, and `evidence/`, `reports/`,
`artifacts/` are rejected by `make check-repository` on purpose (they can hold logs, local paths and
secrets). To share results across machines, copy the **redacted** report plus supporting material
into the committed `handoff/` directory:

```
handoff/
  E1-full-gpu/
    report.json          # copy of the E1 report.json
    campaign-report.json # copy of .cache/tgsrl/gpu-smoke/campaign-report.json
    NOTES.md             # pass/fail, exact error text, environment (driver, CUDA, K8s, Kueue, DRA versions)
    logs/                # relevant, redacted service/worker logs and raw trace NDJSON
```

Then:

```bash
git checkout -b test/e1-evidence
git add handoff/ WORKLOG.local.md
git commit -m "test(e1): add GPU smoke results and status"
git push -u origin test/e1-evidence
```

Rules for pushing results:

- Do NOT commit `kubeconfig`, registry credentials, signing keys, `.env`, or
  `configs/hardware/environment.json`. Those stay local (they are git-ignored).
- If a log contains a token/credential, redact it before committing.
  `make check-public-content` will also flag credential-like content.
- Use `handoff/` (committed), not `evidence/` / `reports/` / `artifacts/` (those are rejected by
  `make check-repository`).
- Prefer a `test/*` branch and let the dev machine pull, review, fix, and push code back.
- Update the "Test-machine run log" table below in the same commit.

## What the dev machine will do with it

- Pull the `test/*` branch, read `handoff/**/NOTES.md` + `report.json`, reproduce the failure logic
  in CPU/Mock/process tests where possible, fix, and push code back for a re-run.
- Never weaken identity, safe-point, generation, receipt or readback checks just to make a smoke pass.
- Keep `gpu_stack_integrated: false` until accepted real E1 evidence exists.

## Test-machine run log

Append one row per run so both sides share history.

| Date | Commit | Experiment | Result | Evidence path | Notes |
|---|---|---|---|---|---|
| (pending) | | E1 | not run | | GPU machine has not run yet |

---

## Work still requiring target-environment evidence

- Full GPU Kubernetes/Kueue/DRA/CDI E1; MIG DeviceClass/parent-UUID/rebind E2.
- MPS server PID visibility, share mutation and readback.
- Complete model-training callbacks and distributed veRL/Ray behavior.
- E3-E8 workload/action/fault hooks and nine calibrated thresholds.
- Single-node performance, recovery and multi-node convergence evidence.
- Production ingress/auth/TLS, HA, resource limits/PDB and long-term Trace storage.

## Production-hardening backlog (P1/P2, not blocking smoke)

- P1: unified ingress TLS/auth/authz/tenant-isolation/rate-limit (currently local/isolated only).
- P1: generic Secret/ConfigMap reference model for Jobs (only string env vars today).
- P1: Helm resource requests/limits, PDB, multi-replica/HA.
- P1: hardware driver UID/resourceVersion precondition between GET and DELETE; force explicit
  `kube_context` and record cluster UID/hash in the evidence fingerprint.
- P1: full-stack metrics and unified correlation logging (only Scheduler exposes Prometheus today).
- P1: long-term Trace cold storage (SQLite hot storage only).
- P2: split the ~2000-line `scripts/hardware_environment_driver.py` before adding E3-E8 platforms.
- P2: rename `_placeholder_launch_spec` to `_failure_context_spec` (naming only).

## Resume checklist (dev machine)

1. Check `git status --short --branch`, local/remote HEAD and any unpushed commits.
2. Pull any `test/*` result branch and read `handoff/**/NOTES.md`.
3. Run `make check-docs`, `make check-governance`, `make check-public-content` and relevant tests.
4. Use a clean checkout before `make gpu-build-images`; the image script rejects a dirty tree.
5. Keep `.cache/`, kubeconfig, Docker credentials, signing keys, logs and secrets out of Git.
6. Commit with a concise Conventional Commit subject and disable any attribution hook that injects
   automated assistant trailers.

## Cleanup before release

This coordination scaffolding is temporary. Before tagging a release, remove `WORKLOG.local.md` and
the `handoff/` directory, and re-add `/WORKLOG.local.md` (and `handoff/` if desired) to
`.gitignore` so they do not reappear.
