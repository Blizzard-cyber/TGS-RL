# TGS-RL handoff and status

Updated: 2026-09-12

This file is a shared handoff record between the development machine and the GPU test machine.
It is committed on purpose so both sides can pull it and stay in sync. It is temporary
coordination scaffolding, not durable product documentation: durable capability claims live in
`README.md` and `docs/` (`docs/reference/current-capabilities.md`,
`docs/project-design-and-code-review.md`, `docs/guides/gpu-smoke.md`). Once the project is ready to
release, this file and the `handoff/` directory should be deleted.

## Current local development batch

This macOS checkout is completing the heterogeneous-GPU and Console batch locally. No ECS command,
GPU image pull or hardware test is part of this batch.

- Scheduler NVIDIA Driver v2 now defaults to capability-aware `auto`: non-MIG GPUs remain Full GPU
  resources, while MIG-enabled GPUs publish existing MIG children only. A physical card is never
  counted in both forms, and Full GPU devices do not advertise MIG-only actions.
- Operator accepts ordered realization profiles. With
  `-gpu-profile=kubernetes-dra,hami-vgpu`, every concrete Binding independently selects the first
  profile that can enforce its UUIDs and share.
- `hami-vgpu` now uses HAMi's public NVIDIA resource/annotation protocol: typed Node inventory,
  `use-gpuuuid`, core/memory percentages, Pod allocation UUID readback and mandatory bootstrap
  device verification.
- The Gateway exposes `GET /v1/resources`; the Chinese Console now has a ninth “算力资源”
  workspace with device capabilities, allocations and responsive desktop/mobile layout.
- HAMi/HAMi-WebUI were used only as Apache-2.0 protocol and information-architecture references.
  No source was copied, vendored or patched.
- Current production hardware support remains NVIDIA-only. The generic `Device`/`DeviceKind`,
  `CapabilitySet` and `CompleteResourceProvider` contracts are the extension boundary for a future
  vendor implementation. The provider contracts now live outside the Mock implementation, and the
  Scheduler composition root uses a factory registry containing only Mock and NVIDIA. No Ascend/NPU
  provider, placeholder device class or fake support claim was added in this batch.

Local validation completed during this batch:

- `make test` passes, including 459 Python/runtime/storage/governance tests, 39 Gateway API tests,
  71 Console unit tests, all Go packages, generated-contract round trips, deployment contracts and
  repository/documentation/governance checks;
- `make lint`, `make staticcheck` and `make race` pass;
- `make test-performance` passes the Scheduler small/large P95 and NVIDIA observation budgets;
- Console browser tests: 17 passed, including all nine routes and
  1366/1180/1024/820/390 px responsive checks.

These results prove code contracts only. HAMi vGPU, MPS and MIG hardware behavior remains unverified.

## Machine roles

- **This machine (macOS, no GPU): development only.** It writes and reviews code, runs CPU/Mock,
  process E2E, unit/contract, static and governance checks. It cannot produce real GPU/Kubernetes/
  veRL evidence and must never mark hardware gates as PASS from local checks.
- **The other machine (Linux + NVIDIA GPU): test only.** It pulls this repo and runs the GPU smoke
  and E1-E8 campaign, then pushes code/results back so the dev machine can review and integrate.

Communication happens through this GitHub repo: dev pushes code, test pushes evidence, both pull.

## Repository state

- Development integration branch: `main`; the clean E1 integration series landed at `8f7494d`.
  GPU validation was performed on the temporary `test/e1-build-fixes` branch and its fixes were
  rewritten into three clean commits.
- The accepted E1 evidence is bound to source revision `d033566`. The released main tree contains
  that validated execution path plus host-tooling and documentation-only follow-ups. Re-run E1
  before release only if a later change alters the E1 execution path.
- Important GPU-host fixes are preserved as small Conventional Commits: DRA ClaimTemplate, bundle
  TypeMeta, prebuilt Workload defaults, admission-suspend semantics, canonical CPU quantities and
  deterministic hardware-driver cleanup.
- All commits must use the repository-configured maintainer identity and must not contain automated
  assistant attribution trailers.
- Pushing the verified integration is authorized by the current handoff task.

## Verified state

Re-ran real checks on this machine (not from memory):

- `go build ./...` and `go vet` pass.
- `go test` across `scheduler-go/ operator-go/ job-controller-go/ internal/ cmd/ storage/` all `ok`.
- `pytest tests/python tests/storage tests/governance tests/api` -> 497 passed (459 + 38).
- No Docker image was pulled or built.

Prior full-suite runs also covered Full-stack CPU Gate, Go race, staticcheck, Ruff, mypy,
generated-proto/migration checks, Helm/deploy contracts, SBOM, repository-hygiene, public-content,
`make check-docs`, and P95 performance budgets.

The GPU host completed E1 on 2026-09-12: `GPU_SINGLE_NODE`, `simulated=false`, 8/8 workload
executions, exact Scheduler/DRA/worker UUID equality, real CUDA events and clean resource teardown.
See `docs/validation/e1-full-gpu-2026-09-12.md`.

## GPU test host and E1 result (2026-09-12)

- A fresh Ubuntu 22.04 x86_64 ECS is reachable through the approved Kerberos ProxyJump path.
- Hardware verified: 14 vCPU, 54 GiB RAM, 181 GiB free disk and one NVIDIA A10 (23 GiB).
- The image-provided NVIDIA 550 runfile driver was safely replaced with the Ubuntu-managed
  `580.178.04` server driver; a reboot confirmed CUDA driver API 13.0 and the same physical UUID.
- Docker 29.8.0, Compose 5.5.1, Buildx 0.37.1 and NVIDIA Container Toolkit 1.20.0 are active.
  CDI publishes both ordinal and exact UUID device names; immutable project images were built and used.
- Kubernetes host modules/sysctls, cgroup v2, zero swap, cache directories and required host ports
  are ready.
- Network evidence: `dl.k8s.io` was about 33 KiB/s and Docker Hub timed out, while DaoCloud file
  proxy, Minikube upstream, Helm upstream, GitHub releases and Tsinghua PyPI were usable.
  `TGSRL_NETWORK_PROFILE=cn` is now committed on `main` and covers Go/npm plus the reusable
  download/Python mirror settings used to finish host setup.
- Using those verified mirrors, the host now has kubectl 1.35.1, Minikube 1.38.1, Helm 4.2.4,
  uv 0.12.7 and Python 3.12.14. Tool archives matched canonical upstream SHA-256 values.
- `uv sync --frozen` completed with the network profile and reusable cache.
- This ECS currently uses root for the disposable smoke environment. Run cluster creation with
  `TGSRL_MINIKUBE_ALLOW_ROOT=1`; normal reusable hosts should use a non-root user in the Docker group.
- Kubernetes 1.35.1 is Ready on the single-node `tgsrl-gpu` profile. Kueue 0.19.2, NFD 0.18.3
  and NVIDIA DRA 0.5.0 Pods are Running; Full GPU and MIG DeviceClasses plus a GPU ResourceSlice
  are published. `ResourceFlavor`, `ClusterQueue` and `LocalQueue` exist. Two first-host bugs were
  found and fixed: the Kueue webhook readiness race and the unsupported `kubectl rollout status
  daemonset --all` invocation. Images were built and E1 was completed on 2026-09-12.

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
| Console (9 workspaces) | Closed locally (9 Chinese pages, resource inventory + Trace, responsive layout) |
| NVIDIA Provider/helper | Full GPU E1 verified; capability-aware Full/MIG code verified locally; MIG/MPS hardware pending |
| Kubernetes / DRA / HAMi | DRA E1 verified; HAMi typed inventory/projection/readback implemented, hardware pending |
| veRL adapter | Implemented, real veRL/Ray/PyTorch/vLLM combination pending |
| Hardware Campaign E1-E8 | E1 PASSED; E2–E8 not run, E3–E8 have 9 thresholds to calibrate |
| Production release | Not admitted (MIG/MPS/full training/E2–E8 evidence missing) |

The single-node Full GPU integration target is complete. Overall graduation/release work remains
incomplete because E2–E8, MIG/MPS and full-model training evidence are separate requirements.
`compatibility/bom/runtime.yaml` now records `gpu_stack_integrated: true` for the narrowly scoped E1
Full GPU chain. The raw evidence remains only on the GPU host under `.cache/tgsrl/gpu-smoke`; the
committed, redacted result is `docs/validation/e1-full-gpu-2026-09-12.md`.

## E1-E8 gate status (verified from configs/gates/e1-e8.json)

```text
E1 Full GPU   | GPU_SINGLE_NODE | 1 rule  | PASSED 2026-09-12
E2 MIG        | GPU_SINGLE_NODE | 1 rule  | no calibration; requires MIG-capable hardware
E3 throughput | GPU_SINGLE_NODE | 2 rules | 1 threshold pending
E4 staleness  | GPU_SINGLE_NODE | 2 rules | 2 thresholds pending
E5 interfere  | GPU_SINGLE_NODE | 1 rule  | 1 threshold pending
E6 lifecycle  | GPU_SINGLE_NODE | 3 rules | 3 thresholds pending
E7 recovery   | GPU_SINGLE_NODE | 2 rules | 1 threshold pending
E8 multi-node | GPU_MULTI_NODE  | 2 rules | 1 threshold pending
```

E1/E2 have no calibration-required thresholds, but E2 still requires a GPU model with MIG enabled.
The current A10 host should be used for Full GPU and HAMi/MPS work, not for E2. E3-E8 hold 9
thresholds that stay `BLOCKED` until calibrated from real baseline data.

---

# Instructions for the GPU test machine

The development machine has **no GPU**; E1 was executed on the NVIDIA host. Use the steps below for
the final integrated-commit rerun and later hardware scenarios.

## What to run

Follow `docs/guides/gpu-smoke.md` exactly; it is the source of truth for a later E1 rerun or new
hardware scenario. Summary:

1. Clean clone / `git pull` to the exact commit under test, then confirm a clean tree
   (`make gpu-build-images` rejects a dirty tree).
2. Host + cluster prep (each step only pulls/builds when explicitly invoked):
   ```bash
   export TGSRL_NETWORK_PROFILE=cn  # for mainland-China/cross-border-limited hosts
   make gpu-install-host        # pinned host tooling (needs sudo)
   make gpu-create-cluster      # GPU-enabled minikube
   make gpu-prepare-cluster     # Kueue / NFD / NVIDIA DRA / smoke queue
   make gpu-configure-access    # scoped external-Operator kubeconfig
   make gpu-build-images
   make gpu-configure-registry  # copy registry credentials after the namespace exists
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
4. On the current A10 host, verify the HAMi single-GPU fractional path described in
   `docs/guides/hami.md`; do not block it on MIG support.
5. Run E2 only after moving to a GPU model with actual MIG support. Once hooks/thresholds are ready,
   continue E3-E8 via `make gate-campaign-run` (see `docs/design/gate-e1-e8.md`).

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
- Never weaken identity, safe-point, generation, receipt or readback checks merely to force a smoke pass.
- Keep `gpu_stack_integrated: true` scoped to E1 only; do not use it to claim E2–E8.

## Test-machine run log

Append one row per run so both sides share history.

| Date | Commit | Experiment | Result | Evidence path | Notes |
|---|---|---|---|---|---|
| 2026-09-12 | `d033566` | E1 | PASSED | `.cache/tgsrl/gpu-smoke/e1-full-gpu/report.json` on GPU host; committed summary in `docs/validation/e1-full-gpu-2026-09-12.md` | 8/8 executions; exact UUID; real CUDA; cleanup clean |

---

## Work still requiring target-environment evidence

- Preserve the accepted E1 evidence fingerprint. Re-run E1 only if a later change touches the E1
  execution path.
- On A10 or another non-MIG GPU, run a separate HAMi fractional-allocation smoke and record
  Scheduler UUID = Node inventory UUID = Pod allocation UUID = worker-visible UUID.
- Run MIG DeviceClass/parent-UUID/rebind E2 only on hardware that actually supports and enables MIG.
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
