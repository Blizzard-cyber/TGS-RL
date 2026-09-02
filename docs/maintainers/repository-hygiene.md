# Repository hygiene and release contents

This page defines what a clean clone must contain and what must remain local. `make check-repository`
enforces the high-confidence parts of this contract.

## Must be committed

| Category | Paths | Why |
|---|---|---|
| Source and tests | `scheduler-go/`, `runtime-python/`, `operator-go/`, `job-controller-go/`, `gateway-python/`, `console/src/`, `tests/` | Product behavior and regression coverage |
| Wire contracts | `proto/tgsrl/v1/`, `buf.yaml`, `buf.gen.yaml` | Cross-language source of truth |
| Generated contracts | `gen/go/`, `gen/python/`, `api/openapi.json` | A clone builds without requiring code generation first |
| Dependency locks | `go.sum`, `uv.lock`, `console/package-lock.json` | Reproducible dependency resolution |
| Runtime compatibility | `compatibility/`, `configs/`, `upstream/` | Capability, policy, scenario and patch provenance |
| Database migrations | `runtime-python/tgsrl_runtime/storage/migrations/` | Existing state can be upgraded deterministically |
| Deployment contracts | Dockerfiles, `compose.yaml`, `deploy/` | Local and Kubernetes packaging |
| Open-source entry points | `README.md`, `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, `.gitattributes` | First-run, licensing, contribution, security and cross-platform behavior |
| Safe examples | files ending in `.example.*`, such as `configs/hardware/environment.example.json` | Document required shape without real environment data |

Generated Proto, OpenAPI and deterministic SBOM artifacts are intentional source-controlled outputs.
Change their source first, regenerate them with the documented command, and commit source plus output in
the same change.

## Must not be committed

| Category | Examples |
|---|---|
| Dependencies and environments | `node_modules/`, `.venv/`, `venv/` |
| Caches | `.cache/`, `__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.hypothesis/` |
| Build and test output | `bin/`, `dist/`, `build/`, coverage, Playwright reports and test results |
| Runtime state | SQLite databases, journals, checkpoints, PID/socket files and logs |
| Gate evidence | `artifacts/`, `evidence/`, `reports/`, raw traces and downloaded archives |
| Real environment configuration | `.env`, kubeconfig, `configs/hardware/environment.json`, `values.production.yaml` |
| Secrets | private keys, certificates, credentials, access tokens and production signing keys |
| Workstation files | `.DS_Store`, IDE folders, swap files and `WORKLOG.local.md` |

Before committing, run:

```bash
git status --short --untracked-files=all
git status --short --ignored
git diff --check
make check-repository
make check-public-content
```

If a file is ignored but contains source code, first determine why the ignore rule matched. Do not use
`git add -f` until the path has been reviewed and the ignore rule has been narrowed or documented.
The repository check scans ignored files under source and configuration roots specifically to catch this
class of “works only on the author's machine” failure.
