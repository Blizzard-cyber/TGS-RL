# Contributing to TGS-RL

Thank you for improving TGS-RL. Start with the [system design](docs/design/system-design.md) and
[maintainer walkthrough](docs/maintainers/code-walkthrough.md) before changing a cross-service
contract or control path.

## Development setup

The Docker-only product stack needs Docker Engine and Docker Compose v2:

```bash
make doctor
make local-up
make compose-smoke
```

Source development uses the pinned Go, Python, Node.js, Buf and uv versions documented in
[`compatibility/bom/runtime.yaml`](compatibility/bom/runtime.yaml):

```bash
make doctor-dev
uv sync --frozen
npm --prefix console ci
```

Kubernetes integration additionally requires Helm, kubectl and minikube. Check that toolchain with
`make doctor-kubernetes`.

## Before opening a pull request

Run checks in proportion to the change. The complete local suite is:

```bash
git diff --check
make check-repository
make check-docs
make lint
make test
make race
make test-performance
make check-generated
make check-governance
make check-public-content
```

Changes to the product path should also run `make product-e2e`. Console changes should run
`make test-console-browser`. Compose or packaging changes should run `make local-up`,
`make compose-smoke`, and `make local-down`.
Changes to GPU setup, campaign or workload packaging should also run the governance GPU tests and
ShellCheck locally; they must not be described as hardware-verified until accepted target-machine
evidence exists.

## Change boundaries

- Treat `proto/tgsrl/v1/` as the only cross-language wire-contract source. Do not edit `gen/` by hand.
- Keep desired state separate from observed state. A successful request is not authoritative workload
  convergence.
- Put external side effects behind idempotency keys, generation fences, durable receipts and readback.
- Add tests for a durable contract, not for incidental implementation details.
- Preserve the distinction between live, replay and synthetic evidence.
- Do not claim GPU, Kubernetes or training-performance validation from CPU/Mock results.

## What belongs in Git

Commit source, tests, documentation, schemas, migrations, generated Proto/OpenAPI artifacts, lockfiles,
SBOM/compatibility manifests and safe example configuration. Do not commit dependency directories,
virtual environments, caches, binaries, build output, databases, journals, logs, test evidence, kubeconfig,
production values or credentials. See [repository hygiene](docs/maintainers/repository-hygiene.md).

Use a focused Conventional Commit message and keep unrelated changes out of the same pull request.
Do not add automated assistant attribution trailers; repository governance scans both the worktree and
reachable commit metadata for this policy.

## Licensing

TGS-RL is licensed under the [Apache License 2.0](LICENSE). Unless explicitly stated otherwise, any
contribution intentionally submitted for inclusion in this project is provided under the same license,
as described by Section 5 of the license. Do not submit code or assets that you do not have the right to
license on these terms.
