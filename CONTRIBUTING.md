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

## Licensing

The repository is publicly readable but does not yet contain a redistribution license. Contributions are
accepted only after the project owner selects and adds a license; until then, opening a pull request does
not grant third parties a right to copy, redistribute or create derivative works from the repository.
