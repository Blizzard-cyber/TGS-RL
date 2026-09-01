SHELL := /bin/sh
.DEFAULT_GOAL := help

BUF_VERSION := 1.72.0
UV_VERSION := 0.12.7
STATICCHECK_VERSION := 2026.1
GATE_CAMPAIGN_DRIVER ?= scripts/tgsrl-hardware-environment-driver
GO_PACKAGES := ./gen/go/... ./internal/... ./scheduler-go/... ./job-controller-go/... ./operator-go/... ./storage/... ./scripts ./cmd/...
PYTHON_PATHS := adapters runtime-python gateway-python tests/python tests/api tests/storage tests/e2e tests/governance scripts/check-proto-roundtrip.py scripts/check-oci-platforms.py scripts/generate-sbom.py scripts/check-compatibility.py scripts/check-upstream-patches.py scripts/gate-tools.py scripts/hardware-campaign-executor.py scripts/hardware_environment_driver.py scripts/tgsrl-hardware-environment-driver scripts/verl-reference-workload.py scripts/gate-full-stack-workload.py scripts/gate-managed-workload.py
GO_FORMAT_PATHS := scheduler-go job-controller-go operator-go storage cmd internal
SCHEDULER_PACKAGE := ./scheduler-go/cmd/scheduler
NVIDIA_BINDING_PACKAGE := ./cmd/tgsrl-nvidia-binding
NVIDIA_RUNTIME_PACKAGE := ./cmd/tgsrl-nvidia-runtime
NVIDIA_MIG_PACKAGE := ./cmd/tgsrl-nvidia-mig
WORKER_BOOTSTRAP_PACKAGE := ./cmd/tgsrl-worker-bootstrap
BIN_DIR ?= bin
SCHEDULER_LISTEN ?= 127.0.0.1:50051
CONTROLLER_LISTEN ?= 127.0.0.1:50061
RUNTIME_LISTEN ?= 127.0.0.1:50071
GATEWAY_LISTEN ?= 127.0.0.1:8080
OPERATOR_LISTEN ?= 127.0.0.1:50081
SCHEDULER_FALLBACK ?= noop

.PHONY: help doctor proto check-generated check-openapi proto-roundtrip check-migrations check-compose check-deploy render-kubernetes check-public-content sbom check-governance gate-campaign gate-campaign-run test-go test-performance test-python test-api test-console test-console-browser lint staticcheck test race build-nvidia-binding build-nvidia-runtime build-nvidia-mig build-worker-bootstrap demo product-e2e gate-cpu-integration run-scheduler run-controller run-runtime run-gateway run-operator run-console

help:
	@printf '%s\n' \
	  'TGS-RL commands:' \
	  '  make doctor           verify the local development toolchain' \
	  '  make proto            lint and regenerate protobuf outputs' \
	  '  make check-generated  regenerate and verify committed outputs are current' \
	  '  make check-openapi    verify committed OpenAPI artifact is current' \
	  '  make proto-roundtrip  verify Go/Python deterministic protobuf compatibility' \
	  '  make check-migrations validate the runtime SQLite schema' \
	  '  make check-compose    validate the complete local Compose stack' \
	  '  make check-deploy     validate full-stack and Operator Kubernetes/Helm contracts' \
	  '  make render-kubernetes render the complete Kubernetes control plane' \
	  '  make check-public-content reject private links, paths, and credential-like content' \
	  '  make sbom             regenerate the deterministic lockfile SBOM' \
	  '  make check-governance validate SBOM, compatibility evidence, and patch ledger' \
	  '  make gate-cpu-integration run the full local process Gate path' \
	  '  make gate-campaign     validate E1-E8 and summarize available evidence' \
	  '  make gate-campaign-run execute E1-E8 with a target-environment driver' \
	  '  make test-go          run all Go tests' \
	  '  make test-performance run non-race Scheduler and Provider P95 budgets' \
	  '  make test-python      sync the locked Python environment and run tests' \
	  '  make lint             run Go vet and Python Ruff/mypy checks' \
	  '  make staticcheck      run the pinned Go static analyzer' \
	  '  make test-api         run northbound API tests' \
	  '  make test-console     type-check, lint, test, and build the web console' \
	  '  make test-console-browser smoke all seven Console routes in Chromium' \
	  '  make test             run all Go, Python, API, and console tests' \
	  '  make race             run Go tests with the race detector' \
	  '  make build-nvidia-binding build the durable NVIDIA binding helper' \
	  '  make build-nvidia-runtime build the managed NVIDIA runtime helper' \
	  '  make build-nvidia-mig     build the safe MIG reconfiguration helper' \
	  '  make build-worker-bootstrap build the workload process supervisor' \
	  '  make demo             run the local Python-to-Go scheduler process E2E' \
	  '  make product-e2e      run the complete local product process E2E' \
	  '  make run-scheduler    start the scheduling service' \
	  '  make run-controller   start the job control service' \
	  '  make run-runtime      start runtime and experiment services' \
	  '  make run-gateway      start the northbound HTTP gateway' \
	  '  make run-operator     start the infrastructure operator worker' \
	  '  make run-console      start the web console dev server'

doctor:
	./scripts/check-environment.sh

proto:
	@if command -v buf >/dev/null 2>&1 && [ "$$(buf --version)" != "$(BUF_VERSION)" ]; then \
	  printf 'error: installed buf is %s; use %s or set BUF_BIN to that version\n' "$$(buf --version)" '$(BUF_VERSION)' >&2; \
	  exit 1; \
	fi
	./scripts/generate-proto.sh
	@buf_bin="$${BUF_BIN:-}"; \
	if [ -z "$$buf_bin" ]; then \
	  if command -v buf >/dev/null 2>&1; then buf_bin=$$(command -v buf); \
	  else buf_bin="$${TMPDIR:-/tmp}/tgsrl-tools/buf-v$(BUF_VERSION)/buf/bin/buf"; fi; \
	fi; \
	test -x "$$buf_bin" || { printf 'error: scripts/generate-proto.sh did not provide buf %s\n' '$(BUF_VERSION)' >&2; exit 1; }; \
	test "$$($$buf_bin --version)" = "$(BUF_VERSION)" || { \
	  printf 'error: buf %s is required, found %s\n' '$(BUF_VERSION)' "$$($$buf_bin --version)" >&2; \
	  exit 1; \
	}; \
	"$$buf_bin" lint

check-generated:
	./scripts/check-generated.sh

check-openapi:
	bash scripts/check-openapi.sh

proto-roundtrip:
	uv run --frozen python scripts/check-proto-roundtrip.py

check-migrations:
	./scripts/check-migrations.sh

check-compose:
	docker compose config -q

check-deploy:
	@command -v helm >/dev/null 2>&1 || { \
	  printf 'error: helm is required for deployment contract validation\n' >&2; \
	  exit 1; \
	}
	ruby deploy/helm/operator/tests/validate.rb
	ruby deploy/helm/tgsrl/tests/validate.rb

render-kubernetes:
	./scripts/deploy-full-stack.sh render

check-public-content:
	./scripts/check-public-content.sh

sbom:
	python3 scripts/generate-sbom.py

check-governance:
	python3 scripts/generate-sbom.py --check
	python3 scripts/check-compatibility.py
	python3 scripts/check-upstream-patches.py
	bash scripts/check-openapi.sh

gate-campaign:
	uv run --frozen python scripts/gate-tools.py campaign-plan --campaign configs/gates/e1-e8.json >/dev/null
	uv run --frozen python scripts/gate-tools.py campaign-evaluate --campaign configs/gates/e1-e8.json

gate-campaign-run:
	uv run --frozen python scripts/gate-tools.py campaign-run \
		--campaign configs/gates/e1-e8.json \
		--reports-dir .cache/tgsrl/e1-e8 \
		--driver "$(GATE_CAMPAIGN_DRIVER)" \
		--require-pass

test-go:
	go test $(GO_PACKAGES)

test-performance:
	go test -tags performance ./scheduler-go/scheduler ./scheduler-go/provider/nvidia -run '^TestPerformanceBudgets$$' -count=1 -v

test-python:
	@command -v uv >/dev/null 2>&1 || { \
	  printf 'error: uv %s is required\n' '$(UV_VERSION)' >&2; \
	  exit 1; \
	}
	@test "$$(uv --version | awk '{print $$2}')" = "$(UV_VERSION)" || { \
	  printf 'error: uv %s is required, found %s\n' '$(UV_VERSION)' "$$(uv --version)" >&2; \
	  exit 1; \
	}
	uv sync --frozen
	uv run --frozen pytest tests/python tests/storage tests/governance

test-api:
	uv run --frozen pytest tests/api

test-console:
	cd console && npm ci && npm run typecheck && npm run lint && npm test && npm run build

test-console-browser:
	cd console && VITE_TGSRL_API_ADAPTER=mock npm run test:browser

lint:
	@test -z "$$(gofmt -l $(GO_FORMAT_PATHS))" || { \
	  printf 'error: gofmt is required for:\n%s\n' "$$(gofmt -l $(GO_FORMAT_PATHS))" >&2; \
	  exit 1; \
	}
	go vet $(GO_PACKAGES)
	@buf_bin="$${BUF_BIN:-}"; \
	if [ -z "$$buf_bin" ]; then \
	  if command -v buf >/dev/null 2>&1; then buf_bin=$$(command -v buf); \
	  else buf_bin="$${TMPDIR:-/tmp}/tgsrl-tools/buf-v$(BUF_VERSION)/buf/bin/buf"; fi; \
	fi; \
	test -x "$$buf_bin" || { printf 'error: run make proto once to bootstrap buf %s\n' '$(BUF_VERSION)' >&2; exit 1; }; \
	test "$$($$buf_bin --version)" = "$(BUF_VERSION)" || { \
	  printf 'error: buf %s is required, found %s\n' '$(BUF_VERSION)' "$$($$buf_bin --version)" >&2; \
	  exit 1; \
	}; \
	"$$buf_bin" lint
	@command -v uv >/dev/null 2>&1 || { \
	  printf 'error: uv %s is required\n' '$(UV_VERSION)' >&2; \
	  exit 1; \
	}
	@test "$$(uv --version | awk '{print $$2}')" = "$(UV_VERSION)" || { \
	  printf 'error: uv %s is required, found %s\n' '$(UV_VERSION)' "$$(uv --version)" >&2; \
	  exit 1; \
	}
	uv sync --frozen
	uv run --frozen ruff format --check $(PYTHON_PATHS)
	uv run --frozen ruff check $(PYTHON_PATHS)
	uv run --frozen mypy adapters runtime-python/tgsrl_runtime gateway-python/tgsrl_gateway tests/python tests/api tests/storage tests/e2e

staticcheck:
	@command -v staticcheck >/dev/null 2>&1 || { \
	  printf 'error: staticcheck %s is required\n' '$(STATICCHECK_VERSION)' >&2; \
	  exit 1; \
	}
	@test "$$(staticcheck -version | awk '{print $$2}')" = "$(STATICCHECK_VERSION)" || { \
	  printf 'error: staticcheck %s is required, found %s\n' '$(STATICCHECK_VERSION)' "$$(staticcheck -version)" >&2; \
	  exit 1; \
	}
	staticcheck $(GO_PACKAGES)

test: test-go test-python test-api test-console proto-roundtrip check-migrations check-deploy check-public-content check-governance

race:
	go test -race $(GO_PACKAGES)

build-nvidia-binding:
	@mkdir -p "$(BIN_DIR)"
	go build -trimpath -o "$(BIN_DIR)/tgsrl-nvidia-binding" $(NVIDIA_BINDING_PACKAGE)

build-nvidia-runtime:
	@mkdir -p "$(BIN_DIR)"
	go build -trimpath -o "$(BIN_DIR)/tgsrl-nvidia-runtime" $(NVIDIA_RUNTIME_PACKAGE)

build-nvidia-mig:
	@mkdir -p "$(BIN_DIR)"
	go build -trimpath -o "$(BIN_DIR)/tgsrl-nvidia-mig" $(NVIDIA_MIG_PACKAGE)

build-worker-bootstrap:
	@mkdir -p "$(BIN_DIR)"
	go build -trimpath -o "$(BIN_DIR)/tgsrl-worker-bootstrap" $(WORKER_BOOTSTRAP_PACKAGE)

demo:
	@test -f scripts/demo.sh || { \
	  printf '%s\n' \
	    'error: scripts/demo.sh is not available yet.' \
	  exit 1; \
	}
	@TGSRL_DEMO_ADDRESS="$(SCHEDULER_LISTEN)" TGS_RL_DEMO_ADDRESS="$(SCHEDULER_LISTEN)" bash scripts/demo.sh

product-e2e:
	./scripts/product-e2e.sh

gate-cpu-integration:
	./scripts/gate-full-stack.sh

run-scheduler:
	go run ./scheduler-go/cmd/scheduler -listen "$(SCHEDULER_LISTEN)"

run-controller:
	go run ./job-controller-go/cmd/job-controller -listen "$(CONTROLLER_LISTEN)"

run-runtime:
	uv run --frozen python -m tgsrl_runtime.runtime_app --bind "$(RUNTIME_LISTEN)" --scheduler-target "$(SCHEDULER_LISTEN)" --operator-target "$(OPERATOR_LISTEN)" --job-control-target "$(CONTROLLER_LISTEN)" --state-db .cache/tgsrl/runtime.db

run-gateway:
	uv run --frozen python -m tgsrl_gateway serve --host 127.0.0.1 --port 8080 --backend-mode grpc --job-control-target "$(CONTROLLER_LISTEN)" --scheduler-target "$(SCHEDULER_LISTEN)" --runtime-target "$(RUNTIME_LISTEN)" --experiment-target "$(RUNTIME_LISTEN)"

run-operator:
	go run ./cmd/operator --mode=fake --listen "$(OPERATOR_LISTEN)" --scheduler "$(SCHEDULER_LISTEN)" --control "$(CONTROLLER_LISTEN)" --runtime "$(RUNTIME_LISTEN)" --cursor-dir .cache/tgsrl/operator

run-console:
	cd console && VITE_TGSRL_API_ADAPTER=http npm run dev
