# Security Policy

## Supported versions

Security fixes are applied to the current `main` branch. This project has not declared a stable release
line or long-term support policy.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use the repository's private GitHub Security
Advisory reporting flow and include the affected commit, component, reproduction steps, impact and any
suggested mitigation. Do not include real credentials, private cluster data or personal information.

## Deployment boundary

The default local stack binds published ports to `127.0.0.1`. Gateway, gRPC and Prometheus endpoints do
not provide built-in TLS, authentication, authorization, tenant isolation or rate limiting. Do not expose
them directly to the public Internet or an untrusted shared network. Production-like deployments require
an external authenticated ingress, transport encryption, authorization, audit controls, secret management
and network policy.

The development signing key in `compose.yaml` is intentionally local-only. Never reuse it outside the
local CPU/Mock stack.
