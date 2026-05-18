# Examples

Small reference services for learning jb-mesh.

These examples are intentionally minimal. For production-facing services, prefer the canonical starter templates documented in [`../docs/authoring/service-starter.md`](../docs/authoring/service-starter.md).

| Example | Description | Transport |
| --- | --- | --- |
| `calculator` | Basic arithmetic; the hello-world service | REPL |
| `embed` | Text embedding service example | REPL |

## Run the calculator example

```bash
# From the repository root
# First: local manifest/code/dependency contract check
jb-mesh preflight ./examples/calculator

# Then: install and call the service
jb-mesh install ./examples/calculator
jb-mesh call calculator.add a=2 b=3
```

Preflight is intentionally earlier than install: it catches broken manifests, missing dependencies, method drift, and import failures before a tool reaches a running mesh node. See [`../docs/authoring/preflight.md`](../docs/authoring/preflight.md).

## Remote install from git

When installing from a git repository, use a public URL or a private URL that every target node can access:

```bash
jb-mesh install https://github.com/<owner>/<repo>.git --node node-a --path examples/calculator
```

Avoid committing machine-local hostnames, private IPs, or deployment paths into example manifests or docs.

## Writing your own service

A mesh service needs:

1. `jumpboot.yaml` — runtime, dependencies, RPC schema, health, and optional release smoke metadata
2. `main.py` — Python implementation using `jb-service`

Start from a template when possible:

```bash
jb-mesh templates list
jb-mesh init-service my-service
jb-mesh preflight ./my-service
```
