# Preflight

`jb-mesh preflight` is the fast local gate for a service before you install it on a mesh node or run a managed release.

It answers one question:

> Does this service's repo-level contract look runnable before we touch a live node?

Preflight is not a full deployment test. It does not prove that the target node has the right GPU, files, credentials, model weights, network access, or runtime state. It catches the avoidable local failures first, so deployment failures are narrower and easier to reason about.

## Where it fits

Use preflight in the normal tool development loop:

```text
author/edit service
        ↓
jb-mesh preflight .        # local contract check
        ↓
commit + push              # canonical repo is clean and shareable
        ↓
jb-mesh service-release    # target-node update + health + smoke + proof
```

For quick local experimentation, run it before `jb-mesh install`:

```bash
jb-mesh preflight ./my-service
jb-mesh install ./my-service
jb-mesh call my-service.health
```

For production-facing services, preflight should pass before the branch is considered ready for review or release.

## What preflight checks

Preflight currently validates the service from the local checkout:

1. **Manifest loads** — `jumpboot.yaml` parses and satisfies the manifest schema.
2. **Entrypoint exists** — the configured Python entrypoint is present.
3. **Python AST parses** — the entrypoint can be inspected without executing service code.
4. **RPC parity** — manifest `rpc.methods` match `@method` definitions in Python.
5. **Hook parity** — declared `health` / `setup` hooks point at real code.
6. **Config schema sanity** — required config keys exist in the manifest schema.
7. **Top-level import warnings** — known-fragile bootstrap patterns, such as top-level `jumpboot` imports, are flagged.
8. **Clean venv install** — runtime package declarations can install into a temporary virtualenv.
9. **Import smoke** — the entrypoint imports successfully in that clean venv with `JB_TOOL_CONFIG={}`.

The clean venv and import smoke checks are important: they catch undeclared dependencies and import-time side effects that often only show up after deployment.

## What preflight deliberately does not prove

Preflight does **not** prove:

- the service works on every target node
- node-local files, devices, GPUs, models, or credentials exist
- NATS connectivity is available from the target node
- long-running behavior is healthy after startup
- the manifest smoke call succeeds against a live deployed service
- the deployed checkout is clean or in sync with upstream

Those are `service-release`, `health`, smoke-call, and operator-runbook responsibilities.

## Interpreting results

Treat failures as release blockers. A service with manifest/code drift or missing dependencies should not be deployed until fixed.

Treat warnings as review items. Some warnings may be acceptable for a specific service, but they should be intentional and documented.

A passing preflight means:

> This service is locally coherent and has a reasonable chance of installing cleanly.

It does not mean:

> This service is deployed, healthy, or production-ready.

## Useful flags

```bash
# Use a specific Python interpreter for venv/import checks
jb-mesh preflight . --python python3.11

# Keep the temporary venv for debugging failed installs/imports
jb-mesh preflight . --keep-venv

# Run only static checks, skipping package install and import smoke
jb-mesh preflight . --skip-install
```

Use `--skip-install` sparingly. It is useful when offline or when debugging manifest/static issues, but it skips the checks most likely to catch missing package declarations.

## Relationship to service-release

`service-release` is the live deployment gate. It verifies repo cleanliness/sync, performs the managed update, runs health, runs the manifest-declared smoke call, and writes proof.

Preflight is earlier and cheaper:

- **preflight** catches local service-contract mistakes
- **service-release** catches live deployment/runtime mistakes

Both matter. Preflight keeps bad service definitions from reaching the mesh; service-release proves the selected node actually took the update and can run the service.
