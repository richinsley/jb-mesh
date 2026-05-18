# Python SDK

This folder contains the Python authoring surface for jb-mesh services.

## `jb-service`

[`jb-service/`](jb-service/) is the SDK used by Python services launched through `jb-mesh` and `jumpboot`.

It is kept in this repository so the service authoring contract is visible alongside the mesh runtime that executes it:

- `jb-mesh` owns node discovery, install/update, routing, logs, files, and release proof.
- `jb-service` owns Python method exposure, lifecycle hooks, transports, event subscriptions, file helpers, and structured logging helpers.

## Why the SDK lives here

The Python SDK and the mesh runtime are one product surface. Keeping them together makes the public release easier to understand and safer to evolve:

- examples and templates point at this repo, not a hidden second repo
- protocol changes can land with matching SDK changes
- documentation for service authors lives next to the SDK source
- future `jb-mesh` binaries can embed a compatible SDK bundle for offline/LAN-friendly installs

## Current install path

Until SDK embedding is wired into the binary, service manifests can install the SDK from this repository subdirectory:

```yaml
runtime:
  python: "3.11"
  mode: oneshot
  packages:
    - pydantic>=2.0
    - git+https://github.com/richinsley/jb-mesh.git#subdirectory=sdk/python/jb-service
```

The intended future shape is for `jb-mesh` to inject its bundled compatible SDK automatically, so new service manifests do not need to know a separate SDK package URL.
