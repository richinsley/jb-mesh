# Canonical Service Starter (#103)

This document defines the **canonical jb-mesh service authoring path**.
It is not just an example. It is the contract future services should follow so they are:
- easy to author
- easy to validate locally
- easy to release safely
- hard to let drift into one-off snowflakes

`examples/calculator` remains a useful toy example, but it is **not** the canonical starter shape for production-facing services.

## Goals

The starter should make the correct path the easiest path:
1. inspect what the deployed binary can scaffold via `jb-mesh templates list/show`
2. preview rendered output via `jb-mesh templates render --name ...` when you want a non-writing check
3. author in one canonical local repo
4. declare a real `health` method
5. declare a portable `x-deploy.smoke` call in `jumpboot.yaml`
6. pass `jb-mesh preflight`
7. release via `jb-mesh service-release`
8. keep machine-local deployment mapping out of the repo

## Non-goals

The starter is **not** trying to cover every service shape with one giant template.
We explicitly separate:
- a **baseline** starter for normal REPL / straightforward services
- a **persistent msgpack** starter for services that need persistent state, noisy stdout isolation, or long-lived runtime behavior

This avoids a bloated template that teaches people to cargo-cult options they do not need.

---

## Canonical file tree

### 1) Baseline starter

```text
my-service/
├── jumpboot.yaml
├── main.py
├── README.md
├── .gitignore
└── tests/
    └── test_smoke.md   # optional notes / manual checks, not framework-required
```

Minimum required files are still just:
- `jumpboot.yaml`
- `main.py`

But the **canonical production starter** should include a repo-local `README.md` and `.gitignore` from day one.

### 2) Persistent / msgpack starter

```text
my-service/
├── jumpboot.yaml
├── main.py
├── README.md
├── .gitignore
├── state/
│   └── .gitkeep
└── tests/
    └── test_smoke.md
```

The `state/` directory is optional at runtime, but including it in the starter makes long-lived service expectations explicit.

---

## Required invariants

These are the rules the canonical starter is meant to encode.

### A. Source of truth discipline

There is exactly one canonical local repo for a service.
That repo is what gets validated before release.

The repo must be:
- clean before release
- on a tracked upstream branch
- in sync with upstream before release

This is already enforced by `jb-mesh service-release`.
The starter should teach this workflow, not work around it.

### B. Real health method

Every canonical service must expose a real `health` method.
Not a placeholder if the service has meaningful dependencies.

Good health checks verify the thing that actually matters:
- model loaded
- dependency reachable
- state initialized
- scheduler/event bus attached
- service runtime not degraded

### C. Portable smoke call in manifest

Every canonical service must declare:

```yaml
x-deploy:
  smoke:
    method: health
```

or a more meaningful safe method call.

Rules for smoke calls:
- safe to run immediately after deploy
- meaningful enough to catch obvious breakage
- portable across nodes
- no machine-local paths
- no destructive side effects

### D. Preflight cleanliness

A canonical starter should pass `jb-mesh preflight` with no failures.

Preflight is the fast local contract check in the service development loop. Run it before installing onto a mesh node, before asking for review, and before `service-release`. It catches manifest/code drift, missing entrypoints, undeclared dependencies, broken hook declarations, and import-time failures in a temporary clean virtualenv.

That means in practice:
- manifest loads cleanly
- entrypoint exists
- Python AST can be inspected
- manifest RPC methods match `@method` definitions
- `health` / `setup` hooks point to real code when declared
- config schema is internally consistent
- runtime packages install in a temporary clean venv
- import smoke works in that clean venv

See [`preflight.md`](preflight.md) for the full model and where it fits in the dev/release cycle.

### E. Transport choice is intentional

Use baseline REPL unless you actually need msgpack.
Choose msgpack when:
- the service is persistent
- the runtime or subprocesses may write noisy stdout
- progress / binary-safe IPC matters
- the service wraps CLIs that are stdout-heavy

### F. Local-only deployment mapping stays local

Node selection and canonical repo path mapping belong in:

```text
~/.jb-mesh/release-targets.yaml
```

For services that live below the repo root, the local mapping may also include a `subdir` entry:

```yaml
services:
  sable-log-reader:
    node: node-a
    repo: /path/to/jb-mesh
    subdir: services/sable-log-reader
```

Never commit machine-local deployment targeting, private hostnames, local paths, or credentials into service repos.
The repo owns the portable release contract (`x-deploy`), not the operator’s machine topology.
`service-release` should still validate git cleanliness/sync at the checkout root while loading the manifest from the service subdirectory.

---

## Baseline starter contract

Use this for simple, direct services.

### `jumpboot.yaml`
Should include:
- `name`
- `version`
- `description`
- `runtime.python`
- `runtime.mode`
- runtime package declarations
- `rpc.methods`
- `health.method`
- `x-deploy.smoke`

Recommended defaults:
- `runtime.mode: oneshot`
- default REPL transport (omit `runtime.transport` unless needed)
- `x-deploy.smoke.method: health` until a better smoke call exists

### `main.py`
Should:
- define exactly one service class
- use `Service` base class
- decorate callable RPC surface with `@method`
- implement `health`
- defer heavyweight imports/setup work appropriately

---

## Persistent / msgpack starter contract

Use this when the service is long-lived or wraps noisy subprocess behavior.

### `jumpboot.yaml`
Recommended defaults:

```yaml
runtime:
  mode: persistent
  transport: msgpack
```

### `main.py`
Should:
- use `MessagePackService`
- avoid stdout protocol conflicts
- make persistent state explicit
- keep `health` meaningful for long-lived state

---

## Source of truth for starter templates

The deployed `jb-mesh` binary is the authority for what can actually be scaffolded.
That means:

- `cmd/jb-mesh/templates/service-starter/` is the authoritative embedded source used by `jb-mesh init-service` and `jb-mesh templates render`
- `templates/service-starter/` at repo root is the human-readable mirror for browsing, review, and docs
- if those trees drift, the binary wins — so maintenance should focus on keeping the repo-readable copies synchronized with the embedded ones

This split exists because the binary must be self-describing for both humans and agents, while Go embed can only read files inside the package tree.

## Dogfood testing notes

Dogfooding the starter surfaced one important runtime wrinkle:

- `jb-mesh install <local-path>` stages the tool locally under `~/.jb-mesh/tools`, but it does **not** by itself hot-register that tool into an already-running node.
- For a real mesh-visible smoke test, prefer installing onto a running node (`jb-mesh install <path-or-git> --node <node-name>`) or restarting/launching the local node after staging.
- For authoring validation, `jb-mesh preflight` is the first gate; direct executor-based tests are a useful second gate before live release.

## Author → validate → release workflow

The canonical workflow is:

```bash
# inspect what the binary knows how to scaffold
jb-mesh templates list
jb-mesh templates render baseline --name my-service

# materialize the starter into a canonical local repo
jb-mesh init-service my-service

# local contract check before touching a live node
jb-mesh preflight /path/to/service

# once preflight passes and the repo is clean/committed/pushed
jb-mesh service-release my-service
```

### What belongs where

**In repo:**
- runtime contract
- method surface
- config schema
- portable smoke metadata in `x-deploy`

**Operator local only:**
- which node receives the release
- where the canonical repo lives on that operator machine

---

## Why split baseline vs persistent/msgpack

Because forcing every service through one "kitchen sink" template causes drift in another form: people stop understanding which pieces matter.

A small baseline starter teaches the default shape.
A separate persistent/msgpack starter teaches the advanced shape.
That is clearer, easier to maintain, and better aligned with how services actually differ.

---

## Suggested next implementation steps

1. keep the repo-shipped starter templates under `templates/service-starter/` in sync with the CLI copies used by `jb-mesh init-service`
2. keep `examples/README.md` clearly separated from the canonical starter path
3. grow `jb-mesh init-service` only when real authoring friction justifies it
4. keep preflight and service-release as the enforcement spine

