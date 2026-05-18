# jb-mesh

jb-mesh is a lightweight distributed tool mesh for running Python services across a set of machines.

It uses [NATS](https://nats.io/) for discovery, routing, events, and file movement, and [jumpboot](https://github.com/richinsley/jumpboot) to create isolated Python runtimes for each tool. A tool can be a tiny `main.py` plus `jumpboot.yaml`, installed on one node, then discovered and called from anywhere in the mesh.

If you're wondering what this is actually good for, start with [Yes, but why?](#yes-but-why).

## Yes, but why?

Once an agent or automation system grows beyond one machine, tool execution gets messy:

- some tools need GPUs
- some tools need local files or devices
- some tools are long-running services
- some tools should run on whichever node is available
- deployment glue turns into SSH scripts and snowflake environments

jb-mesh makes that boring. Nodes join a mesh, advertise their tools, and expose safe RPC methods through a common CLI/API surface.

Concrete things people might use it for:

- Run Whisper, OCR, embeddings, image generation, or other ML tools on the machine with the right GPU.
- Wrap home-lab scripts as discoverable RPC tools instead of SSHing into boxes by hand.
- Expose local-device capabilities — cameras, scanners, printers, capture cards, media ingest — through safe service methods.
- Share expensive resources across a LAN: GPU compute, model caches, large disks, special hardware, or always-on workers.
- Give AI agents a real tool backend where Python services have health checks, versioned manifests, and deployment proof.
- Build small internal function meshes such as `pdf.extract`, `video.transcode`, `embed.text`, or `backup.snapshot`.
- Run persistent services that need state, event subscriptions, or noisy subprocesses without turning every tool into a Docker/Kubernetes project.

## Core ideas

- **Tools are services** — Python classes expose `@method` RPC methods.
- **Manifests are contracts** — `jumpboot.yaml` declares runtime, dependencies, methods, health checks, and release smoke tests.
- **Nodes are peers** — any node can run tools; NATS handles routing and load balancing.
- **Runtimes are isolated** — each tool gets its own jumpboot-managed Python environment.
- **Operations are inspectable** — built-in commands cover status, install/update, logs, preflight, and release proof.

## Quick start

```bash
# Build
make build
# or: go build -o jb-mesh ./cmd/jb-mesh

# Start a seed node with embedded NATS
./jb-mesh serve --seed --name node-a

# In another terminal, install an example tool
./jb-mesh install ./examples/calculator

# List tools
./jb-mesh list

# Call a tool method
./jb-mesh call calculator.add a=2 b=3
# {"ok": true, "result": 5, "node": "node-a"}
```

## Multi-node mesh

For a LAN/dev setup, one node can act as the seed and other nodes can join as leaves.

```bash
# Node A: seed
jb-mesh serve --seed --name node-a

# Node B: leaf, connected to Node A's leaf port
jb-mesh serve --leaf nats-leaf://node-a.local:7422 --name node-b
```

With mDNS enabled, nodes can also discover a seed automatically on networks where Bonjour/Avahi works.

For public or cross-network deployments, put NATS behind a properly authenticated transport and read [`docs/nats-websocket.md`](docs/nats-websocket.md).

## Writing a tool

A minimal service has two files.

**`jumpboot.yaml`**

```yaml
name: hello
version: 0.1.0
description: Minimal jb-mesh service

runtime:
  python: "3.11"
  mode: oneshot
  packages:
    - pydantic>=2.0
    - git+https://github.com/richinsley/jb-mesh.git#subdirectory=sdk/python/jb-service

rpc:
  methods:
    hello:
      description: Say hello
      input:
        type: object
        properties:
          name: { type: string }

health:
  method: health

x-deploy:
  smoke:
    method: hello
    params:
      name: smoke
```

**`main.py`**

```python
from jb_service import Service, method, run


class Hello(Service):
    name = "hello"
    version = "0.1.0"

    @method
    def hello(self, name: str = "world") -> dict:
        return {"ok": True, "message": f"Hello, {name}!"}

    @method
    def health(self) -> dict:
        return {"status": "ok"}


if __name__ == "__main__":
    run(Hello)
```

Then:

```bash
# Check the local service contract before touching a live node
jb-mesh preflight ./hello

# Install and smoke locally or onto a selected node
jb-mesh install ./hello
jb-mesh call hello.hello name=mesh
```

For production-facing services, start with the canonical templates:

```bash
jb-mesh templates list
jb-mesh templates render baseline --name hello
jb-mesh init-service hello
```

See [`docs/authoring/service-starter.md`](docs/authoring/service-starter.md).

## Command overview

| Command | Purpose |
| --- | --- |
| `serve` | Start a mesh node |
| `status` | Show mesh topology and node status |
| `list` | List available tools and methods |
| `call <tool>.<method>` | Call a tool method |
| `install <source>` | Install a service from a local path or git source |
| `update <tool>` | Update a deployed tool |
| `uninstall <tool>` | Remove a deployed tool |
| `preflight <path>` | Validate a service before deployment |
| `init-service` | Scaffold a service from an embedded starter template |
| `templates` | Inspect embedded service templates |
| `events` | Watch mesh events |
| `files` | Use the mesh file/object store |
| `logs` | Query mesh logstore records |
| `service-release` | Inspect → update → verify → write release proof |

## Service release flow

`jb-mesh service-release` is the operator path for updating a live service with evidence.

It checks:

1. the local canonical repo is clean
2. the local branch tracks an upstream and is in sync
3. the deployed checkout is inspectable and clean
4. the managed update succeeds
5. `health` succeeds
6. the manifest-declared smoke call succeeds
7. a proof artifact is written

Portable smoke metadata lives in the service manifest:

```yaml
x-deploy:
  smoke:
    method: health
```

Machine-local deployment mapping stays outside the repo, usually in `~/.jb-mesh/release-targets.yaml`:

```yaml
services:
  example-service:
    node: node-a
    repo: /path/to/example-service
```

Never commit local topology, hostnames, private paths, or credentials into service repos.

## Transport modes

- **REPL** — default transport for simple tools.
- **MessagePack** — persistent, binary-safe transport for long-lived services or stdout-noisy subprocesses.

Use `runtime.transport: msgpack` and `MessagePackService` when a service needs persistent state, streaming/progress behavior, or strict stdout isolation.

## Documentation

- [`docs/authoring/service-starter.md`](docs/authoring/service-starter.md) — canonical service authoring path
- [`docs/authoring/preflight.md`](docs/authoring/preflight.md) — what preflight checks and how it fits into tool development
- [`sdk/python/`](sdk/python/) — visible Python SDK source and service-author documentation
- [`docs/nats-websocket.md`](docs/nats-websocket.md) — WebSocket transport notes
- [`examples/`](examples/) — minimal example services
- [`docs/release-boundary.md`](docs/release-boundary.md) — public/private release boundary guidance

## Release status

jb-mesh is preparing for a public release. The core is usable, but documentation, examples, and public packaging are still being hardened. Treat any pre-release API as subject to change.

## License

MIT
