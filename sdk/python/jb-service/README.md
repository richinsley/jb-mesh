# jb-service

`jb-service` is the Python SDK for writing services that run under [jb-mesh](https://github.com/richinsley/jb-mesh) via [jumpboot](https://github.com/richinsley/jumpboot).

It gives a Python class a small RPC surface: subclass a service base class, decorate methods with `@method`, declare the same methods in `jumpboot.yaml`, and let `jb-mesh` install, run, discover, and call the service.

## Why this exists

`jb-mesh` handles node discovery, NATS routing, lifecycle, install/update, file movement, logs, and release proof.

`jb-service` is the Python side of that contract. It handles:

- method registration
- request/response protocol
- type/schema introspection
- lifecycle hooks
- optional MessagePack transport
- event subscriptions
- mesh file helpers
- structured logging helpers

Most users should encounter `jb-service` while writing a `jb-mesh` tool. The SDK source and docs live in this repository so service authors can inspect the runtime contract directly.

## Installation

For now, install from the SDK subdirectory in the jb-mesh repository:

```bash
pip install git+https://github.com/richinsley/jb-mesh.git#subdirectory=sdk/python/jb-service
```

In a `jb-mesh` service manifest:

```yaml
runtime:
  python: "3.11"
  mode: oneshot
  packages:
    - pydantic>=2.0
    - git+https://github.com/richinsley/jb-mesh.git#subdirectory=sdk/python/jb-service
```

## Quick start

`main.py`:

```python
from jb_service import Service, method, run


class Calculator(Service):
    name = "calculator"
    version = "0.1.0"

    @method
    def add(self, a: float, b: float) -> dict:
        return {"result": a + b}

    @method
    def health(self) -> dict:
        return {"status": "ok"}


if __name__ == "__main__":
    run(Calculator)
```

`jumpboot.yaml`:

```yaml
name: calculator
version: 0.1.0
description: Minimal calculator service

runtime:
  python: "3.11"
  mode: oneshot
  packages:
    - pydantic>=2.0
    - git+https://github.com/richinsley/jb-mesh.git#subdirectory=sdk/python/jb-service

rpc:
  methods:
    add:
      description: Add two numbers
      input:
        type: object
        properties:
          a: { type: number }
          b: { type: number }
    health:
      description: Health check

health:
  method: health

x-deploy:
  smoke:
    method: health
```

Then from the service repo:

```bash
jb-mesh preflight .
jb-mesh install .
jb-mesh call calculator.add a=2 b=3
```

## Service types

### `Service` — REPL transport

Use `Service` for simple tools that do not write unrelated output to stdout.

```python
from jb_service import Service, method, run


class Echo(Service):
    @method
    def echo(self, message: str = "hello") -> dict:
        return {"echo": message}


if __name__ == "__main__":
    run(Echo)
```

### `MessagePackService` — persistent/stdout-safe transport

Use `MessagePackService` for long-lived services, progress-heavy tools, model servers, or wrappers around CLIs that write to stdout.

```python
from jb_service import MessagePackService, method, run


class ImageGenerator(MessagePackService):
    def setup(self):
        # Load models or initialize persistent state here.
        self.ready = True

    @method
    def generate(self, prompt: str) -> dict:
        # Run generation here.
        return {"ok": True, "prompt": prompt}

    @method
    def health(self) -> dict:
        return {"status": "ok", "ready": self.ready}


if __name__ == "__main__":
    run(ImageGenerator)
```

Manifest transport:

```yaml
runtime:
  mode: persistent
  transport: msgpack
```

## Lifecycle hooks

```python
class MyService(Service):
    def setup(self):
        """Called once on startup. Load models, clients, caches, or state."""
        ...

    def teardown(self):
        """Called on shutdown. Cleanup resources."""
        ...

    async def setup_async(self):
        """Async startup is supported."""
        ...

    async def teardown_async(self):
        """Async cleanup is supported."""
        ...
```

## Event subscriptions

Persistent tools can react to mesh events through the built-in event bridge.

### Decorator style

```python
from jb_service import Service, method, on_event


class Watcher(Service):
    @on_event("events.tool.*")
    def on_tool_event(self, event: dict):
        self.log.info(f"tool event: {event['type']}")

    @on_event("events.user.>")
    def on_user_event(self, event: dict):
        self.log.info(f"user event: {event['type']}")

    @method
    def health(self) -> dict:
        return {"ok": True}
```

### Imperative style

```python
from jb_service import Service


class Watcher(Service):
    def setup(self):
        self.subscribe_event("events.node.*", self.on_node_event)

    def on_node_event(self, event: dict):
        self.log.info(f"node event: {event['type']}")
```

Callbacks receive a parsed event envelope:

```python
{
    "type": "tool.started",
    "node": "node-a",
    "timestamp": "2026-01-01T00:00:00Z",
    "data": {"tool": "calculator"},
}
```

Common patterns:

- `events.>` for all events
- `events.tool.*` for tool lifecycle events
- `events.node.*` for node lifecycle events
- `events.user.>` for user-defined events emitted via `emit_event()`

## File handling

### Input types

```python
from jb_service import Service, method, FilePath, Audio, Image


class MediaProcessor(Service):
    @method
    def process_path(self, file: FilePath) -> dict:
        # file is a path string
        return {"path": str(file)}

    @method
    def process_audio(self, audio: Audio) -> dict:
        # audio is loaded as (sample_rate, numpy_array)
        sample_rate, data = audio
        return {"sample_rate": sample_rate, "samples": len(data)}

    @method
    def process_image(self, image: Image) -> dict:
        # image is a PIL.Image
        return {"width": image.size[0], "height": image.size[1]}
```

### Output files

Use helpers such as `save_image()` or return paths directly when the mesh runtime should wrap output as a file reference.

```python
from jb_service import MessagePackService, method, run, save_image


class Generator(MessagePackService):
    @method
    def generate(self, prompt: str) -> dict:
        image = make_image(prompt)
        path = save_image(image, format="png")
        return {"image": path}
```

## Structured logging

Services expose `self.log` plus logstore helpers for bounded structured records.

```python
class MyService(Service):
    @method
    def process(self, data: str) -> dict:
        corr = self.ensure_correlation_id()
        self.log.info("processing started")
        self.log.call_log(
            "info",
            "process completed",
            method="process",
            data={"input_size": len(data)},
            corr=corr,
            ok=True,
        )
        return {"result": "ok"}
```

Helpers include:

```python
corr = self.ensure_correlation_id()
self.log.tool_log("info", "warming model")
self.log.call_log("info", "done", method="process", ok=True)

health = self.logstore.health()
recent = self.logstore.tail(limit=20)
query = self.logstore.query(corr=corr, limit=50)
stats = self.logstore.stats(since="24h", group_by=["node", "kind"])
```

Structured records use schema `jb.mesh.log.v1`. Include only bounded metadata in `data`; avoid logging full prompts, secrets, files, or unbounded payloads.

## Async methods

```python
class AsyncTool(Service):
    @method
    async def fetch_data(self, url: str) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return {"data": await resp.text()}
```

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
```

## API reference

### Classes

| Class | Description |
| --- | --- |
| `Service` | Base class for REPL transport services |
| `MessagePackService` | Base class for persistent MessagePack services |

### Decorators

| Decorator | Description |
| --- | --- |
| `@method` | Expose a method as an RPC endpoint |
| `@on_event` | Subscribe a persistent service method to mesh events |

### Functions

| Function | Description |
| --- | --- |
| `run(ServiceClass)` | Start the service and auto-detect transport |
| `save_image(img, format="png")` | Save a PIL image and return a file path |
| `build_log_record(...)` | Build a structured logstore record |

### Types

| Type | Description |
| --- | --- |
| `FilePath` | Pass file path as string |
| `Audio` | Load audio as `(sample_rate, ndarray)` |
| `Image` | Load image as `PIL.Image` |

## License

MIT
