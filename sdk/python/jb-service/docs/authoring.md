# jb-service authoring guide

`jb-service` is the Python SDK for building services that `jb-mesh` can install, run, discover, and call.

A service author normally writes two files:

```text
my-service/
├── jumpboot.yaml   # runtime + method contract
└── main.py         # Python implementation
```

## The Python side

Expose methods by subclassing `Service` and decorating methods with `@method`.

```python
from jb_service import Service, method, run


class Hello(Service):
    name = "hello"
    version = "0.1.0"

    @method
    def hello(self, name: str = "world") -> dict:
        return {"message": f"Hello, {name}!"}

    @method
    def health(self) -> dict:
        return {"status": "ok"}


if __name__ == "__main__":
    run(Hello)
```

Rules of thumb:

- keep RPC methods small and explicit
- return JSON-shaped values: dicts, lists, strings, numbers, booleans, or file references
- declare a meaningful `health` method for anything beyond a toy example
- avoid hidden global side effects at import time; do heavy startup in `setup()` / `setup_async()`

## The manifest side

Declare the same method surface in `jumpboot.yaml`.

```yaml
name: hello
version: 0.1.0
description: Minimal hello service

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
      description: Health check

health:
  method: health

x-deploy:
  smoke:
    method: health
```

`jb-mesh preflight` checks this contract before install/release:

- manifest parses
- entrypoint exists
- Python methods can be inspected
- manifest `rpc.methods` match Python `@method` definitions
- health/setup hooks point to real methods
- runtime dependencies install into a temporary clean venv
- the entrypoint imports successfully

## Lifecycle hooks

Use lifecycle hooks for startup and cleanup:

```python
class MyService(Service):
    def setup(self):
        self.client = connect_to_dependency()

    def teardown(self):
        self.client.close()
```

Async versions are also supported:

```python
async def setup_async(self): ...
async def teardown_async(self): ...
```

## Service vs MessagePackService

Use `Service` for simple tools that do not write unrelated output to stdout.

Use `MessagePackService` when the service is persistent, long-running, uses progress bars, wraps noisy subprocesses, or needs stdout isolation.

Manifest:

```yaml
runtime:
  mode: persistent
  transport: msgpack
```

Python:

```python
from jb_service import MessagePackService, method, run


class Worker(MessagePackService):
    @method
    def health(self) -> dict:
        return {"status": "ok"}


if __name__ == "__main__":
    run(Worker)
```

## Events

Services can subscribe to mesh events:

```python
from jb_service import Service, method, on_event


class Watcher(Service):
    @on_event("events.tool.*")
    def on_tool_event(self, event: dict):
        self.log.info(f"tool event: {event['type']}")

    @method
    def health(self) -> dict:
        return {"status": "ok"}
```

Or subscribe imperatively in setup:

```python
def setup(self):
    self.subscribe_event("events.node.*", self.on_node_event)
```

## Logging

Use normal logging for human-readable progress and `call_log` / `tool_log` for structured operational evidence.

```python
@method
def process(self, value: str) -> dict:
    corr = self.ensure_correlation_id()
    self.log.call_log(
        "info",
        "process completed",
        method="process",
        corr=corr,
        ok=True,
        data={"input_size": len(value)},
    )
    return {"ok": True}
```

Keep structured log `data` bounded. Do not log secrets, full prompts, file contents, or unbounded payloads.
