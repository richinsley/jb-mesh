# Service Lifecycle And Hard-Kill Contract

This document defines what `jb-mesh` currently guarantees when services stop,
update, uninstall, or crash.

## Current Contract

When `jb-mesh` is allowed to run cleanup, it attempts to stop managed services
before removing their mesh registrations:

- `SIGINT` / `SIGTERM` to `jb-mesh serve` close the node and stop running tools.
- `update` and `uninstall` stop the selected tool before replacing or removing
  it.
- persistent MessagePack services use jumpboot process-group cleanup so child
  processes are terminated with the service process.
- stalled graceful shutdown has a force-terminate fallback for persistent
  services.

These paths cover the normal operator lifecycle: stop the node, update a tool,
uninstall a tool, or recover from a service that does not exit promptly.

## What Is Not Guaranteed

`jb-mesh` cannot run Go cleanup after the `jb-mesh` process itself has been
unconditionally killed or the host has crashed.

Specifically, `kill -9 <jb-mesh-pid>`, power loss, kernel panic, or an external
supervisor killing only the parent process may leave service descendants behind.
That is an operating-system boundary: the killed process gets no chance to call
`Executor.Close`, unregister services, stop health checks, or ask jumpboot to
terminate process groups.

The current public contract is therefore:

- graceful stop: managed cleanup is expected
- `SIGTERM` / `SIGINT`: managed cleanup is expected
- tool update / uninstall: managed cleanup is expected
- hung service shutdown: best-effort force termination is expected
- raw `SIGKILL` / crash: no in-process cleanup guarantee

Public docs and release notes should not claim that `jb-mesh` can clean up after
`kill -9` from inside the killed process.

## Operator Guidance

For long-lived or subprocess-heavy services:

- prefer `runtime.mode: persistent` with `runtime.transport: msgpack`
- make `health` detect stale child-process state when that matters
- run nodes under a supervisor that owns the process group when hard-kill cleanup
  is operationally required
- after host or supervisor crashes, audit service processes before restarting
  sensitive tools

Future hard-kill hardening may add supervisor-owned process groups,
parent-death-signal behavior where available, pidfile/orphan audits, or external
cleanup helpers. Those mechanisms belong outside the killed `jb-mesh` process
or in OS-specific wrappers.
