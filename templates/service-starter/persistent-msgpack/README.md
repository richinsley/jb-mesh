# my-service

Canonical persistent/msgpack jb-mesh service starter.

Use this variant when the service is long-lived or stdout-noisy enough that REPL transport is the wrong fit.

## Workflow

```bash
jb-mesh preflight .
jb-mesh service-release my-service
```
