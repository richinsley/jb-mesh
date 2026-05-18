# my-service

Canonical baseline jb-mesh service starter.

## Workflow

```bash
jb-mesh preflight .
jb-mesh service-release my-service
```

## Notes

- Keep machine-local release mapping out of this repo.
- Put portable smoke metadata in `x-deploy`.
- Make `health` real as the service grows.
