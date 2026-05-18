# Public release boundary

jb-mesh is designed to be useful as a public core project while still supporting private deployments and experiments.

This document defines what belongs in the public repository and what should stay in private deployment/integration repositories.

## Public core

Public jb-mesh material should be generic, reusable, and safe to show to contributors:

- core Go packages and CLI commands
- canonical service starter templates
- minimal example services
- public architecture and authoring docs
- tests that do not depend on private hosts
- portable manifest metadata such as `x-deploy.smoke`

Public docs should use neutral names such as `node-a`, `node-b`, `/path/to/repo`, and `https://github.com/<owner>/<repo>.git`.

## Private or deployment-specific

Keep these out of public releases unless they are rewritten and generalized:

- private IP addresses, hostnames, usernames, or SSH targets
- local paths such as home directories or lab checkout locations
- credentials, tokens, bearer strings, and secret names that imply real infrastructure
- internal service names that are not part of the public project
- customer/workplace-specific references
- incident notes, task-worker briefs, audit trails, and planning scratchpads
- one-off deploy scripts for a particular machine
- persona/agent-specific context or memory

## Contributor posture

The public GitHub repository should be a real contributor surface, not a second-class mirror.

Recommended flow:

1. Public contributors open PRs against GitHub.
2. CI and maintainers review the PR normally.
3. Accepted public changes merge on GitHub.
4. Maintainers pull the public branch back into private lab/deployment checkouts.
5. Private experiments stay in private repos/branches until they are generalized.

## Release checklist

Before pushing a public release or public branch:

- Run tests.
- Run `jb-mesh preflight` for changed service examples/templates; this is the local service-contract gate before install/release.
- Search for private IPs and hostnames.
- Search for local user paths.
- Search for internal project/persona names.
- Verify example dependency URLs are public or clearly placeholders.
- Verify README and docs describe a generic user setup, not one deployment.
- Verify generated binaries, caches, worker notes, and local state are not tracked.

A future dedicated release skill should enforce this checklist before public publishing.
