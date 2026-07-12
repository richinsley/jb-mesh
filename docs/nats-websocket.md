# Embedded NATS WebSocket Listener

`jb-mesh` can expose the embedded NATS server over WebSocket. This is useful when a node needs to connect through an HTTP/WebSocket-aware reverse proxy while still using the normal NATS protocol, including JetStream Object Store.

The listener is disabled by default.

## Start a seed with WebSocket enabled

```bash
jb-mesh serve \
  --seed \
  --no-mdns \
  --websocket-host 127.0.0.1 \
  --websocket-port 8088
```

Keep the listener private unless it is protected by an authenticated TLS-terminating proxy. The embedded listener uses plain WebSocket (`ws://`) because production TLS is expected to terminate at the edge proxy.

## Connect with `jb-mesh` through a proxy path

`jb-mesh` exposes generic WebSocket client flags for any path-mounted NATS proxy:

```bash
jb-mesh serve \
  --name node-b \
  --nats wss://mesh.example.com \
  --nats-ws-proxy-path /mesh/nats \
  --nats-ws-bearer-token "$NATS_WS_TOKEN" \
  --no-mdns
```

If the proxy expects query auth instead of headers, use one or more `--nats-ws-query key=value` flags:

```bash
jb-mesh list \
  --nats ws://127.0.0.1:8787 \
  --nats-ws-proxy-path /mesh/nats \
  --nats-ws-query token=demo-token
```

The same flags apply to mesh client commands such as `list`, `call`, `status`, `files ...`, and `events ...`, so JetStream-backed file store operations use the same WebSocket transport settings.

## Connect with the Go NATS client through a proxy path

When connecting through a reverse proxy that mounts NATS at a path such as `/mesh/nats`, use `nats.ProxyPath`. Do not put the path directly in the URL.

```go
package main

import (
    "net/http"
    "time"

    "github.com/nats-io/nats.go"
)

func connect() (*nats.Conn, error) {
    return nats.Connect(
        "wss://example.com",
        nats.ProxyPath("/mesh/nats"),
        nats.WebSocketConnectionHeaders(http.Header{
            "Authorization": []string{"Bearer <token>"},
        }),
        nats.Timeout(5*time.Second),
    )
}
```

For local development without TLS, replace `wss://example.com` with the proxy origin, for example `ws://127.0.0.1:8787`.

## Smoke test requirement

A valid proxy path should preserve JetStream Object Store semantics, not just basic request/reply subjects. The regression test in `internal/node/websocket_test.go` starts an embedded NATS server with WebSocket enabled and verifies Object Store put/get over WebSocket.

A full edge-proxy smoke should verify:

1. connect to the proxy origin with `nats.ProxyPath("/mesh/nats")`
2. create a temporary Object Store bucket
3. put an object
4. read the object back
5. delete/expire the temporary bucket if the smoke creates persistent state

## Security notes

- Keep WebSocket disabled by default.
- Prefer binding embedded listeners to `127.0.0.1` or a private interface
  (`jb-mesh serve --embed-host 127.0.0.1 --leaf-host 127.0.0.1` for a
  local-only seed).
- Do not expose raw NATS client, leaf, or WebSocket ports publicly.
- Use a reverse proxy for TLS, authentication, rate limiting, and logging.
- Prefer `Authorization: Bearer ...` for WebSocket auth when the client can set headers; fall back to query parameters only when necessary.
- Treat public ingress policy as deployment-specific. `jb-mesh` should only provide the generic NATS WebSocket capability.
