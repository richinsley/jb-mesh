# Reply and cancel settings

Three settings decide who may read the answer to a call, whose messages a caller accepts as that answer, and who may cancel a call. They matter on a mesh where more than one principal logs in to the same seed. On a single-user mesh the defaults are fine and nothing here needs setting.

All three are off by default. They are fields of `mesh.Config` and `node.Config` for a Go program that embeds the packages. The `jb-mesh` command has no flags for them.

| Setting | Where | What it does |
| --- | --- | --- |
| `InboxPrefix` | `mesh.Config`, `node.Config` | Puts this connection's reply subjects under a prefix of its own. |
| `RequireReplyToken` | `mesh.Config`, `node.Config` | Accepts a message as an answer only if it carries the request's token. |
| `RequireCancelProof` | `node.Config` | Makes a call that arrived without a token impossible to cancel remotely. |

## The problem they address

A NATS request names the subject its answer should be sent to. By default every connection's reply subjects are under `_INBOX.>`, so a principal allowed to subscribe there can read other principals' answers. The publisher of a request also chooses its reply subject freely, so one principal can aim a responder's answer at another principal's reply subject. And a call is cancelled by a message on `cancel.<call id>`, which anyone allowed to publish there can send once they know the id.

## InboxPrefix: who can read a reply

With `InboxPrefix` set, every reply subject the connection creates is under that prefix: plain and context-aware requests, stream reply inboxes, service discovery and JetStream API replies. A node passes its prefix to its own mesh connection, including the connection it opens after a demotion.

The setting does nothing alone. It lets the server's permissions do the work: grant each principal subscription to `<its prefix>.>` and not to `_INBOX.>`, and no principal can subscribe to another's replies.

The prefix must be dot-separated literal tokens, with no wildcard and no whitespace. `mesh.ValidateInboxPrefix` checks one; use it before writing a matching server permission from a principal's name.

## RequireReplyToken: which messages count as the answer

Every request carries a fresh random token in the `Jb-Reply-Token` header, whether or not this is set. A node echoes the token on everything it publishes for that request: results, errors, stream frames, file responses and log-store responses.

With `RequireReplyToken` set, the connection accepts only messages that carry the request's token. Anything else on the reply subject is dropped and counted (`Mesh.ReplyRejections`), and the request goes on waiting.

Things to know before turning it on:

- **Nothing without the token ends a request.** That includes the server's "no responders" status, which a publisher can imitate. A call to a subject nobody serves ends at its deadline instead of failing at once. A stream call to a method that is not a streaming method ends when its context does.
- **Service discovery is not covered.** `ListServices` answers come from the NATS micro library and cannot carry a token.
- **Every node the connection calls must echo the token.** A node older than v0.1.5 does not, and all its answers would be rejected.
- **A direct request needs the verified form.** A caller that talks to a responder without `Call` uses `Mesh.RequestVerified` or `Mesh.RequestVerifiedTimeout` to get the same rule.

A responder written outside the node echoes the token with the `replytoken` package: `replytoken.Respond` for one reply, `replytoken.Publish` for each of several.

## Cancel proof: who can stop a call

`CallWithContext` and `Stream` cancel a call by publishing to `cancel.<call id>` when their context ends. Since v0.1.6 that message carries a proof in the `Jb-Cancel-Proof` header: an HMAC-SHA256, keyed with the call's reply token, over a fixed label and the call id. The token itself is never put on the cancel subject.

A node treats cancels this way:

| The call arrived | `RequireCancelProof` off | `RequireCancelProof` on |
| --- | --- | --- |
| with a reply token | cancelled only by a cancel carrying its proof | the same |
| without a reply token | cancelled by any message on its cancel subject | cannot be cancelled remotely |

A cancel that fails the check is ignored and counted (`Node.IgnoredCancels`). The call carries on.

A program that publishes cancels itself, instead of through `CallWithContext` or `Stream`, builds the message with `replytoken.CancelMessage`. A bare publish to `mesh.CancelSubject` does not cancel a call that carried a token.

## What these prove

A reply token and a cancel proof show that the sender holds that request's token. The requester holds it, and so does whoever the server delivered the request to. Neither shows which process produced a message. Use the server's permissions to decide who may send a request to a responder at all.

## Upgrade order

Each setting has an order that keeps a mixed mesh working.

1. **Inbox prefix:** give every client its prefix first, then narrow the seed's reply-subscription permissions. A client still on `_INBOX` stops receiving replies once the permission is narrowed.
2. **Reply tokens:** upgrade the nodes that answer to v0.1.5 or later first, then set `RequireReplyToken` on callers.
3. **Cancel proof:** upgrade clients to v0.1.6 or later before nodes. A v0.1.5 client sends a token and no proof, so a v0.1.6 node will not let it cancel. A newer client calling an older node gets the older node's behaviour.

## Error replies

Separately from the settings above, and always on since v0.1.4: when a Python method raises, the caller is sent the exception's type and message and no traceback. A traceback names files on the node. It stays there: the tool's process writes it to its stderr, and the node logs it with the call's correlation id unless the node's logging is set to quiet. This covers ordinary replies, a stream that fails to start, and an error frame in the middle of a stream.
