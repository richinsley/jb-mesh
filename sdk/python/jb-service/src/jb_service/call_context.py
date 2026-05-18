"""
Per-call context object for jb-service methods that need cancellation or
streaming awareness.

Methods opt in by declaring a keyword-only ``ctx`` parameter:

    @method
    def long_thing(self, x: int, *, ctx: CallContext = None) -> dict:
        for i in range(x):
            ctx.check_cancelled()       # raises CallCancelled if caller went away
            do_work(i)
        return {"ok": True}

The dispatcher (msgpack_protocol._create_method_wrapper) detects ``ctx`` via
inspect.signature, constructs a CallContext bound to the current request_id,
and passes it to the method. Methods without ``ctx`` run unchanged.

Cancellation is **cooperative**: a method that never polls ``ctx.cancelled`` or
calls ``ctx.check_cancelled()`` will run to completion. The underlying signal is
the asyncio.Event held by jumpboot's MessagePackQueueServer (set by the Go
caller via SendCancel → __cancel__).

``ctx.emit(chunk)`` is reserved for Phase 2 (streaming) and is currently a no-op.

Part of the streaming+cancellation design — see jb-mesh/DESIGN-STREAMING-CANCEL.md.
"""
from __future__ import annotations
from typing import Any, Callable, Optional


class CallCancelled(Exception):
    """Raised by ``CallContext.check_cancelled()`` when the caller cancelled.

    Regular Exception subclass (not BaseException) so generic ``except Exception``
    blocks catch it. User code that wants to clean up on cancel should catch
    CallCancelled (or Exception) and re-raise it to propagate cancellation.
    """


class CallContext:
    """Per-call cancellation/streaming context.

    The instance is bound to a single in-flight call and is invalid after the
    method returns. Don't store it on the service; it's intended to be used
    only during the method body.
    """

    def __init__(
        self,
        request_id: str,
        cancel_event: Any = None,
        emit_fn: Optional[Callable[[Any], None]] = None,
    ) -> None:
        self._request_id = request_id
        # asyncio.Event — None if no cancellation was wired (e.g. older
        # MessagePackQueueServer, or this method invoked outside the standard
        # dispatcher path).
        self._cancel_event = cancel_event
        # Callable(chunk) -> None. None when this method was NOT invoked as a
        # streaming call — emit() is then a silent no-op so the same method
        # body can serve both invocation modes.
        self._emit_fn = emit_fn

    @property
    def request_id(self) -> str:
        """The opaque request_id assigned by the calling layer. Useful for
        correlating log messages with jb-mesh logstore entries.
        """
        return self._request_id

    @property
    def cancelled(self) -> bool:
        """True if the caller has signalled cancellation. Polling this is
        the non-raising idiom; ``check_cancelled()`` is the raising one.
        """
        return self._cancel_event is not None and self._cancel_event.is_set()

    def check_cancelled(self) -> None:
        """Raise ``CallCancelled`` if the caller cancelled. Cooperative
        checkpoint: call this at points where stopping is safe.
        """
        if self.cancelled:
            raise CallCancelled(f"call {self._request_id!r} cancelled by caller")

    @property
    def streaming(self) -> bool:
        """True if this call was invoked as a stream (caller will receive
        partials via ``emit()``). Methods can branch on this if they want to
        skip producing partials when nobody will read them.
        """
        return self._emit_fn is not None

    def emit(self, chunk: Any) -> None:
        """Emit a partial result frame to the caller.

        For streaming invocations, ``chunk`` is published as a partial frame
        on the call's reply channel. For non-streaming invocations, this is a
        silent no-op — the same method body can serve both modes; only the
        manifest's ``stream: true`` declaration + the caller's choice of API
        (Mesh.Stream vs Mesh.Call) determine whether partials are delivered.
        """
        if self._emit_fn is not None:
            self._emit_fn(chunk)
