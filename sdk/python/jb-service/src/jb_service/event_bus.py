"""
Event subscription bus for jb-mesh Python tools.

Maintains a long-lived NATS connection in a background thread so
persistent tools can receive mesh lifecycle events and user events
in real time.

Usage (standalone):
    from jb_service.event_bus import EventBus

    bus = EventBus()
    bus.start()
    bus.subscribe("events.>", lambda e: print(e))
    # ... later ...
    bus.stop()

Usage (via Service base class):
    class MyTool(Service):
        def setup(self):
            self.subscribe_event("events.tool.*", self._on_tool_event)
            self.subscribe_event("events.user.>", self._on_user_event)

        def _on_tool_event(self, event):
            self.log.info(f"tool event: {event['type']}")

        def _on_user_event(self, event):
            self.log.info(f"user event: {event['type']}")

Environment:
    JB_NATS_URL — NATS server URL (default: nats://localhost:4222)
"""
import asyncio
import json
import os
import threading
import time
from typing import Callable

import nats


# Event callback: receives a parsed event dict with type, node, timestamp, data.
EventCallback = Callable[[dict], None]


class Subscription:
    """Handle for an active event subscription."""

    __slots__ = ("id", "pattern", "_nats_sub")

    def __init__(self, sub_id: str, pattern: str, nats_sub):
        self.id = sub_id
        self.pattern = pattern
        self._nats_sub = nats_sub


class EventBus:
    """
    NATS-backed event bus that runs in a daemon thread.

    Handles connection, reconnection, and dispatching events to registered
    callbacks.  All callbacks are invoked on the bus thread — keep them fast
    or dispatch to your own worker.
    """

    def __init__(self, nats_url: str | None = None):
        self._nats_url = nats_url or os.environ.get(
            "JB_NATS_URL", "nats://localhost:4222"
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._nc: nats.NATS | None = None
        self._subs: dict[str, Subscription] = {}
        self._callbacks: dict[str, EventCallback] = {}
        self._counter = 0
        self._lock = threading.Lock()
        self._started = threading.Event()
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, timeout: float = 5.0) -> None:
        """Start the bus (connects to NATS in a background thread)."""
        if self._thread is not None and self._thread.is_alive():
            return

        self._started.clear()
        self._thread = threading.Thread(
            target=self._run, name="jb-event-bus", daemon=True
        )
        self._thread.start()
        if not self._started.wait(timeout):
            raise RuntimeError("EventBus failed to connect within timeout")

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the bus and close the NATS connection."""
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._thread is not None:
            self._thread.join(timeout)
        self._thread = None

    @property
    def connected(self) -> bool:
        return self._nc is not None and self._nc.is_connected

    # ------------------------------------------------------------------
    # Subscribe / unsubscribe (thread-safe)
    # ------------------------------------------------------------------

    def subscribe(self, pattern: str, callback: EventCallback) -> str:
        """
        Subscribe to events matching *pattern*.

        Pattern examples:
            "events.>"              — all mesh events
            "events.tool.*"         — all tool lifecycle events
            "events.node.*"         — all node lifecycle events
            "events.user.>"         — all user-defined events
            "events.tool.crashed"   — specific event type

        Returns a subscription ID that can be passed to unsubscribe().
        """
        with self._lock:
            self._counter += 1
            sub_id = f"sub-{self._counter}"

        self._callbacks[sub_id] = callback

        # If already running, create the NATS subscription on the bus loop.
        if self._loop is not None and not self._loop.is_closed():
            future = asyncio.run_coroutine_threadsafe(
                self._add_nats_sub(sub_id, pattern), self._loop
            )
            future.result(timeout=5.0)
        else:
            # Bus not started yet — store pending; will be subscribed on start.
            self._subs[sub_id] = Subscription(sub_id, pattern, None)

        return sub_id

    def unsubscribe(self, sub_id: str) -> None:
        """Remove a subscription by ID."""
        self._callbacks.pop(sub_id, None)
        sub = self._subs.pop(sub_id, None)
        if sub and sub._nats_sub and self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                sub._nats_sub.unsubscribe(), self._loop
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Background thread entry point."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._stop_event = asyncio.Event()
        try:
            self._loop.run_until_complete(self._main())
        finally:
            self._loop.close()
            self._loop = None

    async def _main(self) -> None:
        try:
            self._nc = await asyncio.wait_for(
                nats.connect(
                    self._nats_url,
                    connect_timeout=2,
                    reconnect_time_wait=1,
                    max_reconnect_attempts=-1,  # unlimited reconnect
                ),
                timeout=5,
            )
        except Exception:
            self._started.set()  # unblock caller even on failure
            return

        # Activate any subscriptions registered before start().
        pending = list(self._subs.items())
        for sub_id, sub in pending:
            if sub._nats_sub is None:
                await self._add_nats_sub(sub_id, sub.pattern)

        self._started.set()

        # Wait until stop is requested.
        await self._stop_event.wait()

        # Cleanup
        for sub in list(self._subs.values()):
            if sub._nats_sub:
                try:
                    await sub._nats_sub.unsubscribe()
                except Exception:
                    pass
        self._subs.clear()
        self._callbacks.clear()

        if self._nc and not self._nc.is_closed:
            await self._nc.close()
        self._nc = None

    async def _add_nats_sub(self, sub_id: str, pattern: str) -> None:
        """Create a NATS subscription and wire it to the callback."""
        if self._nc is None or self._nc.is_closed:
            return

        def _make_handler(sid: str):
            async def handler(msg):
                cb = self._callbacks.get(sid)
                if cb is None:
                    return
                try:
                    event = json.loads(msg.data)
                except Exception:
                    return
                try:
                    cb(event)
                except Exception:
                    pass  # don't crash the bus on callback errors
            return handler

        nats_sub = await self._nc.subscribe(pattern, cb=_make_handler(sub_id))
        self._subs[sub_id] = Subscription(sub_id, pattern, nats_sub)
