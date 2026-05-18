"""
Event emission for jb-mesh tools — direct NATS publish.

Events are published directly to the NATS bus without HTTP proxy.
Subject: events.user.<topic>

Usage:
    from jb_service import emit_event

    emit_event("training.complete", {"model": "bert", "loss": 0.01})

Environment:
    JB_NATS_URL — NATS server URL (default: nats://localhost:4222)
                  Injected automatically by jb-mesh executor.
"""
import os
import json
import asyncio
import time
from typing import Optional

import nats


class EventError(Exception):
    """Error emitting an event."""
    pass


def _nats_url() -> str:
    return os.environ.get("JB_NATS_URL", "nats://localhost:4222")


def _run_async(coro, timeout=30):
    """Run an async coroutine synchronously with a hard timeout.
    
    If an event loop is already running (e.g. in async tests or notebooks),
    falls back to creating a thread with its own loop.
    """
    import concurrent.futures

    # Always use a thread so we can enforce a hard timeout
    # (asyncio.run in the main thread can't be interrupted)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"operation timed out after {timeout}s")


async def _emit_async(topic: str, data: dict, node: str, nats_url: str) -> None:
    nc = await asyncio.wait_for(
        nats.connect(
            nats_url,
            connect_timeout=1,
            max_reconnect_attempts=0,
            allow_reconnect=False,
        ),
        timeout=2,
    )
    try:
        subject = f"events.user.{topic}"
        payload = json.dumps({
            "type": f"user.{topic}",
            "node": node,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "data": data,
        }).encode("utf-8")

        await nc.publish(subject, payload)
        await nc.flush()
    finally:
        await nc.close()


def emit_event(topic: str, data: dict = None, node: str = None,
               nats_url: str = None) -> None:
    """
    Emit a user event directly to the NATS bus.

    Published to events.user.<topic>. Any subscriber (other tools,
    OpenClaw plugin, monitoring) receives it.

    Args:
        topic: Event topic (e.g. "training.complete")
        data: Event data payload (JSON-serializable dict)
        node: Node name (default: hostname)
        nats_url: Override NATS URL (default: JB_NATS_URL env)

    Raises:
        EventError: If emission fails
    """
    url = nats_url or _nats_url()
    if node is None:
        import socket
        node = socket.gethostname()

    try:
        _run_async(_emit_async(topic, data or {}, node, url))
    except Exception as e:
        raise EventError(f"emit {topic}: {e}") from e
