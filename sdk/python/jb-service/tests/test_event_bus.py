"""Tests for EventBus — event subscription bridge for Python tools.

Proves that:
1. EventBus can subscribe and receive events over NATS.
2. Lifecycle events (events.tool.*, events.node.*) arrive at Python subscribers.
3. User events (events.user.*) arrive at Python subscribers.
4. Service.subscribe_event() works end-to-end.
5. @on_event decorator auto-subscribes on service start.
"""
import asyncio
import json
import subprocess
import threading
import time

import nats as nats_client
import pytest

from jb_service.event_bus import EventBus
from jb_service.events import emit_event
from jb_service.service import Service
from jb_service.method import method, on_event


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def nats_server_available():
    try:
        return subprocess.run(["which", "nats-server"], capture_output=True).returncode == 0
    except Exception:
        return False


@pytest.fixture
def nats_server(tmp_path):
    """Start a temporary NATS server on port 14223."""
    if not nats_server_available():
        pytest.skip("nats-server not available")

    port = 14223
    proc = subprocess.Popen(
        ["nats-server", "-p", str(port), "-sd", str(tmp_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)
    yield f"nats://localhost:{port}"
    proc.terminate()
    proc.wait(timeout=5)


def publish_event(nats_url: str, event_type: str, data: dict, node: str = "test-node"):
    """Publish a raw event to NATS (simulates the Go mesh node)."""
    async def _pub():
        nc = await nats_client.connect(nats_url)
        subject = f"events.{event_type}"
        payload = json.dumps({
            "type": event_type,
            "node": node,
            "timestamp": "2026-04-17T00:00:00Z",
            "data": data,
        }).encode()
        await nc.publish(subject, payload)
        await nc.flush()
        await nc.close()

    asyncio.run(_pub())


# ---------------------------------------------------------------------------
# EventBus unit tests
# ---------------------------------------------------------------------------

class TestEventBus:
    def test_subscribe_receives_events(self, nats_server):
        """EventBus receives events published to NATS."""
        received = []
        bus = EventBus(nats_url=nats_server)
        bus.start()
        try:
            bus.subscribe("events.>", lambda e: received.append(e))
            time.sleep(0.2)

            publish_event(nats_server, "tool.started", {"tool": "whisper"})
            time.sleep(0.5)

            assert len(received) == 1
            assert received[0]["type"] == "tool.started"
            assert received[0]["data"]["tool"] == "whisper"
        finally:
            bus.stop()

    def test_lifecycle_event(self, nats_server):
        """Lifecycle events (tool.installed, node.joined) are received."""
        received = []
        bus = EventBus(nats_url=nats_server)
        bus.start()
        try:
            bus.subscribe("events.tool.*", lambda e: received.append(e))
            bus.subscribe("events.node.*", lambda e: received.append(e))
            time.sleep(0.2)

            publish_event(nats_server, "tool.installed", {"tool": "whisper", "version": "1.0.0"})
            publish_event(nats_server, "node.joined", {"capabilities": {"tools": 3}})
            time.sleep(0.5)

            types = {e["type"] for e in received}
            assert "tool.installed" in types
            assert "node.joined" in types
        finally:
            bus.stop()

    def test_user_event(self, nats_server):
        """User events (events.user.*) are received."""
        received = []
        bus = EventBus(nats_url=nats_server)
        bus.start()
        try:
            bus.subscribe("events.user.>", lambda e: received.append(e))
            time.sleep(0.2)

            # Use the real emit_event() to publish a user event
            emit_event("training.complete", {"loss": 0.01}, node="leaf-1", nats_url=nats_server)
            time.sleep(0.5)

            assert len(received) == 1
            assert received[0]["type"] == "user.training.complete"
            assert received[0]["data"]["loss"] == 0.01
        finally:
            bus.stop()

    def test_unsubscribe(self, nats_server):
        """Unsubscribed callbacks stop receiving events."""
        received = []
        bus = EventBus(nats_url=nats_server)
        bus.start()
        try:
            sid = bus.subscribe("events.>", lambda e: received.append(e))
            time.sleep(0.2)

            publish_event(nats_server, "tool.started", {"tool": "a"})
            time.sleep(0.3)
            assert len(received) == 1

            bus.unsubscribe(sid)
            time.sleep(0.2)

            publish_event(nats_server, "tool.stopped", {"tool": "a"})
            time.sleep(0.3)
            assert len(received) == 1  # no new events
        finally:
            bus.stop()

    def test_multiple_patterns(self, nats_server):
        """Different patterns route to different callbacks."""
        tool_events = []
        user_events = []
        bus = EventBus(nats_url=nats_server)
        bus.start()
        try:
            bus.subscribe("events.tool.*", lambda e: tool_events.append(e))
            bus.subscribe("events.user.>", lambda e: user_events.append(e))
            time.sleep(0.2)

            publish_event(nats_server, "tool.crashed", {"tool": "x", "error": "oom"})
            emit_event("done", {"ok": True}, nats_url=nats_server)
            time.sleep(0.5)

            assert len(tool_events) == 1
            assert tool_events[0]["type"] == "tool.crashed"
            assert len(user_events) == 1
            assert user_events[0]["type"] == "user.done"
        finally:
            bus.stop()


# ---------------------------------------------------------------------------
# Service integration tests
# ---------------------------------------------------------------------------

class TestServiceEventBridge:
    def test_subscribe_event_in_setup(self, nats_server):
        """Service.subscribe_event() works when called from setup()."""
        received = []

        class MyTool(Service):
            def setup(self):
                self.subscribe_event("events.tool.*", lambda e: received.append(e))

        svc = MyTool()
        svc._event_bus = EventBus(nats_url=nats_server)
        svc.setup()
        svc._start_event_bus()
        time.sleep(0.3)

        try:
            publish_event(nats_server, "tool.started", {"tool": "my-tool"})
            time.sleep(0.5)

            assert len(received) == 1
            assert received[0]["type"] == "tool.started"
        finally:
            svc._stop_event_bus()

    def test_on_event_decorator(self, nats_server):
        """@on_event decorator auto-subscribes handlers on service start."""
        received_tool = []
        received_user = []

        class DecoratedTool(Service):
            @on_event("events.tool.*")
            def on_tool(self, event):
                received_tool.append(event)

            @on_event("events.user.>")
            def on_user(self, event):
                received_user.append(event)

            @method
            def ping(self) -> str:
                return "pong"

        svc = DecoratedTool()
        svc._event_bus = EventBus(nats_url=nats_server)
        svc._start_event_bus()
        time.sleep(0.3)

        try:
            publish_event(nats_server, "tool.installed", {"tool": "deco", "version": "1.0"})
            emit_event("hello", {"msg": "world"}, nats_url=nats_server)
            time.sleep(0.5)

            assert len(received_tool) == 1
            assert received_tool[0]["type"] == "tool.installed"
            assert len(received_user) == 1
            assert received_user[0]["type"] == "user.hello"
        finally:
            svc._stop_event_bus()

    def test_full_lifecycle_and_user_event(self, nats_server):
        """
        End-to-end: a Python tool receives one lifecycle event AND one
        user event through the event bridge.

        This is the proof path required by ticket #64.
        """
        all_events = []

        class LeafTool(Service):
            name = "leaf-demo"

            @on_event("events.>")
            def on_any(self, event):
                all_events.append(event)

            @method
            def status(self) -> dict:
                return {"events_received": len(all_events)}

        svc = LeafTool()
        svc._event_bus = EventBus(nats_url=nats_server)
        svc.setup()
        svc._start_event_bus()
        time.sleep(0.3)

        try:
            # Simulate Go mesh emitting a lifecycle event
            publish_event(nats_server, "node.joined", {"capabilities": {"tools": 5}}, node="seed-1")
            # Simulate another Python tool emitting a user event
            emit_event("training.done", {"model": "bert", "loss": 0.02}, node="leaf-2", nats_url=nats_server)
            time.sleep(0.5)

            assert len(all_events) == 2
            types = {e["type"] for e in all_events}
            assert "node.joined" in types, f"Missing lifecycle event, got: {types}"
            assert "user.training.done" in types, f"Missing user event, got: {types}"

            # Verify the tool can see its own state
            result = svc.status()
            assert result["events_received"] == 2
        finally:
            svc._stop_event_bus()
