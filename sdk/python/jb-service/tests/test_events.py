"""Tests for NATS-based event emission."""
import json
import subprocess
import time
import threading
import pytest

from jb_service.events import emit_event, EventError


def nats_server_available():
    try:
        result = subprocess.run(["which", "nats-server"], capture_output=True)
        return result.returncode == 0
    except Exception:
        return False


@pytest.fixture
def nats_server(tmp_path):
    """Start a temporary NATS server."""
    if not nats_server_available():
        pytest.skip("nats-server not available")

    port = 14222
    proc = subprocess.Popen(
        ["nats-server", "-p", str(port), "-js", "-sd", str(tmp_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)
    yield f"nats://localhost:{port}"
    proc.terminate()
    proc.wait(timeout=5)


def test_emit_event(nats_server):
    """Test that emit_event publishes to NATS without error."""
    # We verify the call succeeds — the message is published to NATS.
    # Full integration (subscribe + receive) is tested in the Go event tests.
    emit_event("test.complete", {"result": "ok"}, node="test-node", nats_url=nats_server)


def test_emit_event_no_data(nats_server):
    """Test event emission with empty data."""
    emit_event("ping", nats_url=nats_server)


def test_emit_event_connection_error():
    """Test that connection failure raises EventError.
    
    nats-py's internal reconnect can hang, so we rely on the hard
    timeout in _run_async to break out.
    """
    from jb_service.events import _run_async, _emit_async

    with pytest.raises((EventError, TimeoutError)):
        _run_async(
            _emit_async("test", {}, "node", "nats://127.0.0.1:1"),
            timeout=5,
        )


def test_emit_multiple_events(nats_server):
    """Test emitting multiple events in sequence."""
    for i in range(5):
        emit_event("step", {"i": i}, node="node-a", nats_url=nats_server)


def test_emit_event_with_subscriber(nats_server):
    """Test that emitted events are received by a NATS subscriber."""
    import asyncio
    import nats as nats_client

    received = []

    async def subscribe_and_wait():
        nc = await nats_client.connect(nats_server)
        sub = await nc.subscribe("events.user.>")
        await nc.flush()

        # Signal that subscription is ready
        ready_event.set()

        try:
            msg = await sub.next_msg(timeout=5.0)
            received.append(json.loads(msg.data))
        except Exception:
            pass
        finally:
            await nc.close()

    ready_event = threading.Event()

    # Run subscriber in a thread
    def run_sub():
        asyncio.run(subscribe_and_wait())

    t = threading.Thread(target=run_sub)
    t.start()

    # Wait for subscription to be ready
    ready_event.wait(timeout=3.0)
    time.sleep(0.1)  # Extra buffer for NATS propagation

    # Emit from main thread
    emit_event("verified", {"check": True}, node="test-node", nats_url=nats_server)

    t.join(timeout=10.0)

    assert len(received) == 1
    assert received[0]["type"] == "user.verified"
    assert received[0]["data"]["check"] is True
    assert received[0]["node"] == "test-node"
