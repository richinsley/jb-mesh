import asyncio
import json
import subprocess
import threading
import time

import nats as nats_client
import pytest

from jb_service import (
    LogstoreClient,
    Service,
    build_log_record,
    clear_correlation_id,
    ensure_correlation_id,
    get_correlation_id,
    set_correlation_id,
)
from jb_service.logging import LogstoreError


def nats_server_available():
    try:
        return subprocess.run(["which", "nats-server"], capture_output=True).returncode == 0
    except Exception:
        return False


@pytest.fixture
def nats_server(tmp_path, monkeypatch):
    if not nats_server_available():
        pytest.skip("nats-server not available")

    port = 14224
    proc = subprocess.Popen(
        ["nats-server", "-p", str(port), "-sd", str(tmp_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)
    url = f"nats://localhost:{port}"
    monkeypatch.setenv("JB_NATS_URL", url)
    yield url
    proc.terminate()
    proc.wait(timeout=5)


class DemoService(Service):
    pass


def test_build_log_record_shape_and_subject_inference(monkeypatch):
    monkeypatch.setenv("JB_NODE_NAME", "mesh-node")
    clear_correlation_id()

    record = build_log_record(
        subject="logs.call.mesh-node.demo-service.health",
        level="warning",
        message="health checked",
        data={"payload": {"token": "not-redacted-by-producer"}},
        duration_ms=12.5,
        ok=True,
    )

    assert record["schema"] == "jb.mesh.log.v1"
    assert record["level"] == "warn"
    assert record["kind"] == "tool_call"
    assert record["node"] == "mesh-node"
    assert record["tool"] == "demo-service"
    assert record["method"] == "health"
    assert record["subject"] == "logs.call.mesh-node.demo-service.health"
    assert record["message"] == "health checked"
    assert record["duration_ms"] == 12.5
    assert record["ok"] is True
    assert record["corr"]
    assert record["redacted"] is True
    assert record["data"]["payload"]["token"] == "not-redacted-by-producer"


def test_correlation_access_and_generation():
    clear_correlation_id()
    assert get_correlation_id() is None

    generated = ensure_correlation_id()
    assert generated
    assert get_correlation_id() == generated

    previous = set_correlation_id("corr-explicit")
    assert previous == generated
    assert get_correlation_id() == "corr-explicit"

    clear_correlation_id()
    assert get_correlation_id() is None


def test_service_helpers_share_correlation_state():
    svc = DemoService()
    svc.set_correlation_id("corr-service")
    assert svc.get_correlation_id() == "corr-service"
    assert svc.ensure_correlation_id() == "corr-service"


async def _collect_one(url: str, subject: str, sink: list[dict]):
    nc = await nats_client.connect(url)
    sub = await nc.subscribe(subject)
    await nc.flush()
    msg = await sub.next_msg(timeout=5.0)
    sink.append(json.loads(msg.data.decode("utf-8")))
    await nc.close()


def test_service_logger_publishes_structured_record(nats_server):
    svc = DemoService()
    received = []
    ready = threading.Event()

    def subscriber():
        async def run():
            nc = await nats_client.connect(nats_server)
            sub = await nc.subscribe("logs.call.>")
            await nc.flush()
            ready.set()
            msg = await sub.next_msg(timeout=5.0)
            received.append(json.loads(msg.data.decode("utf-8")))
            await nc.close()

        asyncio.run(run())

    thread = threading.Thread(target=subscriber)
    thread.start()
    ready.wait(timeout=3)
    time.sleep(0.1)

    svc.set_correlation_id("corr-publish")
    record = svc.log.call_log(
        "info",
        "status complete",
        method="status",
        data={"summary": "ok"},
        duration_ms=4.2,
        ok=True,
    )

    thread.join(timeout=10)
    assert record["corr"] == "corr-publish"
    assert received and received[0]["corr"] == "corr-publish"
    assert received[0]["subject"] == f"logs.call.{svc.log.node}.{svc.name}.status"
    assert received[0]["data"] == {"summary": "ok"}


def test_logstore_client_envelope_methods(nats_server):
    responses = {
        "logstore.health": {"ok": True, "role": "server", "storage_dir": "/tmp/logstore", "subjects": ["logs.>"], "records_written": 3, "bytes_written": 90, "backend": {"kind": "jsonl", "ok": True}},
        "logstore.tail": {"ok": True, "records": [{"message": "tail"}], "truncated": False, "next_cursor": "", "limits": {"limit": 10, "max_query_limit": 1000, "max_query_window": "168h"}},
        "logstore.query": {"ok": True, "records": [{"corr": "c1"}], "truncated": False, "next_cursor": "cursor-1", "limits": {"limit": 5, "max_query_limit": 1000, "max_query_window": "168h"}},
        "logstore.stats": {"ok": True, "since": "24h", "groups": [{"node": "a", "kind": "tool_call", "records": 1, "errors": 0}], "limits": {"max_query_limit": 1000, "max_query_window": "168h"}},
    }
    seen = {}
    stop = threading.Event()

    def responder():
        async def run():
            nc = await nats_client.connect(nats_server)
            for subject, payload in responses.items():
                async def handler(msg, payload=payload, subject=subject):
                    seen[subject] = json.loads(msg.data.decode("utf-8") or "{}")
                    await msg.respond(json.dumps(payload).encode("utf-8"))

                await nc.subscribe(subject, cb=handler)
            await nc.flush()
            while not stop.is_set():
                await asyncio.sleep(0.05)
            await nc.close()

        asyncio.run(run())

    thread = threading.Thread(target=responder)
    thread.start()
    time.sleep(0.2)

    client = LogstoreClient(nats_url=nats_server)
    assert client.health()["storage_dir"] == "/tmp/logstore"
    assert client.tail(node="node-a", kind="tool_call", limit=10)["records"][0]["message"] == "tail"
    assert client.query(corr="c1", limit=5)["next_cursor"] == "cursor-1"
    assert client.stats(since="24h", group_by=["node", "kind"])["groups"][0]["node"] == "a"

    stop.set()
    thread.join(timeout=10)

    assert seen["logstore.health"] == {}
    assert seen["logstore.tail"] == {"node": "node-a", "kind": "tool_call", "limit": 10}
    assert seen["logstore.query"] == {"corr": "c1", "limit": 5}
    assert seen["logstore.stats"] == {"since": "24h", "group_by": ["node", "kind"]}


def test_logstore_client_raises_on_connection_error():
    client = LogstoreClient(nats_url="nats://127.0.0.1:1", timeout=1)
    with pytest.raises(LogstoreError):
        client.health()
