"""Structured log producer and logstore client helpers for jb-service."""
from __future__ import annotations

import asyncio
import contextvars
import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from typing import Any

import nats

SCHEMA_V1 = "jb.mesh.log.v1"
DEFAULT_NATS_URL = "nats://localhost:4222"
_LOG_SUBJECT_KINDS = {
    "node": 2,
    "tool": 3,
    "call": 4,
    "audit": 2,
}

_corr_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "jb_service_correlation_id", default=None
)


class LogstoreError(Exception):
    """Error talking to the jb-mesh logstore service."""


@dataclass(frozen=True)
class CorrelationContext:
    """Context manager token for temporary correlation overrides."""

    token: contextvars.Token


class LogstoreClient:
    """Request/reply client for bounded logstore API methods."""

    def __init__(self, nats_url: str | None = None, timeout: float = 5.0):
        self.nats_url = nats_url or os.environ.get("JB_NATS_URL", DEFAULT_NATS_URL)
        self.timeout = timeout

    def health(self) -> dict[str, Any]:
        return self._request("logstore.health", {})

    def tail(
        self,
        *,
        node: str | None = None,
        kind: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        payload = _compact_dict({"node": node, "kind": kind, "limit": limit})
        return self._request("logstore.tail", payload)

    def query(
        self,
        *,
        since: str | None = None,
        until: str | None = None,
        node: str | None = None,
        tool: str | None = None,
        method: str | None = None,
        kind: str | None = None,
        level: str | None = None,
        corr: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        payload = _compact_dict(
            {
                "since": since,
                "until": until,
                "node": node,
                "tool": tool,
                "method": method,
                "kind": kind,
                "level": level,
                "corr": corr,
                "limit": limit,
            }
        )
        return self._request("logstore.query", payload)

    def stats(
        self,
        *,
        since: str | None = None,
        group_by: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        payload = _compact_dict({"since": since, "group_by": list(group_by) if group_by else None})
        return self._request("logstore.stats", payload)

    def _request(self, subject: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return _run_async(self._request_async(subject, payload), timeout=self.timeout)
        except Exception as exc:
            raise LogstoreError(f"{subject}: {exc}") from exc

    async def _request_async(self, subject: str, payload: dict[str, Any]) -> dict[str, Any]:
        nc = await asyncio.wait_for(
            nats.connect(
                self.nats_url,
                connect_timeout=1,
                max_reconnect_attempts=0,
                allow_reconnect=False,
            ),
            timeout=min(self.timeout, 2),
        )
        try:
            msg = await nc.request(
                subject,
                json.dumps(payload).encode("utf-8"),
                timeout=self.timeout,
            )
            return json.loads(msg.data.decode("utf-8"))
        finally:
            await nc.close()


class ServiceLogger:
    """Structured logger that can emit protocol logs and mesh logstore records."""

    def __init__(
        self,
        name: str,
        *,
        nats_url: str | None = None,
        node: str | None = None,
        transport_enabled: bool = False,
        stderr_writer=None,
    ):
        self.name = name
        self._enabled = transport_enabled
        self._nats_url = nats_url or os.environ.get("JB_NATS_URL", DEFAULT_NATS_URL)
        self._node = node or _default_node_name()
        self._stderr_writer = stderr_writer

    @property
    def node(self) -> str:
        return self._node

    def set_node(self, node: str | None) -> None:
        if node:
            self._node = node

    def get_correlation_id(self) -> str | None:
        return get_correlation_id()

    def set_correlation_id(self, corr: str | None) -> str | None:
        return set_correlation_id(corr)

    def clear_correlation_id(self) -> None:
        clear_correlation_id()

    def ensure_correlation_id(self) -> str:
        return ensure_correlation_id()

    def correlation_context(self, corr: str | None = None) -> CorrelationContext:
        return CorrelationContext(token=_corr_var.set(corr or new_correlation_id()))

    def reset_correlation_context(self, ctx: CorrelationContext) -> None:
        _corr_var.reset(ctx.token)

    def debug(self, message: str, extra: dict | None = None, **kwargs):
        self._emit("debug", message, extra=extra, **kwargs)

    def info(self, message: str, extra: dict | None = None, **kwargs):
        self._emit("info", message, extra=extra, **kwargs)

    def warning(self, message: str, extra: dict | None = None, **kwargs):
        self._emit("warn", message, extra=extra, **kwargs)

    def error(self, message: str, extra: dict | None = None, **kwargs):
        self._emit("error", message, extra=extra, **kwargs)

    def critical(self, message: str, extra: dict | None = None, **kwargs):
        self._emit("error", message, extra=extra, **kwargs)

    def emit(
        self,
        *,
        subject: str,
        level: str = "info",
        kind: str | None = None,
        message: str,
        data: dict[str, Any] | None = None,
        corr: str | None = None,
        duration_ms: float | None = None,
        ok: bool | None = None,
        tool: str | None = None,
        method: str | None = None,
        node: str | None = None,
    ) -> dict[str, Any]:
        record = build_log_record(
            subject=subject,
            level=level,
            kind=kind,
            message=message,
            data=data,
            corr=corr,
            duration_ms=duration_ms,
            ok=ok,
            tool=tool or self.name,
            method=method,
            node=node or self._node,
        )
        _publish_log(self._nats_url, subject, record)
        return record

    def node_log(self, level: str, message: str, *, data: dict[str, Any] | None = None, corr: str | None = None) -> dict[str, Any]:
        return self.emit(
            subject=f"logs.node.{self._node}",
            level=level,
            kind="node",
            message=message,
            data=data,
            corr=corr,
            tool=None,
        )

    def tool_log(self, level: str, message: str, *, data: dict[str, Any] | None = None, corr: str | None = None, tool: str | None = None) -> dict[str, Any]:
        tool_name = tool or self.name
        return self.emit(
            subject=f"logs.tool.{self._node}.{tool_name}",
            level=level,
            kind="tool",
            message=message,
            data=data,
            corr=corr,
            tool=tool_name,
        )

    def call_log(
        self,
        level: str,
        message: str,
        *,
        method: str,
        data: dict[str, Any] | None = None,
        corr: str | None = None,
        duration_ms: float | None = None,
        ok: bool | None = None,
        tool: str | None = None,
    ) -> dict[str, Any]:
        tool_name = tool or self.name
        return self.emit(
            subject=f"logs.call.{self._node}.{tool_name}.{method}",
            level=level,
            kind="tool_call",
            message=message,
            data=data,
            corr=corr,
            duration_ms=duration_ms,
            ok=ok,
            tool=tool_name,
            method=method,
        )

    def _emit(self, level: str, message: str, extra: dict | None = None, **kwargs):
        if self._enabled:
            payload = {"log": {"level": level, "message": message, "name": self.name}}
            if extra:
                payload["log"]["extra"] = extra
            writer = self._stderr_writer
            if writer is None:
                import sys

                writer = sys.stderr
            print(json.dumps(payload), file=writer, flush=True)

        if kwargs.pop("publish", False):
            self.tool_log(level, message, data=extra, **kwargs)


def get_correlation_id() -> str | None:
    return _corr_var.get()


def set_correlation_id(corr: str | None) -> str | None:
    previous = _corr_var.get()
    _corr_var.set(corr)
    return previous


def clear_correlation_id() -> None:
    _corr_var.set(None)


def ensure_correlation_id() -> str:
    current = _corr_var.get()
    if current:
        return current
    current = new_correlation_id()
    _corr_var.set(current)
    return current


def new_correlation_id() -> str:
    return uuid.uuid4().hex


def build_log_record(
    *,
    subject: str,
    level: str,
    message: str,
    kind: str | None = None,
    data: dict[str, Any] | None = None,
    corr: str | None = None,
    duration_ms: float | None = None,
    ok: bool | None = None,
    tool: str | None = None,
    method: str | None = None,
    node: str | None = None,
    schema: str = SCHEMA_V1,
) -> dict[str, Any]:
    inferred = _infer_from_subject(subject)
    node_name = node or inferred.get("node") or _default_node_name()
    record: dict[str, Any] = {
        "schema": schema,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": _normalize_level(level),
        "kind": kind or inferred.get("kind") or "system",
        "node": node_name,
        "subject": subject,
        "corr": corr or ensure_correlation_id(),
        "message": message,
        "redacted": True,
    }
    tool_name = tool or inferred.get("tool")
    if tool_name:
        record["tool"] = tool_name
    method_name = method or inferred.get("method")
    if method_name:
        record["method"] = method_name
    if duration_ms is not None:
        record["duration_ms"] = duration_ms
    if ok is not None:
        record["ok"] = ok
    if data:
        record["data"] = _sanitize_data(data)
    return record


def _sanitize_data(data: dict[str, Any] | None) -> dict[str, Any] | None:
    if not data:
        return None
    out: dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            out[key] = _sanitize_data(value)
        elif isinstance(value, list):
            out[key] = [_sanitize_data(item) if isinstance(item, dict) else item for item in value]
        else:
            out[key] = value
    return out


def _normalize_level(level: str) -> str:
    level = (level or "info").lower()
    return "warn" if level == "warning" else level


def _infer_from_subject(subject: str) -> dict[str, str]:
    parts = subject.split(".")
    if len(parts) < 2 or parts[0] != "logs":
        return {}
    shape = _LOG_SUBJECT_KINDS.get(parts[1])
    if shape is None:
        return {}
    info: dict[str, str] = {
        "kind": {
            "node": "node",
            "tool": "tool",
            "call": "tool_call",
            "audit": "audit",
        }[parts[1]]
    }
    if len(parts) > 2:
        info["node"] = parts[2]
    if len(parts) > 3:
        info["tool"] = parts[3]
    if len(parts) > 4:
        info["method"] = parts[4]
    return info


def _default_node_name() -> str:
    return os.environ.get("JB_NODE_NAME") or socket.gethostname()


def _compact_dict(data: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}


def _run_async(coro, timeout: float = 30):
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"operation timed out after {timeout}s")


async def _publish_log_async(nats_url: str, subject: str, record: dict[str, Any]) -> None:
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
        await nc.publish(subject, json.dumps(record).encode("utf-8"))
        await nc.flush()
    finally:
        await nc.close()


def _publish_log(nats_url: str, subject: str, record: dict[str, Any]) -> None:
    _run_async(_publish_log_async(nats_url, subject, record), timeout=5)
