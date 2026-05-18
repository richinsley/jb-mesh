"""
Mesh file store — direct NATS Object Store access.

Files are stored in the NATS Object Store bucket "mesh-files" and are
accessible from any node in the mesh. No HTTP proxy, no base64 overhead —
raw binary through NATS.

Usage:
    from jb_service import file_store

    # Store a file (binary, no encoding overhead)
    info = file_store.put(image_bytes, "output/image.png", content_type="image/png")

    # Retrieve a file
    data = file_store.get("output/image.png")

    # Metadata only
    info = file_store.head("output/image.png")

    # List with prefix filter
    files = file_store.list(prefix="audio/")

    # Delete
    file_store.delete("output/image.png")

Environment:
    JB_NATS_URL — NATS server URL (default: nats://localhost:4222)
                  Injected automatically by jb-mesh executor.
"""
import os
import asyncio
import hashlib
from io import BytesIO
from typing import List, Optional
from dataclasses import dataclass

import nats
from nats.js.object_store import ObjectStore


# Bucket name must match the Go-side pkg/filestore default
BUCKET_NAME = "mesh-files"


class MeshFileStoreError(Exception):
    """Error from the mesh file store."""
    pass


@dataclass
class MeshFileInfo:
    """Metadata about a file in the mesh store."""
    key: str
    size: int
    content_type: str
    etag: str
    created: str

    @classmethod
    def from_dict(cls, data: dict) -> "MeshFileInfo":
        return cls(
            key=data.get("key", ""),
            size=data.get("size", 0),
            content_type=data.get("content_type", ""),
            etag=data.get("etag", ""),
            created=data.get("created", ""),
        )

    @classmethod
    def from_object_info(cls, info) -> "MeshFileInfo":
        """Convert NATS ObjectInfo to MeshFileInfo."""
        content_type = ""
        etag = ""
        if info.headers:
            content_type = info.headers.get("Content-Type", "")
            etag = info.headers.get("ETag", "")
        return cls(
            key=info.name,
            size=info.size,
            content_type=content_type,
            etag=etag,
            created=str(info.mtime) if info.mtime else "",
        )


def _nats_url() -> str:
    return os.environ.get("JB_NATS_URL", "nats://localhost:4222")


def _run_async(coro, timeout=30):
    """Run an async coroutine synchronously with a hard timeout.
    
    Always uses a thread pool so we can enforce a hard timeout even when
    nats-py's internal reconnect logic hangs.
    """
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"operation timed out after {timeout}s")


async def _get_store(nc) -> ObjectStore:
    """Get or create the mesh-files object store bucket."""
    js = nc.jetstream()
    try:
        return await js.object_store(BUCKET_NAME)
    except Exception:
        # Bucket doesn't exist yet — create it
        return await js.create_object_store(BUCKET_NAME)


async def _put_async(data: bytes, key: str, content_type: str,
                     nats_url: str) -> MeshFileInfo:
    nc = await asyncio.wait_for(nats.connect(nats_url, connect_timeout=1, max_reconnect_attempts=0, allow_reconnect=False), timeout=2)
    try:
        store = await _get_store(nc)
        
        # Compute SHA-256 ETag
        etag = hashlib.sha256(data).hexdigest()
        
        # Put with description containing content type + etag
        # NOTE: headers in ObjectMeta cause nats.go "meta information invalid" errors
        # due to wire format incompatibility between nats-py and nats.go.
        # Use description field for metadata instead.
        from nats.js.api import ObjectMeta
        meta = ObjectMeta(
            name=key,
            description=f"{content_type};etag={etag}",
        )
        info = await store.put(key, data, meta=meta)
        
        return MeshFileInfo(
            key=key,
            size=len(data),
            content_type=content_type,
            etag=etag,
            created=str(info.mtime) if info.mtime else "",
        )
    finally:
        await nc.close()


async def _get_async(key: str, nats_url: str) -> bytes:
    nc = await asyncio.wait_for(nats.connect(nats_url, connect_timeout=1, max_reconnect_attempts=0, allow_reconnect=False), timeout=2)
    try:
        store = await _get_store(nc)
        result = await store.get(key)
        return result.data
    finally:
        await nc.close()


async def _head_async(key: str, nats_url: str) -> MeshFileInfo:
    nc = await asyncio.wait_for(nats.connect(nats_url, connect_timeout=1, max_reconnect_attempts=0, allow_reconnect=False), timeout=2)
    try:
        store = await _get_store(nc)
        info = await store.get_info(key)
        return MeshFileInfo.from_object_info(info)
    finally:
        await nc.close()


async def _list_async(prefix: str, nats_url: str) -> List[MeshFileInfo]:
    nc = await asyncio.wait_for(nats.connect(nats_url, connect_timeout=1, max_reconnect_attempts=0, allow_reconnect=False), timeout=2)
    try:
        store = await _get_store(nc)
        entries = await store.list()
        
        files = []
        for info in entries:
            if info.deleted:
                continue
            if prefix and not info.name.startswith(prefix):
                continue
            files.append(MeshFileInfo.from_object_info(info))
        return files
    finally:
        await nc.close()


async def _delete_async(key: str, nats_url: str) -> None:
    nc = await asyncio.wait_for(nats.connect(nats_url, connect_timeout=1, max_reconnect_attempts=0, allow_reconnect=False), timeout=2)
    try:
        store = await _get_store(nc)
        await store.delete(key)
    finally:
        await nc.close()


# --- Public sync API ---

def put(data: bytes, key: str, content_type: str = "application/octet-stream",
        nats_url: str = None) -> MeshFileInfo:
    """
    Store a file in the mesh file store via NATS Object Store.

    No encoding overhead — raw binary through NATS.

    Args:
        data: File contents as bytes
        key: Storage key (e.g. "output/image.png")
        content_type: MIME type
        nats_url: Override NATS URL (default: JB_NATS_URL env)

    Returns:
        MeshFileInfo with stored file metadata
    """
    url = nats_url or _nats_url()
    try:
        return _run_async(_put_async(data, key, content_type, url))
    except Exception as e:
        raise MeshFileStoreError(f"put {key}: {e}") from e


def get(key: str, nats_url: str = None) -> bytes:
    """
    Retrieve a file from the mesh file store.

    Args:
        key: Storage key
        nats_url: Override NATS URL

    Returns:
        File contents as bytes
    """
    url = nats_url or _nats_url()
    try:
        return _run_async(_get_async(key, url))
    except Exception as e:
        raise MeshFileStoreError(f"get {key}: {e}") from e


def head(key: str, nats_url: str = None) -> MeshFileInfo:
    """
    Get metadata for a file without downloading it.

    Args:
        key: Storage key
        nats_url: Override NATS URL

    Returns:
        MeshFileInfo
    """
    url = nats_url or _nats_url()
    try:
        return _run_async(_head_async(key, url))
    except Exception as e:
        raise MeshFileStoreError(f"head {key}: {e}") from e


def list(prefix: str = "", nats_url: str = None) -> List[MeshFileInfo]:
    """
    List files in the store, optionally filtered by prefix.

    Args:
        prefix: Key prefix filter
        nats_url: Override NATS URL

    Returns:
        List of MeshFileInfo
    """
    url = nats_url or _nats_url()
    try:
        return _run_async(_list_async(prefix, url))
    except Exception as e:
        raise MeshFileStoreError(f"list: {e}") from e


def delete(key: str, nats_url: str = None) -> None:
    """
    Delete a file from the store.

    Args:
        key: Storage key
        nats_url: Override NATS URL
    """
    url = nats_url or _nats_url()
    try:
        _run_async(_delete_async(key, url))
    except Exception as e:
        raise MeshFileStoreError(f"delete {key}: {e}") from e
