"""Tests for NATS Object Store-based mesh file store."""
import subprocess
import time
import pytest

from jb_service.mesh_filestore import (
    put, get, head, list as list_files, delete,
    MeshFileInfo, MeshFileStoreError,
)


def nats_server_available():
    try:
        result = subprocess.run(["which", "nats-server"], capture_output=True)
        return result.returncode == 0
    except Exception:
        return False


@pytest.fixture
def nats_server(tmp_path):
    """Start a temporary NATS server with JetStream."""
    if not nats_server_available():
        pytest.skip("nats-server not available")

    port = 14223  # Different port from events tests
    proc = subprocess.Popen(
        ["nats-server", "-p", str(port), "-js", "-sd", str(tmp_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)

    url = f"nats://localhost:{port}"
    yield url

    proc.terminate()
    proc.wait(timeout=5)


def test_put_and_get(nats_server):
    """Test storing and retrieving a file via NATS Object Store."""
    data = b"hello world"
    info = put(data, "test.txt", content_type="text/plain", nats_url=nats_server)

    assert info.key == "test.txt"
    assert info.size == 11
    assert info.content_type == "text/plain"
    assert info.etag != ""

    retrieved = get("test.txt", nats_url=nats_server)
    assert retrieved == b"hello world"


def test_head(nats_server):
    """Test getting file metadata without downloading."""
    put(b"metadata test", "meta.txt", content_type="text/plain", nats_url=nats_server)

    info = head("meta.txt", nats_url=nats_server)
    assert info.key == "meta.txt"
    assert info.size == 13


def test_list(nats_server):
    """Test listing files with prefix filter."""
    put(b"wav1", "audio/clip1.wav", content_type="audio/wav", nats_url=nats_server)
    put(b"wav2", "audio/clip2.wav", content_type="audio/wav", nats_url=nats_server)
    put(b"png", "image/photo.png", content_type="image/png", nats_url=nats_server)

    all_files = list_files(nats_url=nats_server)
    assert len(all_files) == 3

    audio = list_files(prefix="audio/", nats_url=nats_server)
    assert len(audio) == 2


def test_delete(nats_server):
    """Test deleting a file."""
    put(b"temp", "temp.txt", nats_url=nats_server)
    delete("temp.txt", nats_url=nats_server)

    with pytest.raises(MeshFileStoreError):
        get("temp.txt", nats_url=nats_server)


def test_binary_roundtrip(nats_server):
    """Test that binary data survives NATS Object Store roundtrip."""
    data = bytes(range(256)) * 100  # 25.6KB of binary data
    put(data, "binary.bin", content_type="application/octet-stream", nats_url=nats_server)

    retrieved = get("binary.bin", nats_url=nats_server)
    assert retrieved == data
    assert len(retrieved) == 25600


def test_overwrite(nats_server):
    """Test that putting to the same key overwrites."""
    put(b"version 1", "mutable.txt", nats_url=nats_server)
    put(b"version 2", "mutable.txt", nats_url=nats_server)

    data = get("mutable.txt", nats_url=nats_server)
    assert data == b"version 2"


def test_connection_error():
    """Test that connection failure raises MeshFileStoreError.
    
    nats-py's internal reconnect can hang, so we rely on the hard
    timeout in _run_async to break out.
    """
    from jb_service.mesh_filestore import _run_async, _put_async

    with pytest.raises((MeshFileStoreError, TimeoutError)):
        _run_async(
            _put_async(b"data", "test.txt", "text/plain", "nats://127.0.0.1:1"),
            timeout=5,
        )


def test_mesh_file_info():
    """Test MeshFileInfo construction."""
    info = MeshFileInfo.from_dict({
        "key": "test.txt",
        "size": 100,
        "content_type": "text/plain",
        "etag": "abc",
        "created": "2026-01-01",
    })
    assert info.key == "test.txt"
    assert info.size == 100
