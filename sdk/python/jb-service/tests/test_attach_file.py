"""Tests for Service.attach_file and __mesh_files__ injection."""
import json
import unittest
from unittest.mock import patch, MagicMock
from dataclasses import asdict

from jb_service.service import Service
from jb_service.method import method
from jb_service.types import MeshFileRef


class AttachFileService(Service):
    """Test service with file-returning methods."""

    @method
    def generate_image(self, prompt: str) -> dict:
        ref = self.attach_file(
            b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
            f"output/{prompt}.png",
            "image/png",
        )
        return {"prompt": prompt, "file_id": ref.key}

    @method
    def generate_multiple(self) -> dict:
        self.attach_file(b"img1", "output/a.png", "image/png")
        self.attach_file(b"img2", "output/b.png", "image/png")
        return {"count": 2}

    @method
    def explicit_mesh_files(self) -> dict:
        """Return explicit __mesh_files__ without using attach_file."""
        return {
            "result": "ok",
            "__mesh_files__": [
                {"key": "manual/file.txt", "content_type": "text/plain",
                 "size": 5, "filename": "file.txt"}
            ],
        }

    @method
    def no_files(self) -> dict:
        return {"status": "ok"}

    @method
    def mixed_attach_and_explicit(self) -> dict:
        self.attach_file(b"attached", "output/attached.png", "image/png")
        return {
            "result": "ok",
            "__mesh_files__": [
                {"key": "manual/m.txt", "content_type": "text/plain",
                 "size": 3, "filename": "m.txt"}
            ],
        }


class TestAttachFile(unittest.TestCase):
    """Test attach_file stores file and builds MeshFileRef."""

    @patch("jb_service.service.mesh_filestore")
    def test_attach_file_returns_ref(self, mock_fs):
        mock_fs.put.return_value = MagicMock(
            key="output/test.png", size=108, content_type="image/png", etag="abc123"
        )
        svc = AttachFileService()
        svc.setup()

        ref = svc.attach_file(b"data", "output/test.png", "image/png")

        self.assertIsInstance(ref, MeshFileRef)
        self.assertEqual(ref.key, "output/test.png")
        self.assertEqual(ref.content_type, "image/png")
        self.assertEqual(ref.size, 108)
        mock_fs.put.assert_called_once_with(b"data", "output/test.png", "image/png")

    @patch("jb_service.service.mesh_filestore")
    def test_attach_file_appends_to_list(self, mock_fs):
        mock_fs.put.return_value = MagicMock(key="k", size=1, content_type="t", etag="e")
        svc = AttachFileService()
        svc.setup()

        svc.attach_file(b"a", "k1", "t")
        svc.attach_file(b"b", "k2", "t")

        self.assertEqual(len(svc._attached_files), 2)

    @patch("jb_service.service.mesh_filestore")
    def test_attach_file_default_filename(self, mock_fs):
        mock_fs.put.return_value = MagicMock(
            key="output/deep/path/image.png", size=1, content_type="image/png", etag="e"
        )
        svc = AttachFileService()
        svc.setup()

        ref = svc.attach_file(b"x", "output/deep/path/image.png", "image/png")
        self.assertEqual(ref.filename, "image.png")

    @patch("jb_service.service.mesh_filestore")
    def test_attach_file_custom_filename(self, mock_fs):
        mock_fs.put.return_value = MagicMock(
            key="output/abc123.png", size=1, content_type="image/png", etag="e"
        )
        svc = AttachFileService()
        svc.setup()

        ref = svc.attach_file(b"x", "output/abc123.png", "image/png", filename="photo.png")
        self.assertEqual(ref.filename, "photo.png")

    def test_attach_file_graceful_without_nats(self):
        """attach_file should work even when mesh_filestore is unavailable."""
        svc = AttachFileService()
        svc.setup()

        # mesh_filestore.put will fail (no NATS), but attach_file should still return a ref
        ref = svc.attach_file(b"data", "output/test.png", "image/png")

        self.assertIsInstance(ref, MeshFileRef)
        self.assertEqual(ref.key, "output/test.png")
        self.assertEqual(ref.size, 4)  # len(b"data")


class TestMeshFilesInjection(unittest.TestCase):
    """Test that protocol layers inject __mesh_files__ into results."""

    def _make_protocol(self):
        """Create a Protocol instance with mocked file store."""
        from jb_service.protocol import Protocol
        svc = AttachFileService()
        svc.setup()
        return Protocol(svc), svc

    @patch("jb_service.service.mesh_filestore")
    def test_injection_single_file(self, mock_fs):
        mock_fs.put.return_value = MagicMock(
            key="output/sunset.png", size=108, content_type="image/png", etag="abc"
        )
        proto, svc = self._make_protocol()

        result = proto.handle_call("generate_image", {"prompt": "sunset"})

        self.assertTrue(result["ok"])
        inner = result["result"]
        self.assertIn("__mesh_files__", inner)
        files = inner["__mesh_files__"]
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["key"], "output/sunset.png")
        self.assertEqual(files[0]["content_type"], "image/png")

    @patch("jb_service.service.mesh_filestore")
    def test_injection_multiple_files(self, mock_fs):
        mock_fs.put.side_effect = [
            MagicMock(key="output/a.png", size=4, content_type="image/png", etag="e1"),
            MagicMock(key="output/b.png", size=4, content_type="image/png", etag="e2"),
        ]
        proto, svc = self._make_protocol()

        result = proto.handle_call("generate_multiple", {})

        self.assertTrue(result["ok"])
        files = result["result"]["__mesh_files__"]
        self.assertEqual(len(files), 2)
        keys = {f["key"] for f in files}
        self.assertEqual(keys, {"output/a.png", "output/b.png"})

    def test_no_injection_when_no_files(self):
        proto, svc = self._make_protocol()

        result = proto.handle_call("no_files", {})

        self.assertTrue(result["ok"])
        self.assertNotIn("__mesh_files__", result["result"])

    def test_explicit_mesh_files_preserved(self):
        proto, svc = self._make_protocol()

        result = proto.handle_call("explicit_mesh_files", {})

        self.assertTrue(result["ok"])
        files = result["result"]["__mesh_files__"]
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["key"], "manual/file.txt")

    @patch("jb_service.service.mesh_filestore")
    def test_merge_attach_and_explicit(self, mock_fs):
        mock_fs.put.return_value = MagicMock(
            key="output/attached.png", size=8, content_type="image/png", etag="e"
        )
        proto, svc = self._make_protocol()

        result = proto.handle_call("mixed_attach_and_explicit", {})

        self.assertTrue(result["ok"])
        files = result["result"]["__mesh_files__"]
        self.assertEqual(len(files), 2)
        keys = {f["key"] for f in files}
        self.assertEqual(keys, {"manual/m.txt", "output/attached.png"})

    @patch("jb_service.service.mesh_filestore")
    def test_attached_files_cleared_between_calls(self, mock_fs):
        mock_fs.put.return_value = MagicMock(
            key="output/test.png", size=108, content_type="image/png", etag="abc"
        )
        proto, svc = self._make_protocol()

        # First call attaches a file
        proto.handle_call("generate_image", {"prompt": "first"})

        # Second call should NOT have files from first call
        result = proto.handle_call("no_files", {})
        self.assertNotIn("__mesh_files__", result["result"])


if __name__ == "__main__":
    unittest.main()
