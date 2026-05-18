"""
jb-service: Python SDK for building jb-mesh services.

Usage:
    from jb_service import Service, method, run
    from jb_service.types import FilePath, Audio, Image
    
    # Simple service (REPL transport - no stdout allowed)
    class Calculator(Service):
        @method
        def add(self, a: float, b: float) -> float:
            return a + b
    
    # Complex service with stdout (MessagePack transport)
    from jb_service import MessagePackService
    
    class ImageGenerator(MessagePackService):
        @method
        def generate(self, prompt: str) -> dict:
            # Progress bars, logging, etc. are fine
            result = self.pipeline(prompt)
            return {"image": save_image(result)}
    
    if __name__ == "__main__":
        run(Calculator)  # or run(ImageGenerator)

File Store:
    # Tools can use self.files for persistent file storage
    class MyTool(Service):
        @method
        def process(self, input: str) -> dict:
            output_path = self.do_work(input)
            
            # Import into store with 1 hour TTL
            file_id = self.files.import_file(output_path, ttl=3600)
            
            return {"file_id": file_id}
"""

from .service import Service
from .msgpack_service import MessagePackService
from .method import method, on_event
from .protocol import run
from .types import FilePath, Audio, Image, MeshFileRef, save_image, save_audio
from .filestore import FileStore, FileInfo, FileStoreError, get_filestore
from .events import emit_event, EventError
from .event_bus import EventBus
from .logging import LogstoreClient, LogstoreError, ServiceLogger, build_log_record, clear_correlation_id, ensure_correlation_id, get_correlation_id, new_correlation_id, set_correlation_id
from .call_context import CallContext, CallCancelled
from . import mesh_filestore as file_store

try:
    from jumpboot import MessagePackQueueServer
except Exception:
    MessagePackQueueServer = None

__version__ = "0.1.0"
__all__ = [
    "Service", "MessagePackService", "MessagePackQueueServer", "method", "on_event", "run",
    "FilePath", "Audio", "Image", "MeshFileRef",
    "save_image", "save_audio",
    "FileStore", "FileInfo", "FileStoreError", "get_filestore",
    "emit_event", "EventError",
    "EventBus",
    "LogstoreClient", "LogstoreError", "ServiceLogger",
    "build_log_record", "get_correlation_id", "set_correlation_id",
    "clear_correlation_id", "ensure_correlation_id", "new_correlation_id",
    "CallContext", "CallCancelled",
    "file_store",
    "__version__"
]
