"""
Service base class for jb-mesh services.
"""
import os
import os.path
from typing import Any

from .filestore import FileStore
from .event_bus import EventBus, EventCallback
from .logging import LogstoreClient, ServiceLogger, ensure_correlation_id, get_correlation_id, set_correlation_id
from .types import MeshFileRef
from . import mesh_filestore


class Service:
    """
    Base class for jb-mesh services.
    
    Subclass this and decorate methods with @method to create RPC endpoints.
    
    Example:
        from jb_service import Service, method
        
        class Calculator(Service):
            @method
            def add(self, a: float, b: float) -> float:
                return a + b
    """
    
    # Override in subclass for custom metadata
    name: str = None  # Defaults to class name lowercase
    version: str = "0.0.0"
    
    def __init__(self):
        from .method import is_method

        # Set default name from class name
        if self.name is None:
            self.name = self.__class__.__name__.lower()

        # Initialize logger + logstore helpers
        self.log = ServiceLogger(self.name, node=os.environ.get("JB_NODE_NAME"))
        self.logstore = LogstoreClient()

        # Initialize file store client
        self.files = FileStore()

        # Event bus for subscribing to mesh events (started lazily)
        self._event_bus: EventBus | None = None
        self._pending_event_subs: list[tuple[str, EventCallback]] = []

        # File attachments for current method call (cleared after each call)
        self._attached_files: list[MeshFileRef] = []

        # Discover @method decorated methods
        self._methods: dict[str, Any] = {}
        for attr_name in dir(self):
            if attr_name.startswith('_'):
                continue
            attr = getattr(self, attr_name)
            if is_method(attr):
                self._methods[attr_name] = attr

        # Collect @on_event decorated handlers
        for attr_name in dir(self):
            if attr_name.startswith('__'):
                continue
            attr = getattr(self, attr_name, None)
            if attr is not None and hasattr(attr, '_jb_event_pattern'):
                self._pending_event_subs.append(
                    (attr._jb_event_pattern, attr)
                )
    
    def setup(self):
        """
        Called once when the service starts.
        
        Override to load models, initialize connections, etc.
        """
        pass
    
    async def setup_async(self):
        """
        Async version of setup. Called if defined.
        """
        pass
    
    def teardown(self):
        """
        Called when the service stops.
        
        Override to cleanup resources.
        """
        pass
    
    async def teardown_async(self):
        """
        Async version of teardown. Called if defined.
        """
        pass
    
    def attach_file(
        self,
        data: bytes,
        key: str,
        content_type: str = "application/octet-stream",
        filename: str | None = None,
    ) -> MeshFileRef:
        """
        Store a file in the mesh file store and attach it to the current
        method result. The protocol layer injects __mesh_files__ automatically.

        Args:
            data: File contents as bytes
            key: Storage key (e.g. "output/image.png")
            content_type: MIME type
            filename: Display name (defaults to basename of key)

        Returns:
            MeshFileRef with stored file metadata
        """
        # Default filename from key
        if filename is None:
            filename = os.path.basename(key)

        # Try to store in mesh file store
        size = len(data)
        try:
            info = mesh_filestore.put(data, key, content_type)
            size = info.size
        except Exception as e:
            # NATS may not be available (tests, standalone mode)
            self.log.warning(f"attach_file: could not store in mesh file store: {e}")

        ref = MeshFileRef(key=key, content_type=content_type, size=size, filename=filename)
        self._attached_files.append(ref)
        return ref

    # ------------------------------------------------------------------
    # Event subscription API
    # ------------------------------------------------------------------

    def subscribe_event(self, pattern: str, callback: EventCallback) -> str:
        """
        Subscribe to mesh events matching *pattern*.

        The event bus is started automatically on first subscription.
        Callbacks receive a parsed event dict::

            {"type": "tool.started", "node": "leaf-1",
             "timestamp": "...", "data": {"tool": "my-tool"}}

        Pattern examples::

            "events.>"            — all mesh events
            "events.tool.*"       — tool lifecycle (installed/started/stopped/crashed/removed)
            "events.node.*"       — node lifecycle (joined/left/health)
            "events.user.>"       — all user-defined events
            "events.user.training.complete" — specific user event

        Returns a subscription ID for later unsubscribe().
        """
        self._ensure_event_bus()
        return self._event_bus.subscribe(pattern, callback)

    def unsubscribe_event(self, sub_id: str) -> None:
        """Remove an event subscription by ID."""
        if self._event_bus:
            self._event_bus.unsubscribe(sub_id)

    def _ensure_event_bus(self) -> None:
        """Lazily start the event bus on first use."""
        if self._event_bus is None:
            self._event_bus = EventBus()
        if not self._event_bus.connected:
            self._event_bus.start()

    def _start_event_bus(self) -> None:
        """Activate any @on_event handlers and pending subscriptions."""
        if not self._pending_event_subs:
            return
        self._ensure_event_bus()
        for pattern, cb in self._pending_event_subs:
            self._event_bus.subscribe(pattern, cb)
        self._pending_event_subs.clear()

    def _stop_event_bus(self) -> None:
        """Shut down the event bus if running."""
        if self._event_bus is not None:
            self._event_bus.stop()
            self._event_bus = None

    # ------------------------------------------------------------------

    def get_correlation_id(self) -> str | None:
        return get_correlation_id()

    def set_correlation_id(self, corr: str | None) -> str | None:
        return set_correlation_id(corr)

    def ensure_correlation_id(self) -> str:
        return ensure_correlation_id()

    def _get_method(self, name: str):
        """Get a method by name."""
        if name not in self._methods:
            raise AttributeError(f"Unknown method: {name}")
        return self._methods[name]

    def _list_methods(self) -> list[str]:
        """List available method names."""
        return list(self._methods.keys())
