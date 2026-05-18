"""
Jumpboot communication protocol.

Handles the REPL-based communication between jb-mesh (Go) and the Python service.

The jumpboot REPL works by:
1. Go sends Python code as strings to execute
2. Python executes and returns results

So we don't run an event loop - we just set up global functions that
the REPL can call directly, then return. The service instance stays
alive in the Python process.
"""
import asyncio
import inspect
import json
import sys
import traceback
from typing import Any, Type, get_type_hints

from pydantic import BaseModel, ValidationError, create_model

from .service import Service
from .method import is_method, is_async_method
from .schema import service_to_schema, method_to_schema
from .types import get_file_type_name, convert_file_param, MeshFileRef


def _inject_mesh_files(result, attached_files: list) -> any:
    """
    Inject __mesh_files__ into a dict result if files were attached.

    - If attached_files is empty and result has no __mesh_files__, return as-is.
    - If result is not a dict, wrap it: {"value": result, "__mesh_files__": [...]}.
    - Merges with any existing __mesh_files__ (no duplicates by key).
    """
    if not attached_files and (not isinstance(result, dict) or "__mesh_files__" not in result):
        return result

    # Build file refs from attached files
    attached_refs = [f.to_dict() for f in attached_files]

    if not isinstance(result, dict):
        if attached_refs:
            return {"value": result, "__mesh_files__": attached_refs}
        return result

    # Merge with existing __mesh_files__
    existing = result.get("__mesh_files__", [])
    if not attached_refs:
        return result

    # Deduplicate by key (existing wins)
    existing_keys = {f["key"] for f in existing if isinstance(f, dict)}
    merged = list(existing)
    for ref in attached_refs:
        if ref["key"] not in existing_keys:
            merged.append(ref)

    result["__mesh_files__"] = merged
    return result


def get_type_hints_safe(fn):
    """Get type hints, handling forward references gracefully."""
    try:
        return get_type_hints(fn)
    except Exception:
        return getattr(fn, '__annotations__', {})


def build_pydantic_model(method_func) -> Type[BaseModel] | None:
    """
    Build a Pydantic model for validating method inputs.
    
    Returns None if the method has no parameters (other than self).
    """
    fn = getattr(method_func, '_jb_original', method_func)
    sig = inspect.signature(fn)
    hints = get_type_hints_safe(fn)
    
    fields = {}
    for param_name, param in sig.parameters.items():
        if param_name == 'self':
            continue
        
        annotation = hints.get(param_name, Any)
        
        if param.default is inspect.Parameter.empty:
            fields[param_name] = (annotation, ...)
        else:
            fields[param_name] = (annotation, param.default)
    
    if not fields:
        return None
    
    return create_model(f'{fn.__name__}_Input', **fields)


class Protocol:
    """
    Handles the jb-mesh ↔ Python communication protocol.
    """
    
    def __init__(self, service: Service):
        self.service = service
        self._input_models: dict[str, Type[BaseModel] | None] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        
        # Pre-build input models for all methods
        for name, method in service._methods.items():
            self._input_models[name] = build_pydantic_model(method)
    
    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create an event loop for async methods."""
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        return self._loop
    
    def _validate_params(self, method_name: str, params: dict) -> dict:
        """Validate and coerce input parameters using Pydantic."""
        model = self._input_models.get(method_name)
        if model is None:
            return params
        
        try:
            validated = model(**params)
            return validated.model_dump()
        except ValidationError as e:
            raise ValueError(f"Invalid parameters: {e}")
    
    def _convert_file_params(self, method_name: str, params: dict) -> dict:
        """
        Convert file parameters based on type hints.
        
        If a parameter is typed as Audio, Image, etc., load the file
        and replace the path with the loaded data.
        """
        method = self.service._get_method(method_name)
        fn = getattr(method, '_jb_original', method)
        hints = get_type_hints_safe(fn)
        
        converted = dict(params)
        for param_name, annotation in hints.items():
            if param_name == 'return':
                continue
            if param_name not in converted:
                continue
            
            type_name = get_file_type_name(annotation)
            if type_name and isinstance(converted[param_name], str):
                converted[param_name] = convert_file_param(
                    converted[param_name], type_name
                )
        
        return converted
    
    def handle_call(self, method_name: str, params: dict) -> dict:
        """
        Handle an RPC call synchronously.
        
        Returns a response dict with ok, result/error, and done fields.
        """
        try:
            # Get the method
            method = self.service._get_method(method_name)
            
            # Validate parameters
            validated_params = self._validate_params(method_name, params)
            
            # Convert file parameters (Audio, Image, etc.)
            converted_params = self._convert_file_params(method_name, validated_params)
            
            # Clear attached files before call
            self.service._attached_files.clear()

            # Call the method
            if is_async_method(method):
                # Run async method in event loop
                loop = self._get_loop()
                result = loop.run_until_complete(method(**converted_params))
            else:
                result = method(**converted_params)
            
            # Inject __mesh_files__ if files were attached
            result = _inject_mesh_files(result, self.service._attached_files)
            self.service._attached_files.clear()

            return {"ok": True, "result": result, "done": True}
        
        except Exception as e:
            self.service._attached_files.clear()
            return {
                "ok": False,
                "error": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
                "done": True,
            }
    
    def handle_schema(self) -> dict:
        """Return the service schema."""
        return service_to_schema(self.service.__class__)
    
    def handle_method_schema(self, method_name: str) -> dict:
        """Return schema for a specific method."""
        method = self.service._get_method(method_name)
        return method_to_schema(method)
    
    def handle_methods(self) -> list[str]:
        """Return list of available methods."""
        return self.service._list_methods()


# Global service instance and protocol - set by run()
_service_instance: Service | None = None
_protocol: Protocol | None = None


def run(service_class: Type[Service]):
    """
    Initialize and run a service.
    
    Automatically detects the transport based on the service class:
    - Service subclass: Uses REPL protocol (simple, but stdout-sensitive)
    - MessagePackService subclass: Uses MessagePack queue (robust, stdout-safe)
    
    Usage:
        from jb_service import Service, method, run
        
        class MyService(Service):
            @method
            def hello(self, name: str) -> str:
                return f"Hello, {name}!"
        
        if __name__ == "__main__":
            run(MyService)
    """
    # Check if this is a MessagePackService
    transport = getattr(service_class, '_transport', 'repl')
    
    if transport == 'msgpack':
        from .msgpack_protocol import run_msgpack
        return run_msgpack(service_class)
    
    # Default: REPL protocol
    global _service_instance, _protocol
    
    # Instantiate service
    _service_instance = service_class()
    _protocol = Protocol(_service_instance)
    
    # Call setup
    if asyncio.iscoroutinefunction(service_class.setup_async):
        # Check if setup_async is overridden
        if service_class.setup_async is not Service.setup_async:
            loop = _protocol._get_loop()
            loop.run_until_complete(_service_instance.setup_async())
        else:
            _service_instance.setup()
    else:
        _service_instance.setup()

    # Start event bus (activates @on_event handlers registered during __init__)
    _service_instance._start_event_bus()
    
    # Register globals for jumpboot REPL to call
    # These become available in the REPL's global namespace
    # All functions return JSON strings for easy parsing in Go
    
    def __jb_call__(method: str, params: dict = None) -> str:
        """Call a service method. Returns JSON string {ok, result/error, done}."""
        if params is None:
            params = {}
        result = _protocol.handle_call(method, params)
        return json.dumps(result)
    
    def __jb_schema__() -> str:
        """Get full service schema as JSON string."""
        return json.dumps(_protocol.handle_schema())
    
    def __jb_method_schema__(method_name: str) -> str:
        """Get method schema as JSON string."""
        return json.dumps(_protocol.handle_method_schema(method_name))
    
    def __jb_methods__() -> str:
        """List available method names as JSON array string."""
        return json.dumps(_protocol.handle_methods())
    
    def __jb_shutdown__() -> str:
        """Shutdown the service (calls teardown). Returns JSON string."""
        global _service_instance, _protocol
        if _service_instance is not None:
            # Stop event bus before teardown
            _service_instance._stop_event_bus()

            if asyncio.iscoroutinefunction(type(_service_instance).teardown_async):
                if type(_service_instance).teardown_async is not Service.teardown_async:
                    loop = _protocol._get_loop()
                    loop.run_until_complete(_service_instance.teardown_async())
                else:
                    _service_instance.teardown()
            else:
                _service_instance.teardown()

            _service_instance = None
            _protocol = None
        return json.dumps({"ok": True})
    
    # Register in builtins so they're accessible from REPL
    import builtins
    builtins.__jb_call__ = __jb_call__
    builtins.__jb_schema__ = __jb_schema__
    builtins.__jb_method_schema__ = __jb_method_schema__
    builtins.__jb_methods__ = __jb_methods__
    builtins.__jb_shutdown__ = __jb_shutdown__
    
    # Also register in globals (belt and suspenders)
    globals()['__jb_call__'] = __jb_call__
    globals()['__jb_schema__'] = __jb_schema__
    globals()['__jb_method_schema__'] = __jb_method_schema__
    globals()['__jb_methods__'] = __jb_methods__
    globals()['__jb_shutdown__'] = __jb_shutdown__
    
    # Service is ready - globals are registered
