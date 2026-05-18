"""
MessagePack queue protocol for jb-service.

Uses jumpboot's MessagePackQueueServer for clean RPC without stdout interference.
Includes Pydantic input validation (matching REPL protocol behavior) and
schema introspection handlers.
"""
import asyncio
import inspect
import time
import traceback
from typing import Any, Type, get_type_hints

from pydantic import BaseModel, ValidationError, create_model

from .service import Service
from .method import is_method, is_async_method
from .schema import service_to_schema, method_to_schema
from .types import get_file_type_name, convert_file_param
from .protocol import _inject_mesh_files
from .call_context import CallContext


# Reserved keyword-only parameter name for the per-call context. Methods that
# declare ``ctx: CallContext = None`` get a real CallContext injected by the
# dispatcher; the parameter is excluded from Pydantic validation and file
# conversion. See call_context.py for usage.
_CTX_PARAM = "ctx"


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

    The ``ctx`` parameter is reserved by jb-service for the per-call CallContext
    and is excluded from validation — Pydantic doesn't know how to validate it,
    and the dispatcher injects it after validation runs.
    """
    fn = getattr(method_func, '_jb_original', method_func)
    sig = inspect.signature(fn)
    hints = get_type_hints_safe(fn)

    fields = {}
    for param_name, param in sig.parameters.items():
        if param_name in ('self', _CTX_PARAM):
            continue

        annotation = hints.get(param_name, Any)

        if param.default is inspect.Parameter.empty:
            fields[param_name] = (annotation, ...)
        else:
            fields[param_name] = (annotation, param.default)

    if not fields:
        return None

    return create_model(f'{fn.__name__}_Input', **fields)


def run_msgpack(service_class: Type[Service]):
    """
    Run a service using MessagePack queue transport.

    This uses jumpboot's MessagePackQueueServer for RPC communication,
    which keeps stdout/stderr separate from the protocol.

    Note: The jumpboot module is injected at runtime by jumpboot.QueueProcess,
    not installed via pip.
    """
    # jumpboot is injected at runtime by QueueProcess — import here (not at module level)
    # so it runs after bootstrap has set up sys.modules with jumpboot
    from jumpboot import MessagePackQueueServer

    # Instantiate service first
    service = service_class()

    # Create event loop for async support
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Call setup
    if asyncio.iscoroutinefunction(service_class.setup_async):
        if service_class.setup_async is not Service.setup_async:
            loop.run_until_complete(service.setup_async())
        else:
            service.setup()
    else:
        service.setup()

    # Start event bus (activates @on_event handlers registered during __init__)
    service._start_event_bus()

    # Pre-build Pydantic input models for all methods (same as REPL protocol)
    input_models: dict[str, Type[BaseModel] | None] = {}
    for method_name, method in service._methods.items():
        input_models[method_name] = build_pydantic_model(method)

    # Create server without auto-exposing (we'll register manually)
    server = MessagePackQueueServer(auto_start=False, expose_methods=False)

    # Register each @method as a handler. The server reference is threaded
    # into the wrapper so it can look up the asyncio.Event for cooperative
    # cancellation (CallContext.cancelled).
    for method_name, method in service._methods.items():
        model = input_models.get(method_name)
        wrapper = _create_method_wrapper(service, method_name, method, model, loop, server)
        server.register_handler(method_name, wrapper)

    # Register introspection methods
    async def jb_methods(data, request_id):
        return service._list_methods()
    server.register_handler("__jb_methods__", jb_methods)

    async def jb_schema(data, request_id):
        return service_to_schema(service.__class__)
    server.register_handler("__jb_schema__", jb_schema)

    async def jb_method_schema(data, request_id):
        name = data.get("method") if isinstance(data, dict) else data
        if not name or name not in service._methods:
            raise ValueError(f"Unknown method: {name}")
        return method_to_schema(service._methods[name])
    server.register_handler("__jb_method_schema__", jb_method_schema)

    async def jb_shutdown(data, request_id):
        # Stop event bus before teardown
        service._stop_event_bus()

        if asyncio.iscoroutinefunction(type(service).teardown_async):
            if type(service).teardown_async is not Service.teardown_async:
                loop.run_until_complete(service.teardown_async())
            else:
                service.teardown()
        else:
            service.teardown()
        server.running = False
        return {"ok": True}
    server.register_handler("__jb_shutdown__", jb_shutdown)

    # Start and run
    server.start()
    while server.running:
        # Pump the event loop so async tasks (like executors, listeners) can run
        loop.stop()  # Make run_forever stop after one iteration
        loop.run_forever()
        time.sleep(0.05)  # Small sleep to prevent busy-wait


def _create_method_wrapper(
    service: Service,
    method_name: str,
    method,
    input_model: Type[BaseModel] | None,
    loop,
    server=None,
):
    """Create a wrapper for a service method.

    The wrapper validates inputs via Pydantic (matching REPL behaviour),
    handles file type conversion, optionally injects a CallContext for methods
    that declare ``ctx``, and calls the original method.

    Uses register_handler signature: (data, request_id).
    """
    fn = getattr(method, '_jb_original', method)
    hints = get_type_hints_safe(fn)

    # Detect whether the method opts into a CallContext. The dispatcher only
    # constructs a CallContext when the method declares ``ctx`` — methods
    # without it run on the existing code path with zero behavioral change.
    sig = inspect.signature(fn)
    accepts_ctx = _CTX_PARAM in sig.parameters

    async def wrapper(data, request_id):
        try:
            # Extract kwargs from data
            kwargs = data if isinstance(data, dict) else {}

            # ctx is reserved and injected by the dispatcher; strip it from
            # incoming kwargs in case a misbehaving caller tries to pass one.
            if _CTX_PARAM in kwargs:
                kwargs = {k: v for k, v in kwargs.items() if k != _CTX_PARAM}

            # Validate and coerce params through Pydantic model
            if input_model is not None:
                try:
                    validated = input_model(**kwargs)
                    kwargs = validated.model_dump()
                except ValidationError as e:
                    raise ValueError(f"Invalid parameters for {method_name}: {e}")

            # Convert file parameters based on type hints
            converted = dict(kwargs)
            for param_name, annotation in hints.items():
                if param_name in ('return', _CTX_PARAM):
                    continue
                if param_name not in converted:
                    continue

                type_name = get_file_type_name(annotation)
                if type_name and isinstance(converted[param_name], str):
                    converted[param_name] = convert_file_param(
                        converted[param_name], type_name
                    )

            # Inject CallContext bound to this request_id, if the method asked for it.
            # The cancel_event is the asyncio.Event registered by the underlying
            # MessagePackQueueServer when the command dispatch began. For
            # streaming invocations (Phase 2), emit_fn is wired to publish
            # partial frames via server.send_response with done=False.
            if accepts_ctx:
                cancel_event = None
                emit_fn = None
                if server is not None:
                    if hasattr(server, 'get_cancel_event'):
                        cancel_event = server.get_cancel_event(request_id)
                    if hasattr(server, 'is_streaming') and server.is_streaming(request_id):
                        # Bind request_id in default arg to avoid late-binding bugs
                        # if multiple streaming calls overlap.
                        def _emit(chunk, _rid=request_id, _srv=server):
                            _srv.send_response({"chunk": chunk, "done": False}, _rid)
                        emit_fn = _emit
                converted[_CTX_PARAM] = CallContext(
                    request_id=request_id,
                    cancel_event=cancel_event,
                    emit_fn=emit_fn,
                )

            # Clear attached files before call
            service._attached_files.clear()

            # Call the method
            if is_async_method(method):
                # Run async methods on the service's event loop
                # The server runs on a different event loop, so we need to bridge
                concurrent_future = asyncio.run_coroutine_threadsafe(
                    method(**converted), loop
                )
                result = await asyncio.wrap_future(concurrent_future)
            else:
                result = method(**converted)

            # Inject __mesh_files__ if files were attached
            result = _inject_mesh_files(result, service._attached_files)
            service._attached_files.clear()

            return result

        except Exception as e:
            service._attached_files.clear()
            # Re-raise with traceback info
            raise Exception(f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}")

    return wrapper
