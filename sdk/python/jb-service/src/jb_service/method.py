"""
@method decorator for marking RPC endpoints.
"""
from functools import wraps
from typing import Callable, Any
import inspect


def method(func: Callable = None, *, stream: bool = False) -> Callable:
    """
    Mark a method as an RPC endpoint.
    
    Can be used with or without parentheses:
        @method
        def foo(self): ...
        
        @method()
        def bar(self): ...
        
        @method(stream=True)  # Reserved for future streaming support
        def baz(self): ...
    """
    def decorator(fn: Callable) -> Callable:
        # Mark the function as a jb method
        fn._jb_method = True
        fn._jb_stream = stream
        
        # Preserve function metadata
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        
        # Copy our markers to the wrapper
        wrapper._jb_method = True
        wrapper._jb_stream = stream
        
        # Store original function for schema extraction
        wrapper._jb_original = fn
        
        return wrapper
    
    # Handle @method without parentheses
    if func is not None:
        return decorator(func)
    
    # Handle @method() or @method(stream=True)
    return decorator


def is_method(obj: Any) -> bool:
    """Check if an object is a @method decorated function."""
    return callable(obj) and getattr(obj, '_jb_method', False)


def is_async_method(obj: Any) -> bool:
    """Check if a @method is async."""
    if not is_method(obj):
        return False
    fn = getattr(obj, '_jb_original', obj)
    return inspect.iscoroutinefunction(fn)


def is_stream_method(obj: Any) -> bool:
    """Check if a @method is a streaming method."""
    return is_method(obj) and getattr(obj, '_jb_stream', False)


def on_event(pattern: str) -> Callable:
    """
    Mark a method as an event handler that is auto-subscribed on service start.

    Usage::

        class MyTool(Service):
            @on_event("events.tool.*")
            def handle_tool_event(self, event: dict):
                print(event["type"], event["data"])

            @on_event("events.user.>")
            def handle_user_event(self, event: dict):
                print("user event:", event)

    The decorated method receives a single *event* dict argument with keys:
    type, node, timestamp, data.
    """
    def decorator(fn: Callable) -> Callable:
        fn._jb_event_pattern = pattern
        return fn
    return decorator
