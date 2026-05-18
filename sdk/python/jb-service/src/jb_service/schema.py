"""
Schema generation from type hints and Pydantic models.
"""
from typing import Any, Callable, get_type_hints, get_origin, get_args, Union, Literal
import inspect
from pydantic import BaseModel
from pydantic.fields import FieldInfo


# Type mapping from Python types to JSON schema
TYPE_MAP = {
    str: {"type": "string"},
    int: {"type": "integer"},
    float: {"type": "number"},
    bool: {"type": "boolean"},
    list: {"type": "array"},
    dict: {"type": "object"},
    type(None): {"type": "null"},
}


def python_type_to_schema(py_type: Any) -> dict:
    """Convert a Python type hint to JSON schema."""
    # Handle None
    if py_type is None or py_type is type(None):
        return {"type": "null"}
    
    # Handle basic types
    if py_type in TYPE_MAP:
        return TYPE_MAP[py_type].copy()
    
    # Handle Pydantic models
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        return py_type.model_json_schema()
    
    # Handle generic types (List[str], Dict[str, int], etc.)
    origin = get_origin(py_type)
    args = get_args(py_type)
    
    if origin is list:
        schema = {"type": "array"}
        if args:
            schema["items"] = python_type_to_schema(args[0])
        return schema
    
    if origin is dict:
        schema = {"type": "object"}
        if len(args) >= 2:
            schema["additionalProperties"] = python_type_to_schema(args[1])
        return schema
    
    # Handle Union (including Optional which is Union[T, None])
    if origin is Union:
        non_none_args = [a for a in args if a is not type(None)]
        has_none = len(non_none_args) < len(args)
        
        if len(non_none_args) == 1:
            # Optional[T] -> T with nullable
            schema = python_type_to_schema(non_none_args[0])
            if has_none:
                if "type" in schema:
                    schema["type"] = [schema["type"], "null"]
                else:
                    schema = {"anyOf": [schema, {"type": "null"}]}
            return schema
        else:
            # Union[A, B, C] -> anyOf
            schemas = [python_type_to_schema(a) for a in args]
            return {"anyOf": schemas}
    
    # Handle Literal
    if origin is Literal:
        values = list(args)
        if all(isinstance(v, str) for v in values):
            return {"type": "string", "enum": values}
        elif all(isinstance(v, int) for v in values):
            return {"type": "integer", "enum": values}
        else:
            return {"enum": values}
    
    # Fallback for unknown types
    return {"type": "object"}


def parse_docstring(docstring: str | None) -> dict:
    """
    Parse a docstring to extract description and argument descriptions.
    
    Returns:
        {
            "description": "Main description",
            "args": {"arg_name": "arg description", ...}
        }
    """
    if not docstring:
        return {"description": "", "args": {}}
    
    lines = docstring.strip().split('\n')
    description_lines = []
    args = {}
    current_arg = None
    in_args_section = False
    
    for line in lines:
        stripped = line.strip()
        
        # Check for Args section
        if stripped.lower() in ('args:', 'arguments:', 'parameters:'):
            in_args_section = True
            continue
        
        # Check for other sections that end Args
        if stripped.lower() in ('returns:', 'return:', 'raises:', 'yields:', 'examples:'):
            in_args_section = False
            current_arg = None
            continue
        
        if in_args_section:
            # Try to parse "arg_name: description" or "arg_name (type): description"
            if ':' in stripped and not stripped.startswith(' '):
                parts = stripped.split(':', 1)
                arg_name = parts[0].strip()
                # Remove type annotation if present: "arg_name (type)" -> "arg_name"
                if '(' in arg_name:
                    arg_name = arg_name.split('(')[0].strip()
                arg_desc = parts[1].strip() if len(parts) > 1 else ""
                args[arg_name] = arg_desc
                current_arg = arg_name
            elif current_arg and stripped:
                # Continuation of previous arg description
                args[current_arg] += " " + stripped
        else:
            # Part of main description
            if stripped:
                description_lines.append(stripped)
    
    return {
        "description": " ".join(description_lines),
        "args": args
    }


def method_to_schema(method: Callable) -> dict:
    """
    Generate JSON schema for a @method decorated function.

    Returns:
        {
            "name": "method_name",
            "description": "...",
            "input": { JSON schema for input },
            "output": { JSON schema for output },
            "stream": bool,   # true if declared @method(stream=True)
        }

    The ``ctx`` parameter is jb-service's reserved per-call context and is
    excluded from the input schema — it's injected by the dispatcher, not
    something callers supply.
    """
    # Get the original function if wrapped
    fn = getattr(method, '_jb_original', method)

    # Get type hints
    try:
        hints = get_type_hints(fn)
    except Exception:
        hints = {}

    # Get signature for defaults
    sig = inspect.signature(fn)

    # Parse docstring
    doc_info = parse_docstring(fn.__doc__)

    # Build input schema
    properties = {}
    required = []

    for param_name, param in sig.parameters.items():
        # Skip 'self' and the reserved 'ctx' parameter.
        if param_name in ('self', 'ctx'):
            continue

        # Get type schema
        if param_name in hints:
            prop_schema = python_type_to_schema(hints[param_name])
        else:
            prop_schema = {"type": "object"}  # Unknown type

        # Add description from docstring
        if param_name in doc_info["args"]:
            prop_schema["description"] = doc_info["args"][param_name]

        # Handle default values
        if param.default is not inspect.Parameter.empty:
            prop_schema["default"] = param.default
        else:
            required.append(param_name)

        properties[param_name] = prop_schema

    input_schema = {
        "type": "object",
        "properties": properties,
    }
    if required:
        input_schema["required"] = required

    # Build output schema
    if 'return' in hints:
        output_schema = python_type_to_schema(hints['return'])
    else:
        output_schema = {"type": "object"}

    # Surface the streaming flag (Phase 2). Set by @method(stream=True); the
    # mesh node uses this to decide whether to register a .stream subject.
    is_stream = bool(getattr(method, '_jb_stream', False))

    return {
        "name": fn.__name__,
        "description": doc_info["description"],
        "input": input_schema,
        "output": output_schema,
        "stream": is_stream,
    }


def service_to_schema(service_class: type) -> dict:
    """
    Generate full schema for a Service class.
    
    Returns:
        {
            "name": "service-name",
            "version": "1.0.0",
            "description": "...",
            "methods": { method schemas }
        }
    """
    from .method import is_method
    
    # Get service metadata
    name = getattr(service_class, 'name', service_class.__name__.lower())
    version = getattr(service_class, 'version', '0.0.0')
    description = (service_class.__doc__ or "").strip()
    
    # Get method schemas
    methods = {}
    for attr_name in dir(service_class):
        if attr_name.startswith('_'):
            continue
        attr = getattr(service_class, attr_name)
        if is_method(attr):
            methods[attr_name] = method_to_schema(attr)
    
    return {
        "name": name,
        "version": version,
        "description": description,
        "methods": methods,
    }
