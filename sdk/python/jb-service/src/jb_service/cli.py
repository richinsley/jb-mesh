"""
CLI for jb-service.

Commands:
    jb-service manifest <module.py>  - Generate jumpboot.yaml from a service
    jb-service test <module.py>      - Test a service locally (TODO)
    jb-service init <name>           - Scaffold a new service (TODO)
"""
import argparse
import importlib.util
import json
import sys
from pathlib import Path


def load_service_from_file(filepath: str):
    """Load a Service subclass from a Python file."""
    from .service import Service
    
    path = Path(filepath)
    if not path.exists():
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        sys.exit(1)
    
    # Load the module
    spec = importlib.util.spec_from_file_location("_service_module", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["_service_module"] = module
    spec.loader.exec_module(module)
    
    # Find Service subclasses
    services = []
    for name in dir(module):
        obj = getattr(module, name)
        if (isinstance(obj, type) and 
            issubclass(obj, Service) and 
            obj is not Service):
            services.append(obj)
    
    if not services:
        print(f"Error: No Service subclass found in {filepath}", file=sys.stderr)
        sys.exit(1)
    
    if len(services) > 1:
        print(f"Warning: Multiple services found, using {services[0].__name__}", 
              file=sys.stderr)
    
    return services[0]


def cmd_manifest(args):
    """Generate jumpboot.yaml manifest from a service."""
    from .schema import service_to_schema
    import yaml
    
    service_class = load_service_from_file(args.file)
    schema = service_to_schema(service_class)
    
    # Convert to jumpboot.yaml format
    manifest = {
        "name": schema["name"],
        "version": schema["version"],
        "description": schema["description"],
        "runtime": {
            "python": "3.10",  # Default, user should customize
            "packages": ["jb-service"],
        },
        "rpc": {
            "methods": {}
        }
    }
    
    for method_name, method_schema in schema["methods"].items():
        manifest["rpc"]["methods"][method_name] = {
            "description": method_schema["description"],
            "input": method_schema["input"],
            "output": method_schema["output"],
        }
    
    # Output as YAML
    try:
        import yaml
        print(yaml.dump(manifest, sort_keys=False, default_flow_style=False))
    except ImportError:
        # Fallback to JSON if PyYAML not installed
        print(json.dumps(manifest, indent=2))


def cmd_test(args):
    """Test a service locally."""
    from .service import Service
    from .method import is_method
    
    service_class = load_service_from_file(args.file)
    service = service_class()
    service.setup()
    
    print(f"Service: {service.name} v{service.version}")
    print(f"Methods: {', '.join(service._list_methods())}")
    print()
    
    if args.method:
        # Call a specific method
        method_name = args.method
        method = service._get_method(method_name)
        
        # Parse params from remaining args
        params = {}
        for param in args.params:
            if '=' in param:
                key, value = param.split('=', 1)
                # Try to parse as JSON, otherwise use string
                try:
                    params[key] = json.loads(value)
                except json.JSONDecodeError:
                    params[key] = value
        
        print(f"Calling {method_name}({params})")
        result = method(**params)
        print(f"Result: {json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result}")
    else:
        print("Use --method <name> to call a method, e.g.:")
        print(f"  jb-service test {args.file} --method add a=1 b=2")
    
    service.teardown()


def cmd_init(args):
    """Scaffold a new service."""
    name = args.name
    path = Path(name)
    
    if path.exists():
        print(f"Error: {path} already exists", file=sys.stderr)
        sys.exit(1)
    
    path.mkdir()
    
    # Create main.py
    main_py = f'''"""
{name} service.
"""
from jb_service import Service, method, run


class {name.title().replace("-", "").replace("_", "")}(Service):
    """{name.title()} service."""
    
    name = "{name}"
    version = "0.1.0"
    
    def setup(self):
        """Initialize resources."""
        self.log.info("Starting up...")
    
    @method
    def hello(self, name: str = "world") -> dict:
        """Say hello.
        
        Args:
            name: Who to greet
            
        Returns:
            Greeting message
        """
        return {{"message": f"Hello, {{name}}!"}}


if __name__ == "__main__":
    run({name.title().replace("-", "").replace("_", "")})
'''
    
    (path / "main.py").write_text(main_py)
    
    # Create jumpboot.yaml
    manifest = f'''name: {name}
version: 0.1.0
description: {name.title()} service

runtime:
  python: "3.10"
  packages:
    - jb-service
'''
    
    (path / "jumpboot.yaml").write_text(manifest)
    
    print(f"Created {path}/")
    print(f"  main.py        - Service implementation")
    print(f"  jumpboot.yaml  - Manifest (customize runtime.packages)")
    print()
    print(f"Install with: jb-mesh install ./{name}")


def main():
    parser = argparse.ArgumentParser(
        description="jb-service: Python SDK for jb-mesh"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # manifest command
    manifest_parser = subparsers.add_parser(
        "manifest", 
        help="Generate jumpboot.yaml from a service"
    )
    manifest_parser.add_argument("file", help="Python file containing the service")
    manifest_parser.set_defaults(func=cmd_manifest)
    
    # test command
    test_parser = subparsers.add_parser(
        "test",
        help="Test a service locally"
    )
    test_parser.add_argument("file", help="Python file containing the service")
    test_parser.add_argument("--method", "-m", help="Method to call")
    test_parser.add_argument("params", nargs="*", help="Parameters as key=value")
    test_parser.set_defaults(func=cmd_test)
    
    # init command
    init_parser = subparsers.add_parser(
        "init",
        help="Scaffold a new service"
    )
    init_parser.add_argument("name", help="Service name")
    init_parser.set_defaults(func=cmd_init)
    
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
