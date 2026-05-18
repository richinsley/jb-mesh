"""
Basic tests for jb-service.
"""
import pytest
from jb_service import Service, method
from jb_service.schema import method_to_schema, service_to_schema


class Calculator(Service):
    """A test calculator service."""
    
    name = "calculator"
    version = "1.0.0"
    
    @method
    def add(self, a: float, b: float) -> float:
        """Add two numbers.
        
        Args:
            a: First number
            b: Second number
            
        Returns:
            Sum of a and b
        """
        return a + b
    
    @method
    def divide(self, a: float, b: float = 1.0) -> float:
        """Divide two numbers."""
        return a / b


class TestMethod:
    def test_method_decorator(self):
        """Test that @method marks functions correctly."""
        from jb_service.method import is_method
        
        calc = Calculator()
        assert is_method(calc.add)
        assert is_method(calc.divide)
        assert not is_method(calc.setup)
    
    def test_method_call(self):
        """Test that methods are callable."""
        calc = Calculator()
        assert calc.add(1, 2) == 3
        assert calc.divide(10, 2) == 5
        assert calc.divide(10) == 10  # default b=1.0


class TestSchema:
    def test_method_schema(self):
        """Test schema generation for a method."""
        calc = Calculator()
        schema = method_to_schema(calc.add)
        
        assert schema["name"] == "add"
        assert "Add two numbers" in schema["description"]
        assert schema["input"]["properties"]["a"]["type"] == "number"
        assert schema["input"]["properties"]["b"]["type"] == "number"
        assert schema["input"]["required"] == ["a", "b"]
    
    def test_method_schema_with_default(self):
        """Test schema generation with default values."""
        calc = Calculator()
        schema = method_to_schema(calc.divide)
        
        assert "a" in schema["input"]["required"]
        assert "b" not in schema["input"]["required"]
        assert schema["input"]["properties"]["b"]["default"] == 1.0
    
    def test_service_schema(self):
        """Test schema generation for a service."""
        schema = service_to_schema(Calculator)
        
        assert schema["name"] == "calculator"
        assert schema["version"] == "1.0.0"
        assert "test calculator" in schema["description"].lower()
        assert "add" in schema["methods"]
        assert "divide" in schema["methods"]


class TestService:
    def test_service_init(self):
        """Test service initialization."""
        calc = Calculator()
        assert calc.name == "calculator"
        assert calc.version == "1.0.0"
        assert "add" in calc._methods
        assert "divide" in calc._methods
    
    def test_service_logger(self):
        """Test that logger is available."""
        calc = Calculator()
        assert hasattr(calc, 'log')
        # Just verify it doesn't crash
        calc.log.info("test message")
    
    def test_default_name(self):
        """Test that name defaults to class name."""
        class MyService(Service):
            @method
            def foo(self) -> str:
                return "bar"
        
        svc = MyService()
        assert svc.name == "myservice"
