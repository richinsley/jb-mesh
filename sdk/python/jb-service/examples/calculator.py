"""
Example: Simple calculator service.
"""
from jb_service import Service, method, run


class Calculator(Service):
    """A simple calculator service."""
    
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
    def subtract(self, a: float, b: float) -> float:
        """Subtract b from a."""
        return a - b
    
    @method
    def multiply(self, a: float, b: float) -> float:
        """Multiply two numbers."""
        return a * b
    
    @method
    def divide(self, a: float, b: float = 1.0) -> float:
        """Divide a by b.
        
        Args:
            a: Dividend
            b: Divisor (default: 1.0)
        """
        if b == 0:
            raise ValueError("Cannot divide by zero")
        return a / b


if __name__ == "__main__":
    run(Calculator)
