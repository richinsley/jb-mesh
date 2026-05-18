"""
Calculator service using jb-service SDK.
"""
from jb_service import Service, method, run


class Calculator(Service):
    """A simple calculator service."""
    
    name = "calculator"
    version = "1.0.0"
    
    def setup(self):
        self.log.info("Calculator starting up")
    
    @method
    def add(self, a: float, b: float) -> float:
        """Add two numbers.
        
        Args:
            a: First number
            b: Second number
        """
        self.log.debug(f"Adding {a} + {b}")
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
            b: Divisor (default 1.0)
        """
        if b == 0:
            raise ValueError("Cannot divide by zero")
        return a / b
    
    @method
    def health(self) -> dict:
        """Health check."""
        return {"status": "ok"}


if __name__ == "__main__":
    run(Calculator)
