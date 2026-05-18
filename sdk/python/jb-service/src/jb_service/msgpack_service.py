"""
MessagePack-based service for jb-mesh.

Use MessagePackService instead of Service when your code produces stdout output
(progress bars, logging, etc.) that would interfere with the REPL protocol.
"""
from .service import Service


class MessagePackService(Service):
    """
    Service that uses MessagePack queue transport instead of REPL.
    
    Use this for services that:
    - Have libraries that print to stdout (tqdm, diffusers, etc.)
    - Need verbose logging during execution
    - Do long-running operations with progress output
    
    Example:
        from jb_service import MessagePackService, method
        
        class ImageGenerator(MessagePackService):
            def setup(self):
                # Progress bars are fine - they don't interfere
                self.pipe = Pipeline.from_pretrained("model")
            
            @method
            def generate(self, prompt: str) -> dict:
                # tqdm progress bars are fine
                result = self.pipe(prompt)
                return {"image": save_image(result)}
    """
    
    # Marker for transport detection
    _transport = "msgpack"
