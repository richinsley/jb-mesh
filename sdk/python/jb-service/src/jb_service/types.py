"""
File and mesh types for jb-service methods.

These types tell jb-service how to handle file inputs:
- FilePath: Pass as-is (string path)
- Audio: Load as (sample_rate, numpy_array)
- Image: Load as PIL.Image

Usage:
    from jb_service import Service, method
    from jb_service.types import FilePath, Audio, Image
    
    class MyService(Service):
        @method
        def process_audio(self, audio: Audio) -> dict:
            sample_rate, data = audio
            # data is numpy array
            ...
        
        @method
        def process_image(self, image: Image) -> dict:
            # image is PIL.Image
            width, height = image.size
            ...
        
        @method
        def process_file(self, file: FilePath) -> dict:
            # file is just the path string
            with open(file, 'rb') as f:
                ...
"""
from dataclasses import dataclass, asdict
from typing import NewType, Tuple, Union, TYPE_CHECKING
import os

@dataclass
class MeshFileRef:
    """Reference to a file stored in the mesh file store."""
    key: str                    # Object Store key
    content_type: str           # MIME type
    size: int                   # Size in bytes
    filename: str | None = None # Optional display name (basename of key if omitted)

    def to_dict(self) -> dict:
        """Convert to dict for __mesh_files__ injection."""
        d = {"key": self.key, "content_type": self.content_type, "size": self.size}
        if self.filename:
            d["filename"] = self.filename
        return d


# Type aliases for type hints
FilePath = NewType('FilePath', str)

# For Audio, the actual runtime type is a tuple (sample_rate, numpy_array)
# but we use a NewType for the annotation
if TYPE_CHECKING:
    import numpy as np
    from PIL import Image as PILImage
    Audio = Tuple[int, 'np.ndarray']
    Image = PILImage.Image
else:
    Audio = NewType('Audio', tuple)
    Image = NewType('Image', object)


def is_file_type(annotation) -> bool:
    """Check if an annotation is one of our file types."""
    # Handle NewType - check __supertype__ for the base type name
    type_name = getattr(annotation, '__name__', None)
    if type_name in ('FilePath', 'Audio', 'Image'):
        return True
    
    # Handle string annotations
    if isinstance(annotation, str):
        return annotation in ('FilePath', 'Audio', 'Image')
    
    return False


def get_file_type_name(annotation) -> str | None:
    """Get the name of a file type annotation."""
    type_name = getattr(annotation, '__name__', None)
    if type_name in ('FilePath', 'Audio', 'Image'):
        return type_name
    
    if isinstance(annotation, str) and annotation in ('FilePath', 'Audio', 'Image'):
        return annotation
    
    return None


def load_audio(path: str) -> tuple:
    """
    Load audio file as (sample_rate, numpy_array).
    
    Tries soundfile first (supports many formats), falls back to scipy.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Audio file not found: {path}")
    
    # Try soundfile first (handles more formats)
    try:
        import soundfile as sf
        data, sample_rate = sf.read(path)
        return (sample_rate, data)
    except ImportError:
        pass
    except Exception:
        pass
    
    # Fall back to scipy
    try:
        from scipy.io import wavfile
        sample_rate, data = wavfile.read(path)
        return (sample_rate, data)
    except ImportError:
        pass
    except Exception:
        pass
    
    # Last resort: try librosa
    try:
        import librosa
        data, sample_rate = librosa.load(path, sr=None)
        return (sample_rate, data)
    except ImportError:
        pass
    
    raise ImportError(
        "No audio library available. Install one of: soundfile, scipy, librosa\n"
        "  pip install soundfile\n"
        "  pip install scipy\n"
        "  pip install librosa"
    )


def load_image(path: str):
    """
    Load image file as PIL.Image.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Image file not found: {path}")
    
    try:
        from PIL import Image as PILImage
        return PILImage.open(path)
    except ImportError:
        raise ImportError(
            "PIL not available. Install it:\n"
            "  pip install Pillow"
        )


def convert_file_param(value: str, type_name: str):
    """
    Convert a file path to the appropriate type.
    
    Args:
        value: The file path string
        type_name: One of 'FilePath', 'Audio', 'Image'
    
    Returns:
        The converted value (path string, audio tuple, or PIL Image)
    """
    if type_name == 'FilePath':
        return value
    elif type_name == 'Audio':
        return load_audio(value)
    elif type_name == 'Image':
        return load_image(value)
    else:
        return value


# Output helpers

def save_image(image, format: str = "png", quality: int = 95) -> str:
    """
    Save a PIL Image or numpy array to a temp file and return the path.
    
    Use this to return images from methods - jb-mesh will wrap it as a FileRef.
    
    Args:
        image: PIL.Image or numpy array
        format: Output format (png, jpg, webp)
        quality: JPEG/WebP quality (1-100)
    
    Returns:
        Path to the saved file
    
    Example:
        @method
        def generate(self, prompt: str) -> dict:
            image = self.pipeline(prompt).images[0]
            return {"image": save_image(image, format="png")}
    """
    import tempfile
    import uuid
    
    # Convert numpy to PIL if needed
    try:
        import numpy as np
        if isinstance(image, np.ndarray):
            from PIL import Image as PILImage
            image = PILImage.fromarray(image)
    except ImportError:
        pass
    
    # Save to temp file
    ext = format.lower()
    if ext == "jpg":
        ext = "jpeg"
    
    filename = f"{uuid.uuid4()}.{format.lower()}"
    filepath = os.path.join(tempfile.gettempdir(), "jb-outputs", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    save_kwargs = {}
    if format.lower() in ("jpeg", "jpg", "webp"):
        save_kwargs["quality"] = quality
    
    image.save(filepath, **save_kwargs)
    return filepath


def save_audio(data, sample_rate: int, format: str = "wav") -> str:
    """
    Save audio data to a temp file and return the path.
    
    Args:
        data: numpy array of audio samples
        sample_rate: Sample rate in Hz
        format: Output format (wav, mp3, flac)
    
    Returns:
        Path to the saved file
    """
    import tempfile
    import uuid
    
    filename = f"{uuid.uuid4()}.{format.lower()}"
    filepath = os.path.join(tempfile.gettempdir(), "jb-outputs", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    try:
        import soundfile as sf
        sf.write(filepath, data, sample_rate)
        return filepath
    except ImportError:
        pass
    
    try:
        from scipy.io import wavfile
        import numpy as np
        # scipy expects int16 for wav
        if data.dtype in (np.float32, np.float64):
            data = (data * 32767).astype(np.int16)
        wavfile.write(filepath, sample_rate, data)
        return filepath
    except ImportError:
        pass
    
    raise ImportError(
        "No audio library available. Install one of: soundfile, scipy\n"
        "  pip install soundfile\n"
        "  pip install scipy"
    )
