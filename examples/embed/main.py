"""
Text embedding service using sentence-transformers.
Modernized for jb-mesh with jb-service SDK.
"""

import os
import logging
from typing import Optional

# Suppress noise
os.environ["TOKENIZERS_PARALLELISM"] = "false"
logging.getLogger("sentence_transformers").setLevel(logging.WARNING)

from jb_service import Service, method, run


class EmbedService(Service):
    """Text embedding service using sentence-transformers."""

    name = "embed"
    version = "2.0.0"

    def setup(self):
        """Load the default embedding model."""
        from sentence_transformers import SentenceTransformer

        self.model_name = os.environ.get("EMBED_MODEL", "all-MiniLM-L6-v2")
        self.log.info(f"Loading model: {self.model_name}")
        self.model = SentenceTransformer(self.model_name)
        self.dimensions = self.model.get_sentence_embedding_dimension()
        self.log.info(f"Model loaded: {self.model_name} ({self.dimensions}d)")

    @method
    def embed(self, text: str) -> dict:
        """Generate embedding for a single text.

        Args:
            text: Text to embed

        Returns:
            Dictionary with embedding vector and dimensions
        """
        embedding = self.model.encode(text, convert_to_numpy=True)
        return {
            "embedding": embedding.tolist(),
            "dimensions": self.dimensions,
        }

    @method
    def embed_batch(self, texts: list) -> dict:
        """Generate embeddings for multiple texts.

        Args:
            texts: List of texts to embed

        Returns:
            Dictionary with list of embeddings and count
        """
        embeddings = self.model.encode(texts, convert_to_numpy=True)
        return {
            "embeddings": embeddings.tolist(),
            "count": len(embeddings),
        }

    @method
    def info(self) -> dict:
        """Get model information."""
        return {
            "model": self.model_name,
            "dimensions": self.dimensions,
        }

    @method
    def switch_model(self, model: str) -> dict:
        """Switch to a different embedding model.

        Args:
            model: HuggingFace model name (e.g. 'all-mpnet-base-v2')
        """
        from sentence_transformers import SentenceTransformer

        self.log.info(f"Switching model: {self.model_name} → {model}")
        self.model = SentenceTransformer(model)
        self.model_name = model
        self.dimensions = self.model.get_sentence_embedding_dimension()
        self.log.info(f"Model loaded: {self.model_name} ({self.dimensions}d)")
        return {
            "model": self.model_name,
            "dimensions": self.dimensions,
        }

    @method
    def health(self) -> dict:
        """Health check — verifies model is loaded and can encode."""
        try:
            _ = self.model.encode("health check", convert_to_numpy=True)
            return {"status": "ok", "model": self.model_name}
        except Exception as e:
            return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    run(EmbedService)
