from typing import List, Dict, Any
import asyncio
from sentence_transformers import SentenceTransformer
from .base import EmbedderBase


class HuggingFaceEmbedder(EmbedderBase):
    """HuggingFace SentenceTransformers embedder implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize HuggingFace embedder.
        
        Args:
            config: Configuration dictionary with model_name and other settings
        """
        self.model_name = config.get("model_name", "all-MiniLM-L6-v2")
        self.device = config.get("device", "cpu")
        self._model = None
    
    @property
    def model(self) -> SentenceTransformer:
        """Lazy load the model."""
        if self._model is None:
            self._model = SentenceTransformer(self.model_name, device=self.device)
        return self._model
    
    async def embed(self, text: str) -> List[float]:
        """Convert text to vector embedding.
        
        Args:
            text: Input text to embed
            
        Returns:
            List of float values representing the embedding vector
        """
        embedding = await asyncio.to_thread(self.model.encode, text, convert_to_numpy=True)
        return embedding.tolist()
