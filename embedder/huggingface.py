import logging
import time
from typing import List, Dict, Any
import asyncio
from sentence_transformers import SentenceTransformer
from .base import EmbedderBase

logger = logging.getLogger("water-bottle.embedder.huggingface")


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
        logger.info("action=initialize embedder=huggingface model=%s device=%s", self.model_name, self.device)
    
    @property
    def model(self) -> SentenceTransformer:
        """Lazy load the model."""
        if self._model is None:
            logger.info("action=load_model embedder=huggingface model=%s device=%s", self.model_name, self.device)
            self._model = SentenceTransformer(self.model_name, device=self.device)
            logger.info("action=load_model_complete embedder=huggingface model=%s", self.model_name)
        return self._model
    
    async def embed(self, text: str) -> List[float]:
        """Convert text to vector embedding.
        
        Args:
            text: Input text to embed
            
        Returns:
            List of float values representing the embedding vector
        """
        start_time = time.time()
        logger.info("action=embed embedder=huggingface text_length=%d model=%s", len(text), self.model_name)
        
        try:
            embedding = await asyncio.to_thread(self.model.encode, text, convert_to_numpy=True)
            duration = time.time() - start_time
            logger.info("action=embed_complete embedder=huggingface text_length=%d embedding_dim=%d duration=%.3fs model=%s", len(text), len(embedding), duration, self.model_name)
            return embedding.tolist()
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error("action=embed_failed embedder=huggingface text_length=%d duration=%.3fs error=%s", len(text), duration, str(e))
            raise
