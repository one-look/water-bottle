import logging
from typing import Dict, List, Any
from .base import EmbedderBase

logger = logging.getLogger(__name__)

# --- Lazy Loading for Optional Dependency ---
_EMB_ENGINE = None
_TXTAI_AVAILABLE = None

def _check_and_import_txtai():
    """Lazily imports txtai and caches the result."""
    global _EMB_ENGINE, _TXTAI_AVAILABLE
    
    # Check only once
    if _TXTAI_AVAILABLE is None:
        try:
            from txtai.embeddings import Embeddings
            _EMB_ENGINE = Embeddings
            _TXTAI_AVAILABLE = True
        except ImportError:
            _TXTAI_AVAILABLE = False
    return _TXTAI_AVAILABLE

class TxtaiEmbedder(EmbedderBase):
    """txtai-based embedder implementation for Telegram pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize txtai embedder.
        
        Args:
            config: Configuration dictionary with model_name and other settings
        """
        self.model_name = config.get("model_name", "sentence-transformers/all-MiniLM-L6-v2")
        self.content = config.get("content", True)
        
        if not _check_and_import_txtai():
            raise ImportError("txtai is not installed. Install with: pip install txtai")
        
        # Initialize txtai embeddings - match ETL config format
        self.engine = _EMB_ENGINE({
            "path": self.model_name,
            "content": self.content
        })
        logger.info(f"Initialized txtai embedder with model: {self.model_name}")
    
    def __call__(self, text: str) -> List[float]:
        """Make the embedder callable for compatibility.
        
        Args:
            text: Input text to embed
            
        Returns:
            List of float values representing the embedding
        """
        return self.embed(text)
    
    def embed(self, text: str) -> List[float]:
        """Generate embedding for given text.
        
        Args:
            text: Input text to embed
            
        Returns:
            List of float values representing the embedding
        """
        try:
            # txtai returns numpy array, convert to list
            embedding = self.engine.transform(text)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise
    
    def query(self, text: str) -> List[float]:
        """Generate embedding for query (alias for embed method).
        
        Args:
            text: Query text to embed
            
        Returns:
            List of float values representing the embedding
        """
        return self.embed(text)
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts.
        
        Args:
            texts: List of input texts to embed
            
        Returns:
            List of embedding vectors
        """
        try:
            embeddings = self.engine.transform(texts)
            return [emb.tolist() for emb in embeddings]
        except Exception as e:
            logger.error(f"Failed to generate batch embeddings: {e}")
            raise