import logging
from typing import List, Dict, Any
import google.generativeai as genai 
import os
from .base import EmbedderBase

logger = logging.getLogger(__name__)


class GeminiEmbedder(EmbedderBase):
    """Google Gemini embedder implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Gemini embedder.
        
        Args:
            config: Configuration dictionary with model_name and other settings
        """
        self.model_name = config.get("model_name", "gemini-embedding-001")
        self.task_type = config.get("task_type", "retrieval_query")
        
        # Configure API key
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")
        
        # Configure the API
        genai.configure(api_key=api_key)
        logger.info(f"Initialized GeminiEmbedder with model: {self.model_name}")
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts.
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            List of embedding vectors
        """
        try:
            embeddings = []
            for text in texts:
                response = genai.embed_content(
                    model=self.model_name,
                    content=text,
                    task_type=self.task_type
                )
                embeddings.append(response['embedding'])
            
            logger.info(f"Generated embeddings for {len(texts)} texts using Gemini")
            return embeddings
            
        except Exception as e:
            logger.error(f"Gemini embedding failed: {e}")
            raise
    
    def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text.
        
        Args:
            text: Text string to embed
            
        Returns:
            Embedding vector
        """
        try:
            response = genai.embed_content(
                model=self.model_name,
                content=text,
                task_type=self.task_type
            )
            
            embedding = response['embedding']
            logger.debug(f"Generated embedding for text using Gemini")
            return embedding
            
        except Exception as e:
            logger.error(f"Gemini embedding failed: {e}")
            raise
