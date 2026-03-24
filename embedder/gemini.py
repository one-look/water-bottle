import logging
import time
from typing import List, Dict, Any
from google import genai
import os
import asyncio
from .base import EmbedderBase

logger = logging.getLogger("water-bottle.embedder.gemini")


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
        
        # Create client with API key
        self.client = genai.Client(api_key=api_key)
        logger.info("action=initialize embedder=gemini model=%s task_type=%s", self.model_name, self.task_type)
    
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts.
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            List of embedding vectors
        """
        start_time = time.time()
        logger.info("action=embed_batch embedder=gemini texts_count=%d model=%s", len(texts), self.model_name)
        
        try:
            embeddings = []
            for i, text in enumerate(texts):
                logger.debug("action=embed_single embedder=gemini text_index=%d text_length=%d", i, len(text))
                
                response = await asyncio.to_thread(
                    self.client.models.embed_content,
                    model=self.model_name,
                    contents=text,
                    config={"task_type": self.task_type}
                )
                embeddings.append(response.embeddings[0].values)
                logger.debug("action=embed_complete embedder=gemini text_index=%d embedding_dim=%d", i, len(embeddings[-1]))
            
            duration = time.time() - start_time
            logger.info("action=embed_batch_complete embedder=gemini texts_count=%d duration=%.3fs model=%s", len(texts), duration, self.model_name)
            return embeddings
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error("action=embed_batch_failed embedder=gemini texts_count=%d duration=%.3fs error=%s", len(texts), duration, str(e))
            raise
    
    async def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text.
        
        Args:
            text: Text string to embed
            
        Returns:
            Embedding vector
        """
        start_time = time.time()
        logger.info("action=embed embedder=gemini text_length=%d model=%s", len(text), self.model_name)
        
        try:
            response = await asyncio.to_thread(
                self.client.models.embed_content,
                model=self.model_name,
                contents=text,
                config={"task_type": self.task_type}
            )
            
            embedding = response.embeddings[0].values
            duration = time.time() - start_time
            logger.info("action=embed_complete embedder=gemini text_length=%d embedding_dim=%d duration=%.3fs model=%s", len(text), len(embedding), duration, self.model_name)
            return embedding
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error("action=embed_failed embedder=gemini text_length=%d duration=%.3fs error=%s", len(text), duration, str(e))
            raise
