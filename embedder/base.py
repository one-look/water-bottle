from abc import ABC, abstractmethod
from typing import List


class EmbedderBase(ABC):
    """Abstract base class for text embedding services."""
    
    @abstractmethod
    def embed(self, text: str) -> List[float]:
        """Convert text to vector embedding.
        
        Args:
            text: Input text to embed
            
        Returns:
            List of float values representing the embedding vector
        """
        raise NotImplementedError("embed method must be implemented")
