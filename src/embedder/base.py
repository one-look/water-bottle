from abc import ABC, abstractmethod
from typing import List

class BaseEmbedder(ABC):
    """Abstract base class for text embedding services."""
    
    @abstractmethod
    def embed(self, text: str) -> List[float]:
        """Convert text to vector embedding."""
        pass