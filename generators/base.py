from abc import ABC, abstractmethod
from typing import List, Dict, Any


class GeneratorBase(ABC):
    """Abstract base class for text generation services."""
    
    @abstractmethod
    def generate(self, prompt: str, **kwargs) -> str:
        """Generate text response based on prompt.
        
        Args:
            prompt: Input prompt for generation
            **kwargs: Additional generation parameters
            
        Returns:
            Generated text response
        """
        raise NotImplementedError("generate method must be implemented")
