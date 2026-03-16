from typing import Dict, Any
from .base import EmbedderBase
# from .huggingface import HuggingFaceEmbedder
from .gemini import GeminiEmbedder


class EmbedderFactory:
    """Factory for creating embedder instances."""
    
    @staticmethod
    def create(config: Dict[str, Any], services: Dict[str, Any]) -> EmbedderBase:
        """Create an embedder instance based on configuration.
        
        Args:
            config: Configuration dictionary containing embedder settings
            services: Dictionary of available services
            
        Returns:
            EmbedderBase: Configured embedder instance
            
        Raises:
            ValueError: If embedder type is not supported
        """
        embedder_type = config.get("type", "gemini")
        
        if embedder_type == "huggingface":
            # return HuggingFaceEmbedder(config)
            raise ValueError(f"HuggingFace embedder not yet implemented")
        elif embedder_type == "gemini":
            return GeminiEmbedder(config)
        else:
            raise ValueError(f"Unsupported embedder type: {embedder_type}")
