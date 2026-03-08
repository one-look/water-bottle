from typing import Dict, Any
from .base import EmbedderBase
# from .huggingface import HuggingFaceEmbedder
# Lazy import to avoid import errors when google-generativeai is not installed


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
        
        if embedder_type == "txtai":
            from .txtai import TxtaiEmbedder
            return TxtaiEmbedder(config)
        elif embedder_type == "huggingface":
            # return HuggingFaceEmbedder(config)
            raise ValueError(f"HuggingFace embedder not yet implemented")
        elif embedder_type == "gemini":
            # Lazy import to avoid import errors when google-generativeai is not installed
            try:
                from .gemini import GeminiEmbedder
                return GeminiEmbedder(config)
            except ImportError as e:
                raise ValueError(f"Gemini embedder requires google-generativeai package: {e}")
        else:
            raise ValueError(f"Unsupported embedder type: {embedder_type}")
