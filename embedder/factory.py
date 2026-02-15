from typing import Dict, Any
from .base import EmbedderBase
from .huggingface import HuggingFaceEmbedder


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
        embedder_type = config.get("type", "huggingface")
        
        if embedder_type == "huggingface":
            return HuggingFaceEmbedder(config)
        else:
            raise ValueError(f"Unsupported embedder type: {embedder_type}")
