from typing import Dict, Any
from .base import GeneratorBase
from .litellm import LiteLLMGenerator


class GeneratorFactory:
    """Factory for creating generator instances."""
    
    @staticmethod
    def create(config: Dict[str, Any], services: Dict[str, Any]) -> GeneratorBase:
        """Create a generator instance based on configuration.
        
        Args:
            config: Configuration dictionary containing generator settings
            services: Dictionary of available services
            
        Returns:
            GeneratorBase: Configured generator instance
            
        Raises:
            ValueError: If generator type is not supported
        """
        generator_type = config.get("type", "litellm")
        
        if generator_type == "litellm":
            return LiteLLMGenerator(config)
        else:
            raise ValueError(f"Unsupported generator type: {generator_type}")
