from typing import Dict, Any
from .base import RetrieverBase
from .elasticsearch import ElasticsearchRetriever


class RetrieverFactory:
    """Factory for creating retriever instances."""
    
    @staticmethod
    def create(config: Dict[str, Any], services: Dict[str, Any]) -> RetrieverBase:
        """Create a retriever instance based on configuration.
        
        Args:
            config: Configuration dictionary containing retriever settings
            services: Dictionary of available services (e.g., elasticsearch connection)
            
        Returns:
            RetrieverBase: Configured retriever instance
            
        Raises:
            ValueError: If retriever type is not supported
        """
        retriever_type = config.get("type", "elasticsearch")
        
        if retriever_type == "elasticsearch":
            es_connection = services.get("elasticsearch")
            if not es_connection:
                raise ValueError("Elasticsearch connection required for Elasticsearch retriever")
            return ElasticsearchRetriever(config, es_connection)
        else:
            raise ValueError(f"Unsupported retriever type: {retriever_type}")
