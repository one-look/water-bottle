from typing import Dict, Any
from .base import RetrieverBase
from .pinecone import PineconeRetriever


class RetrieverFactory:
    """Factory for creating retriever instances."""
    
    @staticmethod
    def create(config: Dict[str, Any], services: Dict[str, Any]) -> RetrieverBase:
        """Create a retriever instance based on configuration.
        
        Args:
            config: Configuration dictionary containing retriever settings
            services: Dictionary of available services (e.g., pinecone connection)
            
        Returns:
            RetrieverBase: Configured retriever instance
            
        Raises:
            ValueError: If retriever type is not supported
        """
        retriever_type = config.get("type", "pinecone")
        
        if retriever_type == "pinecone":
            pinecone_connection = services.get("pinecone")
            if not pinecone_connection:
                raise ValueError("Pinecone connection required for Pinecone retriever. Please set PINECONE_API_KEY environment variable.")
            return PineconeRetriever(config, pinecone_connection)
        else:
            raise ValueError(f"Unsupported retriever type: {retriever_type}")
