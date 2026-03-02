"""
Implementation of the Factory pattern to route requests to specific 
embedder classes based on a string identifier.
"""

import logging
from typing import Any

from .txtai import TxtaiEmbeddings

logger = logging.getLogger(__name__)

class EmbedderFactory:
    """
    The Orchestrator class that selects the appropriate embedder 
    class based on user input.
    """

    @classmethod
    def get_embedder(cls, embedder_type: str, data: Any, config: Any):
        """
        Instantiates the requested embedder class using the provided config.

        Args:
            embedder_type (str): The type identifier ('esai', 'jina', etc.).
            data (Any): Data to be embedded.
            config (Any): Configuration data required by the chosen embedder.

        Returns:
            Object: An instance of the requested embedder class.
            
        Raises:
            ValueError: If the embedder_type is not recognized.
        """
        logger.info(f"Factory creating embedder for: {embedder_type}")
        
        embedder_type = embedder_type.lower()
        
        if embedder_type == "txtai":
            return TxtaiEmbeddings(data=data, config=config)
        else:
            error_msg = f"Unsupported embedder type: {embedder_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)