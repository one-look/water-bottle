"""
Implementation of the Factory pattern to route requests to specific 
connector classes based on a string identifier.
"""

import logging
from typing import Any

from .database import DatabaseConnector
from .elasticsearch import ElasticsearchConnector
from .pinecone import PineconeConnector

logger = logging.getLogger(__name__)

class ConnectorFactory:
    """
    The Orchestrator class that selects the appropriate connector 
    class based on user input.
    """

    @classmethod
    def create(cls, connector_type: str, credential_provider):
        """
        Instantiates the requested connector class using the provided credential provider.

        Args:
            connector_type (str): The type identifier ('rdbms').
            credential_provider: Credential provider instance with get_credentials() method.

        Returns:
            Object: An instance of the requested connector class.
            
        Raises:
            ValueError: If the connector_type is not recognized.
        """
        logger.info(f"Factory creating connector for: {connector_type}")
        
        # Get actual config from credential provider
        config = credential_provider.get_credentials()
        
        connector_type = connector_type.lower()
        
        if connector_type == "database":
            return DatabaseConnector(config=config)
        elif connector_type == "elasticsearch":
            return ElasticsearchConnector(config=config)
        elif connector_type == "pinecone":
            return PineconeConnector(config=config)
        else:
            error_msg = f"Unsupported connector type: {connector_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)