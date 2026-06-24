"""
Implementation of the Factory pattern to route requests to specific 
connector classes based on a string identifier.
"""

import logging
from typing import Any

from .rdbms import RDBMSConnector
from .elasticsearch import ElasticsearchConnector

logger = logging.getLogger(__name__)

class ConnectorFactory:
    """
    The Orchestrator class that selects the appropriate connector 
    class based on user input.
    """

    @classmethod
    def get_connector(cls, connector_type: str, config: Any):
        """
        Instantiates the requested connector class using the provided config.

        Args:
            connector_type (str): The type identifier ('rdbms').
            config (Any): Configuration data required by the chosen connector.

        Returns:
            Object: An instance of the requested connector class.
            
        Raises:
            ValueError: If the connector_type is not recognized.
        """
        logger.info(f"Factory creating connector for: {connector_type}")
        
        connector_type = connector_type.lower()
        
        if connector_type == "rdbms":
            return RDBMSConnector(config=config)
        elif connector_type == "elasticsearch":
            return ElasticsearchConnector(config=config)
        else:
            error_msg = f"Unsupported connector type: {connector_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)