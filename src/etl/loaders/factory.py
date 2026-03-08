"""
Factory class to generate the appropriate ingestor.
"""

import logging
from .elasticsearch import ElasticsearchBulkIngestor
from .pinecone import PineconeIngestor

logger = logging.getLogger(__name__)

class LoaderFactory:
    """
    Orchestrates the selection of the loading strategy.
    """
    @staticmethod
    def create(load_type: str, connection, config: dict):
        """
        Returns an instance of a specific ingestor.

        Args:
            load_type (str): 'elasticsearch' or 'pinecone'.
            connection: The established client connection.
            config (dict): Configuration for the ingestor.

        Returns:
            An initialized ingestor instance.

        Raises:
            ValueError: If the load_type is unsupported.
        """
        logger.info(f"LoaderFactory creating '{load_type}' loader.")
        load_type = load_type.lower().strip()

        if load_type == "elasticsearch":
            return ElasticsearchBulkIngestor(connection=connection, config=config)
        elif load_type == "pinecone":
            return PineconeIngestor(connection=connection, config=config)
        else:
            error_msg = f"Loader type '{load_type}' is not supported."
            logger.error(error_msg)
            raise ValueError(error_msg)