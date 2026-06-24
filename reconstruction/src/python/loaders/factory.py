"""
Factory class to generate the appropriate Elasticsearch ingestor.
"""

import logging
from .elasticsearch import ElasticsearchBulkIngestor

logger = logging.getLogger(__name__)

class LoaderFactory:
    """
    Orchestrates the selection of the loading strategy.
    """
    @staticmethod
    def get_loader(load_type: str, connection, config: dict):
        """
        Returns an instance of a specific ingestor.

        Args:
            load_type (str): 'elasticsearch'.
            connection (Elasticsearch): The established ES client.
            config (dict): Configuration for index settings and mappings.

        Returns:
            ElasticsearchBulkIngestor: An initialized BulkIngestor.

        Raises:
            ValueError: If the load_type is unsupported.
        """
        logger.info(f"LoaderFactory creating '{load_type}' loader.")
        load_type = load_type.lower().strip()

        if load_type == "elasticsearch":
            return ElasticsearchBulkIngestor(connection=connection, config=config)
        else:
            error_msg = f"Loader type '{load_type}' is not supported."
            logger.error(error_msg)
            raise ValueError(error_msg)