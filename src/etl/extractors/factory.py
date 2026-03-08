import logging
from typing import Any, Dict
from .database import DatabaseExtractor
from .web import WebExtractor

logger = logging.getLogger(__name__)

"""
factory.py
====================================
Purpose:
    Simplifies the creation of Extractor objects.
"""

class ExtractorFactory:
    """
    Purpose:
        Factory class to route requests to the correct Extractor 
        implementation based on type.
    """

    @staticmethod
    def get_extractor(extractor_type: str, connection: Any, config: Dict[str, Any]):
        """
        Purpose: 
            Returns an initialized extractor instance.

        Args:
            extractor_type (str): Type of extractor ('database').
            connection (Any): Active connection object from the connector layer.
            config (Dict[str, Any]): Extraction logic parameters.

        Returns:
            BaseExtractor: An instance of a specific extractor.

        Raises:
            ValueError: If the extractor type is not supported.
        """
        logger.info(f"ExtractorFactory generating '{extractor_type}' extractor.")
        extractor_type = extractor_type.lower().strip()

        if extractor_type == "database":
            return DatabaseExtractor(connection=connection, config=config)
        elif extractor_type == "web":
            return WebExtractor(connection=connection, config=config)
        else:
            error_msg = f"Unknown extractor type: {extractor_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)