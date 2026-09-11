"""Factory module for instantiating vector retriever services."""

from typing import Any, Dict

from src.config.settings import settings
from src.core.logging import setup_logger
from src.rag.retrievers.qdrant import QdrantRetriever, QdrantRetrieverConfig

logger = setup_logger(__name__)


class RetrieverFactory:
    """Factory class responsible for instantiating configured retriever services."""

    @staticmethod
    def create(config: Dict[str, Any]) -> QdrantRetriever:
        """Instantiates and returns a QdrantRetriever based on configuration dictionary.

        Args:
            config: Application settings dictionary containing a 'qdrant' sub-dictionary.

        Returns:
            QdrantRetriever: An instantiated Qdrant retriever instance.

        Raises:
            RuntimeError: If retriever initialization fails due to invalid parameters.
        """
        try:
            qdrant_config = config.get("qdrant", {})
            if not qdrant_config:
                logger.warning("'qdrant' section missing in configuration dictionary.")

            # Inject Qdrant URL from global environment settings
            qdrant_config["url"] = getattr(settings, "QDRANT_URL", "http://localhost:6333")

            validated_config = QdrantRetrieverConfig(**qdrant_config)
            retriever_instance = QdrantRetriever(config=validated_config)

            logger.info("Successfully instantiated QdrantRetriever via factory.")
            return retriever_instance

        except Exception as e:
            logger.error(f"Failed to create QdrantRetriever instance: {str(e)}", exc_info=True)
            raise RuntimeError(f"RetrieverFactory failed to initialize: {str(e)}") from e