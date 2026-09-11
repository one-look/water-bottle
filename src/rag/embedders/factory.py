"""Factory module for instantiating vector query embedder implementations."""

from typing import Any, Dict

from src.config.settings import settings
from src.core.logging import setup_logger
from src.rag.embedders.gemini import EmbedderQueryConfig, GeminiQueryEmbedder

logger = setup_logger(__name__)


class EmbedderFactory:
    """Factory class responsible for creating configured query embedder instances."""

    @staticmethod
    def create(config: Dict[str, Any]) -> GeminiQueryEmbedder:
        """Instantiates and returns a query embedder based on configuration.

        Args:
            config: Application configuration dictionary containing an 'embedding' section.

        Returns:
            GeminiQueryEmbedder: An instantiated embedder class.

        Raises:
            ValueError: If an unsupported embedding provider is specified.
            RuntimeError: If initialization of the embedder instance fails.
        """
        try:
            embedding_config = config.get("embedding", {})
            if not embedding_config:
                logger.warning(
                    "'embedding' key not found or empty in configuration. "
                    "Falling back to default Gemini settings."
                )

            provider_name = embedding_config.get("provider", "gemini").lower()
            logger.info(f"Initializing query embedder provider: '{provider_name}'")

            if provider_name == "gemini":
                # Inject API key from global settings if missing in config dict
                if "api_key" not in embedding_config:
                    embedding_config["api_key"] = settings.GEMINI_API_KEY

                embedder_config = EmbedderQueryConfig(**embedding_config)
                provider_instance = GeminiQueryEmbedder(config=embedder_config)
                logger.info("Successfully instantiated GeminiQueryEmbedder.")
                return provider_instance

            error_msg = f"Unsupported embedding provider requested: '{provider_name}'"
            logger.error(error_msg)
            raise ValueError(error_msg)

        except (ValueError, KeyError):
            raise

        except Exception as e:
            logger.error(f"Failed to create embedder instance: {str(e)}", exc_info=True)
            raise RuntimeError(f"EmbedderFactory failed to initialize provider: {str(e)}") from e