"""Factory module for instantiating document extractor provider implementations."""

from typing import Any, Dict

from src.etl.core.config import AWSConfig
from src.etl.core.exceptions import ExtractionError
from src.etl.core.logger import setup_logger
from src.etl.infrastructure.extractors.extractors import S3DocumentExtractor
from src.etl.infrastructure.extractors.webextractor import WebExtractor

logger = setup_logger(__name__)


class ExtractorFactory:
    """Factory class responsible for creating configured document extractor instances."""

    @staticmethod
    def create(config: Dict[str, Any]) -> Any:
        """Instantiates and returns a document extractor based on the application configuration.

        Args:
            config: General application settings dictionary containing extraction parameters.

        Returns:
            An instantiated extractor class implementing the BaseExtractor interface (e.g., S3DocumentExtractor or WebExtractor).

        Raises:
            KeyError: If required configuration keys are missing.
            ValueError: If an unsupported extractor provider string is specified.
            ExtractionError: If initialization of the extractor instance fails.
        """
        try:
            extraction_config = config.get("extraction", {})
            if not extraction_config:
                logger.warning("'extraction' key not found or empty in configuration. Falling back to default S3 settings.")

            provider_name = extraction_config.get("provider", "s3").lower()
            logger.info(f"Initializing document extractor provider: '{provider_name}'")

            if provider_name == "s3":
                aws_data = extraction_config.get("aws", config.get("aws", {}))
                aws_config = AWSConfig(**aws_data)
                provider_instance = S3DocumentExtractor(aws_config)
                logger.info("Successfully instantiated S3DocumentExtractor.")
                return provider_instance

            elif provider_name == "web":
                max_pages = extraction_config.get("max_pages", 150)
                provider_instance = WebExtractor(max_pages=max_pages)
                logger.info("Successfully instantiated WebExtractor.")
                return provider_instance

            else:
                error_msg = f"Unsupported extractor provider requested: '{provider_name}'"
                logger.error(error_msg)
                raise ValueError(error_msg)

        except (ValueError, KeyError) as e:
            # Re-raise direct configuration validation errors
            raise

        except Exception as e:
            logger.error(f"Failed to create extractor provider instance: {str(e)}", exc_info=True)
            raise ExtractionError(f"ExtractorFactory failed to initialize provider: {str(e)}") from e