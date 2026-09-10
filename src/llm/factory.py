"""Factory module for instantiating LLM provider implementations."""

from typing import Any, Dict

from src.core.logging import setup_logger
from src.llm.gemini import GeminiProvider

logger = setup_logger(__name__)


class LLMFactory:
    """Factory class responsible for creating configured LLM provider instances."""

    @staticmethod
    def create(config: Dict[str, Any]) -> GeminiProvider:
        """Instantiates and returns an LLM provider based on the application configuration.

        Args:
            config: General application settings dictionary containing an 'llm' sub-dictionary.

        Returns:
            An instantiated provider class implementing the LLM interface (e.g., GeminiProvider).

        Raises:
            KeyError: If the 'llm' key is missing from configuration.
            ValueError: If an unsupported LLM provider string is specified.
            RuntimeError: If initialization of the provider instance fails.
        """
        try:
            llm_config = config.get("llm", {})
            if not llm_config:
                logger.warning("'llm' key not found or empty in configuration. Falling back to default Gemini settings.")

            provider_name = llm_config.get("provider", "gemini").lower()
            logger.info(f"Initializing LLM provider: '{provider_name}'")

            if provider_name == "gemini":
                provider_instance = GeminiProvider(llm_config)
                logger.info("Successfully instantiated GeminiProvider.")
                return provider_instance
            else:
                error_msg = f"Unsupported LLM provider requested: '{provider_name}'"
                logger.error(error_msg)
                raise ValueError(error_msg)

        except (ValueError, KeyError) as e:
            # Re-raise direct configuration validation errors
            raise

        except Exception as e:
            logger.error(f"Failed to create LLM provider instance: {str(e)}", exc_info=True)
            raise RuntimeError(f"LLMFactory failed to initialize provider: {str(e)}") from e