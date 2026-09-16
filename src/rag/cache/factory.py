"""Factory module for instantiating semantic cache implementations."""

from typing import Any, Dict

from src.core.logging import setup_logger
from src.rag.cache.redis import RedisCacheConfig, SemanticCache

logger = setup_logger(__name__)


class CacheFactory:
    """Factory class responsible solely for instantiating semantic cache objects."""

    @staticmethod
    def create(config: Dict[str, Any]) -> SemanticCache:
        """Instantiates and returns a semantic cache instance based on configuration.

        Args:
            config: The application configuration dictionary containing a 'cache' or 'redis' section.

        Returns:
            SemanticCache: An instantiated semantic cache instance.
        """
        try:
            cache_config = config.get("cache", {}) or config.get("redis", {})
            if not cache_config:
                logger.warning(
                    "'cache' or 'redis' key not found or empty in configuration. "
                    "Falling back to default Redis settings."
                )

            provider_name = cache_config.get("provider", "redis").lower()
            logger.info(f"Initializing semantic cache provider: '{provider_name}'")

            if provider_name == "redis":
                validated_config = RedisCacheConfig(**cache_config)
                cache_instance = SemanticCache(config=validated_config)
                logger.info("Successfully instantiated SemanticCache.")
                return cache_instance

            error_msg = f"Unsupported cache provider requested: '{provider_name}'"
            logger.error(error_msg)
            raise ValueError(error_msg)

        except (ValueError, KeyError):
            raise

        except Exception as e:
            logger.error(f"Failed to create cache instance: {str(e)}", exc_info=True)
            raise RuntimeError(f"CacheFactory failed to initialize provider: {str(e)}") from e