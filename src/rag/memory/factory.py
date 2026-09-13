"""Factory module for instantiating conversation memory implementations."""

from typing import Any, Dict

from src.core.logging import setup_logger
from src.rag.memory.redis import RedisConversationMemory, RedisMemoryConfig

logger = setup_logger(__name__)


class MemoryFactory:
    '''
    Factory class responsible solely for instantiating memory store objects.
    '''

    @staticmethod
    def create(config: Dict[str, Any]) -> RedisConversationMemory:
        '''
        Instantiates and returns a memory store based on configuration.

        Args:
            config: The application configuration dictionary containing a 'redis' section.

        Returns:
            RedisConversationMemory: An instantiated memory store instance.
        '''
        try:
            memory_config = config.get("redis", {})
            if not memory_config:
                logger.warning(
                    "'redis' key not found or empty in configuration. "
                    "Falling back to default Redis settings."
                )

            provider_name = memory_config.get("provider", "redis").lower()
            logger.info(f"Initializing conversation memory provider: '{provider_name}'")

            if provider_name == "redis":
                validated_config = RedisMemoryConfig(**memory_config)
                memory_instance = RedisConversationMemory(config=validated_config)
                logger.info("Successfully instantiated RedisConversationMemory.")
                return memory_instance

            error_msg = f"Unsupported memory provider requested: '{provider_name}'"
            logger.error(error_msg)
            raise ValueError(error_msg)

        except (ValueError, KeyError):
            raise

        except Exception as e:
            logger.error(f"Failed to create memory instance: {str(e)}", exc_info=True)
            raise RuntimeError(f"MemoryFactory failed to initialize provider: {str(e)}") from e
