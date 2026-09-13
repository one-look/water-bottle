"""Centralized Redis client management with connection pooling."""

from typing import Optional
import redis.asyncio as redis
from pydantic import BaseModel, Field, validate_call

from src.core.logging import setup_logger

logger = setup_logger(__name__)


class RedisConfig(BaseModel):
    '''
    Redis connection configuration.
    '''

    url: str = Field(..., description="The URL of the Redis server.")


class RedisManager:
    '''
    Manages Redis connection pool lifecycle.
    '''

    def __init__(self) -> None:
        self._redis_client: Optional[redis.Redis] = None

    @validate_call
    def init_client(self, config: RedisConfig) -> None:
        '''
        Initializes the Redis client connection pool if it does not already exist.
        
        Args:
            config: The Redis configuration.

        Returns:
            None
        '''
        if not self._redis_client:
            logger.info("Initializing Redis connection pool...")
            self._redis_client = redis.from_url(
                config.url,
                encoding="utf-8",
                decode_responses=True,
            )

    def get_client(self) -> redis.Redis:
        '''
        Returns the initialized Redis client instance.

        Returns:
            The initialized Redis client instance.
        '''
        if not self._redis_client:
            raise RuntimeError("Redis client is not initialized. Call init_client() first.")
        return self._redis_client

    async def close(self) -> None:
        '''
        Closes the connection pool.

        Returns:
            None
        '''
        if self._redis_client:
            logger.info("Closing Redis connection pool...")
            await self._redis_client.close()
            self._redis_client = None


redis_manager = RedisManager()
