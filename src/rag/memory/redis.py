"""Redis-backed conversation sliding window memory store with Pydantic validation."""

import json
from typing import Dict, List
from pydantic import BaseModel, Field, validate_call
import redis.asyncio as redis

from src.core.logging import setup_logger
from src.core.redis import RedisConfig, redis_manager

logger = setup_logger(__name__)


class RedisMemoryConfig(BaseModel):
    '''
    Configuration schema for Redis conversation memory instantiation.
    '''

    url: str = Field("redis://localhost:6379/0", min_length=1, description="Redis server URL")
    max_messages: int = Field(5, ge=1, description="Sliding window size")
    ttl_seconds: int = Field(86400, ge=1, description="Session TTL in seconds")


class RedisConversationMemory:
    '''
    Stores and retrieves sliding window chat history using Redis lists.
    '''

    @validate_call
    def __init__(self, config: RedisMemoryConfig) -> None:
        '''
        Initializes Redis via the shared pool and stores sliding-window settings.

        Args:
            config: The validated RedisMemoryConfig instance.
        '''

        redis_manager.init_client(RedisConfig(url=config.url))
        self.redis: redis.Redis = redis_manager.get_client()
        self.max_messages = config.max_messages
        self.ttl_seconds = config.ttl_seconds
        logger.info(
            "Initialized RedisConversationMemory "
            f"(max_messages={self.max_messages}, ttl_seconds={self.ttl_seconds})"
        )

    def _get_key(self, session_id: str) -> str:
        '''
        Generates tenant-isolated session key.

        Args:
            session_id: The session identifier.

        Returns:
            The tenant-isolated session key.
        '''
        return f"chat:session:{session_id}"

    async def get_history(self, session_id: str) -> List[Dict[str, str]]:
        '''
        Retrieves last N messages for a given session from Redis.

        Args:
            session_id: The session identifier.

        Returns:
            The last N messages for the given session.
        '''
        key = self._get_key(session_id)
        raw_messages = await self.redis.lrange(key, 0, -1)
        if not raw_messages:
            return []

        return [json.loads(msg) for msg in raw_messages]

    async def add_message(self, session_id: str, role: str, content: str) -> None:
        '''
        Appends a new message, trims sliding window (LTRIM), and resets key TTL.

        Args:
            session_id: The session identifier.
            role: The role of the message.
            content: The content of the message.
        '''
        key = self._get_key(session_id)
        message_data = json.dumps({"role": role, "content": content})

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.rpush(key, message_data)
            pipe.ltrim(key, -self.max_messages, -1)
            pipe.expire(key, self.ttl_seconds)
            await pipe.execute()

        logger.debug(f"Added '{role}' message to Redis session key '{key}'.")
