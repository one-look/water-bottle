"""Semantic cache service utilizing standard Upstash-compatible keys."""

import hashlib
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, validate_call
import redis.asyncio as redis

from src.config.settings import settings
from src.core.logging import setup_logger
from src.core.redis import RedisConfig, redis_manager

logger = setup_logger(__name__)


class UpstashCacheConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    url: str = Field(
        default_factory=lambda: settings.REDIS_URL,
        min_length=1,
    )
    ttl_seconds: int = Field(604800, ge=1)
    similarity_threshold: float = Field(0.92, ge=0.0, le=1.0)
    vector_dim: int = Field(3072, ge=1)


class UpstashSemanticCache:
    """Handles query caching for Upstash Redis compatibility."""

    @validate_call
    def __init__(self, config: UpstashCacheConfig) -> None:
        redis_manager.init_client(RedisConfig(url=config.url))
        self.redis_client: redis.Redis = redis.from_url(config.url, decode_responses=True)
        self.ttl_seconds = config.ttl_seconds

    def _cache_key(self, tenant_id: str, user_query: str) -> str:
        clean_query = user_query.strip().lower()
        query_hash = hashlib.sha256(clean_query.encode("utf-8")).hexdigest()
        return f"cache:{tenant_id}:{query_hash}"

    async def get(
        self,
        query_vector: list[float],
        tenant_id: str,
        user_query: Optional[str] = None,
    ) -> Optional[str]:
        try:
            if user_query:
                cache_key = self._cache_key(tenant_id, user_query)
                cached_response = await self.redis_client.hget(cache_key, "response")
                if cached_response:
                    logger.info(f"Cache HIT for tenant '{tenant_id}'")
                    return cached_response if isinstance(cached_response, str) else cached_response.decode("utf-8")

            logger.info(f"Cache MISS for tenant '{tenant_id}'")
            return None
        except Exception as e:
            logger.warning(f"Cache lookup error: {str(e)}. Proceeding as MISS.")
            return None

    async def set(
        self,
        query_vector: list[float],
        user_query: str,
        response_text: str,
        tenant_id: str,
    ) -> None:
        try:
            cache_key = self._cache_key(tenant_id, user_query)
            await self.redis_client.hset(
                cache_key,
                mapping={
                    "tenant_id": tenant_id,
                    "user_query": user_query,
                    "response": response_text,
                },
            )
            await self.redis_client.expire(cache_key, self.ttl_seconds)
            logger.info(f"Saved response to cache under key '{cache_key}'")
        except Exception as e:
            logger.error(f"Failed to write cache entry: {str(e)}")