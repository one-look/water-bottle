import logging
import time
import os
import hashlib
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

from .base import MemoryBase

logger = logging.getLogger("water-bottle.memory.redis_cache")


class RedisCacheMemory(MemoryBase):
    """Redis cache for query responses with fallback to underlying memory."""
    
    def __init__(self, underlying_memory: MemoryBase, redis_url: str = None, ttl: int = 7200):
        """
        Initialize Redis cache memory.
        
        Args:
            underlying_memory: Base memory implementation for fallback
            redis_url: Redis connection URL
            ttl: Cache TTL in seconds (default: 2 hours)
        """
        self.underlying_memory = underlying_memory
        self.ttl = ttl
        self.redis_client = None
        
        if not REDIS_AVAILABLE:
            logger.warning("action=redis_unavailable memory=redis_cache reason=module_not_found")
            return
            
        if redis_url:
            try:
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                logger.info("action=redis_connected memory=redis_cache ttl=%d", ttl)
            except Exception as e:
                logger.warning("action=redis_connection_failed memory=redis_cache error=%s", str(e))
        else:
            redis_env = os.getenv("REDIS_URL")
            if redis_env:
                try:
                    self.redis_client = redis.from_url(redis_env, decode_responses=True)
                    logger.info("action=redis_connected memory=redis_cache ttl=%d", ttl)
                except Exception as e:
                    logger.warning("action=redis_connection_failed memory=redis_cache error=%s", str(e))
            else:
                logger.info("action=redis_disabled memory=redis_cache reason=env_missing")
    
    def _get_cache_key(self, query: str) -> str:
        """Generate cache key for query."""
        normalized = query.strip().lower()
        return f"rag:cache:{hashlib.md5(normalized.encode()).hexdigest()}"
    
    async def get_history(self, session_id: str) -> list:
        """Get conversation history from underlying memory."""
        return await self.underlying_memory.get_history(session_id)
    
    async def add_message(self, session_id: str, role: str, content: str) -> None:
        """Add message to underlying memory."""
        await self.underlying_memory.add_message(session_id, role, content)
    
    async def clear_session(self, session_id: str) -> None:
        """Clear session from underlying memory."""
        await self.underlying_memory.clear_session(session_id)
    
    async def get_cached_response(self, query: str) -> str:
        """Get cached response for query."""
        if not REDIS_AVAILABLE or not self.redis_client:
            return None
        
        try:
            cache_key = self._get_cache_key(query)
            cached = await self.redis_client.get(cache_key)
            if cached:
                logger.debug("action=cache_hit memory=redis_cache query_length=%d", len(query))
                return cached
        except Exception as e:
            logger.warning("action=cache_get_failed memory=redis_cache error=%s", str(e))
        
        return None
    
    async def cache_response(self, query: str, response: str) -> None:
        """Cache response for query."""
        if not REDIS_AVAILABLE or not self.redis_client:
            return
        
        try:
            cache_key = self._get_cache_key(query)
            await self.redis_client.set(cache_key, response, ex=self.ttl)
            logger.debug("action=cache_set memory=redis_cache query_length=%d response_length=%d", len(query), len(response))
        except Exception as e:
            logger.warning("action=cache_set_failed memory=redis_cache error=%s", str(e))
