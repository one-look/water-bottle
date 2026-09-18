"""Semantic cache service utilizing Redis for similarity search on query embeddings."""

import hashlib
import struct
from typing import Any, Optional
from pydantic import BaseModel, Field, validate_call
import redis.asyncio as redis

from src.core.logging import setup_logger
from src.core.redis import RedisConfig, redis_manager

logger = setup_logger(__name__)


class RedisCacheConfig(BaseModel):
    '''
    Configuration schema for Redis semantic cache instantiation.
    '''

    url: str = Field("redis://localhost:6379/0", min_length=1, description="Redis server URL")
    ttl_seconds: int = Field(604800, ge=1, description="Cache TTL in seconds (default 7 days)")
    similarity_threshold: float = Field(0.92, ge=0.0, le=1.0, description="Minimum cosine similarity for cache hit")
    vector_dim: int = Field(3072, ge=1, description="Embedding vector dimension")


class SemanticCache:
    '''
    Handles semantic caching of LLM responses using Redis vector similarity.
    '''

    @validate_call
    def __init__(self, config: RedisCacheConfig) -> None:
        """Initializes Redis via the shared pool and stores cache settings."""
        redis_manager.init_client(RedisConfig(url=config.url))

        # Binary-safe client for raw FT.SEARCH and HSET vector operations
        self.redis_bytes: redis.Redis = redis.from_url(
            config.url,
            decode_responses=False,
        )

        self.ttl_seconds = config.ttl_seconds
        self.similarity_threshold = config.similarity_threshold
        self.vector_dim = config.vector_dim
        self.index_name = "idx:semantic_cache"

    async def ensure_index_exists(self) -> None:
        """Ensures the RediSearch vector index is created."""
        try:
            await self.redis_bytes.execute_command("FT.INFO", self.index_name)
        except Exception:
            logger.info(f"Creating RediSearch index '{self.index_name}'...")
            try:
                await self.redis_bytes.execute_command(
                    "FT.CREATE",
                    self.index_name,
                    "ON", "HASH",
                    "PREFIX", "1", "cache:",
                    "SCHEMA",
                    "tenant_id", "TAG",
                    "user_query", "TEXT",
                    "response", "TEXT",
                    "vector", "VECTOR", "HNSW", "6",
                    "TYPE", "FLOAT32",
                    "DIM", str(self.vector_dim),
                    "DISTANCE_METRIC", "COSINE",
                )
                logger.info(f"Successfully created RediSearch index '{self.index_name}'.")
            except Exception as create_err:
                logger.error(f"Failed to create index '{self.index_name}': {str(create_err)}")

    def _pack_vector(self, vector: list[float]) -> bytes:
        return struct.pack(f"<{len(vector)}f", *vector)

    def _cache_key(self, tenant_id: str, user_query: str) -> str:
        clean_query = user_query.strip().lower()
        query_hash = hashlib.sha256(clean_query.encode("utf-8")).hexdigest()
        return f"cache:{tenant_id}:{query_hash}"

    @staticmethod
    def _to_str(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    def _parse_knn_hit(self, raw_result: Any) -> Optional[tuple[str, float]]:
        """Extracts (response, cosine_distance) from Redis 8 dict or legacy array replies."""
        field_dict: dict[str, str] = {}

        if isinstance(raw_result, dict):
            results = raw_result.get(b"results") or raw_result.get("results") or []
            if not results:
                return None
            extras = results[0].get(b"extra_attributes") or results[0].get("extra_attributes") or {}
            field_dict = {self._to_str(key): self._to_str(val) for key, val in extras.items()}
        elif isinstance(raw_result, list) and len(raw_result) > 1 and int(raw_result[0]) > 0:
            fields = raw_result[2]
            for i in range(0, len(fields), 2):
                key = self._to_str(fields[i])
                val = self._to_str(fields[i + 1])
                field_dict[key] = val
        else:
            return None

        response = field_dict.get("response")
        if response is None:
            return None

        distance = float(field_dict.get("score", field_dict.get("__vector_score", "1.0")))
        return response, distance

    async def get(
        self,
        query_vector: list[float],
        tenant_id: str,
        user_query: Optional[str] = None,
    ) -> Optional[str]:
        """Looks up an exact query hash first, then tenant-scoped vector similarity."""
        try:
            if user_query:
                exact_key = self._cache_key(tenant_id, user_query)
                exact_hit = await self.redis_bytes.hget(exact_key, b"response")
                if exact_hit:
                    logger.info(f"Cache HIT (exact query) for tenant '{tenant_id}'")
                    return self._to_str(exact_hit)

            await self.ensure_index_exists()

            escaped_tenant = tenant_id.replace("-", "\\-")
            query = f"(@tenant_id:{{{escaped_tenant}}})=>[KNN 1 @vector $vec AS score]"
            packed_vec = self._pack_vector(query_vector)

            raw_result = await self.redis_bytes.execute_command(
                "FT.SEARCH",
                self.index_name,
                query,
                "PARAMS",
                "2",
                "vec",
                packed_vec,
                "RETURN",
                "2",
                "response",
                "score",
                "DIALECT",
                "2",
            )

            parsed = self._parse_knn_hit(raw_result)
            if parsed is not None:
                response, distance = parsed
                similarity = 1.0 - distance
                if similarity >= self.similarity_threshold:
                    logger.info(
                        f"Cache HIT (similarity: {similarity:.4f}) for tenant '{tenant_id}'"
                    )
                    return response

            logger.info(f"Cache MISS for tenant '{tenant_id}'")
            return None

        except Exception as e:
            logger.warning(f"Cache lookup failed: {str(e)}. Proceeding as Cache MISS.", exc_info=True)
            return None

    async def set(
        self,
        query_vector: list[float],
        user_query: str,
        response_text: str,
        tenant_id: str,
    ) -> None:
        """Stores query vector, user query, answer, and tenant ID in Redis as a Hash."""
        try:
            cache_id = self._cache_key(tenant_id, user_query)

            mapping = {
                b"tenant_id": tenant_id.encode("utf-8"),
                b"user_query": user_query.encode("utf-8"),
                b"response": response_text.encode("utf-8"),
                b"vector": self._pack_vector(query_vector),
            }

            await self.redis_bytes.hset(cache_id, mapping=mapping)
            await self.redis_bytes.expire(cache_id, self.ttl_seconds)

            logger.info(f"Saved response to semantic cache under key '{cache_id}'")

        except Exception as e:
            logger.error(f"Failed to write entry to semantic cache: {str(e)}", exc_info=True)
