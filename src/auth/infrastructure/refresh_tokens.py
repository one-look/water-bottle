import json
from typing import Optional

from src.auth.domain.entities import RefreshTokenRecord
from src.auth.domain.interfaces import RefreshTokenStore
from src.core.redis import redis_manager

REFRESH_KEY_PREFIX = "auth:refresh:"


class RedisRefreshTokenStore(RefreshTokenStore):
    '''
    Redis-backed opaque refresh token store with TTL.
    '''

    def _key(self, token_id: str) -> str:
        return f"{REFRESH_KEY_PREFIX}{token_id}"

    async def save(self, record: RefreshTokenRecord, ttl_seconds: int) -> None:
        '''
        Args:
            record (RefreshTokenRecord): Refresh token metadata to persist.
            ttl_seconds (int): Redis key TTL.

        Returns:
            None
        '''
        client = redis_manager.get_client()
        payload = {
            "sub": record.sub,
            "client_id": record.client_id,
            "tenant_id": record.tenant_id,
            "role": record.role,
        }
        await client.setex(self._key(record.token_id), ttl_seconds, json.dumps(payload))

    async def get(self, token_id: str) -> Optional[RefreshTokenRecord]:
        '''
        Args:
            token_id (str): Opaque refresh token identifier.

        Returns:
            Optional[RefreshTokenRecord]: Stored record if present.
        '''
        client = redis_manager.get_client()
        raw = await client.get(self._key(token_id))
        if not raw:
            return None
        data = json.loads(raw)
        return RefreshTokenRecord(
            token_id=token_id,
            sub=data["sub"],
            client_id=data["client_id"],
            tenant_id=data["tenant_id"],
            role=data.get("role"),
        )

    async def delete(self, token_id: str) -> None:
        '''
        Args:
            token_id (str): Refresh token id to revoke.

        Returns:
            None
        '''
        client = redis_manager.get_client()
        await client.delete(self._key(token_id))
