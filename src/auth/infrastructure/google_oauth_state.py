import json
from typing import Any

from src.core.redis import redis_manager

STATE_KEY_PREFIX = "auth:google:state:"
STATE_TTL_SECONDS = 600


class GoogleOAuthStateStore:
    '''
    Stores OAuth state, PKCE code verifiers, and frontend redirect URIs for Google login.
    '''

    def _key(self, state: str) -> str:
        '''
        Args:
            state (str): OAuth state parameter.

        Returns:
            str: Redis key for the state entry.
        '''
        return f"{STATE_KEY_PREFIX}{state}"

    async def save(self, state: str, code_verifier: str, redirect_uri: str) -> None:
        '''
        Persist state, PKCE verifier, and frontend redirect until callback.

        Args:
            state (str): OAuth state parameter.
            code_verifier (str): PKCE code verifier.
            redirect_uri (str): Frontend URL to return the user to after login.

        Returns:
            None
        '''
        client = redis_manager.get_client()
        await client.setex(
            self._key(state),
            STATE_TTL_SECONDS,
            json.dumps(
                {"code_verifier": code_verifier, "redirect_uri": redirect_uri}
            ),
        )

    async def pop(self, state: str) -> dict[str, Any] | None:
        '''
        Consume state and return stored OAuth login metadata (one-time use).

        Args:
            state (str): OAuth state parameter.

        Returns:
            dict[str, Any] | None: code_verifier and redirect_uri if state was valid.
        '''
        client = redis_manager.get_client()
        key = self._key(state)
        raw = await client.get(key)
        if not raw:
            return None
        await client.delete(key)
        return json.loads(raw)
