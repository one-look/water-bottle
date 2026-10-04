import base64
import hashlib
import secrets
from typing import Any
from urllib.parse import urlencode

import httpx

from src.auth.domain.interfaces import UserIdentityRepository
from src.auth.exceptions import AuthError, InvalidTokenError
from src.auth.infrastructure.google_oauth_state import GoogleOAuthStateStore
from src.auth.infrastructure.google_oidc import GoogleIdTokenVerifier
from src.config.settings import settings

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"


class GoogleAuthService:
    '''
    Google OAuth2 authorization code + PKCE login; returns Google OIDC tokens.
    '''

    def __init__(
        self,
        state_store: GoogleOAuthStateStore,
        id_token_verifier: GoogleIdTokenVerifier,
        user_repository: UserIdentityRepository,
    ) -> None:
        '''
        Args:
            state_store (GoogleOAuthStateStore): PKCE/state persistence.
            id_token_verifier (GoogleIdTokenVerifier): Google JWKS verifier.
            user_repository (UserIdentityRepository): Application user lookup.
        '''
        self._state = state_store
        self._verifier = id_token_verifier
        self._users = user_repository
        self._client_id = settings.GOOGLE_OAUTH_CLIENT_ID
        self._client_secret = settings.GOOGLE_OAUTH_CLIENT_SECRET
        self._redirect_uri = settings.google_oauth_redirect_uri

    @staticmethod
    def _pkce_challenge(verifier: str) -> str:
        '''
        Args:
            verifier (str): PKCE code verifier.

        Returns:
            str: S256 code challenge.
        '''
        digest = hashlib.sha256(verifier.encode("ascii")).digest()
        return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

    async def start_login(self, redirect_uri: str) -> str:
        '''
        Begin Google login and return the authorization URL.

        Args:
            redirect_uri (str): Frontend URL to return after login (stored in state).

        Returns:
            str: URL to redirect the user to Google.
        '''
        state = secrets.token_urlsafe(32)
        code_verifier = secrets.token_urlsafe(64)
        await self._state.save(state, code_verifier, redirect_uri.strip())
        params = {
            "client_id": self._client_id,
            "redirect_uri": self._redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "state": state,
            "code_challenge": self._pkce_challenge(code_verifier),
            "code_challenge_method": "S256",
            "access_type": "offline",
            "prompt": "consent",
        }
        return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"

    async def complete_login(self, code: str, state: str) -> dict[str, Any]:
        '''
        Exchange authorization code and issue login response with tenant context.

        Args:
            code (str): Authorization code from Google.
            state (str): OAuth state parameter.

        Returns:
            dict[str, Any]: id_token, redirect_uri, and application user claims.

        Raises:
            AuthError: If login or user lookup fails.
        '''
        stored = await self._state.pop(state)
        if not stored:
            raise AuthError("Invalid or expired OAuth state")
        code_verifier = stored.get("code_verifier")
        frontend_redirect = stored.get("redirect_uri")
        if not code_verifier or not frontend_redirect:
            raise AuthError("Invalid or expired OAuth state")

        token_payload = await self._exchange_code(code, code_verifier)
        id_token = token_payload.get("id_token")
        if not id_token:
            raise AuthError("Google did not return an id_token")

        try:
            claims = self._verifier.verify(id_token)
        except InvalidTokenError as exc:
            raise AuthError(str(exc)) from exc

        email = str(claims["email"])
        user = self._users.get_by_email(email)
        if user is None:
            raise AuthError("User is not registered in this application")

        return {
            "id_token": id_token,
            "redirect_uri": frontend_redirect.rstrip("/"),
            "access_token": token_payload.get("access_token"),
            "refresh_token": token_payload.get("refresh_token"),
            "expires_in": token_payload.get("expires_in"),
            "token_type": token_payload.get("token_type", "Bearer"),
            "sub": user.email,
            "tenant_id": user.tenant_id,
            "role": user.role,
        }

    async def _exchange_code(self, code: str, code_verifier: str) -> dict[str, Any]:
        '''
        Exchange authorization code for tokens at Google token endpoint.

        Args:
            code (str): Authorization code from Google.
            code_verifier (str): PKCE code verifier.

        Returns:
            dict[str, Any]: Google token response JSON.

        Raises:
            AuthError: If the HTTP exchange fails.
        '''
        data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "code": code,
            "code_verifier": code_verifier,
            "grant_type": "authorization_code",
            "redirect_uri": self._redirect_uri,
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(GOOGLE_TOKEN_URL, data=data)
        if response.status_code != 200:
            raise AuthError("Failed to exchange Google authorization code")
        return response.json()
