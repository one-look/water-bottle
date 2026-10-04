from typing import Any

import jwt
from jwt import PyJWKClient

from src.auth.exceptions import InvalidTokenError

GOOGLE_JWKS_URL = "https://www.googleapis.com/oauth2/v3/certs"
GOOGLE_ISSUERS = ("https://accounts.google.com", "accounts.google.com")


class GoogleIdTokenVerifier:
    '''
    Validates Google OIDC ID tokens using Google's JWKS endpoint.
    '''

    def __init__(self, client_id: str) -> None:
        '''
        Args:
            client_id (str): Google OAuth client id (expected JWT aud).
        '''
        self._client_id = client_id
        self._jwks_client = PyJWKClient(GOOGLE_JWKS_URL)

    def verify(self, id_token: str) -> dict[str, Any]:
        '''
        Args:
            id_token (str): Google ID token from the token endpoint.

        Returns:
            dict[str, Any]: Verified JWT claims (email, sub, etc.).

        Raises:
            InvalidTokenError: If validation fails.
        '''
        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(id_token)
            claims = jwt.decode(
                id_token,
                signing_key.key,
                algorithms=["RS256"],
                audience=self._client_id,
                options={"require": ["exp", "iss", "sub"]},
            )
        except jwt.PyJWTError as exc:
            raise InvalidTokenError(str(exc)) from exc

        if claims.get("iss") not in GOOGLE_ISSUERS:
            raise InvalidTokenError("Invalid Google token issuer")
        if not claims.get("email"):
            raise InvalidTokenError("Google token missing email claim")
        return claims
