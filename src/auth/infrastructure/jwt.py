import time
import uuid
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from src.auth.domain.entities import Principal
from src.auth.domain.interfaces import AccessTokenIssuer
from src.auth.exceptions import InvalidTokenError


class JwtAccessTokenIssuer(AccessTokenIssuer):
    '''
    RS256 JWT access-token issuer and verifier for the built-in authorization server.
    '''

    def __init__(
        self,
        private_key_pem: str,
        public_key_pem: str,
        issuer: str,
        audience: str,
        key_id: str = "water-bottle-1",
    ) -> None:
        '''
        Args:
            private_key_pem (str): PEM-encoded RSA private key.
            public_key_pem (str): PEM-encoded RSA public key.
            issuer (str): Canonical issuer URL (JWT iss claim).
            audience (str): Expected audience (JWT aud claim).
            key_id (str): JWK key id (kid header).
        '''
        self._private_key_pem = private_key_pem
        self._public_key_pem = public_key_pem
        self._issuer = issuer
        self._audience = audience
        self._key_id = key_id

    @classmethod
    def generate_keypair(cls) -> tuple[str, str]:
        '''
        Generate an RSA key pair for tests or local key provisioning.

        Returns:
            tuple[str, str]: (private_pem, public_pem).
        '''
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("utf-8")
        public_pem = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ).decode("utf-8")
        return private_pem, public_pem

    def issue_access_token(self, principal: Principal, expires_in_seconds: int) -> str:
        '''
        Args:
            principal (Principal): Token subject and tenant claims.
            expires_in_seconds (int): Access token TTL.

        Returns:
            str: Encoded JWT.
        '''
        now = int(time.time())
        payload: dict[str, Any] = {
            "iss": self._issuer,
            "sub": principal.sub,
            "aud": self._audience,
            "iat": now,
            "exp": now + expires_in_seconds,
            "jti": str(uuid.uuid4()),
            "tenant_id": principal.tenant_id,
            "client_id": principal.client_id,
        }
        if principal.role is not None:
            payload["role"] = principal.role

        return jwt.encode(
            payload,
            self._private_key_pem,
            algorithm="RS256",
            headers={"kid": self._key_id},
        )

    def verify_access_token(self, token: str) -> Principal:
        '''
        Args:
            token (str): Encoded JWT access token.

        Returns:
            Principal: Verified principal claims.

        Raises:
            InvalidTokenError: If verification fails.
        '''
        try:
            claims = jwt.decode(
                token,
                self._public_key_pem,
                algorithms=["RS256"],
                audience=self._audience,
                issuer=self._issuer,
            )
        except jwt.PyJWTError as exc:
            raise InvalidTokenError(str(exc)) from exc

        tenant_id = claims.get("tenant_id")
        client_id = claims.get("client_id")
        if not tenant_id or not client_id:
            raise InvalidTokenError("Missing tenant_id or client_id in token")

        return Principal(
            sub=claims["sub"],
            tenant_id=tenant_id,
            client_id=client_id,
            role=claims.get("role"),
        )

    def jwks_document(self) -> dict[str, Any]:
        '''
        Returns:
            dict[str, Any]: JWKS containing the public signing key.
        '''
        public_key = serialization.load_pem_public_key(self._public_key_pem.encode("utf-8"))
        json_public_key = RSAAlgorithm.to_jwk(public_key, as_dict=True)
        json_public_key["kid"] = self._key_id
        json_public_key["use"] = "sig"
        json_public_key["alg"] = "RS256"
        return {"keys": [json_public_key]}
