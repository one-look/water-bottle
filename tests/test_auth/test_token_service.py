import asyncio
from unittest.mock import AsyncMock

import pytest

from src.auth.application.config import AuthConfig, OAuthClientConfig
from src.auth.application.token_service import TokenService
from src.auth.exceptions import AuthError
from src.auth.domain.entities import RefreshTokenRecord
from src.auth.infrastructure.jwt import JwtAccessTokenIssuer
from src.auth.infrastructure.user_repository import DbFileUserRepository


@pytest.fixture
def token_service():
    private_pem, public_pem = JwtAccessTokenIssuer.generate_keypair()
    jwt_issuer = JwtAccessTokenIssuer(
        private_key_pem=private_pem,
        public_key_pem=public_pem,
        issuer="http://test.example.com",
        audience="water-bottle",
    )
    config = AuthConfig(
        issuer="http://test.example.com",
        audience="water-bottle",
        access_token_expire_minutes=15,
        refresh_token_expire_days=7,
        clients=[
            OAuthClientConfig(
                client_id="test-client",
                tenant_id="nmc",
                allowed_grant_types=[
                    "client_credentials",
                    "user_token",
                    "refresh_token",
                ],
            )
        ],
    )
    refresh_store = AsyncMock()
    refresh_store.save = AsyncMock()
    refresh_store.delete = AsyncMock()
    refresh_store.get = AsyncMock(return_value=None)
    service = TokenService.from_config(
        config=config,
        client_secrets={"test-client": "super-secret"},
        user_repository=DbFileUserRepository(),
        refresh_store=refresh_store,
        token_issuer=jwt_issuer,
    )
    return service, refresh_store, jwt_issuer


def test_client_credentials_grant(token_service):
    service, refresh_store, jwt_issuer = token_service
    pair = asyncio.run(service.issue_client_credentials("test-client", "super-secret"))
    assert pair.access_token
    assert pair.refresh_token
    jwt_issuer.verify_access_token(pair.access_token)
    refresh_store.save.assert_awaited()


def test_user_token_grant(token_service):
    service, _, jwt_issuer = token_service
    pair = asyncio.run(
        service.issue_user_token("test-client", "super-secret", "p23dsc103@nmc.ac.in")
    )
    principal = jwt_issuer.verify_access_token(pair.access_token)
    assert principal.sub == "p23dsc103@nmc.ac.in"
    assert principal.role == "student"


def test_refresh_rotates_token(token_service):
    service, refresh_store, jwt_issuer = token_service
    pair = asyncio.run(service.issue_client_credentials("test-client", "super-secret"))
    refresh_store.get.return_value = RefreshTokenRecord(
        token_id=pair.refresh_token,
        sub="test-client",
        client_id="test-client",
        tenant_id="nmc",
        role=None,
    )
    new_pair = asyncio.run(
        service.refresh("test-client", "super-secret", pair.refresh_token)
    )
    refresh_store.delete.assert_awaited_with(pair.refresh_token)
    jwt_issuer.verify_access_token(new_pair.access_token)


def test_invalid_client_secret(token_service):
    service, _, _ = token_service
    with pytest.raises(AuthError):
        asyncio.run(service.issue_client_credentials("test-client", "wrong"))
