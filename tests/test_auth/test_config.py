import pytest

from src.auth.application.config import AuthConfig, OAuthClientConfig, load_auth_config
from src.auth.application.secrets import resolve_auth_issuer, resolve_client_secrets


def test_load_auth_config_injects_issuer():
    raw = {
        "audience": "water-bottle",
        "access_token_expire_minutes": 15,
        "refresh_token_expire_days": 7,
        "clients": [
            {
                "client_id": "water-bottle-client",
                "tenant_id": "nmc",
                "allowed_grant_types": ["client_credentials"],
            }
        ],
    }
    config = load_auth_config(raw, issuer="https://api.example.com")
    assert config.issuer == "https://api.example.com"
    assert config.audience == "water-bottle"


def test_load_auth_config_rejects_unknown_grant():
    raw = {
        "audience": "water-bottle",
        "access_token_expire_minutes": 15,
        "refresh_token_expire_days": 7,
        "clients": [
            {
                "client_id": "c",
                "tenant_id": "nmc",
                "allowed_grant_types": ["password"],
            }
        ],
    }
    with pytest.raises(ValueError):
        load_auth_config(raw, issuer="https://api.example.com")


def test_resolve_client_secrets_from_settings():
    secrets = resolve_client_secrets({"water-bottle-client"})
    assert secrets["water-bottle-client"]
