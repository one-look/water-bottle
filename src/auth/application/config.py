from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

SUPPORTED_GRANT_TYPES = frozenset(
    {"client_credentials", "user_token", "refresh_token"},
)


class OAuthClientConfig(BaseModel):
    '''
    OAuth client registration from application YAML (non-secret fields).
    '''

    model_config = ConfigDict(extra="forbid")

    client_id: str = Field(..., min_length=1)
    tenant_id: str = Field(..., min_length=1)
    allowed_grant_types: list[str] = Field(..., min_length=1)

    @field_validator("allowed_grant_types")
    @classmethod
    def validate_grant_types(cls, values: list[str]) -> list[str]:
        '''
        Ensure each grant type is supported by the authorization server.

        Args:
            values (list[str]): Grant types from configuration.

        Returns:
            list[str]: Validated grant types.

        Raises:
            ValueError: If an unsupported grant type is present.
        '''
        unknown = set(values) - SUPPORTED_GRANT_TYPES
        if unknown:
            raise ValueError(f"Unsupported grant types: {sorted(unknown)}")
        return values


class AuthConfig(BaseModel):
    '''
    Full auth configuration used at runtime (YAML policy plus resolved issuer).
    '''

    model_config = ConfigDict(extra="forbid")

    issuer: str = Field(..., min_length=1)
    audience: str = Field(..., min_length=1)
    access_token_expire_minutes: int = Field(..., gt=0)
    refresh_token_expire_days: int = Field(..., gt=0)
    clients: list[OAuthClientConfig] = Field(..., min_length=1)

    @property
    def access_token_ttl_seconds(self) -> int:
        '''
        Access token lifetime in seconds.

        Returns:
            int: TTL derived from access_token_expire_minutes.
        '''
        return self.access_token_expire_minutes * 60

    @property
    def refresh_token_ttl_seconds(self) -> int:
        '''
        Refresh token lifetime in seconds.

        Returns:
            int: TTL derived from refresh_token_expire_days.
        '''
        return self.refresh_token_expire_days * 24 * 60 * 60


def load_auth_config(raw: dict[str, Any], issuer: str) -> AuthConfig:
    '''
    Load and validate auth configuration from the application config dict.

    Args:
        raw (dict[str, Any]): The ``auth`` section from config.yml.
        issuer (str): Canonical issuer URL from environment settings.

    Returns:
        AuthConfig: Validated runtime auth configuration.

    Raises:
        ValueError: If the auth block is missing or invalid.
    '''
    if not raw:
        raise ValueError("Missing 'auth' configuration block in config.yml")
    payload = dict(raw)
    payload.pop("issuer", None)
    return AuthConfig(issuer=issuer.rstrip("/"), **payload)
