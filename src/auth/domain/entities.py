from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class UserIdentity:
    email: str
    tenant_id: str
    role: str


@dataclass(frozen=True)
class OAuthClient:
    client_id: str
    client_secret: str
    tenant_id: str
    allowed_grant_types: frozenset[str]


@dataclass(frozen=True)
class Principal:
    sub: str
    tenant_id: str
    client_id: str
    role: Optional[str] = None


@dataclass(frozen=True)
class TokenPair:
    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int = 0


@dataclass(frozen=True)
class RefreshTokenRecord:
    token_id: str
    sub: str
    client_id: str
    tenant_id: str
    role: Optional[str]
