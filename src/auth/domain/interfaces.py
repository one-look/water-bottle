from abc import ABC, abstractmethod
from typing import Any, Optional

from src.auth.domain.entities import Principal, RefreshTokenRecord, TokenPair, UserIdentity


class UserIdentityRepository(ABC):
    @abstractmethod
    def get_by_email(self, email: str) -> Optional[UserIdentity]:
        """Look up a user by email address."""
        pass


class RefreshTokenStore(ABC):
    @abstractmethod
    async def save(
        self, record: RefreshTokenRecord, ttl_seconds: int
    ) -> None:
        pass

    @abstractmethod
    async def get(self, token_id: str) -> Optional[RefreshTokenRecord]:
        pass

    @abstractmethod
    async def delete(self, token_id: str) -> None:
        pass


class AccessTokenIssuer(ABC):
    @abstractmethod
    def issue_access_token(self, principal: Principal, expires_in_seconds: int) -> str:
        pass

    @abstractmethod
    def verify_access_token(self, token: str) -> Principal:
        pass

    @abstractmethod
    def jwks_document(self) -> dict[str, Any]:
        pass
