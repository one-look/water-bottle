import secrets
import uuid
from typing import Optional

from src.auth.application.config import AuthConfig
from src.auth.domain.entities import OAuthClient, Principal, RefreshTokenRecord, TokenPair
from src.auth.domain.interfaces import AccessTokenIssuer, RefreshTokenStore, UserIdentityRepository
from src.auth.exceptions import AuthError


class TokenService:
    '''
    OAuth2 token issuance, refresh rotation, and access-token verification facade.
    '''

    def __init__(
        self,
        config: AuthConfig,
        clients: dict[str, OAuthClient],
        user_repository: UserIdentityRepository,
        refresh_store: RefreshTokenStore,
        token_issuer: AccessTokenIssuer,
    ) -> None:
        self._config = config
        self._clients = clients
        self._users = user_repository
        self._refresh_store = refresh_store
        self._issuer = token_issuer

    @classmethod
    def from_config(
        cls,
        config: AuthConfig,
        client_secrets: dict[str, str],
        user_repository: UserIdentityRepository,
        refresh_store: RefreshTokenStore,
        token_issuer: AccessTokenIssuer,
    ) -> "TokenService":
        '''
        Build TokenService from validated config and environment-backed client secrets.

        Args:
            config (AuthConfig): Runtime auth configuration.
            client_secrets (dict[str, str]): client_id to secret mapping from Settings.
            user_repository (UserIdentityRepository): User identity port.
            refresh_store (RefreshTokenStore): Refresh token persistence port.
            token_issuer (AccessTokenIssuer): JWT access-token port.

        Returns:
            TokenService: Configured token service.

        Raises:
            ValueError: If a configured client is missing a secret.
        '''
        clients: dict[str, OAuthClient] = {}
        for client_cfg in config.clients:
            secret = client_secrets.get(client_cfg.client_id)
            if not secret:
                raise ValueError(
                    f"Missing client secret for OAuth client_id '{client_cfg.client_id}'"
                )
            clients[client_cfg.client_id] = OAuthClient(
                client_id=client_cfg.client_id,
                client_secret=secret,
                tenant_id=client_cfg.tenant_id,
                allowed_grant_types=frozenset(client_cfg.allowed_grant_types),
            )
        return cls(config, clients, user_repository, refresh_store, token_issuer)

    def authenticate_client(self, client_id: str, client_secret: str) -> OAuthClient:
        '''
        Validate OAuth client credentials.

        Args:
            client_id (str): Registered client identifier.
            client_secret (str): Client secret from the token request.

        Returns:
            OAuthClient: Authenticated client registration.

        Raises:
            AuthError: If credentials are invalid.
        '''
        client = self._clients.get(client_id)
        if not client or not secrets.compare_digest(client.client_secret, client_secret):
            raise AuthError("Invalid client credentials")
        return client

    def _ensure_grant_allowed(self, client: OAuthClient, grant_type: str) -> None:
        if grant_type not in client.allowed_grant_types:
            raise AuthError(f"Grant type '{grant_type}' is not allowed for this client")

    async def issue_client_credentials(
        self, client_id: str, client_secret: str
    ) -> TokenPair:
        '''
        Issue tokens for the client_credentials grant.

        Args:
            client_id (str): OAuth client id.
            client_secret (str): OAuth client secret.

        Returns:
            TokenPair: Access and refresh tokens.
        '''
        client = self.authenticate_client(client_id, client_secret)
        self._ensure_grant_allowed(client, "client_credentials")
        principal = Principal(
            sub=client.client_id,
            tenant_id=client.tenant_id,
            client_id=client.client_id,
            role=None,
        )
        return await self._issue_pair(principal)

    async def issue_user_token(
        self, client_id: str, client_secret: str, email: str
    ) -> TokenPair:
        '''
        Issue tokens for the user_token grant using dbfile identity.

        Args:
            client_id (str): OAuth client id.
            client_secret (str): OAuth client secret.
            email (str): User email to resolve from identity store.

        Returns:
            TokenPair: Access and refresh tokens.

        Raises:
            AuthError: If the user is unknown.
        '''
        client = self.authenticate_client(client_id, client_secret)
        self._ensure_grant_allowed(client, "user_token")
        user = self._users.get_by_email(email)
        if user is None:
            raise AuthError("Unknown user email")
        principal = Principal(
            sub=user.email,
            tenant_id=user.tenant_id,
            client_id=client.client_id,
            role=user.role,
        )
        return await self._issue_pair(principal)

    async def refresh(self, client_id: str, client_secret: str, refresh_token: str) -> TokenPair:
        '''
        Rotate refresh token and issue a new access token.

        Args:
            client_id (str): OAuth client id.
            client_secret (str): OAuth client secret.
            refresh_token (str): Opaque refresh token id.

        Returns:
            TokenPair: New access and refresh tokens.

        Raises:
            AuthError: If refresh token or client is invalid.
        '''
        client = self.authenticate_client(client_id, client_secret)
        self._ensure_grant_allowed(client, "refresh_token")
        record = await self._refresh_store.get(refresh_token)
        if record is None or record.client_id != client.client_id:
            raise AuthError("Invalid refresh token")
        await self._refresh_store.delete(refresh_token)
        principal = Principal(
            sub=record.sub,
            tenant_id=record.tenant_id,
            client_id=record.client_id,
            role=record.role,
        )
        return await self._issue_pair(principal)

    def verify_access_token(self, token: str) -> Principal:
        '''
        Verify a Bearer access JWT and return the authenticated principal.

        Args:
            token (str): JWT access token.

        Returns:
            Principal: Claims extracted from the token.
        '''
        return self._issuer.verify_access_token(token)

    def jwks_document(self) -> dict:
        '''
        Return the JWKS document for this authorization server.

        Returns:
            dict: JWKS JSON structure.
        '''
        return self._issuer.jwks_document()

    @property
    def config(self) -> AuthConfig:
        return self._config

    async def _issue_pair(self, principal: Principal) -> TokenPair:
        ttl = self._config.access_token_ttl_seconds
        access_token = self._issuer.issue_access_token(principal, ttl)
        refresh_id = str(uuid.uuid4())
        record = RefreshTokenRecord(
            token_id=refresh_id,
            sub=principal.sub,
            client_id=principal.client_id,
            tenant_id=principal.tenant_id,
            role=principal.role,
        )
        await self._refresh_store.save(record, self._config.refresh_token_ttl_seconds)
        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_id,
            expires_in=ttl,
        )
