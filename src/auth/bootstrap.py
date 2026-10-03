from src.auth.application.config import load_auth_config
from src.auth.application.secrets import resolve_auth_issuer, resolve_client_secrets
from src.auth.application.token_service import TokenService
from src.auth.infrastructure.jwt import JwtAccessTokenIssuer
from src.auth.infrastructure.refresh_tokens import RedisRefreshTokenStore
from src.auth.infrastructure.user_repository import DbFileUserRepository
from src.auth.state import set_token_service
from src.config.settings import settings
from src.core.logging import setup_logger

logger = setup_logger(__name__)


def init_auth(app_config: dict) -> None:
    '''
    Initialize OAuth2/OIDC services from YAML policy and environment secrets.

    Args:
        app_config (dict): Full application configuration from config.yml.

    Returns:
        None

    Raises:
        ValueError: If auth configuration or required secrets are invalid.
    '''
    issuer = resolve_auth_issuer()
    auth_config = load_auth_config(app_config.get("auth"), issuer=issuer)
    client_ids = {client.client_id for client in auth_config.clients}
    client_secrets = resolve_client_secrets(client_ids)

    token_issuer = JwtAccessTokenIssuer(
        private_key_pem=settings.AUTH_PRIVATE_KEY,
        public_key_pem=settings.AUTH_PUBLIC_KEY,
        issuer=auth_config.issuer,
        audience=auth_config.audience,
    )
    service = TokenService.from_config(
        config=auth_config,
        client_secrets=client_secrets,
        user_repository=DbFileUserRepository(),
        refresh_store=RedisRefreshTokenStore(),
        token_issuer=token_issuer,
    )
    set_token_service(service, auth_config)
    logger.info("OAuth2/OIDC auth initialized")
