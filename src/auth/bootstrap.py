from src.auth.application.google_auth_service import GoogleAuthService
from src.auth.application.google_principal import GooglePrincipalResolver
from src.auth.infrastructure.google_oauth_state import GoogleOAuthStateStore
from src.auth.infrastructure.google_oidc import GoogleIdTokenVerifier
from src.auth.infrastructure.user_repository import DbFileUserRepository
from src.auth.state import set_google_auth
from src.config.settings import settings
from src.core.logging import setup_logger

logger = setup_logger(__name__)


def init_auth(app_config: dict) -> None:
    '''
    Initialize Google OAuth login and ID-token verification (Google JWKS only).

    Args:
        app_config (dict): Application configuration (unused; auth is env-driven).

    Returns:
        None
    '''
    user_repository = DbFileUserRepository()
    verifier = GoogleIdTokenVerifier(settings.GOOGLE_OAUTH_CLIENT_ID)
    resolver = GooglePrincipalResolver(verifier, user_repository)
    google_auth = GoogleAuthService(
        state_store=GoogleOAuthStateStore(),
        id_token_verifier=verifier,
        user_repository=user_repository,
    )
    set_google_auth(google_auth, resolver)
    logger.info("Google OAuth authentication initialized")
