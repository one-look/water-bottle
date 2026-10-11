from src.auth.application.google_auth_service import GoogleAuthService
from src.auth.application.google_principal import GooglePrincipalResolver
from src.auth.infrastructure.google_oauth_state import GoogleOAuthStateStore
from src.auth.infrastructure.google_oidc import GoogleIdTokenVerifier
from src.auth.infrastructure.user_repository import PostgresUserRepository
from src.database.psql import psql_manager
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
    if not psql_manager.is_ready():
        raise RuntimeError("PostgreSQL is not initialized; set DATABASE_URL")

    database_cfg = app_config.get("database")
    if not isinstance(database_cfg, dict):
        raise ValueError("config database section is required")

    user_table = database_cfg.get("user_table")
    if not isinstance(user_table, str) or not user_table.strip():
        raise ValueError("config database.user_table is required")

    user_repository = PostgresUserRepository(user_table.strip())
    verifier = GoogleIdTokenVerifier(settings.GOOGLE_OAUTH_CLIENT_ID)
    resolver = GooglePrincipalResolver(verifier, user_repository)
    google_auth = GoogleAuthService(
        state_store=GoogleOAuthStateStore(),
        id_token_verifier=verifier,
        user_repository=user_repository,
    )
    set_google_auth(google_auth, resolver)
    logger.info("Google OAuth authentication initialized")
