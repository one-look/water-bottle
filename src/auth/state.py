from typing import Optional

from src.auth.application.google_auth_service import GoogleAuthService
from src.auth.application.google_principal import GooglePrincipalResolver

_google_auth: Optional[GoogleAuthService] = None
_principal_resolver: Optional[GooglePrincipalResolver] = None


def set_google_auth(service: GoogleAuthService, resolver: GooglePrincipalResolver) -> None:
    '''
    Register Google OAuth and Bearer token resolution.

    Args:
        service (GoogleAuthService): Google login service.
        resolver (GooglePrincipalResolver): ID token to principal resolver.

    Returns:
        None
    '''
    global _google_auth, _principal_resolver
    _google_auth = service
    _principal_resolver = resolver


def get_google_auth_service() -> GoogleAuthService:
    '''
    Returns:
        GoogleAuthService: Initialized Google OAuth service.

    Raises:
        RuntimeError: If auth has not been bootstrapped.
    '''
    if _google_auth is None:
        raise RuntimeError("Google auth is not initialized")
    return _google_auth


def get_principal_resolver() -> GooglePrincipalResolver:
    '''
    Returns:
        GooglePrincipalResolver: Verifier for API Bearer tokens.

    Raises:
        RuntimeError: If auth has not been bootstrapped.
    '''
    if _principal_resolver is None:
        raise RuntimeError("Google auth is not initialized")
    return _principal_resolver
