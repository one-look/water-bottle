from typing import Optional

from src.auth.application.config import AuthConfig
from src.auth.application.token_service import TokenService

_token_service: Optional[TokenService] = None
_auth_config: Optional[AuthConfig] = None


def set_token_service(service: TokenService, config: AuthConfig) -> None:
    '''
    Register the global token service and auth configuration.

    Args:
        service (TokenService): Initialized token service.
        config (AuthConfig): Runtime auth configuration.

    Returns:
        None
    '''
    global _token_service, _auth_config
    _token_service = service
    _auth_config = config


def get_token_service() -> TokenService:
    '''
    Returns:
        TokenService: The initialized token service.

    Raises:
        RuntimeError: If auth has not been bootstrapped.
    '''
    if _token_service is None:
        raise RuntimeError("Auth is not initialized")
    return _token_service


def get_auth_config() -> AuthConfig:
    '''
    Returns:
        AuthConfig: The runtime auth configuration.

    Raises:
        RuntimeError: If auth has not been bootstrapped.
    '''
    if _auth_config is None:
        raise RuntimeError("Auth is not initialized")
    return _auth_config
