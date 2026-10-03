from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.auth.exceptions import AuthError, InvalidTokenError
from src.auth.domain.entities import Principal
from src.auth.state import get_token_service
from src.core.multitenancy import get_current_principal

_bearer = HTTPBearer(auto_error=False)


async def require_principal(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> Principal:
    '''
    Require an authenticated principal from middleware or Bearer JWT.

    Args:
        credentials (HTTPAuthorizationCredentials | None): Authorization header.

    Returns:
        Principal: Authenticated user or client principal.

    Raises:
        HTTPException: If authentication is missing or invalid.
    '''
    principal = get_current_principal()
    if principal is not None:
        return principal
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        return get_token_service().verify_access_token(credentials.credentials)
    except (InvalidTokenError, AuthError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
