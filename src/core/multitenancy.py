from contextvars import ContextVar
from typing import Optional

from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.auth.domain.entities import Principal
from src.auth.exceptions import InvalidTokenError

_tenant_ctx: ContextVar[str] = ContextVar("tenant_id", default="default")
_principal_ctx: ContextVar[Optional[Principal]] = ContextVar("principal", default=None)


def get_current_tenant() -> str:
    '''
    Get current tenant ID from context.

    Returns:
        str: Active Tenant ID
    '''
    return _tenant_ctx.get()


def get_current_principal() -> Optional[Principal]:
    '''
    Get authenticated principal from context, if any.
    '''
    return _principal_ctx.get()


def _is_public_path(path: str) -> bool:
    return (
        path in {"/", "/health", "/openapi.json", "/gemini.json"}
        or path.startswith("/docs")
        or path.startswith("/redoc")
        or path == "/auth/token"
        or path.startswith("/.well-known/")
    )


class TenantMiddleware(BaseHTTPMiddleware):
    '''
    Middleware to validate Bearer JWT and populate tenant/principal context.
    '''

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if _is_public_path(path):
            tenant_token = _tenant_ctx.set("default")
            principal_token = _principal_ctx.set(None)
            try:
                return await call_next(request)
            finally:
                _tenant_ctx.reset(tenant_token)
                _principal_ctx.reset(principal_token)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.lower().startswith("bearer "):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Authorization Bearer token is required"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        raw_token = auth_header.split(" ", 1)[1].strip()
        try:
            from src.auth.state import get_token_service

            principal = get_token_service().verify_access_token(raw_token)
        except (InvalidTokenError, RuntimeError) as exc:
            detail = str(exc) if not isinstance(exc, RuntimeError) else "Auth is not initialized"
            status_code = (
                status.HTTP_503_SERVICE_UNAVAILABLE
                if isinstance(exc, RuntimeError)
                else status.HTTP_401_UNAUTHORIZED
            )
            return JSONResponse(
                status_code=status_code,
                content={"detail": detail},
                headers={"WWW-Authenticate": "Bearer"},
            )

        tenant_token = _tenant_ctx.set(principal.tenant_id)
        principal_token = _principal_ctx.set(principal)
        try:
            return await call_next(request)
        finally:
            _tenant_ctx.reset(tenant_token)
            _principal_ctx.reset(principal_token)
