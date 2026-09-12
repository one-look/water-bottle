from contextvars import ContextVar
from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

# Single source of truth for execution context across middleware, logging, and endpoints
_tenant_ctx: ContextVar[str] = ContextVar("tenant_id", default="default")


def get_current_tenant() -> str:
    '''
    Get current tenant ID from context.

    Returns:
        str: Active Tenant ID
    '''
    return _tenant_ctx.get()


class TenantMiddleware(BaseHTTPMiddleware):
    '''
    Middleware to handle tenant context and header validation.
    '''

    async def dispatch(self, request: Request, call_next):
        '''
        Handle dispatch of request.

        Args:
            request (Request): Request object
            call_next (callable): Next middleware or endpoint

        Returns:
            Response: Response object
        '''
        path = request.url.path

        # Exclude root, health, and documentation sub-paths
        is_public = (
            path in {"/", "/health", "/openapi.json", "/gemini.json"}
            or path.startswith("/docs")
            or path.startswith("/redoc")
        )

        tenant_id = request.headers.get("X-Tenant-ID")

        if not tenant_id and not is_public:
            # Return JSONResponse directly to avoid unhandled ASGI exceptions inside middleware
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "X-Tenant-ID header is required"},
            )

        token = _tenant_ctx.set(tenant_id or "default")
        try:
            response = await call_next(request)
            return response
        finally:
            _tenant_ctx.reset(token)