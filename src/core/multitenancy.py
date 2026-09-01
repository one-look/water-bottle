from contextvars import ContextVar
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware

_tenant_ctx: ContextVar[str] = ContextVar("tenant_id", default="default")

def get_current_tenant() -> str:
    '''
    Get current tenant ID from context

    Returns:
        str: Tenant ID
    '''
    return _tenant_ctx.get()

class TenantMiddleware(BaseHTTPMiddleware):
    '''
    Middleware to handle tenant context
    '''
    async def dispatch(self, request: Request, call_next):
        '''
        Handle dispatch of request
        
        Args:
            request (Request): Request object
            call_next (callable): Next middleware or endpoint
        
        Returns:
            Response: Response object
        '''
        tenant_id = request.headers.get("X-Tenant-ID")
        if not tenant_id and request.url.path not in ["/health", "/docs", "/gemini.json"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="X-Tenant-ID header is required"
            )
        token = _tenant_ctx.set(tenant_id or "default")
        try:
            response = await call_next(request)
            return response
        finally:
            _tenant_ctx.reset(token)