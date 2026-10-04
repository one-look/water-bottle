from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse

from src.auth.exceptions import AuthError, InvalidTokenError
from src.auth.state import get_google_auth_service, get_principal_resolver
from src.core.logging import setup_logger

logger = setup_logger(__name__)

router = APIRouter(tags=["Auth"])


@router.get("/auth/google/login", summary="Start Continue with Google")
async def google_login(
    redirect_uri: str = Query(
        ...,
        min_length=1,
        description="Frontend URL to return after login (per-tenant Lovable app)",
    ),
) -> RedirectResponse:
    '''
    Redirect the browser to Google OAuth (authorization code + PKCE).

    Args:
        redirect_uri (str): Stored with OAuth state for post-login redirect.

    Returns:
        RedirectResponse: Redirect to Google authorization URL.
    '''
    url = await get_google_auth_service().start_login(redirect_uri)
    return RedirectResponse(url=url, status_code=status.HTTP_302_FOUND)


@router.get("/auth/google/callback", summary="Google OAuth callback")
async def google_callback(
    code: str = Query(...),
    state: str = Query(...),
) -> RedirectResponse:
    '''
    Complete Google login and redirect to the frontend with id_token in the URL hash.

    Args:
        code (str): Authorization code from Google.
        state (str): OAuth state parameter.

    Returns:
        RedirectResponse: HTTP 302 to redirect_uri with #id_token=...

    Raises:
        HTTPException: If login fails.
    '''
    try:
        result = await get_google_auth_service().complete_login(code, state)
    except AuthError as exc:
        logger.warning(f"Google login failed: {exc}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    target = result["redirect_uri"]
    token = quote(result["id_token"], safe="")
    return RedirectResponse(
        url=f"{target}#id_token={token}",
        status_code=status.HTTP_302_FOUND,
    )


@router.get("/auth/userinfo", summary="UserInfo from Google id_token")
async def userinfo(request: Request) -> dict[str, Any]:
    '''
    Return user claims from a valid Google id_token Bearer.

    Args:
        request (Request): HTTP request with Authorization header.

    Returns:
        dict[str, Any]: sub, tenant_id, client_id, and optional role.

    Raises:
        HTTPException: If Bearer token is missing or invalid.
    '''
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer Google id_token required",
        )
    token = auth_header.split(" ", 1)[1].strip()
    try:
        principal = get_principal_resolver().from_id_token(token)
    except (AuthError, InvalidTokenError) as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    body: dict[str, Any] = {
        "sub": principal.sub,
        "tenant_id": principal.tenant_id,
        "client_id": principal.client_id,
    }
    if principal.role is not None:
        body["role"] = principal.role
    return body
