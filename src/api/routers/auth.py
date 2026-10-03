from typing import Any

from fastapi import APIRouter, Form, HTTPException, Request, status
from fastapi.responses import JSONResponse

from src.auth.exceptions import AuthError, InvalidTokenError
from src.auth.state import get_auth_config, get_token_service
from src.core.logging import setup_logger

logger = setup_logger(__name__)

router = APIRouter(tags=["Auth"])


def _token_error(detail: str, status_code: int = status.HTTP_400_BAD_REQUEST) -> JSONResponse:
    '''
    Build an OAuth2-style error response.

    Args:
        detail (str): Human-readable error description.
        status_code (int): HTTP status code.

    Returns:
        JSONResponse: OAuth error payload.
    '''
    return JSONResponse(
        status_code=status_code,
        content={"error": "invalid_request", "error_description": detail},
    )


@router.post("/auth/token", summary="OAuth2 token endpoint")
async def token_endpoint(
    grant_type: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
    email: str | None = Form(None),
    refresh_token: str | None = Form(None),
) -> dict[str, Any]:
    '''
    OAuth2 token endpoint (client_credentials, user_token, refresh_token).

    Returns:
        dict[str, Any]: Token response on success.
    '''
    service = get_token_service()
    try:
        if grant_type == "client_credentials":
            pair = await service.issue_client_credentials(client_id, client_secret)
        elif grant_type == "user_token":
            if not email:
                return _token_error("email is required for user_token grant")
            pair = await service.issue_user_token(client_id, client_secret, email)
        elif grant_type == "refresh_token":
            if not refresh_token:
                return _token_error("refresh_token is required for refresh_token grant")
            pair = await service.refresh(client_id, client_secret, refresh_token)
        else:
            return _token_error(f"Unsupported grant_type: {grant_type}")
    except AuthError as exc:
        logger.warning(f"Token request failed: {exc}")
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"error": "invalid_client", "error_description": str(exc)},
        )

    return {
        "access_token": pair.access_token,
        "token_type": pair.token_type,
        "expires_in": pair.expires_in,
        "refresh_token": pair.refresh_token,
    }


@router.get("/auth/userinfo", summary="OIDC UserInfo")
async def userinfo(request: Request) -> dict[str, Any]:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
        )
    token = auth_header.split(" ", 1)[1].strip()
    try:
        principal = get_token_service().verify_access_token(token)
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


@router.get("/.well-known/openid-configuration", include_in_schema=False)
async def openid_configuration() -> dict[str, Any]:
    '''
    OIDC discovery document using the canonical issuer from configuration.

    Returns:
        dict[str, Any]: OpenID Provider metadata.
    '''
    issuer = get_auth_config().issuer.rstrip("/")
    return {
        "issuer": issuer,
        "token_endpoint": f"{issuer}/auth/token",
        "userinfo_endpoint": f"{issuer}/auth/userinfo",
        "jwks_uri": f"{issuer}/.well-known/jwks.json",
        "grant_types_supported": [
            "client_credentials",
            "user_token",
            "refresh_token",
        ],
        "token_endpoint_auth_methods_supported": ["client_secret_post"],
        "id_token_signing_alg_values_supported": ["RS256"],
    }


@router.get("/.well-known/jwks.json", include_in_schema=False)
async def jwks() -> dict[str, Any]:
    return get_token_service().jwks_document()
