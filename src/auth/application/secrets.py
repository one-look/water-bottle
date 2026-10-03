from src.config.settings import settings

# Maps OAuth client_id (config.yml) to Settings attribute holding the client secret.
CLIENT_SECRET_FIELDS: dict[str, str] = {
    "water-bottle-client": "AUTH_WATER_BOTTLE_CLIENT_SECRET",
}


def resolve_auth_issuer() -> str:
    '''
    Resolve the canonical OAuth/OIDC issuer URL from environment settings.

    Args:
        None

    Returns:
        str: Issuer URL without trailing slash.

    Raises:
        ValueError: If neither AUTH_ISSUER nor PUBLIC_BASE_URL is set.
    '''
    issuer = (settings.AUTH_ISSUER or settings.PUBLIC_BASE_URL).strip().rstrip("/")
    if not issuer:
        raise ValueError(
            "OAuth issuer is required: set AUTH_ISSUER or PUBLIC_BASE_URL in the environment"
        )
    return issuer


def resolve_client_secrets(registered_client_ids: set[str]) -> dict[str, str]:
    '''
    Load client secrets from Settings for each registered OAuth client.

    Args:
        registered_client_ids (set[str]): client_id values from config.yml.

    Returns:
        dict[str, str]: Mapping of client_id to client secret.

    Raises:
        ValueError: If a registered client has no secret mapping or empty secret.
    '''
    secrets: dict[str, str] = {}
    for client_id in registered_client_ids:
        field_name = CLIENT_SECRET_FIELDS.get(client_id)
        if not field_name:
            raise ValueError(
                f"No Settings field mapped for OAuth client_id '{client_id}'. "
                f"Update CLIENT_SECRET_FIELDS in src/auth/application/secrets.py"
            )
        value = getattr(settings, field_name, "").strip()
        if not value:
            raise ValueError(
                f"Missing OAuth client secret: set {field_name} in the environment"
            )
        secrets[client_id] = value
    return secrets
