'''SQLAlchemy engine factory from application config and secrets.'''

from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class SqlPoolConfig(BaseModel):
    '''
    Non-secret PostgreSQL pool options (from config.yml database section).
    '''

    pool_size: int = Field(5, ge=1)
    max_overflow: int = Field(10, ge=0)
    pool_pre_ping: bool = True


def _normalize_url(database_url: str) -> str:
    url = database_url.strip()
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


def build_engine(yaml_config: dict[str, Any] | None, database_url: str) -> Engine:
    '''
    Build a SQLAlchemy engine for PostgreSQL (e.g. Neon).

    Args:
        yaml_config: Optional ``database`` block from config.yml.
        database_url: Connection string from settings (DATABASE_URL).

    Returns:
        Engine: Configured SQLAlchemy engine with a connection pool.
    '''
    pool_fields = set(SqlPoolConfig.model_fields)
    pool_payload = {k: v for k, v in (yaml_config or {}).items() if k in pool_fields}
    pool = SqlPoolConfig(**pool_payload)
    return create_engine(
        _normalize_url(database_url),
        pool_size=pool.pool_size,
        max_overflow=pool.max_overflow,
        pool_pre_ping=pool.pool_pre_ping,
    )
