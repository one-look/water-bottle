'''PostgreSQL access: pool lifecycle and IP location event writes.'''

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import Request
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, insert
from sqlalchemy.engine import Engine

from src.core.logging import setup_logger

logger = setup_logger(__name__)

metadata = MetaData()

ip_location_events = Table(
    "ip_location_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("session_id", String(128), nullable=False),
    Column("tenant_id", String(64), nullable=False),
    Column("chat_mode", String(16), nullable=False),
    Column("user_sub", String(256), nullable=True),
    Column("ip_address", String(64), nullable=True),
    Column("city", String(128), nullable=True),
    Column("region", String(128), nullable=True),
    Column("country", String(64), nullable=True),
)


class PsqlManager:
    '''
    Manages SQLAlchemy engine lifecycle for PostgreSQL.
    '''

    def __init__(self) -> None:
        self._engine: Optional[Engine] = None

    def init(self, engine: Engine, create_tables: bool = True) -> None:
        '''
        Args:
            engine: SQLAlchemy engine from the SQL connector.
            create_tables: Create tracking table if missing.
        '''
        self._engine = engine
        if create_tables:
            metadata.create_all(engine)
            logger.info("PostgreSQL pool ready (ip_location_events).")

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def is_ready(self) -> bool:
        return self._engine is not None

    @property
    def engine(self) -> Engine:
        if not self._engine:
            raise RuntimeError("PostgreSQL is not initialized.")
        return self._engine


psql_manager = PsqlManager()


def client_ip(request: Request) -> str:
    '''
    Resolve client IP behind reverse proxies when possible.
    '''
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    if request.client:
        return request.client.host
    return ""


def _lookup_geo(ip: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not ip or ip.startswith("127.") or ip == "::1":
        return None, None, None
    try:
        with httpx.Client(timeout=3.0) as client:
            response = client.get(
                f"http://ip-api.com/json/{ip}",
                params={"fields": "status,city,regionName,country"},
            )
            response.raise_for_status()
            data = response.json()
        if data.get("status") != "success":
            return None, None, None
        return data.get("city"), data.get("regionName"), data.get("country")
    except Exception as exc:
        logger.warning("IP geolocation lookup failed: %s", exc)
        return None, None, None


def insert_ip_location_event(
    *,
    session_id: str,
    tenant_id: str,
    chat_mode: str,
    user_sub: Optional[str],
    ip_address: str,
    city: Optional[str],
    region: Optional[str],
    country: Optional[str],
) -> None:
    '''
    Insert one IP / geolocation row for a RAG request.
    '''
    row = {
        "created_at": datetime.now(timezone.utc),
        "session_id": session_id,
        "tenant_id": tenant_id,
        "chat_mode": chat_mode,
        "user_sub": user_sub,
        "ip_address": ip_address or None,
        "city": city,
        "region": region,
        "country": country,
    }
    with psql_manager.engine.connect() as conn:
        conn.execute(insert(ip_location_events).values(**row))
        conn.commit()


def track_rag_request(
    request: Request,
    *,
    session_id: str,
    tenant_id: str,
    chat_mode: str,
    user_sub: Optional[str] = None,
) -> None:
    '''
    Best-effort IP geolocation + persist (no-op if DB not initialized).
    '''
    if not psql_manager.is_ready():
        return
    ip = client_ip(request)
    city, region, country = _lookup_geo(ip)
    insert_ip_location_event(
        session_id=session_id,
        tenant_id=tenant_id,
        chat_mode=chat_mode,
        user_sub=user_sub,
        ip_address=ip,
        city=city,
        region=region,
        country=country,
    )
