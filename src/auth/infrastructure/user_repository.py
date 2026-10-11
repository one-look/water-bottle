import re
from typing import Optional

from sqlalchemy import Column, MetaData, String, Table, func, select
from sqlalchemy.exc import SQLAlchemyError

from src.auth.domain.entities import UserIdentity
from src.auth.domain.interfaces import UserIdentityRepository
from src.core.logging import setup_logger
from src.database.psql import psql_manager

logger = setup_logger(__name__)

_TABLE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


class PostgresUserRepository(UserIdentityRepository):
    '''
    User identity repository backed by PostgreSQL dim_user (or configured table).
    '''

    def __init__(self, user_table: str) -> None:
        name = user_table.strip()
        if not _TABLE_NAME_RE.fullmatch(name):
            raise ValueError(f"Invalid database user table name: {user_table!r}")
        self._table = Table(
            name,
            MetaData(),
            Column("email", String),
            Column("tenant_id", String),
            Column("role", String),
        )

    def get_by_email(self, email: str) -> Optional[UserIdentity]:
        '''
        Args:
            email (str): User email address.

        Returns:
            Optional[UserIdentity]: User record if found, else None.
        '''
        candidate = email.strip()
        if not candidate or "@" not in candidate:
            return None

        stmt = select(
            self._table.c.email,
            self._table.c.tenant_id,
            self._table.c.role,
        ).where(func.lower(self._table.c.email) == candidate.lower())

        try:
            with psql_manager.engine.connect() as conn:
                row = conn.execute(stmt).mappings().first()
        except SQLAlchemyError as exc:
            logger.error("User lookup failed: %s", exc)
            raise

        if row is None:
            return None

        resolved_email = (row["email"] or "").strip()
        tenant_id = (row["tenant_id"] or "").strip()
        role = (row["role"] or "").strip()
        if not resolved_email or not tenant_id or not role:
            logger.warning("Incomplete user row for email lookup")
            return None

        return UserIdentity(
            email=resolved_email,
            tenant_id=tenant_id,
            role=role,
        )
