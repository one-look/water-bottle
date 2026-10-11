import pytest

from src.auth.infrastructure.user_repository import PostgresUserRepository


def test_postgres_user_repository_rejects_invalid_table_name():
    with pytest.raises(ValueError):
        PostgresUserRepository("dim-user")


def test_get_by_email_rejects_invalid_input():
    repo = PostgresUserRepository("dim_user")
    assert repo.get_by_email("") is None
    assert repo.get_by_email("   ") is None
    assert repo.get_by_email("not-email") is None
