import pytest
from fastapi import HTTPException

from src.auth.domain.entities import Principal
from src.rag.uploads.access import upload_scope_for_principal


def _principal(role: str) -> Principal:
    return Principal(sub="u@school.edu", tenant_id="nmc", client_id="c", role=role)


def test_teacher_gets_session_scope():
    assert upload_scope_for_principal(_principal("teacher")) == "session"


@pytest.mark.parametrize("role", ["employee", "office", "staff"])
def test_employee_roles_get_tenant_scope(role: str):
    assert upload_scope_for_principal(_principal(role)) == "tenant"


def test_student_forbidden():
    with pytest.raises(HTTPException) as exc:
        upload_scope_for_principal(_principal("student"))
    assert exc.value.status_code == 403


def test_guest_forbidden():
    with pytest.raises(HTTPException) as exc:
        upload_scope_for_principal(None)
    assert exc.value.status_code == 401
