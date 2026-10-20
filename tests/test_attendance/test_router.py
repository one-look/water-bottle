from datetime import date

import pytest
from pydantic import ValidationError

from src.api.routers.attendance import AttendanceSubmitRequest, _require_teacher
from src.auth.domain.entities import Principal
from fastapi import HTTPException


def test_request_rejects_duplicate_register_numbers():
    with pytest.raises(ValidationError):
        AttendanceSubmitRequest(
            attendance_date=date(2026, 10, 19),
            period=1,
            records=[
                {"register_number": "DS001", "status": "present"},
                {"register_number": "DS001", "status": "absent"},
            ],
        )


def test_request_rejects_period_outside_range():
    with pytest.raises(ValidationError):
        AttendanceSubmitRequest(
            attendance_date=date(2026, 10, 19),
            period=6,
            records=[{"register_number": "DS001", "status": "present"}],
        )


def test_teacher_role_required():
    student = Principal(
        sub="student@school.edu",
        tenant_id="nmc",
        client_id="test-client",
        role="student",
    )
    with pytest.raises(HTTPException) as exc:
        _require_teacher(student)
    assert exc.value.status_code == 403
