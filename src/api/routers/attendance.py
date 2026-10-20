'''Teacher attendance submission.'''

from datetime import date
from typing import Literal

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field, field_validator, model_validator

from src.attendance.exceptions import (
    AttendancePersistenceError,
    DuplicateAttendance,
    InvalidAttendance,
    TeacherNotRegistered,
    UnknownStudents,
)
from src.attendance.operations import AttendanceMark, AttendanceOperations, AttendanceSubmission
from src.auth.domain.entities import Principal
from src.core.logging import setup_logger
from src.core.multitenancy import get_current_principal

logger = setup_logger(__name__)

router = APIRouter(prefix="/teacher/attendance", tags=["Attendance"])

TEACHER_ROLE = "teacher"


class AttendanceRecordRequest(BaseModel):
    '''
    One student status in a submission.
    '''

    register_number: str = Field(..., min_length=1)
    status: Literal["present", "absent"]

    @field_validator("register_number")
    @classmethod
    def strip_register_number(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("register_number is required")
        return cleaned


class AttendanceSubmitRequest(BaseModel):
    '''
    Attendance for one class period. Identity is not accepted from the client.
    '''

    attendance_date: date
    period: int = Field(..., ge=1, le=5)
    records: list[AttendanceRecordRequest] = Field(..., min_length=1)

    @model_validator(mode="after")
    def reject_duplicate_register_numbers(self) -> "AttendanceSubmitRequest":
        numbers = [record.register_number for record in self.records]
        if len(numbers) != len(set(numbers)):
            raise ValueError("Duplicate register numbers")
        return self


class AttendanceSubmitResponse(BaseModel):
    '''
    Result of a successful submission.
    '''

    attendance_date: date
    period: int
    inserted: int


def _require_teacher(principal: Principal | None) -> Principal:
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if (principal.role or "").strip().lower() != TEACHER_ROLE:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Teacher role is required",
        )
    return principal


@router.post(
    "/submit",
    response_model=AttendanceSubmitResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit attendance for one class period",
)
async def submit_attendance(body: AttendanceSubmitRequest) -> AttendanceSubmitResponse:
    '''
    Accept attendance from an authenticated teacher and persist it.

    Args:
        body: Date, period, and student statuses.

    Returns:
        AttendanceSubmitResponse: Date, period, and inserted row count.
    '''
    principal = _require_teacher(get_current_principal())
    submission = AttendanceSubmission(
        attendance_date=body.attendance_date,
        period=body.period,
        records=tuple(
            AttendanceMark(register_number=record.register_number, status=record.status)
            for record in body.records
        ),
    )
    try:
        inserted = AttendanceOperations().submit(submission, principal)
    except TeacherNotRegistered as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except InvalidAttendance as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except UnknownStudents as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except DuplicateAttendance as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except (AttendancePersistenceError, RuntimeError) as exc:
        logger.error("Attendance submission failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Attendance could not be saved",
        ) from exc

    return AttendanceSubmitResponse(
        attendance_date=body.attendance_date,
        period=body.period,
        inserted=inserted,
    )
