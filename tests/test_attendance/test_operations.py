from datetime import date

import pytest
from sqlalchemy import create_engine, insert, select

from src.attendance.exceptions import (
    DuplicateAttendance,
    InvalidAttendance,
    TeacherNotRegistered,
    UnknownStudents,
)
from src.attendance.operations import AttendanceMark, AttendanceOperations, AttendanceSubmission
from src.attendance.tables import (
    attendance_records,
    departments,
    metadata,
    programs,
    students,
    teachers,
)
from src.auth.domain.entities import Principal


def _principal() -> Principal:
    return Principal(
        sub="teacher@school.edu",
        tenant_id="nmc",
        client_id="test-client",
        role="teacher",
    )


def _payload(*marks: tuple[str, str]) -> AttendanceSubmission:
    return AttendanceSubmission(
        attendance_date=date(2026, 10, 19),
        period=1,
        records=tuple(
            AttendanceMark(register_number=register_number, status=status)
            for register_number, status in marks
        ),
    )


@pytest.fixture
def operations():
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(insert(teachers).values(teacher_id=7, email="teacher@school.edu"))
        conn.execute(insert(students).values(register_number="DS001"))
        conn.execute(insert(students).values(register_number="DS002"))
    return AttendanceOperations(engine)


def test_submit_inserts_teacher_and_tenant_from_principal(operations):
    inserted = operations.submit(_payload(("DS001", "present"), ("DS002", "absent")), _principal())

    assert inserted == 2
    with operations._engine.connect() as conn:
        rows = conn.execute(
            select(
                attendance_records.c.register_number,
                attendance_records.c.status,
                attendance_records.c.marked_by,
                attendance_records.c.tenant_id,
            ).order_by(attendance_records.c.register_number)
        ).mappings().all()

    assert [dict(row) for row in rows] == [
        {
            "register_number": "DS001",
            "status": "present",
            "marked_by": 7,
            "tenant_id": "nmc",
        },
        {
            "register_number": "DS002",
            "status": "absent",
            "marked_by": 7,
            "tenant_id": "nmc",
        },
    ]


def test_submit_rejects_duplicate_register_numbers_without_insert(operations):
    with pytest.raises(InvalidAttendance):
        operations.submit(_payload(("DS001", "present"), ("DS001", "absent")), _principal())

    with operations._engine.connect() as conn:
        count = conn.execute(select(attendance_records.c.register_number)).all()
    assert count == []


def test_submit_rejects_unknown_student_and_rolls_back(operations):
    with pytest.raises(UnknownStudents):
        operations.submit(_payload(("DS001", "present"), ("MISSING", "absent")), _principal())

    with operations._engine.connect() as conn:
        count = conn.execute(select(attendance_records.c.register_number)).all()
    assert count == []


def test_submit_rejects_unknown_teacher(operations):
    principal = Principal(
        sub="other@school.edu",
        tenant_id="nmc",
        client_id="test-client",
        role="teacher",
    )
    with pytest.raises(TeacherNotRegistered):
        operations.submit(_payload(("DS001", "present")), principal)


def test_submit_rejects_existing_period_without_overwrite(operations):
    operations.submit(_payload(("DS001", "present")), _principal())

    with pytest.raises(DuplicateAttendance):
        operations.submit(_payload(("DS001", "absent"), ("DS002", "present")), _principal())

    with operations._engine.connect() as conn:
        rows = conn.execute(
            select(attendance_records.c.register_number, attendance_records.c.status)
        ).mappings().all()

    assert [dict(row) for row in rows] == [
        {"register_number": "DS001", "status": "present"},
    ]


def test_fetch_returns_students_for_program_department_and_year():
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(insert(teachers).values(teacher_id=7, email="teacher@school.edu"))
        conn.execute(insert(programs).values(program_id=1, program_code="BSC"))
        conn.execute(insert(departments).values(department_id=1, department_code="DS"))
        conn.execute(
            insert(students).values(
                register_number="DS002",
                student_name="Second",
                academic_year=1,
                program_id=1,
                department_id=1,
            )
        )
        conn.execute(
            insert(students).values(
                register_number="DS001",
                student_name="First",
                academic_year=1,
                program_id=1,
                department_id=1,
            )
        )
        conn.execute(
            insert(students).values(
                register_number="DS003",
                student_name="Other Year",
                academic_year=2,
                program_id=1,
                department_id=1,
            )
        )

    result = AttendanceOperations(engine).fetch(
        program="bsc",
        department="ds",
        academic_year="1st_year",
        period=2,
        principal=_principal(),
    )

    assert result.period == 2
    assert [(student.register_number, student.student_name) for student in result.students] == [
        ("DS001", "First"),
        ("DS002", "Second"),
    ]


def test_fetch_rejects_unknown_year_label():
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    with pytest.raises(InvalidAttendance):
        AttendanceOperations(engine).fetch(
            program="BSC",
            department="DS",
            academic_year="first",
            period=2,
            principal=_principal(),
        )
