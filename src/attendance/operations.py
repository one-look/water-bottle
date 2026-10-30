'''Attendance submission against the attendance database.'''

from dataclasses import dataclass
from datetime import date

from sqlalchemy import func, insert, select
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from src.attendance.exceptions import (
    AttendancePersistenceError,
    DuplicateAttendance,
    InvalidAttendance,
    TeacherNotRegistered,
    UnknownStudents,
)
from src.attendance.tables import attendance_records, departments, programs, students, teachers
from src.auth.domain.entities import Principal
from src.core.logging import setup_logger
from src.database.attendance import attendance_db

logger = setup_logger(__name__)

_MAX_PERIOD = 8
_ALLOWED_STATUS = frozenset({"present", "absent"})
_YEAR_LABELS = {
    "1st_year": 1,
    "2nd_year": 2,
    "3rd_year": 3,
    "4th_year": 4,
    "5th_year": 5,
}


@dataclass(frozen=True)
class AttendanceMark:
    '''
    One student's status for a period.
    '''

    register_number: str
    status: str


@dataclass(frozen=True)
class StudentRecord:
    '''
    A student returned for an attendance period.
    '''

    register_number: str
    student_name: str


@dataclass(frozen=True)
class StudentList:
    '''
    Students matching a program, department, and academic year.
    '''

    period: int
    students: tuple[StudentRecord, ...]


@dataclass(frozen=True)
class AttendanceReportRow:
    '''
    One student with optional saved status for a date and period.
    '''

    register_number: str
    student_name: str
    status: str | None


@dataclass(frozen=True)
class AttendanceReport:
    '''
    Class roster merged with attendance rows for one date and period.
    '''

    attendance_date: date
    period: int
    total: int
    present: int
    absent: int
    students: tuple[AttendanceReportRow, ...]


@dataclass(frozen=True)
class AttendanceSubmission:
    '''
    Validated attendance payload. Identity fields are not part of this payload.
    '''

    attendance_date: date
    period: int
    records: tuple[AttendanceMark, ...]


class AttendanceOperations:
    '''
    Submit a class period of attendance in one database transaction.
    '''

    def __init__(self, engine: Engine | None = None) -> None:
        '''
        Args:
            engine: Attendance database engine. Defaults to the shared startup engine.
        '''
        self._engine = engine if engine is not None else attendance_db.engine

    def fetch(
        self,
        *,
        program: str,
        department: str,
        academic_year: str,
        period: int,
        principal: Principal,
    ) -> StudentList:
        '''
        List students for the selected class period.

        Args:
            program: Program code, such as BSC.
            department: Department code, such as DS.
            academic_year: Year label, such as 1st_year.
            period: Period number from 1 to 8.
            principal: Verified teacher.

        Returns:
            StudentList: Selected period and matching students.

        Raises:
            InvalidAttendance: Year or period is not valid.
            TeacherNotRegistered: Email is not in public.teachers.
        '''
        year = _academic_year(academic_year)
        _validate_period(period)

        stmt = (
            select(students.c.register_number, students.c.student_name)
            .join(programs, programs.c.program_id == students.c.program_id)
            .join(departments, departments.c.department_id == students.c.department_id)
            .where(
                func.upper(programs.c.program_code) == program.strip().upper(),
                func.upper(departments.c.department_code) == department.strip().upper(),
                students.c.academic_year == year,
            )
            .order_by(students.c.register_number)
        )
        with self._engine.connect() as conn:
            self._teacher_id(conn, principal.sub)
            rows = conn.execute(stmt).mappings().all()

        return StudentList(
            period=period,
            students=tuple(
                StudentRecord(
                    register_number=row["register_number"],
                    student_name=row["student_name"] or "",
                )
                for row in rows
            ),
        )

    def fetch_period_attendance(
        self,
        *,
        program: str,
        department: str,
        academic_year: str,
        period: int,
        attendance_date: date,
        principal: Principal,
    ) -> AttendanceReport:
        '''
        Return the class roster with saved statuses for one date and period.

        Args:
            program: Program code, such as BSC.
            department: Department code, such as DS.
            academic_year: Year label, such as 1st_year.
            period: Period number from 1 to 8.
            attendance_date: Date to read attendance for.
            principal: Verified teacher.

        Returns:
            AttendanceReport: Counts and per-student statuses (null if not recorded).
        '''
        if not isinstance(attendance_date, date):
            raise InvalidAttendance("attendance_date must be a date")
        roster = self.fetch(
            program=program,
            department=department,
            academic_year=academic_year,
            period=period,
            principal=principal,
        )
        numbers = [student.register_number for student in roster.students]
        if not numbers:
            return AttendanceReport(
                attendance_date=attendance_date,
                period=period,
                total=0,
                present=0,
                absent=0,
                students=(),
            )

        stmt = select(
            attendance_records.c.register_number,
            attendance_records.c.status,
        ).where(
            attendance_records.c.attendance_date == attendance_date,
            attendance_records.c.period == period,
            attendance_records.c.tenant_id == principal.tenant_id,
            attendance_records.c.register_number.in_(numbers),
        )
        with self._engine.connect() as conn:
            saved = {
                row["register_number"]: row["status"]
                for row in conn.execute(stmt).mappings().all()
            }

        present = 0
        absent = 0
        rows: list[AttendanceReportRow] = []
        for student in roster.students:
            raw = saved.get(student.register_number)
            status = raw.strip().lower() if isinstance(raw, str) else None
            if status == "present":
                present += 1
            elif status == "absent":
                absent += 1
            else:
                status = None
            rows.append(
                AttendanceReportRow(
                    register_number=student.register_number,
                    student_name=student.student_name,
                    status=status,
                )
            )

        return AttendanceReport(
            attendance_date=attendance_date,
            period=period,
            total=len(rows),
            present=present,
            absent=absent,
            students=tuple(rows),
        )

    def submit(self, payload: AttendanceSubmission, principal: Principal) -> int:
        '''
        Persist one period of attendance for the authenticated teacher.

        Args:
            payload: Attendance date, period, and student statuses.
            principal: Verified user. Email and tenant come from this object.

        Returns:
            int: Number of rows inserted.

        Raises:
            InvalidAttendance: Payload is empty, out of range, or contains duplicates.
            TeacherNotRegistered: Email is not in public.teachers.
            UnknownStudents: A register number is not in public.students.
            DuplicateAttendance: A row already exists for that student, date, and period.
            AttendancePersistenceError: The database rejected the insert.
        '''
        records = self._validated_records(payload)
        try:
            with self._engine.begin() as conn:
                teacher_id = self._teacher_id(conn, principal.sub)
                self._require_students(conn, [record.register_number for record in records])
                self._reject_existing(conn, payload, records)
                return self.insert(
                    conn,
                    records,
                    attendance_date=payload.attendance_date,
                    period=payload.period,
                    marked_by=teacher_id,
                    tenant_id=principal.tenant_id,
                )
        except IntegrityError as exc:
            logger.error("Attendance insert failed: %s", exc)
            kind = _constraint_kind(exc)
            if kind == "unique":
                raise DuplicateAttendance("Attendance for this period is already submitted") from exc
            if kind == "foreign_key":
                raise UnknownStudents("One or more register numbers are not registered students") from exc
            raise AttendancePersistenceError("Attendance could not be saved") from exc

    def insert(
        self,
        conn: Connection,
        records: tuple[AttendanceMark, ...],
        *,
        attendance_date: date,
        period: int,
        marked_by: int,
        tenant_id: str,
    ) -> int:
        '''
        Insert one attendance row per student on the caller's transaction.

        Args:
            conn: Open transaction connection.
            records: Student statuses.
            attendance_date: Class date.
            period: Period number.
            marked_by: Teacher id from public.teachers.
            tenant_id: Tenant from the authenticated principal.

        Returns:
            int: Number of rows inserted.
        '''
        rows = [
            {
                "register_number": record.register_number,
                "attendance_date": attendance_date,
                "period": period,
                "status": record.status,
                "marked_by": marked_by,
                "tenant_id": tenant_id,
            }
            for record in records
        ]
        conn.execute(insert(attendance_records), rows)
        return len(rows)

    def _validated_records(self, payload: AttendanceSubmission) -> tuple[AttendanceMark, ...]:
        if not isinstance(payload.attendance_date, date):
            raise InvalidAttendance("attendance_date must be a date")
        _validate_period(payload.period)
        if not payload.records:
            raise InvalidAttendance("records must not be empty")

        seen: set[str] = set()
        cleaned: list[AttendanceMark] = []
        for record in payload.records:
            register_number = record.register_number.strip()
            status = record.status.strip().lower()
            if not register_number:
                raise InvalidAttendance("register_number is required")
            if status not in _ALLOWED_STATUS:
                raise InvalidAttendance("status must be present or absent")
            if register_number in seen:
                raise InvalidAttendance("Duplicate register numbers")
            seen.add(register_number)
            cleaned.append(AttendanceMark(register_number=register_number, status=status))
        return tuple(cleaned)

    def _teacher_id(self, conn: Connection, email: str) -> int:
        stmt = select(teachers.c.teacher_id).where(
            func.lower(teachers.c.email) == email.strip().lower()
        )
        teacher_id = conn.execute(stmt).scalar_one_or_none()
        if teacher_id is None:
            raise TeacherNotRegistered("Teacher is not registered")
        return int(teacher_id)

    def _require_students(self, conn: Connection, register_numbers: list[str]) -> None:
        '''
        public.students has no tenant column. A register number is accepted when
        it exists in that table. The attendance row's tenant_id is set from the
        principal, not from the student row.
        '''
        stmt = select(students.c.register_number).where(
            students.c.register_number.in_(register_numbers)
        )
        found = set(conn.execute(stmt).scalars().all())
        if found != set(register_numbers):
            raise UnknownStudents("One or more register numbers are not registered students")

    def _reject_existing(
        self,
        conn: Connection,
        payload: AttendanceSubmission,
        records: tuple[AttendanceMark, ...],
    ) -> None:
        stmt = select(attendance_records.c.register_number).where(
            attendance_records.c.attendance_date == payload.attendance_date,
            attendance_records.c.period == payload.period,
            attendance_records.c.register_number.in_(
                [record.register_number for record in records]
            ),
        )
        if conn.execute(stmt).first() is not None:
            raise DuplicateAttendance("Attendance for this period is already submitted")


def _validate_period(period: int) -> None:
    if period < 1 or period > _MAX_PERIOD:
        raise InvalidAttendance(f"period must be between 1 and {_MAX_PERIOD}")


def _academic_year(value: str) -> int:
    key = value.strip().lower()
    if key in _YEAR_LABELS:
        return _YEAR_LABELS[key]
    if key.isdigit() and int(key) in _YEAR_LABELS.values():
        return int(key)
    raise InvalidAttendance("academic_year must be like 1st_year")


def _constraint_kind(exc: IntegrityError) -> str:
    orig = exc.orig
    code = getattr(orig, "sqlstate", None)
    if code == "23505":
        return "unique"
    if code == "23503":
        return "foreign_key"
    message = str(orig).lower()
    if "unique" in message:
        return "unique"
    if "foreign key" in message:
        return "foreign_key"
    return "other"
