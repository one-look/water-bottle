'''SQLAlchemy Core tables for the attendance database.'''

from sqlalchemy import Column, Date, Integer, MetaData, SmallInteger, String, Table, UniqueConstraint

metadata = MetaData()

teachers = Table(
    "teachers",
    metadata,
    Column("teacher_id", Integer, primary_key=True),
    Column("email", String, nullable=False),
)

programs = Table(
    "programs",
    metadata,
    Column("program_id", Integer, primary_key=True),
    Column("program_code", String, nullable=False),
)

departments = Table(
    "departments",
    metadata,
    Column("department_id", Integer, primary_key=True),
    Column("department_code", String, nullable=False),
)

students = Table(
    "students",
    metadata,
    Column("register_number", String, primary_key=True),
    Column("student_name", String),
    Column("academic_year", SmallInteger),
    Column("program_id", Integer),
    Column("department_id", Integer),
)

attendance_records = Table(
    "attendance_records",
    metadata,
    Column("register_number", String, nullable=False),
    Column("attendance_date", Date, nullable=False),
    Column("period", SmallInteger, nullable=False),
    Column("status", String, nullable=False),
    Column("marked_by", Integer, nullable=False),
    Column("tenant_id", String, nullable=False),
    UniqueConstraint("register_number", "attendance_date", "period"),
)
