'''SQLAlchemy Core tables for the attendance database.'''

from sqlalchemy import Column, Date, Integer, MetaData, SmallInteger, String, Table, UniqueConstraint

metadata = MetaData()

teachers = Table(
    "teachers",
    metadata,
    Column("teacher_id", Integer, primary_key=True),
    Column("email", String, nullable=False),
)

students = Table(
    "students",
    metadata,
    Column("register_number", String, primary_key=True),
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
