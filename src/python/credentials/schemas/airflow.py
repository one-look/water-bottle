from pydantic import BaseModel
from typing import Optional

class AirflowConnectionSchema(BaseModel):
    """
    Validates and masks sensitive data fetched from Airflow.
    """

    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None