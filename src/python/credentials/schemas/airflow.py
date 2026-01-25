from pydantic import BaseModel
from typing import Optional

class AirflowConnectionSchema(BaseModel):
    """
    Validates and masks sensitive data fetched from Airflow.
    """

    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    type: Optional[str] = None
    verify_certs: Optional[bool] = None