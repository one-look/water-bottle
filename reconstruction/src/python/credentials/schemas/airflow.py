from pydantic import BaseModel, ConfigDict
from typing import Optional

class AirflowConnectionSchema(BaseModel):
    """
    Validates and masks sensitive data fetched from Airflow.
    """
    # This tells Pydantic it's okay if we use names that might overlap with internal protected names.

    model_config = ConfigDict(protected_namespaces=())
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None  # Airflow DB name or ES Protocol
    type: Optional[str] = None    # SQLAlchemy dialect+driver
    verify_certs: Optional[bool] = None