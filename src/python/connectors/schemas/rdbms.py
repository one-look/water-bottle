from pydantic import BaseModel
from typing import Optional

class RDBMSConfig(BaseModel):
    """
    Configuration schema for RDBMS connections.
    """

    type: str
    host: str
    port: int
    login: str
    password: Optional[str]
    database: str