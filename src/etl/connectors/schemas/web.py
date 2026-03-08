from pydantic import BaseModel, HttpUrl
from typing import Optional

class WebConfig(BaseModel):
    url: str
    timeout: int = 30
    verify_ssl: bool = True