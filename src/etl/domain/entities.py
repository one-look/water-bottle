from typing import Any, Dict
from pydantic import BaseModel, Field

class Document(BaseModel):
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)