from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class Document(BaseModel):
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Chunk(BaseModel):
    chunk_id: str
    text: str
    embedding: Optional[List[float]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)