from pydantic import BaseModel

class IngestorConfig(BaseModel):
    api_key: str