from pydantic import BaseModel, ConfigDict

class EmbeddingsConfig(BaseModel):
    # This line tells Pydantic to raise an error for unknown fields
    model_config = ConfigDict(extra="forbid")
    
    # Default model if none is provided in Airflow
    path: str
    content: bool
    backend: str