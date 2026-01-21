from pydantic import BaseModel, ConfigDict, Optional, Any

class JsonTransformerConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    index_name: Optional[str] = None

class StructuredRecord(BaseModel):
    """
    Ensures that every SQL row has an ID. 
    The 'extra="allow"' setting lets it keep all other columns automatically.
    """
    model_config = ConfigDict(extra="allow") 
    id: Any