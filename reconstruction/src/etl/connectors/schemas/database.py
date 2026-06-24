from pydantic import BaseModel, Field, AliasChoices
from typing import Optional

class DatabaseConfig(BaseModel):
    """
    Configuration schema for Database connections.
    """
    
    type: str
    host: str
    port: int
    login: str
    password: Optional[str]

    # This tells Pydantic: "Try to find 'database' first. 
    # If it's not there, look for 'schema'."
    database: str = Field(validation_alias=AliasChoices('database', 'schema'))