from pydantic import BaseModel, ConfigDict
from typing import List, Optional

class DatabaseTableConfig(BaseModel):
    """Schema for a single table entry in RDBMS"""

    model_config = ConfigDict(protected_namespaces=())
    table_name: str 
    schema: str
    columns: Optional[List[str]]

class DatabaseExtractorConfig(BaseModel):
    
    # This validates that 'tables' is a list of DatabaseTableConfig objects
    tables: List[DatabaseTableConfig]