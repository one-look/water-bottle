from pydantic import BaseModel, ConfigDict
from typing import List, Optional

class RDBMSTableConfig(BaseModel):
    """Schema for a single table entry in RDBMS"""
    model_config = ConfigDict(extra="allow")

    table_name: str
    schema_name: str
    columns: Optional[List[str]]

class RDBMSExtractorConfig(BaseModel):
    
    # This validates that 'tables' is a list of RDBMSTableConfig objects
    tables: List[RDBMSTableConfig]