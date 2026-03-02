from pydantic import BaseModel, ConfigDict

class ElasticsearchConfig(BaseModel):
    """
    Configuration schema for Elasticsearch connections.
    """
    # This silences the warning for using the name 'schema'

    model_config = ConfigDict(protected_namespaces=())
    schema: str
    host: str
    port: int
    verify_certs: bool