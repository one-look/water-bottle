import os
import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AWSConfig(BaseModel):
    s3_bucket: str = Field(..., min_length=1)
    region_name: str = Field(default="ap-south-2")
    tenant_id: str = Field(..., min_length=1)


class TransformerConfig(BaseModel):
    chunk_size: int = Field(default=512, gt=0)
    chunk_overlap: int = Field(default=50, ge=0)


class EmbedderConfig(BaseModel):
    model_name: str = Field(default="gemini-embedding-001")
    batch_size: int = Field(default=16, gt=0)
    max_retries: int = Field(default=5, ge=1)


class QdrantConfig(BaseSettings):
    """Qdrant configuration that automatically reads Cloud credentials from .env using Pydantic Settings."""
    url: str = Field(..., validation_alias="QDRANT_URL")
    api_key: str = Field(..., validation_alias="QDRANT_API_KEY")
    collection_name: str
    vector_size: int
    distance: str
    tenant_payload_key: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


class AppConfig(BaseModel):
    aws: AWSConfig
    transformer: TransformerConfig = Field(default_factory=TransformerConfig)
    embedder: EmbedderConfig = Field(default_factory=EmbedderConfig)
    qdrant: QdrantConfig = Field(default_factory=QdrantConfig)

    @classmethod
    def load_from_yaml(cls, config_path: str = "etl_config.yml") -> "AppConfig":
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f)

        return cls(**raw_data)