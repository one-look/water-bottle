import os
import yaml
from pydantic import BaseModel, Field


class AWSConfig(BaseModel):
    s3_bucket: str = Field(..., min_length=1)
    region_name: str = Field(default="ap-south-2")
    tenant_id: str


class AppConfig(BaseModel):
    aws: AWSConfig

    @classmethod
    def load_from_yaml(cls, config_path: str = "etl_config.yml") -> "AppConfig":
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f)
            
        return cls(**raw_data)