import yaml
from typing import Optional, Any
from pydantic import BaseModel

class EmbedConfig(BaseModel):
    enabled: bool = False
    provider: str = "local"
    model_name: str = "all-MiniLM-L6-v2"
    project_id: Optional[str] = None
    location: str = "us-central1"

class Application:
    def __init__(self, config_path: str = "config.yml"):
        with open(config_path, "r") as f:
            raw_data = yaml.safe_load(f) or {}

        services = raw_data.get("services", {})
        self.embed_config = EmbedConfig(**services.get("embedder", {}))
        self.embedder: Optional[Any] = None

        self._bootstrap()

    def _bootstrap(self):
        if self.embed_config.enabled:
            from src.embedder.factory import EmbedderFactory
            self.embedder = EmbedderFactory.create(
                provider=self.embed_config.provider,
                model_name=self.embed_config.model_name,
                project_id=self.embed_config.project_id,
                location=self.embed_config.location
            )