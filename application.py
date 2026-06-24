import yaml
from typing import Optional, Any
from pydantic import BaseModel

class EmbedConfig(BaseModel):
    enabled: bool = False
    provider: str = "local"
    model_name: str = "all-MiniLM-L6-v2"
    project_id: Optional[str] = None
    location: str = "us-central1"

class RagConfig(BaseModel):
    enabled: bool = False
    vector_db: str = ""
    index_name: str = ""

class AttendanceConfig(BaseModel):
    enabled: bool = False


# Central Registry
class Application:
    def __init__(self, config_path: str = "config.yml"):
        with open(config_path, "r") as f:
            raw_data = yaml.safe_load(f) or {}

        services = raw_data.get("services", {})

        # Pydantic validates raw data right here
        self.embed_config = EmbedConfig(**services.get("embedder", {}))
        self.rag_config = RagConfig(**services.get("rag", {}))
        self.attendance_config = AttendanceConfig(**services.get("attendance", {}))

        # Runtime engine slots
        self.embedder: Optional[Any] = None
        self.rag: Optional[Any] = None
        self.attendance: Optional[Any] = None

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

        if self.rag_config.enabled:
            from src.rag.engine import RagEngine
            self.rag = RagEngine(
                config=self.rag_config
            )

        if self.attendance_config.enabled:
            from src.attendance.engine import AttendanceEngine
            self.attendance = AttendanceEngine(
                config=self.attendance_config
            )