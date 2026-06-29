from typing import Optional
from .base import BaseEmbedder
from .huggingface import HuggingFaceEmbedder
from .vertexai import VertexAIEmbedder

class EmbedderFactory:
    """Factory for creating embedder instances matching the application bootstrap configuration."""
    
    @staticmethod
    def create(provider: str, model_name: str, project_id: Optional[str], location: str) -> BaseEmbedder:
        if provider in ("huggingface", "local"):
            return HuggingFaceEmbedder(model_name=model_name)
        elif provider == "vertex":
            if not project_id:
                raise ValueError("project_id is required for VertexAI configuration.")
            return VertexAIEmbedder(project_id=project_id, location=location, model_name=model_name)
        else:
            raise ValueError(f"Unsupported embedder provider: {provider}")