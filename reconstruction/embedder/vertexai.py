import logging
from typing import List
from .base import BaseEmbedder
from vertexai.language_models import TextEmbeddingModel
import vertexai

logger = logging.getLogger("embedding_service")

class VertexAIEmbedder(BaseEmbedder):
    def __init__(self, project_id: str, location: str, model_name: str):
        logger.info(f"Initializing vertex AI model: {model_name}")
        try:
            vertexai.init(project=project_id, location=location)
            self.model = TextEmbeddingModel.from_pretrained(model_name)
        except Exception as e:
            logger.critical(f"Failed to authenticate or contact vertex ai cloud: {e}")
            raise e

    def embed(self, text: str) -> List[float]:
        try:
            embeddings = self.model.get_embeddings([text])
            return embeddings[0].values
        except Exception as e:
            logger.error(f"Vertex AI API call failed: {e}")
            raise RuntimeError(f"Vertex Cloud Engine error: {e}")