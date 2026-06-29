import logging
from typing import List
from .base import BaseEmbedder
from vertexai.language_models import TextEmbeddingModel
import vertexai

logger = logging.getLogger("embedding_service")

class VertexAIEmbedder(BaseEmbedder):
    """Vertex AI text embedding implementation."""

    def __init__(self, project_id: str, location: str, model_name: str):
        """
        Initialize the Vertex AI embedder.

        Args:
            project_id (str): Google Cloud project ID.
            location (str): Google Cloud location.
            model_name (str): Name of the text embedding model.

        Returns:
            None
        """
        logger.info(f"Initializing Vertex AI model: {model_name}")
        try:
            vertexai.init(project=project_id, location=location)
            self.model = TextEmbeddingModel.from_pretrained(model_name)
        except Exception as e:
            logger.critical(f"Failed to authenticate or contact Vertex AI Cloud: {e}")
            raise e

    def embed(self, text: str) -> List[float]:
        """
        Embed the input data.

        Args:
            text (str): Input data to embed.

        Returns:
            List[float]: Embedding of the input data.
        """
        try:
            embeddings = self.model.get_embeddings([text])
            return embeddings[0].values
        except Exception as e:
            logger.error(f"Vertex AI API call failed: {e}")
            raise RuntimeError(f"Vertex Cloud Engine error: {e}")