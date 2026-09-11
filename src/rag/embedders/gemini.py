"""Gemini-backed implementation for runtime query vector embedding generation."""

from typing import List
from google import genai
from google.genai.errors import APIError, ClientError
from pydantic import BaseModel, Field, validate_call

from src.core.logging import setup_logger

logger = setup_logger(__name__)


class EmbedderQueryConfig(BaseModel):
    """Configuration schema for query embedder instantiation."""

    api_key: str = Field(..., min_length=1, description="Google GenAI API key")
    model_name: str = Field("text-embedding-001", min_length=1, description="Embedding model identifier")


class GeminiQueryEmbedder:
    """Generates vector embeddings for search queries using Google Gemini API."""

    @validate_call
    def __init__(self, config: EmbedderQueryConfig):
        """Initializes the Gemini GenAI client with validated configuration.

        Args:
            config: Validated EmbedderQueryConfig instance.
        """
        logger.info(f"Initializing Gemini client with embedding model '{config.model_name}'")
        self.client = genai.Client(api_key=config.api_key)
        self.model_name = config.model_name

    def embed_query(self, text: str) -> List[float]:
        """Generates a dense vector embedding for a single query string.

        Args:
            text: Raw input query string to embed.

        Returns:
            List[float]: Vector representation of input text.

        Raises:
            ValueError: If input query text is empty or blank.
            RuntimeError: If API call fails or returns empty embeddings.
        """
        clean_text = text.strip()
        if not clean_text:
            logger.error("Attempted to embed empty or whitespace-only query string.")
            raise ValueError("Query text cannot be empty.")

        logger.info(f"Generating query vector embedding via model '{self.model_name}'")

        try:
            response = self.client.models.embed_content(
                model=self.model_name,
                contents=clean_text,
            )

            if not response.embeddings or not response.embeddings[0].values:
                logger.error("API response returned empty embeddings array.")
                raise RuntimeError("Empty embedding returned from Gemini API.")

            vector = response.embeddings[0].values
            logger.info(f"Successfully generated embedding vector of dimension {len(vector)}")
            return vector

        except (APIError, ClientError) as e:
            logger.error(f"Gemini API error during embedding generation: {str(e)}", exc_info=True)
            raise RuntimeError(f"Gemini API embedding failure: {str(e)}") from e

        except Exception as e:
            logger.error(f"Unexpected error generating query embedding: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to generate query vector: {str(e)}") from e