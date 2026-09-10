import time
from typing import Any, Iterator, List, cast
from google import genai
from google.genai.errors import APIError, ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.etl.core.config import EmbedderConfig
from src.etl.core.exceptions import EmbeddingError
from src.etl.core.logger import setup_logger
from src.etl.domain.entities import Chunk
from src.etl.domain.interfaces import BaseEmbedder

logger = setup_logger(__name__)


class GeminiEmbedder(BaseEmbedder):
    """Generates vector embeddings using Google Gemini API with rate-limit retries."""

    def __init__(self, api_key: str, config: EmbedderConfig):
        """Initializes the Gemini client and embedding configuration.

        Args:
            api_key: Secret API key for Google GenAI authentication.
            config: Embedder settings containing model name and batch sizes.
        """
        self.client = genai.Client(api_key=api_key)
        self.model_name = config.model_name
        self.batch_size = config.batch_size
        self.max_retries = config.max_retries

    def _embed_batch_with_retry(self, batch: List[Chunk]) -> List[Chunk]:
        """Executes API embedding requests with exponential backoff retries.

        Args:
            batch: List of Chunk objects to be embedded.

        Returns:
            List of Chunk objects populated with vector embedding values.

        Raises:
            EmbeddingError: If API call fails after exhausting all retries.
        """
        @retry(
            stop=stop_after_attempt(10),  # Increased retry count to survive rate limit resets
            wait=wait_exponential(multiplier=3, min=10, max=60),  # Exponential backoff up to 60s
            retry=retry_if_exception_type((APIError, ClientError, Exception)),
            reraise=True,
        )
        def _execute_api_call():
            logger.info(f"Embedding batch of {len(batch)} chunks using {self.model_name}")
            texts = [chunk.text for chunk in batch]

            response = self.client.models.embed_content(
                model=self.model_name,
                contents=cast(Any, texts),
            )

            if response.embeddings:
                for idx, emb in enumerate(response.embeddings):
                    batch[idx].embedding = emb.values
            return batch

        try:
            return _execute_api_call()
        except Exception as e:
            logger.error(f"Failed to generate embeddings after retries: {str(e)}")
            raise EmbeddingError(f"Embedding API request failed: {str(e)}") from e

    def embed_chunks(self, chunks: Iterator[Chunk]) -> Iterator[List[Chunk]]:
        """Batches and streams chunk embeddings back to caller.

        Args:
            chunks: Generator stream yielding Chunk entities.

        Returns:
            Generator yielding lists of embedded Chunk objects per batch.
        """
        batch: List[Chunk] = []

        for chunk in chunks:
            batch.append(chunk)
            if len(batch) >= self.batch_size:
                embedded_batch = self._embed_batch_with_retry(batch)
                yield embedded_batch
                batch = []
                time.sleep(7)  # Safe pace delay between batch requests (approx 8.5 RPM)

        if batch:
            embedded_batch = self._embed_batch_with_retry(batch)
            yield embedded_batch