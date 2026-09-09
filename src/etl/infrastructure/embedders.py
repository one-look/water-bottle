import time
from typing import Any, Iterator, List, cast
from google import genai
from google.genai.errors import APIError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.etl.core.config import EmbedderConfig
from src.etl.core.logger import setup_logger
from src.etl.domain.entities import Chunk
from src.etl.domain.interfaces import BaseEmbedder

logger = setup_logger(__name__)


class GeminiEmbedder(BaseEmbedder):
    def __init__(self, api_key: str, config: EmbedderConfig):
        self.client = genai.Client(api_key=api_key)
        self.model_name = config.model_name
        self.batch_size = config.batch_size
        self.max_retries = config.max_retries

    def _embed_batch_with_retry(self, batch: List[Chunk]) -> List[Chunk]:
        """Call Gemini API with exponential backoff on rate limits."""
        @retry(
            stop=stop_after_attempt(6),
            wait=wait_exponential(multiplier=2, min=5, max=60),
            retry=retry_if_exception_type((APIError, Exception)),
            reraise=True,
        )
        def _execute_api_call():
            logger.info(f"Embedding batch of {len(batch)} chunks using {self.model_name}")
            
            # Pass raw string list and cast to Any to resolve the Google GenAI SDK type hint union bug
            texts = [chunk.text for chunk in batch]
            
            response = self.client.models.embed_content(
                model=self.model_name,
                contents=cast(Any, texts),
            )
            
            if response.embeddings:
                for idx, emb in enumerate(response.embeddings):
                    batch[idx].embedding = emb.values
            return batch

        return _execute_api_call()

    def embed_chunks(self, chunks: Iterator[Chunk]) -> Iterator[List[Chunk]]:
        batch: List[Chunk] = []

        for chunk in chunks:
            batch.append(chunk)
            if len(batch) >= self.batch_size:
                yield self._embed_batch_with_retry(batch)
                batch = []
                time.sleep(5)

        if batch:
            yield self._embed_batch_with_retry(batch)