from abc import ABC, abstractmethod
from typing import Iterator, List
from src.etl.domain.entities import Chunk, Document


class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, source_key: str, tenant_id: str) -> Document:
        """Extract a single document from source for a specific tenant."""
        pass


class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, document: Document) -> Iterator[Chunk]:
        """Transform extracted document into a stream/generator of chunks."""
        pass


class BaseEmbedder(ABC):
    @abstractmethod
    def embed_chunks(self, chunks: Iterator[Chunk]) -> Iterator[List[Chunk]]:
        """Batch and embed a stream of chunks, yielding embedded batches."""
        pass