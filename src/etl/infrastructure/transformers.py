import os
import uuid
from typing import Iterator

import nltk
from llama_index.core import Document as LlamaDocument
from llama_index.core.node_parser import SentenceSplitter

from src.etl.core.config import TransformerConfig
from src.etl.core.exceptions import TransformationError
from src.etl.core.logger import setup_logger
from src.etl.domain.entities import Chunk, Document
from src.etl.domain.interfaces import BaseTransformer

logger = setup_logger(__name__)

# Persistent NLTK directory configuration
nltk_data_path = os.getenv("NLTK_DATA", os.path.expanduser("~/nltk_data"))
if nltk_data_path not in nltk.data.path:
    nltk.data.path.insert(0, nltk_data_path)


class SentenceSplitterTransformer(BaseTransformer):
    """Splits raw documents into smaller semantic text chunks using SentenceSplitter."""

    def __init__(self, config: TransformerConfig):
        """Initializes the transformer with sentence boundary rules.

        Args:
            config: Configuration rules containing chunk size and overlap values.
        """
        self.splitter = SentenceSplitter(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap,
        )

    def transform(self, document: Document) -> Iterator[Chunk]:
        """Transforms an extracted document into a streaming generator of chunks.

        Args:
            document: Raw document domain object containing text content and metadata.

        Returns:
            Generator yielding individual Chunk objects.

        Raises:
            TransformationError: If parsing or chunk generation fails.
        """
        try:
            llama_doc = LlamaDocument(
                text=document.content, metadata=document.metadata
            )

            raw_nodes = self.splitter.get_nodes_from_documents([llama_doc])
            tenant_id = document.metadata.get("tenant_id", "default")
            source_key = document.metadata.get("source_key", "unknown")

            logger.info(
                f"Splitting document '{source_key}' into {len(raw_nodes)} chunks for tenant '{tenant_id}'"
            )

            for idx, node in enumerate(raw_nodes):
                chunk_seed = f"{tenant_id}:{source_key}:{idx}"
                deterministic_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_seed))

                chunk_meta = {
                    **document.metadata,
                    "chunk_index": idx,
                    "start_char_idx": getattr(node, "start_char_idx", None),
                    "end_char_idx": getattr(node, "end_char_idx", None),
                }

                yield Chunk(
                    chunk_id=deterministic_id,
                    tenant_id=tenant_id,
                    text=node.get_content(),
                    metadata=chunk_meta,
                )
        except Exception as e:
            logger.error(f"Failed to transform document content: {str(e)}")
            raise TransformationError(f"Document transformation failed: {str(e)}") from e