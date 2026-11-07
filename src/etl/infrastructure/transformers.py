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
    """Splits raw documents into parent context blocks and granular child search chunks."""

    def __init__(self, config: TransformerConfig):
        """Initializes both parent and child splitters based on configuration."""
        self.parent_splitter = SentenceSplitter(
            chunk_size=config.parent_chunk_size,
            chunk_overlap=config.parent_chunk_overlap,
        )
        self.child_splitter = SentenceSplitter(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap,
        )

    def transform(self, document: Document) -> Iterator[Chunk]:
        """Transforms an extracted document into a streaming generator of child chunks linked to their parent context.

        Args:
            document: Raw document domain object containing text content and metadata.

        Returns:
            Generator yielding individual Chunk objects containing parent references in metadata.

        Raises:
            TransformationError: If parsing or chunk generation fails.
        """
        try:
            llama_doc = LlamaDocument(
                text=document.content, metadata=document.metadata
            )

            # Step 1: Split into larger parent chunks
            parent_nodes = self.parent_splitter.get_nodes_from_documents([llama_doc])
            tenant_id = document.metadata.get("tenant_id", "default")
            source_key = document.metadata.get("source_key", "unknown")
            section_identifier = document.metadata.get("title") or document.metadata.get("section_key") or "section"

            logger.info(
                f"Splitting document '{source_key}' into {len(parent_nodes)} parent context blocks for tenant '{tenant_id}'"
            )

            for parent_idx, parent_node in enumerate(parent_nodes):
                parent_text = parent_node.get_content()
                parent_seed = f"{tenant_id}:{source_key}:{section_identifier}:parent:{parent_idx}"
                parent_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, parent_seed))

                # Step 2: Split each parent into smaller child chunks for fine-grained search
                child_llama_doc = LlamaDocument(text=parent_text, metadata=document.metadata)
                child_nodes = self.child_splitter.get_nodes_from_documents([child_llama_doc])

                for child_idx, child_node in enumerate(child_nodes):
                    child_seed = f"{parent_id}:child:{child_idx}"
                    child_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, child_seed))

                    chunk_meta = {
                        **document.metadata,
                        "parent_id": parent_id,
                        "parent_text": parent_text,
                        "parent_index": parent_idx,
                        "child_index": child_idx,
                        "start_char_idx": getattr(child_node, "start_char_idx", None),
                        "end_char_idx": getattr(child_node, "end_char_idx", None),
                    }

                    yield Chunk(
                        chunk_id=child_id,
                        tenant_id=tenant_id,
                        text=child_node.get_content(),
                        metadata=chunk_meta,
                    )
        except Exception as e:
            logger.error(f"Failed to transform document content: {str(e)}")
            raise TransformationError(f"Document transformation failed: {str(e)}") from e