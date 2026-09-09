import os
import uuid
from typing import Iterator

import nltk
from llama_index.core import Document as LlamaDocument
from llama_index.core.node_parser import SentenceSplitter

from src.etl.core.config import TransformerConfig
from src.etl.domain.entities import Chunk, Document
from src.etl.domain.interfaces import BaseTransformer

# Persistent NLTK directory configuration
nltk_data_path = os.getenv("NLTK_DATA", os.path.expanduser("~/nltk_data"))
if nltk_data_path not in nltk.data.path:
    nltk.data.path.insert(0, nltk_data_path)


class SentenceSplitterTransformer(BaseTransformer):
    def __init__(self, config: TransformerConfig):
        self.splitter = SentenceSplitter(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap,
        )

    def transform(self, document: Document) -> Iterator[Chunk]:
        llama_doc = LlamaDocument(
            text=document.content, metadata=document.metadata
        )

        raw_nodes = self.splitter.get_nodes_from_documents([llama_doc])
        tenant_id = document.metadata.get("tenant_id", "default")
        source_key = document.metadata.get("source_key", "unknown")

        for idx, node in enumerate(raw_nodes):
            # Deterministic, idempotent UUID namespace generation
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
                text=node.get_content(),
                metadata=chunk_meta,
            )