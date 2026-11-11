'''Ingest chat uploads into Qdrant using the ETL chunk / embed / load pipeline.'''

from typing import Any, Dict, Literal

from src.config.settings import settings
from src.core.logging import setup_logger
from src.etl.core.config import EmbedderConfig, QdrantConfig, TransformerConfig
from src.etl.domain.entities import Document
from src.etl.infrastructure.embedders import GeminiEmbedder
from src.etl.infrastructure.loaders import QdrantVectorLoader
from src.etl.infrastructure.transformers import SentenceSplitterTransformer

logger = setup_logger(__name__)

UploadScope = Literal["tenant", "session"]
_VECTOR_SIZE = 3072


class ChatDocumentIngestService:
    '''Chunk, embed, and upsert one uploaded document.'''

    def __init__(self, app_config: Dict[str, Any]) -> None:
        qdrant_yaml = app_config.get("qdrant", {})
        collection = qdrant_yaml.get("collection_name", "data_science")
        self._transformer = SentenceSplitterTransformer(TransformerConfig())
        self._embedder = GeminiEmbedder(
            settings.GEMINI_API_KEY,
            EmbedderConfig(
                model_name=app_config.get("embedding", {}).get("model_name", "gemini-embedding-001"),
                task_type="RETRIEVAL_DOCUMENT",
                sparse_model_name=qdrant_yaml.get("sparse_model_name", "Qdrant/bm25"),
            ),
        )
        self._loader = QdrantVectorLoader(
            QdrantConfig(
                collection_name=collection,
                vector_size=_VECTOR_SIZE,
                distance="Cosine",
                tenant_payload_key="tenant_id",
            )
        )

    def ingest(
        self,
        *,
        text: str,
        filename: str,
        tenant_id: str,
        scope: UploadScope,
        session_id: str | None,
        uploaded_by: str,
    ) -> int:
        '''
        Returns:
            Number of chunk points written to Qdrant.
        '''
        metadata: Dict[str, Any] = {
            "tenant_id": tenant_id,
            "source_key": f"upload:{filename}",
            "title": filename,
            "scope": scope,
            "uploaded_by": uploaded_by,
            "content_type": "chat_upload",
        }
        if scope == "session":
            if not session_id or not session_id.strip():
                raise ValueError("session_id is required for teacher uploads")
            metadata["session_id"] = session_id.strip()

        document = Document(content=text, metadata=metadata)
        chunk_stream = self._transformer.transform(document)
        total = 0
        for batch in self._embedder.embed_chunks(chunk_stream):
            total += self._loader.load_batch(batch)
        logger.info(
            "Ingested upload '%s' scope=%s tenant=%s chunks=%s",
            filename,
            scope,
            tenant_id,
            total,
        )
        return total
