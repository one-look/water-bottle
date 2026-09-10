import logging
from typing import List
from qdrant_client import QdrantClient, models
from qdrant_client.models import Distance, PointStruct, VectorParams

from src.etl.core.config import QdrantConfig
from src.etl.core.exceptions import LoadError
from src.etl.core.logger import setup_logger
from src.etl.domain.entities import Chunk
from src.etl.domain.interfaces import BaseLoader

logger = setup_logger(__name__)


class QdrantVectorLoader(BaseLoader):
    """Loads vector embeddings and metadata payloads into a Qdrant cluster."""

    def __init__(self, config: QdrantConfig):
        """Initializes connection to Qdrant and validates target collection status.

        Args:
            config: Qdrant database host details and collection configuration parameters.
        """
        self.config = config
        try:
            self.client = QdrantClient(
                host=config.host,
                port=config.port,
                grpc_port=config.grpc_port,
                prefer_grpc=config.prefer_grpc,
            )
            self._ensure_collection()
        except Exception as e:
            logger.error(f"Failed to connect or initialize Qdrant client: {str(e)}")
            raise LoadError(f"Qdrant client initialization error: {str(e)}") from e

    def _ensure_collection(self) -> None:
        """Verifies target collection exists or creates it with a tenant keyword index."""
        try:
            exists = self.client.collection_exists(self.config.collection_name)
            if not exists:
                logger.info(f"Creating Qdrant collection: '{self.config.collection_name}'")
                distance_map = {
                    "Cosine": Distance.COSINE,
                    "Euclid": Distance.EUCLID,
                    "Dot": Distance.DOT,
                }

                self.client.create_collection(
                    collection_name=self.config.collection_name,
                    vectors_config=VectorParams(
                        size=self.config.vector_size,
                        distance=distance_map.get(self.config.distance, Distance.COSINE),
                    ),
                )

                self.client.create_payload_index(
                    collection_name=self.config.collection_name,
                    field_name=self.config.tenant_payload_key,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
        except Exception as e:
            logger.error(f"Failed to setup collection '{self.config.collection_name}': {str(e)}")
            raise LoadError(f"Collection initialization failed: {str(e)}") from e

    def load_batch(self, chunks: List[Chunk]) -> int:
        """Upserts a batch of embedded chunks into Qdrant in real-time.

        Args:
            chunks: List of embedded Chunk objects containing vector data.

        Returns:
            Integer count of points successfully loaded.

        Raises:
            LoadError: If database write/upsert operation fails.
        """
        points: List[PointStruct] = []

        for chunk in chunks:
            if chunk.embedding is None:
                continue

            payload = {
                "tenant_id": chunk.tenant_id,
                "text": chunk.text,
                **chunk.metadata,
            }

            points.append(
                PointStruct(
                    id=chunk.chunk_id,
                    vector=chunk.embedding,
                    payload=payload,
                )
            )

        if points:
            try:
                self.client.upsert(
                    collection_name=self.config.collection_name,
                    points=points,
                    wait=True,
                )
                return len(points)
            except Exception as e:
                logger.error(f"Failed to upsert points to Qdrant: {str(e)}")
                raise LoadError(f"Qdrant upsert failed: {str(e)}") from e
        return 0