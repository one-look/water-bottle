import logging
from .base import BaseLoader
from .schemas import IngestorConfig

logger = logging.getLogger(__name__)

class PineconeIngestor(BaseLoader):
    def __init__(self, connection, config):
        """
        Initializes with a Pinecone client and a config dict.
        Config should include: index_name, dimension, metric, and spec.
        """
        self.connection = connection
        self.config = IngestorConfig(**config)
        self.index = None  # Will be set in create() method

    def create(self) -> None:
        """
        Creates the index if it doesn't exist using settings from config.
        """
        name = self.config.index_name
        if name not in self.connection.list_indexes().names():
            logger.info(f"Creating Pinecone index: {name}")
            
            # Get spec from config or create a default one
            spec = self.config.settings.get("spec")
            if spec is None:
                # Create a default serverless spec
                from pinecone import ServerlessSpec
                spec = ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            
            self.connection.create_index(
                name=name,
                dimension=self.config.settings.get("dimension", 128),
                metric=self.config.settings.get("metric", "cosine"),
                spec=spec
            )
        else:
            logger.debug(f"Index {name} already exists.")
        
        # Now get the index handle
        self.index = self.connection.Index(name)

    def __call__(self, data):
        self.create()
        return self.load(data)

class PineconeBulkIngestor(PineconeIngestor):
    """
    Handles batch ingestion into Pinecone.
    """
    def load(self, data):
        """
        Args:
            data (list): List of dicts in format: 
                         {"id": str, "values": list[float], "metadata": dict}
        """
        logger.info(f"Starting bulk ingestion to index: {self.config.index_name}")
        try:
            # Pinecone's upsert is natively optimized for batches
            response = self.index.upsert(vectors=data, batch_size=100)
            logger.info(f"Successfully upserted {response['upserted_count']} records.")
            return response
        except Exception as e:
            logger.exception("Pinecone bulk upsert failed.")
            raise