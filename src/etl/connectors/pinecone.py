import logging
from pinecone import Pinecone
from .schemas import IngestorConfig

logger = logging.getLogger(__name__)

class PineconeConnector:
    """
    Wrapper around the official Pinecone client.
    Handles configuration parsing and lazy initialization of the API connection.
    """

    def __init__(self, config: dict):
        """
        Args:
            config (dict): Contains connection parameters.
                           Expected keys: 'api_key' (often mapped from 'password').
        """
        if "api_key" not in config and "password" in config:
            config["api_key"] = config["password"]

        self.config = IngestorConfig(**config)
        self._client = None
        logger.debug("PineconeConnector initialized.")

    def __call__(self) -> Pinecone:
        """Ensures the connection is established before returning the client."""
        self.connect()
        return self._client

    def connect(self) -> None:
        """
        Initializes the Pinecone client. 
        Note: Pinecone v5+ doesn't use a 'ping', but we verify by listing indexes.
        """
        try:
            logger.info("Attempting to initialize Pinecone client.")
            self._client = Pinecone(api_key=self.config.api_key)
            
            # Connection verification: list_indexes() triggers an API call
            self._client.list_indexes()
            logger.info("Successfully verified Pinecone connection.")

        except Exception as e:
            logger.exception(f"Failed to initialize Pinecone client: {e}")
            raise