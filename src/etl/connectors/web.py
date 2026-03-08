import logging
import httpx
from .schemas import WebConfig

logger = logging.getLogger(__name__)

class WebConnector:
    def __init__(self, config: dict):
        self.config = WebConfig(**config)
        self._client = None

    def __call__(self):
        self.connect()
        return self._client

    def connect(self) -> None:
        try:
            self._client = httpx.Client(
                timeout=self.config.timeout,
                verify=self.config.verify_ssl
            )
            # Connectivity test
            self._client.head(self.config.url).raise_for_status()
            logger.info(f"Connected to {self.config.url}")
        except Exception as e:
            logger.exception("Web connection failed")
            raise