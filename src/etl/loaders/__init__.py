"""
Handles final data delivery to Elasticsearch/OpenSearch.
"""

from .factory import LoaderFactory
from .base import BaseLoader
from .schemas.ingestor import IngestorConfig

__all__ = [
    "LoaderFactory",
    "BaseLoader",
    "IngestorConfig"
]