import logging

from .factory import EmbedderFactory
from .txtai import TxtaiEmbeddings
from .schemas import EmbeddingsConfig

# Define the public API for the package
__all__ = [
    "EmbedderFactory",
    "TxtaiEmbeddings",
    "EmbeddingsConfig",
]

# Set a default logger for the package to prevent "No handler found" warnings
logging.getLogger(__name__).addHandler(logging.NullHandler())   