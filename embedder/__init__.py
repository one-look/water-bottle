from .factory import EmbedderFactory
from .base import EmbedderBase
# from .huggingface import HuggingFaceEmbedder

# Lazy import GeminiEmbedder to avoid import errors when google.generativeai is not installed
try:
    from .gemini import GeminiEmbedder
    _gemini_available = True
except ImportError:
    GeminiEmbedder = None
    _gemini_available = False

__all__ = [
    "EmbedderFactory",
    "EmbedderBase",
    # "HuggingFaceEmbedder",
    "GeminiEmbedder",
]
