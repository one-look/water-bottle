"""
Extractors Package
==================
Purpose:
    Handles logic for pulling and normalizing data from various sources.
"""

from .database import DatabaseExtractor
from .web import WebExtractor
from .factory import ExtractorFactory

__all__ = [
    "DatabaseExtractor",
    "WebExtractor",
    "ExtractorFactory",
]