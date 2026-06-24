"""
Extractors Package
==================
Purpose:
    Handles logic for pulling and normalizing data from various sources.
"""

from .rdbms import RDBMSExtractor

__all__ = [
    "RDBMSExtractor",
]