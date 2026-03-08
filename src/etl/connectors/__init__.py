"""
A unified interface for connecting to various data sources.
"""

import logging

# Import the Factory and Connectors for easy external access
from .factory import ConnectorFactory
from .database import DatabaseConnector
from .gmail import GmailConnector
from .web import WebConnector

# Define the public API for the package
__all__ = [
    "ConnectorFactory",
    "DatabaseConnector",
    "GmailConnector",
    "WebConnector",
]

# Set a default logger for the package to prevent "No handler found" warnings
logging.getLogger(__name__).addHandler(logging.NullHandler())