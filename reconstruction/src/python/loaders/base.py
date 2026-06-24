"""
Defines the abstract interface for all data loaders.
"""

import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseLoader(ABC):
    """
    Abstract base class that enforces a 'load' method across all 
    ingestion strategies (Single, Bulk, etc.).
    """

    @abstractmethod
    def load(self, data):
        """
        Enforces the implementation of the load method in child classes.
        
        Args:
            data (Any): The data or iterator to be loaded into the destination.
            
        Returns:
            None
        """
        raise NotImplementedError("Child classes must implement the load method!")