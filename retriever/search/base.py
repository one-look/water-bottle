from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class SearchResult:
    """Data class for search results."""
    content: str
    score: float
    metadata: Dict[str, Any]


class RetrieverBase(ABC):
    """Abstract base class for document retrieval services."""
    
    @abstractmethod
    async def search(self, query_vector: List[float], limit: int = 5) -> List[SearchResult]:
        """Search for similar documents using vector similarity.
        
        Args:
            query_vector: Query embedding vector
            limit: Maximum number of results to return
            
        Returns:
            List of SearchResult objects
        """
        raise NotImplementedError("search method must be implemented")
