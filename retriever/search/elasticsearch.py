from typing import List, Dict, Any
from .base import RetrieverBase, SearchResult


class ElasticsearchRetriever(RetrieverBase):
    """Elasticsearch implementation of vector-based document retrieval."""
    
    def __init__(self, config: Dict[str, Any], es_connection):
        """Initialize Elasticsearch retriever.
        
        Args:
            config: Configuration with index_name, vector_field, and other settings
            es_connection: Elasticsearch client connection
        """
        self.index_name = config.get("index_name")
        self.vector_field = config.get("vector_field")
        self.content_field = config.get("content_field")
        self.es = es_connection
    
    def search(self, query_vector: List[float], limit: int = 5) -> List[SearchResult]:
        # Top-level knn configuration
        knn_config = {
            "field": self.vector_field,      # Must be the literal string "field"
            "query_vector": query_vector,
            "k": limit,
            "num_candidates": 100
        }
        
        try:
            # Using keyword arguments for knn and _source is the cleanest way in ES 8.x
            response = self.es.search(
                index=self.index_name,
                knn=knn_config,
                source=[self.content_field], # Use 'source' instead of '_source' as a param
                size=limit
            )
            
            results = []
            for hit in response["hits"]["hits"]:
                source = hit.get("_source", {})
                content = source.get(self.content_field, "")
                score = hit.get("_score", 0)
                
                # Metadata extraction looks good!
                metadata = {
                    "id": hit["_id"],
                    "index": hit["_index"],
                    **{k: v for k, v in source.items() if k != self.content_field}
                }
                
                results.append(SearchResult(
                    content=content,
                    score=score,
                    metadata=metadata
                ))
            
            return results
            
        except Exception as e:
            # Detailed error logging helps debug mapping issues
            raise RuntimeError(f"Elasticsearch search failed: {str(e)}")