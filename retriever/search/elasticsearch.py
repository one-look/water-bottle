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
        self.index_name = config.get("index_name", "documents")
        self.vector_field = config.get("vector_field", "embedding")
        self.content_field = config.get("content_field", "content")
        self.es = es_connection
    
    def search(self, query_vector: List[float], limit: int = 5) -> List[SearchResult]:
        """Search for similar documents using vector similarity.
        
        Args:
            query_vector: Query embedding vector
            limit: Maximum number of results to return
            
        Returns:
            List of SearchResult objects
        """
        search_body = {
            "size": limit,
            "query": {
                "knn": {
                    self.vector_field: {
                        "vector": query_vector,
                        "k": limit
                    }
                }
            }
        }
        
        try:
            response = self.es.search(index=self.index_name, body=search_body)
            
            results = []
            for hit in response["hits"]["hits"]:
                content = hit["_source"].get(self.content_field, "")    # original document data
                score = hit["_score"]
                metadata = {
                    "id": hit["_id"],                                   # document id
                    "index": hit["_index"],                             # index name
                    #   This line adds all fields from the original document except the content field
                    **{k: v for k, v in hit["_source"].items() if k != self.content_field}
                }
                
                results.append(SearchResult(
                    content=content,                                    # original document data
                    score=score,                                        # similarity score
                    metadata=metadata                                   # other metadata
                ))
            
            return results
            
        except Exception as e:
            raise RuntimeError(f"Elasticsearch search failed: {str(e)}")
