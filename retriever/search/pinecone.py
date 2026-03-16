import logging
from typing import Dict, Any, List
import asyncio
from .base import RetrieverBase, SearchResult

logger = logging.getLogger(__name__)

class PineconeRetriever(RetrieverBase):
    """Pinecone-based document retriever."""
    
    def __init__(self, config: Dict[str, Any], connection):
        """Initialize Pinecone retriever.
        
        Args:
            config: Configuration dictionary
            connection: Pinecone client instance
        """
        self.config = config
        self.connection = connection
        self.index_name = config.get("index_name", "default")
        self.vector_field = config.get("vector_field", "values")
        self.content_field = config.get("content_field", "metadata")
        
        # Get index handle
        self.index = connection.Index(self.index_name)
        
        logger.info(f"Initialized PineconeRetriever for index: {self.index_name}")
    
    async def search(self, query_vector: List[float], top_k: int = 5) -> List[SearchResult]:
        """Search for similar documents in Pinecone.
        
        Args:
            query_vector: Query embedding vector
            top_k: Number of results to return
            
        Returns:
            List of search results with scores and content
        """
        try:
            # Query Pinecone index asynchronously
            response = await asyncio.to_thread(
                self.index.query,
                vector=[query_vector],
                top_k=top_k,
                include_metadata=True
            )
            
            # Convert to SearchResult format
            results = []
            for match in response.get("matches", []):
                metadata = match.get("metadata", {})
                content = metadata.get("text", "")  # Changed from "content" to "text"
                
                result = SearchResult(
                    content=content,
                    score=match.get("score", 0.0),
                    metadata=metadata
                )
                results.append(result)
            
            logger.info(f"Found {len(results)} results from Pinecone")
            return results
            
        except Exception as e:
            logger.error(f"Pinecone search failed: {e}")
            return []
    
    async def add_document(self, doc_id: str, content: str, vector: List[float], metadata: Dict[str, Any] = None) -> None:
        """Add a document to Pinecone index.
        
        Args:
            doc_id: Document ID
            content: Document content
            vector: Document embedding vector
            metadata: Additional metadata
        """
        try:
            if metadata is None:
                metadata = {}
            
            # Add content to metadata for retrieval
            metadata["content"] = content
            
            # Upsert to Pinecone asynchronously
            await asyncio.to_thread(
                self.index.upsert,
                vectors=[{
                    "id": doc_id,
                    "values": vector,
                    "metadata": metadata
                }]
            )
            
            logger.info(f"Added document {doc_id} to Pinecone")
            
        except Exception as e:
            logger.error(f"Failed to add document to Pinecone: {e}")
    
    async def delete_document(self, doc_id: str) -> None:
        """Delete a document from Pinecone index.
        
        Args:
            doc_id: Document ID to delete
        """
        try:
            await asyncio.to_thread(self.index.delete, ids=[doc_id])
            logger.info(f"Deleted document {doc_id} from Pinecone")
            
        except Exception as e:
            logger.error(f"Failed to delete document from Pinecone: {e}")
