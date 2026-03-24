import logging
import time
from typing import Dict, Any, List
import asyncio
from .base import RetrieverBase, SearchResult

logger = logging.getLogger("water-bottle.retriever.pinecone")

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
        
        logger.info("action=initialize retriever=pinecone index=%s vector_field=%s content_field=%s", self.index_name, self.vector_field, self.content_field)
    
    async def search(self, query_vector: List[float], top_k: int = 5) -> List[SearchResult]:
        """Search for similar documents in Pinecone.
        
        Args:
            query_vector: Query embedding vector
            top_k: Number of results to return
            
        Returns:
            List of search results with scores and content
        """
        start_time = time.time()
        logger.info("action=search retriever=pinecone index=%s top_k=%d vector_dim=%d", self.index_name, top_k, len(query_vector))
        
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
            for i, match in enumerate(response.get("matches", [])):
                metadata = match.get("metadata", {})
                content = metadata.get("text", "")  # Changed from "content" to "text"
                score = match.get("score", 0.0)
                
                logger.debug("action=process_match retriever=pinecone match_index=%d score=%.3f content_length=%d", i, score, len(content))
                
                result = SearchResult(
                    content=content,
                    score=score,
                    metadata=metadata
                )
                results.append(result)
            
            duration = time.time() - start_time
            logger.info("action=search_complete retriever=pinecone index=%s results_count=%d duration=%.3fs top_k=%d", self.index_name, len(results), duration, top_k)
            return results
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error("action=search_failed retriever=pinecone index=%s duration=%.3fs error=%s", self.index_name, duration, str(e))
            return []
    
    async def add_document(self, doc_id: str, content: str, vector: List[float], metadata: Dict[str, Any] = None) -> None:
        """Add a document to Pinecone index.
        
        Args:
            doc_id: Document ID
            content: Document content
            vector: Document embedding vector
            metadata: Additional metadata
        """
        start_time = time.time()
        logger.info("action=add_document retriever=pinecone index=%s doc_id=%s content_length=%d vector_dim=%d", self.index_name, doc_id, len(content), len(vector))
        
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
            
            duration = time.time() - start_time
            logger.info("action=add_document_complete retriever=pinecone index=%s doc_id=%s duration=%.3fs", self.index_name, doc_id, duration)
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error("action=add_document_failed retriever=pinecone index=%s doc_id=%s duration=%.3fs error=%s", self.index_name, doc_id, duration, str(e))
    
    async def delete_document(self, doc_id: str) -> None:
        """Delete a document from Pinecone index.
        
        Args:
            doc_id: Document ID to delete
        """
        start_time = time.time()
        logger.info("action=delete_document retriever=pinecone index=%s doc_id=%s", self.index_name, doc_id)
        
        try:
            await asyncio.to_thread(self.index.delete, ids=[doc_id])
            duration = time.time() - start_time
            logger.info("action=delete_document_complete retriever=pinecone index=%s doc_id=%s duration=%.3fs", self.index_name, doc_id, duration)
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error("action=delete_document_failed retriever=pinecone index=%s doc_id=%s duration=%.3fs error=%s", self.index_name, doc_id, duration, str(e))
