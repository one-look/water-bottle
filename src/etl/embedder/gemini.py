import logging
from typing import Dict, List, Any, Iterator
import os
import time

# Use the new google-genai package
from google import genai

logger = logging.getLogger(__name__)

class GeminiEmbeddings:
    """
    Standalone Gemini embedder for ETL pipeline.
    Uses Google GenAI SDK directly without depending on RAG embedder.
    """
    
    def __init__(self, data: Iterator[Dict[str, Any]], config: Dict[str, Any]):
        """
        Initialize with data stream and config.
        
        Args:
            data: Iterator of records to embed (ETL interface)
            config: Configuration for Gemini embedder
        """
        self.data = data
        self.model_name = config.get("model_name", "gemini-embedding-001")
        self.task_type = config.get("task_type", "retrieval_query")
        
        # Configure API key
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")
        
        # Create client with new SDK
        self.client = genai.Client(api_key=api_key)
        logger.info(f"Initialized standalone Gemini ETL embedder with model: {self.model_name}")
    
    def embed(self) -> Iterator[Dict[str, Any]]:
        """
        Embed all records in the data stream.
        
        Yields:
            Dict[str, Any]: Records with vectors added in ETL format
        """
        for record in self.data:
            # Handle both _source wrapped and flat formats
            if "_source" in record:
                source_data = record.get("_source", {})
                text = source_data.get("text", "")
                record_id = source_data.get("id", "unknown")
            else:
                # Flat format
                text = record.get("text", "")
                record_id = record.get("id", "unknown")
                source_data = record
            
            if text:
                logger.debug(f"Generating embedding for record ID: {record_id}")
                # Rate limiting: wait 0.5 seconds between requests to avoid quota limits
                time.sleep(0.5)
                
                # Use new Google GenAI SDK
                response = self.client.models.embed_content(
                    model=self.model_name,
                    contents=text,
                    config={"task_type": self.task_type}
                )
                
                # Extract embedding from response - new API format
                if hasattr(response, 'embeddings') and response.embeddings:
                    vector = response.embeddings[0].values
                    source_data["vector"] = vector
                    logger.debug(f"Generated vector of length: {len(vector)}")
                else:
                    logger.error(f"No embedding returned from Gemini API for record ID: {record_id}")
                    logger.error(f"Response type: {type(response)}")
                    source_data["vector"] = []
            else:
                logger.warning(f"Empty text for record ID: {record_id}")
                source_data["vector"] = []
            
            # Return in ETL format
            yield record
