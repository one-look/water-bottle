import logging
import os
import time
from typing import Dict, List, Any, Iterator
from google import genai

logger = logging.getLogger(__name__)

class GeminiEmbeddings:
    """
    High-performance Gemini embedder with Batching and Exponential Backoff.
    Designed to handle Rate Limits (429) gracefully during large ETL jobs.
    """
    
    def __init__(self, data: Iterator[Dict[str, Any]], config: Dict[str, Any]):
        self.data = data
        self.config = config
        self.model_name = config.get("model_name", "gemini-embedding-001")
        self.task_type = config.get("task_type", "retrieval_query")
        
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")
        
        self.client = genai.Client(api_key=api_key)
        logger.info(f"Initialized Gemini Embedder: {self.model_name}")

    def embed(self) -> Iterator[Dict[str, Any]]:
        """
        Processes records in configurable batches to maximize throughput and avoid 429s.
        """
        # 1. Convert iterator to list to allow batch slicing
        all_records = list(self.data)
        batch_size = self.config.get("batch_size", 50)
        
        for i in range(0, len(all_records), batch_size):
            batch = all_records[i : i + batch_size]
            texts = []
            
            # Prepare texts for this batch
            for record in batch:
                source = record.get("_source", record)
                texts.append(source.get("text", " "))

            # 2. Retry Logic (Exponential Backoff)
            attempts = 0
            max_retries = 5
            success = False

            while attempts < max_retries and not success:
                try:
                    logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} chunks)...")
                    
                    # 3. Batch Request to Google
                    response = self.client.models.embed_content(
                        model=self.model_name,
                        contents=texts,
                        config={"task_type": self.task_type}
                    )
                    
                    # 4. Map results back and yield
                    for idx, record in enumerate(batch):
                        source = record.get("_source", record)
                        if hasattr(response, 'embeddings') and response.embeddings:
                            source["vector"] = response.embeddings[idx].values
                        else:
                            source["vector"] = []
                        yield record
                    
                    success = True
                    # Short cool-down between successful batches
                    time.sleep(1)

                except Exception as e:
                    if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                        wait_time = (2 ** attempts) + 5
                        logger.warning(f"Rate limit hit. Sleeping {wait_time}s (Attempt {attempts+1})")
                        time.sleep(wait_time)
                        attempts += 1
                    else:
                        logger.error(f"Fatal error at batch {i}: {e}")
                        raise e

            if not success:
                logger.error(f"Failed to embed batch starting at index {i} after {max_retries} attempts.")

        logger.info("Embedding process completed for all chunks.")