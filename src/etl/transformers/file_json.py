import logging
import uuid
from typing import List, Dict, Any, Iterator
from .base import BaseTransformer

logger = logging.getLogger(__name__)

class FileJson(BaseTransformer):
    """
    Flattens Nested College JSON (Page -> Sections) into individual chunks.
    """
    def __init__(self, data: List[Dict[str, Any]], config: Dict[str, Any]):
        super().__init__(config)
        self.data = data  # This is now your List of Pages
        self.conf = config.get("transformer", {})

    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Main entry point called by your etlnmc.py
        """
        embedder_data = []
        chunk_count = 0

        for page in raw_data:
            url = page.get("url")
            title = page.get("title")
            sections = page.get("sections", [])

            for section in sections:
                content = section.get("content")

                if not content:
                    continue

                # Create the specific structure you requested
                record = {
                    "id": f"p0_nmc_chunk_{chunk_count}",
                    "text": content,
                    "metadata": {
                        "url": url,
                        "title": title
                    }
                }
                
                embedder_data.append(record)
                chunk_count += 1

        return {"embedder_data": embedder_data}