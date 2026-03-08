import logging
from bs4 import BeautifulSoup
from typing import Dict, Any, List
from .base import BaseTransformer
from .chunker import Chunker

logger = logging.getLogger(__name__)

class WebTransformer(BaseTransformer):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.chunk_config = self.config.get("chunking", {})
        self.index_script = self.config.get("index_script", {})
        self.extraction_rules = self.config.get("extraction_rules", {})

    def _extract_page_text(self, html_content: str) -> str:
        """Helper: Parses single HTML page for unique Titles and Paragraphs."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove Noise
        for noise in soup(["nav", "footer", "header", "script", "style", "aside"]):
            noise.decompose()

        tags = self.extraction_rules.get("tags", ['h1', 'h2', 'h3', 'p'])
        min_len = self.extraction_rules.get("min_text_length", 45)
        
        blocks = []
        seen = set()
        
        for tag in soup.find_all(tags):
            text = " ".join(tag.get_text().split()).strip()
            if text and len(text) > min_len and text not in seen:
                blocks.append(text)
                seen.add(text)
        
        return "\n\n".join(blocks)

    def transform(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Processes a LIST of pages and returns a single combined payload.
        """
        all_embedder_data = []
        total_chunks_processed = 0

        # Internally loop through the list of pages provided by Extractor
        for page_num, page_data in enumerate(data):
            url = page_data.get("url")
            raw_html = page_data.get("content", "")
            
            # 1. Clean this specific page
            clean_text = self._extract_page_text(raw_html)
            
            # 2. Chunk this page
            chunks = Chunker.split(clean_text, self.chunk_config)
            
            # 3. Build payload for this page
            id_template = self.index_script.get("id_format", "chunk_{i}")
            meta_template = self.index_script.get("metadata", {})

            for i, chunk in enumerate(chunks):
                # Generate a globally unique ID (PageNum_ChunkNum)
                chunk_id = f"p{page_num}_" + id_template.format(i=i)
                
                metadata = {
                    k: (v.format(i=i, len=len(chunks)) if isinstance(v, str) else v)
                    for k, v in meta_template.items()
                }
                # Inject URL into metadata so the bot knows where this text came from
                metadata["url"] = url 
                
                all_embedder_data.append({
                    "id": chunk_id,
                    "text": chunk,
                    "metadata": metadata
                })
            
            total_chunks_processed += len(chunks)

        logger.info(f"Transformer processed {len(data)} pages into {total_chunks_processed} total chunks.")
        
        # Wrap in standard structure
        result = super().transform({"processed_pages": len(data)})
        result["embedder_data"] = all_embedder_data
        
        return result