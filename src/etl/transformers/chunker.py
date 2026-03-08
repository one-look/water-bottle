from typing import List, Dict, Any

class Chunker:
    """
    Utility for breaking strings into smaller segments.
    Strictly follows YAML configuration for size and overlap.
    """
    
    @staticmethod
    def split(text: str, config: Dict[str, Any]) -> List[str]:
        if not text:
            return []
            
        size = config.get("chunk_size", 500)
        overlap = config.get("overlap", 50)
        
        if overlap >= size:
            overlap = size // 2
            
        chunks = []
        start = 0
        text_len = len(text)

        while start < text_len:
            end = start + size
            
            if end < text_len:
                # Find the last space to avoid cutting a word in half
                last_space = text.rfind(' ', start, end)
                if last_space > start:
                    end = last_space
            
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            
            # Move start point back by the overlap amount
            start = end - overlap
            
        return chunks