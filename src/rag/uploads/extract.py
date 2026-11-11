'''Extract plain text from chat uploads (PDF / text).'''

import io

from pypdf import PdfReader

from src.core.logging import setup_logger

logger = setup_logger(__name__)

_ALLOWED_SUFFIXES = (".pdf", ".txt", ".md")
_MAX_BYTES = 15 * 1024 * 1024


def extract_text_from_bytes(filename: str, data: bytes) -> str:
    '''
    Args:
        filename: Original upload name (used for type detection).
        data: Raw file bytes.

    Returns:
        Extracted UTF-8 text.

    Raises:
        ValueError: Unsupported type, empty file, or size limit exceeded.
    '''
    if len(data) > _MAX_BYTES:
        raise ValueError(f"File exceeds {_MAX_BYTES // (1024 * 1024)}MB limit")
    if not data:
        raise ValueError("File is empty")

    name = filename.strip().lower()
    if not any(name.endswith(suffix) for suffix in _ALLOWED_SUFFIXES):
        raise ValueError("Supported formats: .pdf, .txt, .md")

    if name.endswith(".pdf"):
        reader = PdfReader(io.BytesIO(data))
        pages = [page.extract_text() or "" for page in reader.pages]
        text = "\n".join(pages).strip()
    else:
        text = data.decode("utf-8", errors="ignore").strip()

    if not text:
        raise ValueError("No readable text found in file")

    logger.info("Extracted %s characters from upload '%s'", len(text), filename)
    return text
