class BaseETLException(Exception):
    """Base exception for all ETL pipeline components."""
    pass


class ExtractionError(BaseETLException):
    """Raised when document extraction from external storage fails."""
    pass


class TransformationError(BaseETLException):
    """Raised when document chunking or node parsing fails."""
    pass


class EmbeddingError(BaseETLException):
    """Raised when vector embedding generation fails."""
    pass


class LoadError(BaseETLException):
    """Raised when loading vector payloads into storage fails."""
    pass