class BaseETLException(Exception):
    """Base exception for all ETL components."""
    pass


class ExtractionError(BaseETLException):
    """Raised when document extraction from S3 fails."""
    pass