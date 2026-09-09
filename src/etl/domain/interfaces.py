from abc import ABC, abstractmethod
from src.etl.domain.entities import Document

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, source_key: str, tenant_id: str) -> Document:
        '''
        Extract a single document from source for a specific tenant.
        '''
        pass