from pydantic import BaseModel, Field
from src.etl.vectorstore import BaseVectorStoreProvider
from src.core.logging import setup_logger
from src.core.exceptions import RAGError

logger = setup_logger("RAG")

class QueryRequest(BaseModel):
    """
    Pydantic schema defining inbound RAG query requests.
    """
    query: str = Field(..., min_length=3, description="Query string for RAG")

class QueryResponse(BaseModel):
    """
    Pydantic schema defining output RAG responses.
    """
    answer: str
    tenant_id: str

class RAGService:
    """
    Handles natural language query execution against tenant vector stores.
    """
    def __init__(self, store_provider: BaseVectorStoreProvider):
        """
        Initialize RAG execution service.

        Args:
            store_provider (BaseVectorStoreProvider): Vector store backend provider.
        """
        self.store_provider = store_provider

    async def answer_query(self, query: str, tenant_id: str) -> QueryResponse:
        """
        Processes a prompt query against a tenant's isolated index space.

        Args:
            query (str): Natural language question.
            tenant_id (str): Context tenant ID for data isolation.

        Returns:
            QueryResponse: Generated answer along with context metadata.

        Raises:
            RAGError: If query execution or index initialization fails.
        """
        try:
            logger.info(f"Processing RAG query: '{query}'", extra={"tenant_id": tenant_id})
            index = self.store_provider.get_index(tenant_id)
            query_engine = index.as_query_engine()
            response = query_engine.query(query)

            return QueryResponse(answer=str(response), tenant_id=tenant_id)
        except Exception as e:
            logger.error(f"Error querying RAG engine: {str(e)}", extra={"tenant_id": tenant_id})
            raise RAGError(f"RAG query execution failed: {str(e)}") from e