from fastapi import APIRouter, Depends, HTTPException, status
from src.core.multitenancy import get_current_tenant
from src.core.exceptions import CMSError
from src.etl.vectorstore import QdrantStoreProvider
from src.services.rag import RAGService, QueryRequest, QueryResponse
from pydantic import BaseModel

router = APIRouter(tags=["RAG & ETL"])

store_provider = QdrantStoreProvider()
rag_service = RAGService(store_provider=store_provider)

@router.post("/rag/query", response_model=QueryResponse)
async def query_cms(
    payload: QueryRequest,
    tenant_id: str = Depends(get_current_tenant)
):
    try:
        return await rag_service.answer_query(query=payload.query, tenant_id=tenant_id)
    except CMSError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        