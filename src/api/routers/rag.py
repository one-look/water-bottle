"""FastAPI router for end-to-end RAG processing."""

from typing import Any, Dict, List
from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel, Field

from src.api import application
from src.core.logging import setup_logger
from src.core.multitenancy import get_current_tenant
from src.core.ratelimit import limiter
from src.rag.retrievers.qdrant import RetrivedDocument
from src.rag.workflow import RAGWorkflow

logger = setup_logger(__name__)

router = APIRouter(prefix="/rag", tags=["RAG"])


class RAGRequest(BaseModel):
    query: str = Field(..., min_length=1, description="User search or prompt query")


class RAGResponse(BaseModel):
    tenant_id: str
    answer: str
    retrieved_documents: List[RetrivedDocument]


@router.post(
    "/generate",
    response_model=RAGResponse,
    status_code=status.HTTP_200_OK,
    summary="Execute RAG query with strict tenant vector context",
)
@limiter.limit("15/minute")
async def generate_rag_response(
    request: Request,
    body: RAGRequest,
    x_tenant_id: str = Header(..., alias="X-Tenant-ID", description="Tenant Identifier"),
    x_session_id: str = Header(..., alias="X-Session-ID", description="Session Identifier"),
) -> RAGResponse:
    """Router endpoint delegating orchestration to RAGWorkflow."""
    current_tenant = get_current_tenant()
    logger.info(f"Processing RAG request for tenant '{current_tenant}'")

    try:
        app_instance = application.get()
        if not app_instance or not hasattr(app_instance, "config"):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Application configuration is not initialized.",
            )

        workflow = RAGWorkflow(config=app_instance.config)
        answer, retrieved_docs = await workflow.execute(
            query=body.query,
            tenant_id=current_tenant,
            session_id=x_session_id,
        )

        return RAGResponse(
            tenant_id=current_tenant,
            answer=answer,
            retrieved_documents=retrieved_docs,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute RAG pipeline: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred during RAG execution: {str(e)}",
        ) from e