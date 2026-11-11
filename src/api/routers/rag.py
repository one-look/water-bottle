"""FastAPI router for end-to-end RAG processing."""

import asyncio
from typing import List

from fastapi import APIRouter, File, Header, HTTPException, Request, UploadFile, status
from pydantic import BaseModel, Field

from src.api import application
from src.core.logging import setup_logger
from src.core.multitenancy import get_current_principal, get_current_tenant
from src.core.ratelimit import limiter
from src.database.psql import track_rag_request
from src.rag.retrievers.qdrant import RetrivedDocument
from src.rag.uploads.access import upload_scope_for_principal
from src.rag.uploads.extract import extract_text_from_bytes
from src.rag.uploads.ingest import ChatDocumentIngestService
from src.rag.workflow import RAGWorkflow

logger = setup_logger(__name__)

router = APIRouter(prefix="/rag", tags=["RAG"])


class RAGRequest(BaseModel):
    query: str = Field(..., min_length=1, description="User search or prompt query")


class RAGResponse(BaseModel):
    tenant_id: str
    answer: str
    retrieved_documents: List[RetrivedDocument]


class RAGUploadResponse(BaseModel):
    tenant_id: str
    filename: str
    scope: str
    chunks_indexed: int


@router.post(
    "/upload",
    response_model=RAGUploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload a document for RAG (teachers: session; employees: tenant)",
)
@limiter.limit("5/minute")
async def upload_rag_document(
    request: Request,
    file: UploadFile = File(...),
    x_session_id: str = Header(..., alias="X-Session-ID", description="Session Identifier"),
) -> RAGUploadResponse:
    '''Teachers store chunks for this session only; employees store for the whole tenant.'''
    principal = get_current_principal()
    scope = upload_scope_for_principal(principal)
    tenant_id = get_current_tenant()
    filename = file.filename or "upload"

    app_instance = application.get()
    if not app_instance or not hasattr(app_instance, "config"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Application configuration is not initialized.",
        )

    try:
        raw = await file.read()
        text = extract_text_from_bytes(filename, raw)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    ingest = ChatDocumentIngestService(app_instance.config)
    try:
        chunks = await asyncio.to_thread(
            ingest.ingest,
            text=text,
            filename=filename,
            tenant_id=tenant_id,
            scope=scope,
            session_id=x_session_id if scope == "session" else None,
            uploaded_by=principal.sub,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Document ingest failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Document could not be processed",
        ) from exc

    return RAGUploadResponse(
        tenant_id=tenant_id,
        filename=filename,
        scope=scope,
        chunks_indexed=chunks,
    )


@router.post(
    "/generate",
    response_model=RAGResponse,
    status_code=status.HTTP_200_OK,
    summary="RAG for guest (no Bearer) or login chat (Google id_token)",
)
@limiter.limit("15/minute")
async def generate_rag_response(
    request: Request,
    body: RAGRequest,
    x_session_id: str = Header(..., alias="X-Session-ID", description="Session Identifier"),
) -> RAGResponse:
    """Guest chat (no Bearer) or login chat (Google id_token Bearer)."""
    current_tenant = get_current_tenant()
    principal = get_current_principal()
    chat_mode = "login" if principal else "guest"
    logger.info(f"Processing RAG {chat_mode} for tenant '{current_tenant}'")

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

        try:
            track_rag_request(
                request,
                session_id=x_session_id,
                tenant_id=current_tenant,
                chat_mode=chat_mode,
                user_sub=principal.sub if principal else None,
            )
        except Exception as exc:
            logger.warning("IP tracking skipped: %s", exc)

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