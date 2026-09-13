"""FastAPI router for end-to-end RAG (Retrieval-Augmented Generation) processing."""

from typing import Any, Dict, List
from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel, Field

from src.api import application
from src.core.logging import setup_logger
from src.core.multitenancy import get_current_tenant
from src.llm.factory import LLMFactory
from src.rag.embedders.factory import EmbedderFactory
from src.rag.retrievers.factory import RetrieverFactory
from src.rag.retrievers.qdrant import RetrivedDocument

from src.rag.memory.memory import memory_store

logger = setup_logger(__name__)

router = APIRouter(prefix="/rag", tags=["RAG"])


class RAGRequest(BaseModel):
    """Request payload for RAG query execution."""

    query: str = Field(..., min_length=1, description="User search or prompt query")


class RAGResponse(BaseModel):
    """Response payload containing generated text and retrieved citations."""

    tenant_id: str
    answer: str
    retrieved_documents: List[RetrivedDocument]


def _build_rag_prompt(query: str, documents: List[RetrivedDocument], history: List[Dict[str, str]]) -> str:
    """Formats retrieved context documents and user query into a single structured prompt.

    Args:
        query: Raw user query string.
        documents: List of retrieved text chunks from vector search.

    Returns:
        str: Formatted system prompt with context instructions.
    """
    # 1. format context documents
    if not documents:
        context_str = "No relevant context found in tenant database."
    else:
        context_blocks = [
            f"[Doc {idx + 1} - ID: {doc.document_id}]\n{doc.text}"
            for idx, doc in enumerate(documents)
        ]
        context_str = "\n\n".join(context_blocks)

    # format chat history (if any exists)
    if not history:
        history_str = "No prior conversation history."
    else:
        history_blocks = [f"{msg['role'].capitalize()}: {msg['content']}" for msg in history]
        history_str = "\n".join(history_blocks)

    return f"""You are a helpful multi-tenant enterprise assistant. Answer the user's question accurately using ONLY the provided context below. If the context does not contain enough information to answer, state clearly that you do not know based on the available data.

Prior Conversation History:
---------------------
{history_str}
---------------------

Context Information:
---------------------
{context_str}
---------------------

User Question: {query}

Answer:"""


@router.post(
    "/generate",
    response_model=RAGResponse,
    status_code=status.HTTP_200_OK,
    summary="Execute RAG query with strict tenant vector context",
)
async def generate_rag_response(
    request: RAGRequest,
    x_tenant_id: str = Header(
        ..., alias="X-Tenant-ID", description="Tenant Identifier"
    ),
    x_session_id: str = Header(..., alias="X-Session-ID", description="Session Identifier"),
) -> RAGResponse:
    """Orchestrates query embedding, multi-tenant vector retrieval, and LLM text generation."""
    current_tenant = get_current_tenant()
    logger.info(f"Processing RAG request for query: '{request.query[:50]}...'")

    try:
        app_instance = application.get()
        if not app_instance or not hasattr(app_instance, "config"):
            logger.error("Application state or configuration is missing.")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Application configuration is not initialized.",
            )

        config: Dict[str, Any] = app_instance.config

        # Step 0: Get conversation history
        history = memory_store.get_history(x_session_id)

        # Step 1: Embed input query
        logger.info("Instantiating embedder service...")
        embedder = EmbedderFactory.create(config)
        query_vector = embedder.embed_query(request.query)

        # Step 2: Retrieve tenant-isolated vector documents
        logger.info("Instantiating retriever service...")
        retriever = RetrieverFactory.create(config)
        retrieved_docs = retriever.retrieve(
            query_vector=query_vector, tenant_id=current_tenant
        )

        # Step 3: Build RAG augmented prompt
        augmented_prompt = _build_rag_prompt(
            query=request.query, documents=retrieved_docs, history=history
        )

        # Step 4: Generate LLM response using context
        logger.info("Instantiating LLM service for generation...")
        llm_provider = LLMFactory.create(config)
        generated_answer = llm_provider.generate(augmented_prompt)

        # Step 5: Add response to conversation history
        memory_store.add_message(x_session_id, role="user", content=request.query)
        memory_store.add_message(x_session_id, role="assistant", content=generated_answer)

        logger.info("Successfully generated RAG response.")
        return RAGResponse(
            tenant_id=current_tenant,
            answer=generated_answer,
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