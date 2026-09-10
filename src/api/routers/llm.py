"""FastAPI router endpoint for directly querying the LLM service."""

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from src.api import application
from src.core.logging import setup_logger

logger = setup_logger(__name__)

# Create router instance
router = APIRouter(prefix="/llm", tags=["LLM"])


class QueryRequest(BaseModel):
    """Request payload schema for LLM generation endpoint."""

    prompt: str = Field(..., min_length=1, description="User query or context prompt")
    tenant_id: str = Field("default", description="Identifier for multi-tenant tracking")


class QueryResponse(BaseModel):
    """Response payload schema for LLM generation endpoint."""

    tenant_id: str
    response: str


@router.post(
    "/generate",
    response_model=QueryResponse,
    status_code=status.HTTP_200_OK,
    summary="Generate response using LLM provider",
)
async def generate(request: QueryRequest) -> QueryResponse:
    """Executes generation through the global application LLM provider.

    Args:
        request: Validated QueryRequest containing prompt string and optional tenant_id.

    Returns:
        QueryResponse containing the tenant_id and generated text response.

    Raises:
        HTTPException: HTTP 503 if LLM service is uninitialized.
        HTTPException: HTTP 500 if an internal generation error occurs.
    """
    logger.info(f"Received generation request for tenant '{request.tenant_id}'")

    try:
        app_instance = application.get()

        if not app_instance or not hasattr(app_instance, "llm") or not app_instance.llm:
            logger.error("LLM instance is missing or uninitialized on Application state.")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="LLM service is not initialized.",
            )

        # Generate response from underlying LLM provider
        generated_text = app_instance.llm.generate(request.prompt)

        logger.info(f"Successfully generated response for tenant '{request.tenant_id}'")
        return QueryResponse(tenant_id=request.tenant_id, response=generated_text)

    except HTTPException:
        # Re-raise explicit HTTP exceptions (e.g., 503 Service Unavailable)
        raise

    except Exception as e:
        logger.error(f"Unexpected error during generation for tenant '{request.tenant_id}': {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while generating text: {str(e)}",
        ) from e