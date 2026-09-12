"""FastAPI router endpoint for directly querying the LLM service."""
from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel, Field

from src.api import application
from src.core.logging import setup_logger
from src.core.multitenancy import get_current_tenant
from src.llm.factory import LLMFactory

logger = setup_logger(__name__)

# Create router instance
router = APIRouter(prefix="/llm", tags=["LLM"])


class QueryRequest(BaseModel):
    """Request payload schema for LLM generation endpoint."""

    prompt: str = Field(..., min_length=1, description="User query or context prompt")


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
async def generate(
    request: QueryRequest,
    x_tenant_id: str = Header(
        ..., alias="X-Tenant-ID", description="Tenant Identifier"
    ),
) -> QueryResponse:
    """Executes generation through the global application LLM provider.

    Args:
        request: Validated QueryRequest containing prompt string.
        x_tenant_id: Tenant ID passed via X-Tenant-ID header.

    Returns:
        QueryResponse containing the tenant_id and generated text response.

    Raises:
        HTTPException: HTTP 503 if LLM service is uninitialized.
        HTTPException: HTTP 500 if an internal generation error occurs.
    """
    current_tenant = get_current_tenant()
    logger.info("Received generation request")

    try:
        app_instance = application.get()

        if not app_instance or not hasattr(app_instance, "config"):
            logger.error("LLM instance is missing or uninitialized on Application state.")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="LLM service is not initialized.",
            )

        # Instantiate the llm provider dynamically from app_instance.config using LLMFactory
        llm_provider = LLMFactory.create(app_instance.config)

        # Generate response from underlying LLM provider
        generated_text = llm_provider.generate(request.prompt)

        logger.info("Successfully generated response")
        return QueryResponse(tenant_id=current_tenant, response=generated_text)

    except HTTPException:
        # Re-raise explicit HTTP exceptions (e.g., 503 Service Unavailable)
        raise

    except Exception as e:
        logger.error(f"Unexpected error during generation: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while generating text: {str(e)}",
        ) from e