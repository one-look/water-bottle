from fastapi import APIRouter
import logging

logger = logging.getLogger("water-bottle.health")
router = APIRouter()

@router.get("/health")
async def health_check():
    """Health check endpoint for load balancers."""
    return {"status": "healthy", "service": "water-bottle-api"}

@router.get("/ready")
async def readiness_check():
    """Readiness check - verifies all services are ready."""
    try:
        from ..application import get
        instance = get()
        
        # Check if essential services are loaded
        if not instance.get("workflow"):
            return {"status": "not_ready", "reason": "workflow not loaded"}
        
        if not instance.get("telegram_workflow"):
            return {"status": "not_ready", "reason": "telegram workflow not loaded"}
        
        return {"status": "ready"}
        
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        return {"status": "not_ready", "reason": str(e)}
