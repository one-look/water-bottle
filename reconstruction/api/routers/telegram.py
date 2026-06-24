from fastapi import APIRouter, Request, HTTPException
import logging
from typing import Dict, Any

logger = logging.getLogger("water-bottle.telegram")
router = APIRouter()

@router.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    """Telegram bot webhook endpoint that forwards messages to RAG workflow."""
    try:
        from ..application import get
        instance = get()
        
        telegram_workflow = instance.get("telegram_workflow")
        if not telegram_workflow:
            logger.error("Telegram workflow not ready")
            raise HTTPException(status_code=503, detail="Telegram workflow not ready")
        
        # Get raw JSON data for flexibility
        data = await request.json()
        logger.info(f"Received Telegram update: {data}")
        
        # Process the Telegram update (this handles RAG + sending response)
        result = await telegram_workflow(data)
        
        return result
        
    except Exception as e:
        logger.error(f"Telegram webhook error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")