import logging
import time
from fastapi import APIRouter, Request, HTTPException
from typing import Dict, Any

logger = logging.getLogger("water-bottle.api.telegram")
router = APIRouter()

@router.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    """Telegram bot webhook endpoint that forwards messages to RAG workflow."""
    start_time = time.time()
    logger.info("action=webhook_request endpoint=/telegram/webhook method=POST")
    
    try:
        from ..application import get
        instance = get()
        
        telegram_workflow = instance.get("telegram_workflow")
        if not telegram_workflow:
            duration = time.time() - start_time
            logger.error("action=webhook_error endpoint=/telegram/webhook error=workflow_not_ready duration=%.3fs", duration)
            raise HTTPException(status_code=503, detail="Telegram workflow not ready")
        
        # Get raw JSON data for flexibility
        data = await request.json()
        update_id = data.get("update_id", "unknown")
        message_info = data.get("message", {})
        chat_id = message_info.get("chat", {}).get("id", "unknown")
        text = message_info.get("text", "")
        
        logger.info("action=telegram_update endpoint=/telegram/webhook update_id=%s chat_id=%s text_length=%d", update_id, chat_id, len(text))
        
        # Process the Telegram update (this handles RAG + sending response)
        result = await telegram_workflow(data)
        
        duration = time.time() - start_time
        logger.info("action=webhook_response endpoint=/telegram/webhook status=success duration=%.3fs update_id=%s chat_id=%s", duration, update_id, chat_id)
        return result
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error("action=webhook_error endpoint=/telegram/webhook error=%s duration=%.3fs", str(e), duration)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")