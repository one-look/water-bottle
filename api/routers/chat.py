import logging
import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

logger = logging.getLogger("water-bottle.api.chat")
router = APIRouter()

@router.post("/api/v1/chat/test")
async def chat_test(query: str, session_id: str = "test_session") -> Dict[str, Any]:
    """Test endpoint for load testing - processes RAG workflow without external dependencies."""
    start_time = time.time()
    logger.info("action=api_request endpoint=/api/v1/chat/test method=POST query_length=%d session_id=%s", len(query), session_id)
    
    try:
        from ..application import get
        instance = get()

        workflow = instance.get("workflow")
        if not workflow:
            duration = time.time() - start_time
            logger.error("action=api_error endpoint=/api/v1/chat/test error=workflow_not_ready duration=%.3fs", duration)
            raise HTTPException(status_code=503, detail="RAG workflow not ready")

        # Process the query through RAG workflow
        response = await workflow.run(query, session_id)
        
        duration = time.time() - start_time
        logger.info("action=api_response endpoint=/api/v1/chat/test status=success response_length=%d duration=%.3fs session_id=%s", len(response), duration, session_id)

        return {
            "response": response,
            "session_id": session_id,
            "status": "success"
        }

    except Exception as e:
        duration = time.time() - start_time
        logger.error("action=api_error endpoint=/api/v1/chat/test error=%s duration=%.3fs session_id=%s", str(e), duration, session_id)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

class ChatRequest(BaseModel):
    query: str

@router.post("/chat")
async def chat(request: ChatRequest):
    start_time = time.time()
    logger.info("action=api_request endpoint=/chat method=POST query_length=%d", len(request.query))
    
    try:
        from ..application import get
        instance = get()
        
        workflow = instance.get("workflow")
        if not workflow:
            duration = time.time() - start_time
            logger.error("action=api_error endpoint=/chat error=workflow_not_ready duration=%.3fs", duration)
            raise HTTPException(status_code=503, detail="Workflow not ready")
        
        response = await workflow.run(request.query, session_id="global_chat")
        duration = time.time() - start_time
        logger.info("action=api_response endpoint=/chat status=success response_length=%d duration=%.3fs", len(response), duration)
        return {"response": response}
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error("action=api_error endpoint=/chat error=%s duration=%.3fs", str(e), duration)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")