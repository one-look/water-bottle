from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import logging
from typing import Dict, Any

logger = logging.getLogger("water-bottle.chat")
router = APIRouter()

@router.post("/api/v1/chat/test")
async def chat_test(query: str, session_id: str = "test_session") -> Dict[str, Any]:
    """Test endpoint for load testing - processes RAG workflow without external dependencies."""
    try:
        from ..application import get
        instance = get()

        workflow = instance.get("workflow")
        if not workflow:
            raise HTTPException(status_code=503, detail="RAG workflow not ready")

        # Process the query through RAG workflow
        response = await workflow.run(query, session_id)

        return {
            "response": response,
            "session_id": session_id,
            "status": "success"
        }

    except Exception as e:
        logger.error(f"Chat test error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

class ChatRequest(BaseModel):
    query: str

@router.post("/chat")
async def chat(request: ChatRequest):
    from ..application import get
    instance = get()
    
    workflow = instance.get("workflow")
    if not workflow:
        raise HTTPException(status_code=503, detail="Workflow not ready")
    
    response = await workflow.run(request.query, session_id="global_chat")
    return {"response": response}