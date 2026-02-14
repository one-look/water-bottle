from fastapi import APIRouter, HTTPException
import uuid

from ..application import INSTANCE

router = APIRouter()


@router.post("/chat")
async def chat(query: dict):
    """Process chat query through RAG workflow.
    
    Args:
        query: Chat query containing query and optional session_id
        
    Returns:
        Chat response with generated text and session_id
    """
    try:
        # Get workflow from global instance
        workflow = INSTANCE.get("workflow")
        if not workflow:
            raise HTTPException(status_code=503, detail="Workflow not initialized")
        
        # Generate session_id if not provided
        session_id = query.get("session_id") or str(uuid.uuid4())
        
        # Process query through workflow
        response = workflow.process_query(query.get("query"), session_id)
        
        return {
            "response": response,
            "session_id": session_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing chat request: {str(e)}")