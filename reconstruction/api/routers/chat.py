from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

class ChatRequest(BaseModel):
    query: str

@router.post("/chat")
async def chat(request: ChatRequest):
    from ..application import get
    instance = get()
    
    workflow = instance.get("workflow")
    if not workflow:
        raise HTTPException(status_code=503, detail="Workflow not ready")
    
    response = workflow.run(request.query, session_id="global_chat")
    return {"response": response}