from fastapi import APIRouter, Request

router = APIRouter()

@router.post("/")
async def embed_text(request: Request, text: str):
    # Resolves runtime application lifecycle instance context safely
    embedder = request.app.state.ctx["app_ctx"].embedder
    vector = embedder.embed(text)
    return {"embedding": vector}