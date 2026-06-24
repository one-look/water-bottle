from fastapi import FastAPI
from contextlib import asynccontextmanager
from .application import Application

ctx = {}

def register_active_routes(app: FastAPI, app_ctx: Application):
    """
    Dynamically imports and includes routers based on enabled configurations.

    Args:

    Returns:

    """
    if app_ctx.embed_config.enabled:
        from src.embedder.router import router as embed_router
        app.include_router(embed_router, prefix="/v1/embed", tags=["Embedding"])

    if app_ctx.rag_config.enabled:
        from src.rag.router import router as rag_router
        app.include_router(rag_router, prefix="/v1/rag", tags=["RAG"])

    if app_ctx.attendance_config.enabled:
        from src.attendance.router import router as attendance_router
        app.include_router(attendance_router, prefix="/v1/attendance", tags=["Attendance"])

@asynccontextmanager
async def lifespan(app: FastAPI):
    app_ctx = Application("config.yml")
    ctx["app_ctx"] = app_ctx

    register_active_routes(app, app_ctx)
    yield
    ctx.clear()

app = FastAPI(title="Project Water Bottle", lifespan=lifespan)