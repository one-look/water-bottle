from fastapi import FastAPI
from contextlib import asynccontextmanager
from .application import Application

def register_active_routes(app: FastAPI, app_ctx: Application):
    if app_ctx.embed_config.enabled:
        from src.embedder.routers import router as embed_router
        app.include_router(embed_router, prefix="/v1/embed", tags=["Embedding"])

@asynccontextmanager
async def lifespan(app: FastAPI):
    app_ctx = Application("config.yml")
    # Store clean configuration explicitly in app.state context
    app.state.ctx = {"app_ctx": app_ctx}
    
    register_active_routes(app, app_ctx)
    yield
    app.state.ctx.clear()

app = FastAPI(title="Project Water Bottle", lifespan=lifespan)