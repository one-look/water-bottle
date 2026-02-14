# Used to manage application startup and shutdown lifecycle.
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .routers.chat import router
from typing import Dict, Any
import yaml
import os

from .loader import AppLoader

def get():
    """ 
    returns a global INSTANCE
    """
    return INSTANCE

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    print("Starting Water Bottle RAG application...")

    global INSTANCE
    
    # Load configuration
    config_path = os.getenv("CONFIG_PATH", "config.yaml")
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Config file not found at {config_path}")
    
    # Initialize application loader
    app_loader = AppLoader(config)
    
    # Load services and workflow
    services = app_loader.load_services()
    workflow = app_loader.load_workflow()
    
    # Store in global instance
    INSTANCE.update({
        "config": config,
        "services": services,
        "workflow": workflow,
        "loader": app_loader
    })
    
    print("Application startup complete!")
    
    yield
    
    # Shutdown
    print("Shutting down Water Bottle RAG application...")
    
    # Cleanup Elasticsearch connection if exists
    if "elasticsearch" in services:
        services["elasticsearch"].close()
    
    print("Application shutdown complete!")

def create() -> FastAPI:
    """Create and configure FastAPI application.
    
    Returns:
        Configured FastAPI application
    """
    app = FastAPI(
        title="Water Bottle - RAG Application",
        description="Modular RAG application with Dependency Injection",
        version="1.0.0",
        lifespan=lifespan
    )
    
    app.include_router(router, prefix="/api/v1")
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {"status": "healthy", "message": "Water Bottle RAG application is running"}
    
    return app


# Create application instance
app, INSTANCE = create(), None
