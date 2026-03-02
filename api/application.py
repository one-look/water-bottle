import os, yaml
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .loader import AppLoader
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

INSTANCE = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global INSTANCE
    
    # 1. Check for Environment Variable
    config_path = os.getenv("CONFIG")
    if not config_path:
        raise RuntimeError("Environment variable 'CONFIG' is not set.")
    
    # 2. Check if file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    print(f"Loading configuration from: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    loader = AppLoader(config)
    services = loader.load_services()
    
    INSTANCE.update({
        "config": config,
        "services": services,
        "workflow": loader.load_workflow(),
        "telegram_workflow": loader.load_telegram_workflow()
    })
    
    yield
    
    # Shutdown logic
    if "elasticsearch" in INSTANCE.get("services", {}):
        INSTANCE["services"]["elasticsearch"].close()

def create() -> FastAPI:
    app = FastAPI(title="Water Bottle RAG", lifespan=lifespan)
    
    from .routers.chat import router
    app.include_router(router, prefix="/api/v1")
    
    from .routers.telegram import router as telegram_router
    app.include_router(telegram_router, prefix="/api/v1")
    
    from .routers.health import router as health_router
    app.include_router(health_router)
    
    return app

app = create()

def get():
    return INSTANCE