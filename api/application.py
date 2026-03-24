import os, yaml
import logging
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .loader import AppLoader
from dotenv import load_dotenv

logger = logging.getLogger("water-bottle.application")

# Load environment variables from .env file
load_dotenv()

INSTANCE = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global INSTANCE
    
    start_time = time.time()
    logger.info("action=app_startup application=water-bottle")
    
    # 1. Check for Environment Variable
    config_path = os.getenv("CONFIG")
    if not config_path:
        duration = time.time() - start_time
        logger.error("action=app_startup_failed application=water-bottle error=config_env_missing duration=%.3fs", duration)
        raise RuntimeError("Environment variable 'CONFIG' is not set.")
    
    # 2. Check if file exists
    if not os.path.exists(config_path):
        duration = time.time() - start_time
        logger.error("action=app_startup_failed application=water-bottle error=config_file_not_found path=%s duration=%.3fs", config_path, duration)
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    logger.info("action=config_load application=water-bottle path=%s", config_path)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    logger.info("action=services_init application=water-bottle services_count=%d", len(config.get("services", {})))
    loader = AppLoader(config)
    services = loader.load_services()
    
    logger.info("action=workflow_init application=water-bottle")
    
    INSTANCE.update({
        "config": config,
        "services": services,
        "workflow": loader.load_workflow(),
        "telegram_workflow": loader.load_telegram_workflow()
    })
    
    duration = time.time() - start_time
    logger.info("action=app_startup_complete application=water-bottle duration=%.3fs services_loaded=%d", duration, len(services))
    
    yield
    
    # Shutdown logic
    shutdown_start = time.time()
    logger.info("action=app_shutdown application=water-bottle")
    
    if "elasticsearch" in INSTANCE.get("services", {}):
        INSTANCE["services"]["elasticsearch"].close()
        logger.debug("action=elasticsearch_close application=water-bottle")
    
    shutdown_duration = time.time() - shutdown_start
    logger.info("action=app_shutdown_complete application=water-bottle duration=%.3fs", shutdown_duration)

def create() -> FastAPI:
    logger.info("action=app_create application=water-bottle")
    app = FastAPI(title="Water Bottle RAG", lifespan=lifespan)
    
    from .routers.chat import router
    app.include_router(router, prefix="/api/v1")
    logger.debug("action=router_register application=water-bottle router=chat prefix=/api/v1")
    
    from .routers.telegram import router as telegram_router
    app.include_router(telegram_router, prefix="/api/v1")
    logger.debug("action=router_register application=water-bottle router=telegram prefix=/api/v1")
    
    from .routers.health import router as health_router
    app.include_router(health_router)
    logger.debug("action=router_register application=water-bottle router=health")
    
    logger.info("action=app_create_complete application=water-bottle routers_count=3")
    return app

app = create()

def get():
    return INSTANCE