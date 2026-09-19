import inspect
from fastapi import FastAPI, APIRouter
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from src.api import routers
from src.app import Application
from src.core import TenantMiddleware

from src.core.ratelimit import limiter_manager

load_dotenv()

INSTANCE = None

def get():
    '''
    Get the application instance.
    '''
    return INSTANCE

def apirouters() -> dict:
    '''
    List available APIrouters.

    Returns:
        {router name: router instance}
    '''
    available = {}
    for name, module in inspect.getmembers(routers, inspect.ismodule):
        if hasattr(module, "router") and isinstance(module.router, APIRouter):
            available[name] = module.router
    return available

def enabled(config, name):
    '''
    Check router is enabled.

    Args:
        config (dict): application configuration
        name (str): router name

    Returns:
        bool: True if router is enabled, False otherwise.
    '''
    return config.get("routers", {}).get(name, True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    '''
    FastAPI lifespan event handler.

    Args:
        app: The FastAPI application
    '''
    global INSTANCE
    INSTANCE = Application()

    INSTANCE.ratelimiter(app)

    config = INSTANCE.config
    
    routers = apirouters()
    for name, router in routers.items():
        if name in config and enabled(config, name):
            app.include_router(router)
    
    print("water bottle is starting...")
    
    yield

    if INSTANCE:
        await INSTANCE.close()
        print("connections closed. application is shutting down...")

def create() -> FastAPI:
    '''
    Create fastapi application.
    '''
    app = FastAPI(title="water bottle", lifespan=lifespan)

    # add tenant middleware to global application instance
    app.add_middleware(TenantMiddleware)
    app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict this to your website domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
    return app

app = create()