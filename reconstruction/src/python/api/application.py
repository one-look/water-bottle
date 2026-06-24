""" 
Fastapi Application module
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI

from .base import API 

def get():
    """ 
    Returns global API instance 
    
    Returns:
        API instance
    """

    return INSTANCE

def create():
    """ 
    Creates a FastAPI instance.
    """
    
    return FastAPI(lifespan=lifespan)

@asynccontextmanager
async def lifespan(application: FastAPI):
    """ 
    FastAPI lifespan event handle

    Args:
        application: FastAPI application to initialize
    """

    global INSTANCE
    
    print("server startup completed...")
    
    yield

    print("server shutting down...")

def start():
    """ 
    Starts the lifespan handler.
    """
    
    list(lifespan(app))

app, INSTANCE = create(), None