from fastapi.testclient import TestClient
from src.python.api.application import app, get

# The TestClient triggers the 'lifespan' events automatically
client = TestClient(app)

def test_app_exists():
    """ Check if the FastAPI instance was created successfully """
    assert app is not None

def test_lifespan_startup():
    """ 
    Checks if the server can start up and shut down without errors.
    The 'with' statement triggers the @asynccontextmanager lifespan.
    """
    with TestClient(app) as client:
        pass

def test_get_instance():
    """ 
    Verifies that the global INSTANCE is accessible.
    Right now it might be None, but the function should exist.
    """

    instance = get()
    assert instance is None