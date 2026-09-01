from fastapi import APIRouter
from src.api import application

router = APIRouter()

@router.get("/")
async def home():
    app_instance = application.get()
    home = app_instance.config.get("home", {}) if app_instance else {}

    message = home.get("message")

    return {"message": message}
