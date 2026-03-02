from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def home():
    return {"status": "Home route is active"}