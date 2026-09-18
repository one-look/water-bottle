from fastapi import FastAPI
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from typing import Optional
from pydantic import BaseModel, Field

from src.core.logging import setup_logger

logger = setup_logger(__name__)


class RateLimitConfig(BaseModel):
    enabled: bool = Field(True, description="Whether rate limiting is enabled.")
    url: str = Field(..., description="Redis storage URL for rate limiting.")
    default_limits: list[str] = Field(default_factory=lambda: ["60/minute"])


class RateLimitManager:
    def __init__(self) -> None:
        self._limiter = Limiter(
            key_func=get_remote_address,
            default_limits=["60/minute"],
        )

    def init_app(self, app: FastAPI, config: RateLimitConfig) -> Limiter:
        logger.info("Initializing Rate Limiter...")
        self._limiter.storage_uri = config.url
        self._limiter.enabled = config.enabled
        self._limiter._default_limits = config.default_limits
        
        app.state.limiter = self._limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        return self._limiter

    def get_limiter(self) -> Limiter:
        if not self._limiter:
            raise RuntimeError("Rate limiter is not initialized.")
        return self._limiter

limiter_manager = RateLimitManager()
limiter = limiter_manager.get_limiter()