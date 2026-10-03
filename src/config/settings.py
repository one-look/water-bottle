from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    '''
    Application secrets and deployment-specific configuration from the environment.
    '''

    PROJECT_NAME: str
    GEMINI_API_KEY: str
    LOG_LEVEL: str

    QDRANT_URL: str
    QDRANT_API_KEY: str
    REDIS_URL: str

    PUBLIC_BASE_URL: str = Field(
        ...,
        min_length=1,
        description="Public HTTPS URL of this service (e.g. Render external URL).",
    )
    AUTH_ISSUER: str = Field(
        default="",
        description="Optional OAuth issuer override; defaults to PUBLIC_BASE_URL when empty.",
    )
    AUTH_PRIVATE_KEY: str = Field(..., min_length=1)
    AUTH_PUBLIC_KEY: str = Field(..., min_length=1)
    AUTH_WATER_BOTTLE_CLIENT_SECRET: str = Field(..., min_length=1)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
