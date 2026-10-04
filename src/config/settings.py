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

    GOOGLE_OAUTH_CLIENT_ID: str = Field(..., min_length=1)
    GOOGLE_OAUTH_CLIENT_SECRET: str = Field(..., min_length=1)
    GOOGLE_OAUTH_REDIRECT_URI: str = Field(
        default="",
        description="Override; default is PUBLIC_BASE_URL/auth/google/callback",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def google_oauth_redirect_uri(self) -> str:
        '''
        Google OAuth redirect URI registered in Google Cloud Console.

        Returns:
            str: Callback URL for the authorization code flow.
        '''
        if self.GOOGLE_OAUTH_REDIRECT_URI.strip():
            return self.GOOGLE_OAUTH_REDIRECT_URI.strip()
        return f"{self.PUBLIC_BASE_URL.rstrip('/')}/auth/google/callback"


settings = Settings()
