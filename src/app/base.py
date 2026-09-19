import os
import yaml

from fastapi import FastAPI
from qdrant_client import AsyncQdrantClient

from src.core.ratelimit import RateLimitConfig, limiter_manager
from src.core.redis import redis_manager, RedisConfig
from src.config.settings import settings

class Application:
    '''
    application configuration manager.
    '''
    @staticmethod
    def read(data):
        '''
        read configuration from file or input

        Args:
            data (str): file path or yaml string

        Returns:
            dict: configuration
        '''
        if isinstance(data, str):
            if os.path.exists(data):
                # read yaml from file
                with open(data, "r", encoding="utf-8") as f:
                    # read configuration
                    return yaml.safe_load(f)

            # attempt to read yaml from input
            data = yaml.safe_load(data)
            if not isinstance(data, str):
                return data

            # file not found and input is not yaml, raise error
            raise FileNotFoundError(f"File not found: {data}")
        
        # return unmodified
        return data

    def __init__(self, config=None):
        '''
        initialize application configuration.

        Args:
            config (dict): configuration.
        '''
        self.settings = settings

        self.config = self.read(config or os.environ.get("CONFIG", "config.yml"))
        self.ratelimit_config = RateLimitConfig(**self.config.get("ratelimit", {}))

        # initialize redis connection pool
        redis_manager.init_client(RedisConfig(url=settings.REDIS_URL))

        self.qdrant_client = AsyncQdrantClient(url=settings.QDRANT_URL)

    async def close(self):
        '''
        close all global connections.
        '''
        await redis_manager.close()
        await self.qdrant_client.close()

    def ratelimiter(self, app: FastAPI) -> None:
        '''

        '''
        limiter_manager.init_app(app, self.ratelimit_config)
        
    def __call__(self, data):
        '''

        '''
        pass