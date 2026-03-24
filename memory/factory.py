from typing import Dict, Any
from .base import MemoryBase
from .history import SessionMemory
from .window_memory import WindowMemory
from .redis_cache import RedisCacheMemory


class MemoryFactory:
    """Factory for creating memory instances."""
    
    @staticmethod
    def create(config: Dict[str, Any], services: Dict[str, Any]) -> MemoryBase:
        """Create a memory instance based on configuration.
        
        Args:
            config: Configuration dictionary containing memory settings
            services: Dictionary of available services
            
        Returns:
            MemoryBase: Configured memory instance
            
        Raises:
            ValueError: If memory type is not supported
        """
        memory_type = config.get("type", "window")
        
        # Create base memory
        if memory_type == "session":
            max_messages = config.get("max_messages", 50)
            base_memory = SessionMemory(max_messages=max_messages)
        elif memory_type == "window":
            window_size = config.get("window_size", 10)
            base_memory = WindowMemory(window_size=window_size)
        else:
            raise ValueError(f"Unsupported memory type: {memory_type}")
        
        # Wrap with Redis cache if enabled
        if config.get("redis_cache", False):
            redis_url = config.get("redis_url")
            ttl = config.get("cache_ttl", 7200)
            return RedisCacheMemory(base_memory, redis_url, ttl)
        
        return base_memory
