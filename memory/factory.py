from typing import Dict, Any
from .base import MemoryBase
from .history import SessionMemory
from .window_memory import WindowMemory


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
        
        if memory_type == "session":
            max_messages = config.get("max_messages", 50)
            return SessionMemory(max_messages=max_messages)
        elif memory_type == "window":
            window_size = config.get("window_size", 10)
            return WindowMemory(window_size=window_size)
        else:
            raise ValueError(f"Unsupported memory type: {memory_type}")
