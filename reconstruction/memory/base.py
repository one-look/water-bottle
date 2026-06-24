from abc import ABC, abstractmethod
from typing import List, Dict, Any


class MemoryBase(ABC):
    """Abstract base class for conversation memory services."""
    
    @abstractmethod
    def get_history(self, session_id: str) -> List[Dict[str, Any]]:
        """Retrieve conversation history for a session.
        
        Args:
            session_id: Unique identifier for the conversation session
            
        Returns:
            List of conversation messages with role and content
        """
        raise NotImplementedError("get_history method must be implemented")
    
    @abstractmethod
    def add_message(self, session_id: str, role: str, content: str) -> None:
        """Add a message to the conversation history.
        
        Args:
            session_id: Unique identifier for the conversation session
            role: Message role (e.g., 'user', 'assistant')
            content: Message content
        """
        raise NotImplementedError("add_message method must be implemented")
    
    @abstractmethod
    def clear_session(self, session_id: str) -> None:
        """Clear conversation history for a session.
        
        Args:
            session_id: Unique identifier for the conversation session
        """
        raise NotImplementedError("clear_session method must be implemented")
