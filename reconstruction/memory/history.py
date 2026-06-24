from typing import List, Dict, Any
from collections import defaultdict, deque
from .base import MemoryBase


class SessionMemory(MemoryBase):
    """Dictionary-backed session-based memory implementation."""
    
    def __init__(self, max_messages: int = 50):
        """Initialize session memory.
        
        Args:
            max_messages: Maximum number of messages to keep per session
        """
        self.max_messages = max_messages
        self._sessions: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_messages))
    
    def get_history(self, session_id: str) -> List[Dict[str, Any]]:
        """Retrieve conversation history for a session.
        
        Args:
            session_id: Unique identifier for the conversation session
            
        Returns:
            List of conversation messages with role and content
        """
        return list(self._sessions[session_id])
    
    def add_message(self, session_id: str, role: str, content: str) -> None:
        """Add a message to the conversation history.
        
        Args:
            session_id: Unique identifier for the conversation session
            role: Message role (e.g., 'user', 'assistant')
            content: Message content
        """
        message = {
            "role": role,
            "content": content
        }
        self._sessions[session_id].append(message)
    
    def clear_session(self, session_id: str) -> None:
        """Clear conversation history for a session.
        
        Args:
            session_id: Unique identifier for the conversation session
        """
        if session_id in self._sessions:
            del self._sessions[session_id]
