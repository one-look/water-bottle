from typing import List, Dict, Any
from collections import deque
import logging
from .base import MemoryBase

# Set up logging to track RAM usage if needed
logger = logging.getLogger(__name__)

class WindowMemory(MemoryBase):
    """
    High-concurrency sliding window memory implementation.
    Optimized for efficient memory usage with configurable window size.
    """
    
    def __init__(self, window_size: int = 5):
        """
        Initialize sliding window memory.
        
        Args:
            window_size: Number of recent messages to retain in memory
        """
        self.window_size = window_size
        # Key: session_id, Value: deque of message objects
        self._sessions: Dict[str, deque] = {}

    async def get_history(self, session_id: str) -> List[Dict[str, str]]:
        """
        Retrieve the windowed conversation history for a session.
        
        Args:
            session_id: Unique identifier for the conversation session
            
        Returns:
            List of recent messages within the window size
        """
        if session_id not in self._sessions:
            return []
        
        # Returns only the content currently in the sliding window
        return list(self._sessions[session_id])

    async def add_message(self, session_id: str, role: str, content: str) -> None:
        """
        Add a new message to the session's sliding window.
        
        Args:
            session_id: Unique identifier for the conversation session
            role: Message role (e.g., 'user', 'assistant')
            content: Message content
        """
        if session_id not in self._sessions:
            self._sessions[session_id] = deque(maxlen=self.window_size)
        
        # Store only role and content to minimize memory footprint
        self._sessions[session_id].append({
            "role": role,
            "content": content
        })

    async def clear_session(self, session_id: str) -> None:
        """
        Clear all messages for a specific session and free memory.
        
        Args:
            session_id: Unique identifier for the conversation session
        """
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.info(f"Session {session_id} cleared. Memory released.")

    def get_memory_stats(self) -> Dict[str, int]:
        """
        Get memory usage statistics for monitoring.
        
        Returns:
            Dictionary with active session count and window size limit
        """
        return {
            "active_sessions": len(self._sessions),
            "window_size_limit": self.window_size
        }