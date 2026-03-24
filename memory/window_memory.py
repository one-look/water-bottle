import logging
import time
from typing import List, Dict, Any
from collections import deque
from .base import MemoryBase

# Set up logging to track RAM usage if needed
logger = logging.getLogger("water-bottle.memory.window")

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
        logger.info("action=initialize memory=window window_size=%d", window_size)

    async def get_history(self, session_id: str) -> List[Dict[str, str]]:
        """
        Retrieve the windowed conversation history for a session.
        
        Args:
            session_id: Unique identifier for the conversation session
            
        Returns:
            List of recent messages within the window size
        """
        start_time = time.time()
        logger.debug("action=get_history memory=window session_id=%s", session_id)
        
        if session_id not in self._sessions:
            logger.debug("action=get_history_empty memory=window session_id=%s", session_id)
            return []
        
        history = list(self._sessions[session_id])
        duration = time.time() - start_time
        logger.debug("action=get_history_complete memory=window session_id=%s messages_count=%d duration=%.3fs", session_id, len(history), duration)
        return history

    async def add_message(self, session_id: str, role: str, content: str) -> None:
        """
        Add a new message to the session's sliding window.
        
        Args:
            session_id: Unique identifier for the conversation session
            role: Message role (e.g., 'user', 'assistant')
            content: Message content
        """
        start_time = time.time()
        logger.debug("action=add_message memory=window session_id=%s role=%s content_length=%d", session_id, role, len(content))
        
        if session_id not in self._sessions:
            self._sessions[session_id] = deque(maxlen=self.window_size)
            logger.debug("action=create_session memory=window session_id=%s", session_id)
        
        # Store only role and content to minimize memory footprint
        self._sessions[session_id].append({
            "role": role,
            "content": content
        })
        
        duration = time.time() - start_time
        session_messages = len(self._sessions[session_id])
        logger.debug("action=add_message_complete memory=window session_id=%s role=%s session_messages=%d duration=%.3fs", session_id, role, session_messages, duration)

    async def clear_session(self, session_id: str) -> None:
        """
        Clear all messages for a specific session and free memory.
        
        Args:
            session_id: Unique identifier for the conversation session
        """
        start_time = time.time()
        logger.info("action=clear_session memory=window session_id=%s", session_id)
        
        if session_id in self._sessions:
            messages_count = len(self._sessions[session_id])
            del self._sessions[session_id]
            duration = time.time() - start_time
            logger.info("action=clear_session_complete memory=window session_id=%s messages_cleared=%d duration=%.3fs", session_id, messages_count, duration)
        else:
            logger.debug("action=clear_session_not_found memory=window session_id=%s", session_id)

    def get_memory_stats(self) -> Dict[str, int]:
        """
        Get memory usage statistics for monitoring.
        
        Returns:
            Dictionary with active session count and window size limit
        """
        total_messages = sum(len(messages) for messages in self._sessions.values())
        stats = {
            "active_sessions": len(self._sessions),
            "window_size_limit": self.window_size,
            "total_messages": total_messages
        }
        logger.debug("action=get_memory_stats memory=window active_sessions=%d total_messages=%d window_size=%d", stats["active_sessions"], stats["total_messages"], stats["window_size_limit"])
        return stats