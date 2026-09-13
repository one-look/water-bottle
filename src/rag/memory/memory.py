from collections import defaultdict
from typing import List, Dict

class ConversationMemory:
    """Stores the last N messages per session in memory."""
    def __init__(self, max_messages: int = 5):
        self.max_messages = max_messages
        # Structure: { session_id: [{"role": "user/assistant", "content": "..."}, ...] }
        self._store: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    def get_history(self, session_id: str) -> List[Dict[str, str]]:
        return self._store[session_id]

    def add_message(self, session_id: str, role: str, content: str) -> None:
        self._store[session_id].append({"role": role, "content": content})
        # Maintain sliding window of last N messages
        if len(self._store[session_id]) > self.max_messages:
            self._store[session_id] = self._store[session_id][-self.max_messages:]

# Global memory instance
memory_store = ConversationMemory(max_messages=5)