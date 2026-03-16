import pytest
import asyncio
from memory.window_memory import WindowMemory


class TestWindowMemory:
    """Test suite for WindowMemory implementation."""
    
    @pytest.fixture
    def memory(self):
        """Create a WindowMemory instance for testing."""
        return WindowMemory(window_size=3)
    
    @pytest.mark.asyncio
    async def test_add_and_get_messages(self, memory):
        """Test adding and retrieving messages."""
        session_id = "test_session"
        
        await memory.add_message(session_id, "user", "Hello")
        await memory.add_message(session_id, "assistant", "Hi there!")
        await memory.add_message(session_id, "user", "How are you?")
        
        history = await memory.get_history(session_id)
        assert len(history) == 3
        assert history[0]["role"] == "user"
        assert history[0]["content"] == "Hello"
        assert history[1]["role"] == "assistant"
        assert history[2]["role"] == "user"
    
    @pytest.mark.asyncio
    async def test_window_size_limit(self, memory):
        """Test that window size is respected."""
        session_id = "test_session"
        
        # Add 5 messages (window size is 3)
        for i in range(5):
            await memory.add_message(session_id, "user", f"Message {i}")
        
        history = await memory.get_history(session_id)
        assert len(history) == 3
        assert history[0]["content"] == "Message 2"
        assert history[1]["content"] == "Message 3"
        assert history[2]["content"] == "Message 4"
    
    @pytest.mark.asyncio
    async def test_clear_session(self, memory):
        """Test clearing a session."""
        session_id = "test_session"
        
        await memory.add_message(session_id, "user", "Hello")
        assert len(await memory.get_history(session_id)) == 1
        
        await memory.clear_session(session_id)
        assert len(await memory.get_history(session_id)) == 0
    
    @pytest.mark.asyncio
    async def test_memory_stats(self, memory):
        """Test getting memory statistics."""
        session_id = "test_session"
        
        # Initially no sessions
        stats = memory.get_memory_stats()
        assert stats["active_sessions"] == 0
        assert stats["window_size_limit"] == 3
        
        # Add a session
        await memory.add_message(session_id, "user", "Hello")
        stats = memory.get_memory_stats()
        assert stats["active_sessions"] == 1
        assert stats["window_size_limit"] == 3
    
    @pytest.mark.asyncio
    async def test_multiple_sessions(self, memory):
        """Test handling multiple sessions independently."""
        session1 = "session1"
        session2 = "session2"
        
        await memory.add_message(session1, "user", "Session 1 message")
        await memory.add_message(session2, "user", "Session 2 message")
        
        history1 = await memory.get_history(session1)
        history2 = await memory.get_history(session2)
        
        assert len(history1) == 1
        assert len(history2) == 1
        assert history1[0]["content"] == "Session 1 message"
        assert history2[0]["content"] == "Session 2 message"
