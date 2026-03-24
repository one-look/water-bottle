import logging
import time
from typing import List, Dict, Any

logger = logging.getLogger("water-bottle.prompt.behavior")


class PromptManager:
    """Manages system prompts and behavior for the RAG application."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize prompt manager.
        
        Args:
            config: Configuration dictionary with prompt settings
        """
        self.config = config or {}
        self.system_prompt = self.config.get(
            "system_prompt",
            "You are a helpful AI assistant for Nehru Memorial College. Keep responses short, simple."
            "Use the provided context to answer questions accurately. Only elaborate if specifically asked for details. "
            "If the context doesn't contain relevant information, say so politely."
        )
        logger.info("action=initialize prompt=manager system_prompt_length=%d", len(self.system_prompt))
    
    def build_prompt(self, query: str, context: List[str], history: List[Dict[str, Any]]) -> str:
        """Build a complete prompt including system message, context, history, and query.
        
        Args:
            query: User query
            context: List of retrieved context documents
            history: Conversation history
            
        Returns:
            Complete prompt string
        """
        start_time = time.time()
        logger.debug("action=build_prompt prompt=manager query_length=%d context_count=%d history_count=%d", len(query), len(context), len(history))
        
        prompt_parts = [f"System: {self.system_prompt}"]
        
        # Add conversation history
        if history:
            prompt_parts.append("\nConversation History:")
            for msg in history[-10:]:  # Limit to last 10 messages
                role = msg.get("role", "unknown").capitalize()
                content = msg.get("content", "")
                prompt_parts.append(f"{role}: {content}")
            logger.debug("action=add_history prompt=manager history_messages=%d", min(len(history), 10))
        
        # Add context
        if context:
            prompt_parts.append("\nContext:")
            for i, ctx in enumerate(context, 1):
                prompt_parts.append(f"Context {i}: {ctx}")
            logger.debug("action=add_context prompt=manager context_documents=%d", len(context))
        
        # Add current query
        prompt_parts.append(f"\nUser: {query}")
        prompt_parts.append("\nAssistant:")
        
        final_prompt = "\n".join(prompt_parts)
        duration = time.time() - start_time
        logger.info("action=build_prompt_complete prompt=manager prompt_length=%d duration=%.3fs context_count=%d history_count=%d", len(final_prompt), duration, len(context), len(history))
        return final_prompt
    
    def get_system_prompt(self) -> str:
        """Get the system prompt.
        
        Returns:
            System prompt string
        """
        logger.debug("action=get_system_prompt prompt=manager prompt_length=%d", len(self.system_prompt))
        return self.system_prompt
    
    def update_system_prompt(self, prompt: str) -> None:
        """Update the system prompt.
        
        Args:
            prompt: New system prompt
        """
        old_length = len(self.system_prompt)
        self.system_prompt = prompt
        logger.info("action=update_system_prompt prompt=manager old_length=%d new_length=%d", old_length, len(prompt))
