from typing import List, Dict, Any


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
            "You are a helpful AI assistant for Nehru Memorial College. Keep responses short, simple, and conversational (5 lines maximum). "
            "Use the provided context to answer questions accurately. Only elaborate if specifically asked for details. "
            "If the context doesn't contain relevant information, say so politely."
        )
    
    def build_prompt(self, query: str, context: List[str], history: List[Dict[str, Any]]) -> str:
        """Build a complete prompt including system message, context, history, and query.
        
        Args:
            query: User query
            context: List of retrieved context documents
            history: Conversation history
            
        Returns:
            Complete prompt string
        """
        prompt_parts = [f"System: {self.system_prompt}"]
        
        # Add conversation history
        if history:
            prompt_parts.append("\nConversation History:")
            for msg in history[-10:]:  # Limit to last 10 messages
                role = msg.get("role", "unknown").capitalize()
                content = msg.get("content", "")
                prompt_parts.append(f"{role}: {content}")
        
        # Add context
        if context:
            prompt_parts.append("\nContext:")
            for i, ctx in enumerate(context, 1):
                prompt_parts.append(f"Context {i}: {ctx}")
        
        # Add current query
        prompt_parts.append(f"\nUser: {query}")
        prompt_parts.append("\nAssistant:")
        
        return "\n".join(prompt_parts)
    
    def get_system_prompt(self) -> str:
        """Get the system prompt.
        
        Returns:
            System prompt string
        """
        return self.system_prompt
    
    def update_system_prompt(self, prompt: str) -> None:
        """Update the system prompt.
        
        Args:
            prompt: New system prompt
        """
        self.system_prompt = prompt
