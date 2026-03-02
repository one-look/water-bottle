import logging
from typing import Dict, Any

logger = logging.getLogger("water-bottle.telegram.input")

class TelegramInput:
    """Handles parsing Telegram webhook input data."""
    
    @staticmethod
    def get(update_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Telegram update and extract relevant information.
        
        Args:
            update_data: Raw Telegram webhook data
            
        Returns:
            Parsed data with chat_id, query, etc.
        """
        if "message" not in update_data or "text" not in update_data["message"]:
            logger.warning("No message text found in update")
            return {"status": "skip"}
        
        chat_id = update_data["message"]["chat"]["id"]
        user_question = update_data["message"]["text"]
        
        # Skip empty messages
        if not user_question.strip():
            return {"status": "skip"}
        
        return {
            "status": "ok",
            "chat_id": chat_id,
            "query": user_question,
            "session_id": f"telegram_{chat_id}"
        }