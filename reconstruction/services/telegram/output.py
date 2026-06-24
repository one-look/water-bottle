import httpx
import logging
from typing import Dict, Any

logger = logging.getLogger("water-bottle.telegram.output")

class TelegramOutput:
    """Handles sending responses to Telegram."""
    
    def __init__(self, token: str):
        self.token = token
    
    async def send(self, chat_id: int, response: str) -> None:
        """Send response message to Telegram chat.
        
        Args:
            chat_id: Telegram chat ID
            response: Response text to send
        """
        telegram_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        
        async with httpx.AsyncClient() as client:
            payload = {
                "chat_id": chat_id,
                "text": response,
                "parse_mode": "HTML"
            }
            
            response = await client.post(telegram_url, json=payload, timeout=30.0)
            
            if response.status_code != 200:
                logger.error(f"Failed to send Telegram message: {response.text}")
                raise Exception(f"Failed to send response to Telegram: {response.text}")
            
            logger.info(f"Successfully sent response to Telegram chat {chat_id}")