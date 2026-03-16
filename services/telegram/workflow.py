import os
import logging
from typing import Dict, Any
from .input import TelegramInput
from .output import TelegramOutput

logger = logging.getLogger("water-bottle.telegram.workflow")

class TelegramWorkflow:
    """Main Telegram workflow orchestrator."""
    
    def __init__(self, workflow, config: Dict[str, Any]):
        self.workflow = workflow
        self.config = config
        token = os.getenv("TELEGRAM_BOT_TOKEN") or config.get("telegram", {}).get("bot_token")
        
        if not token:
            logger.error("Telegram bot token not configured")
            raise ValueError("Telegram bot token not configured")
        
        # Initialize components
        self.input_handler = TelegramInput()
        self.output_handler = TelegramOutput(token)

    def __call__(self, update_data: Dict[str, Any]) -> Dict[str, Any]:
        return self.run(update_data)
    
    async def run(self, update_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a Telegram update through the complete pipeline.
        
        Args:
            update_data: Raw Telegram webhook data
            
        Returns:
            Processing result status
        """
        try:
            logger.info(f"Processing Telegram update: {update_data}")
            
            # Step 1: Parse update
            parsed_data = self.input_handler.get(update_data)
            if parsed_data["status"] == "skip":
                return {"status": "ok"}
            
            # Step 2: Process through RAG workflow (async)
            logger.info(f"Processing Telegram message from chat {parsed_data['chat_id']}: {parsed_data['query'][:50]}...")
            response = await self.workflow.run(parsed_data["query"], session_id=parsed_data["session_id"])
            
            # Step 3: Send response back to Telegram
            await self.output_handler.send(parsed_data["chat_id"], response)
            
            return {"status": "ok"}
            
        except Exception as e:
            logger.error(f"Error processing Telegram update: {str(e)}")
            raise