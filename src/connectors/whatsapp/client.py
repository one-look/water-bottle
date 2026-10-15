'''Meta WhatsApp Cloud API HTTP client.'''

import httpx

from src.config.settings import settings
from src.core.logging import setup_logger

logger = setup_logger(__name__)

_GRAPH_API = "https://graph.facebook.com/v21.0"
_MAX_TEXT_LENGTH = 4096


async def send_text_message(recipient_id: str, text: str) -> None:
    '''
    Send a WhatsApp text message to a user (E.164 without + in Meta payloads).

    Args:
        recipient_id (str): WhatsApp user phone id from webhook ``from`` field.
        text (str): Message body.

    Raises:
        httpx.HTTPError: On transport or non-success API response.
    '''
    body = text if len(text) <= _MAX_TEXT_LENGTH else text[: _MAX_TEXT_LENGTH - 3] + "..."
    url = f"{_GRAPH_API}/{settings.WHATSAPP_PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": recipient_id,
        "type": "text",
        "text": {"body": body},
    }
    headers = {
        "Authorization": f"Bearer {settings.WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, json=payload, headers=headers)
        response.raise_for_status()
    logger.info("WhatsApp message sent to %s", recipient_id)
