'''Parse Meta WhatsApp webhook JSON payloads.'''

from typing import Any, Optional


class WhatsAppPayloadParser:
    '''
    Extract sender and text from Cloud API webhook notification bodies.
    '''

    @staticmethod
    def parse_incoming_text(body: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
        '''
        Args:
            body: Parsed JSON POST body from Meta.

        Returns:
            tuple[Optional[str], Optional[str]]: (sender_phone, message_text) or (None, None).
        '''
        try:
            entry = body["entry"][0]
            change = entry["changes"][0]["value"]
            messages = change.get("messages")
            if not messages:
                return None, None
            msg = messages[0]
            if msg.get("type") != "text":
                return None, None
            sender = msg.get("from")
            text_body = msg.get("text", {}).get("body")
            if not sender or not text_body or not str(text_body).strip():
                return None, None
            return str(sender), str(text_body).strip()
        except (KeyError, IndexError, TypeError):
            return None, None
