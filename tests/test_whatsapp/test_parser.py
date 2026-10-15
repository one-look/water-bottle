from src.connectors.whatsapp.parser import WhatsAppPayloadParser


def _text_payload(body_text: str, sender: str = "919876543210") -> dict:
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "messages": [
                                {
                                    "from": sender,
                                    "type": "text",
                                    "text": {"body": body_text},
                                }
                            ]
                        }
                    }
                ]
            }
        ]
    }


def test_parse_incoming_text_success():
    sender, text = WhatsAppPayloadParser.parse_incoming_text(
        _text_payload("Hello campus")
    )
    assert sender == "919876543210"
    assert text == "Hello campus"


def test_parse_incoming_text_ignores_non_text():
    payload = _text_payload("hi")
    payload["entry"][0]["changes"][0]["value"]["messages"][0]["type"] = "image"
    assert WhatsAppPayloadParser.parse_incoming_text(payload) == (None, None)


def test_parse_incoming_text_empty_body():
    assert WhatsAppPayloadParser.parse_incoming_text({}) == (None, None)
