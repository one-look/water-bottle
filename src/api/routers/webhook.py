'''Meta WhatsApp webhook: verification and inbound message handling.'''

import secrets
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response, status

from src.api import application
from src.config.settings import settings
from src.connectors.whatsapp import WhatsAppPayloadParser, send_text_message
from src.core.logging import setup_logger
from src.core.multitenancy import get_current_tenant
from src.rag.workflow import RAGWorkflow

logger = setup_logger(__name__)

router = APIRouter(tags=["WhatsApp"])


@router.get("/webhook")
async def verify_webhook(request: Request) -> Response:
    '''
    Meta webhook subscription verification (hub.challenge).
    '''
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token") or ""
    challenge = params.get("hub.challenge")
    if (
        mode == "subscribe"
        and challenge
        and secrets.compare_digest(token, settings.WHATSAPP_VERIFY_TOKEN)
    ):
        return Response(content=challenge, media_type="text/plain")
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Verification failed")


@router.post("/webhook")
async def receive_webhook(request: Request) -> dict[str, str]:
    '''
    Receive WhatsApp events; run RAG for inbound text and reply on the same thread.
    '''
    try:
        body: dict[str, Any] = await request.json()
    except Exception as exc:
        logger.warning("Invalid WhatsApp webhook JSON: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON body",
        ) from exc

    sender, user_query = WhatsAppPayloadParser.parse_incoming_text(body)
    if not sender or not user_query:
        return {"status": "ok"}

    logger.info("Processing WhatsApp RAG for tenant '%s'", get_current_tenant())

    app_instance = application.get()
    if not app_instance or not hasattr(app_instance, "config"):
        logger.error("Application not initialized for WhatsApp RAG")
        return {"status": "ok"}

    tenant_id = get_current_tenant()
    session_id = f"whatsapp:{sender}"

    try:
        workflow = RAGWorkflow(config=app_instance.config)
        answer, _ = await workflow.execute(
            query=user_query,
            tenant_id=tenant_id,
            session_id=session_id,
        )
        await send_text_message(recipient_id=sender, text=answer)
    except Exception as exc:
        logger.error("WhatsApp RAG reply failed: %s", exc, exc_info=True)
        try:
            await send_text_message(
                recipient_id=sender,
                text="Sorry, I could not process your message. Please try again later.",
            )
        except Exception as send_exc:
            logger.error("WhatsApp error reply failed: %s", send_exc)

    return {"status": "ok"}
