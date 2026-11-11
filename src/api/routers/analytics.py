'''Read-only analytics endpoints (e.g. IP geolocation events for maps).'''

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from src.core.logging import setup_logger
from src.core.multitenancy import get_current_tenant
from src.database.psql import list_ip_location_events

logger = setup_logger(__name__)

router = APIRouter(prefix="/analytics", tags=["Analytics"])


class IpLocationEventResponse(BaseModel):
    '''One tracked RAG request location row.'''

    id: int
    created_at: datetime
    session_id: str
    tenant_id: str
    chat_mode: str
    user_sub: str | None = None
    ip_address: str | None = None
    city: str | None = None
    region: str | None = None
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class IpLocationEventListResponse(BaseModel):
    '''Paged list of location events for the current tenant.'''

    tenant_id: str
    total: int
    events: list[IpLocationEventResponse]


@router.get(
    "/ip-location-events",
    response_model=IpLocationEventListResponse,
    status_code=status.HTTP_200_OK,
    summary="List IP geolocation events for map visualization",
)
async def get_ip_location_events(
    limit: int = Query(500, ge=1, le=2000, description="Max rows to return"),
) -> IpLocationEventListResponse:
    '''
    Return recent ip_location_events for the authenticated tenant.

    Rows without latitude/longitude may still include city/country from IP lookup.
    '''
    tenant_id = get_current_tenant()
    try:
        rows = list_ip_location_events(tenant_id, limit=limit)
    except RuntimeError as exc:
        logger.error("IP location list failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is not available",
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    events = [IpLocationEventResponse.model_validate(row) for row in rows]
    return IpLocationEventListResponse(tenant_id=tenant_id, total=len(events), events=events)
