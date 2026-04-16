"""
Delivery Integration CRUD endpoints
Manages per-restaurant connections to Uber Eats, Just Eat, Deliveroo etc.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from uuid import UUID
import logging

from ..db import get_pool

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/delivery-integrations", tags=["delivery-integrations"])

WEBHOOK_BASE_URL = "https://testenv.corpv3.com/api/v1/webhooks"

PLATFORM_WEBHOOK_PATHS = {
    "uber_eats": f"{WEBHOOK_BASE_URL}/uber-eats",
    "just_eat": f"{WEBHOOK_BASE_URL}/just-eat",
    "deliveroo": f"{WEBHOOK_BASE_URL}/deliveroo",
}


class IntegrationUpsert(BaseModel):
    restaurant_id: UUID
    platform: str  # uber_eats | just_eat | deliveroo
    external_store_id: str
    is_active: bool = True


class IntegrationResponse(BaseModel):
    id: str
    restaurant_id: str
    platform: str
    external_store_id: str
    is_active: bool
    webhook_url: Optional[str]
    created_at: str
    updated_at: str


def _row_to_dict(row) -> dict:
    return {
        "id": str(row["id"]),
        "restaurant_id": str(row["restaurant_id"]),
        "platform": row["platform"],
        "external_store_id": row["external_store_id"],
        "is_active": row["is_active"],
        "webhook_url": PLATFORM_WEBHOOK_PATHS.get(row["platform"]),
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }


@router.get("/{restaurant_id}", response_model=List[IntegrationResponse])
async def list_integrations(restaurant_id: UUID):
    """List all delivery platform integrations for a restaurant."""
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM delivery_integrations WHERE restaurant_id = $1 ORDER BY platform",
        restaurant_id
    )
    return [_row_to_dict(r) for r in rows]


@router.post("", response_model=IntegrationResponse)
async def upsert_integration(body: IntegrationUpsert):
    """Create or update a delivery platform integration (upsert by restaurant+platform)."""
    valid_platforms = {"uber_eats", "just_eat", "deliveroo"}
    if body.platform not in valid_platforms:
        raise HTTPException(status_code=400, detail=f"Invalid platform. Must be one of: {valid_platforms}")

    pool = await get_pool()
    row = await pool.fetchrow("""
        INSERT INTO delivery_integrations (restaurant_id, platform, external_store_id, is_active, webhook_url)
        VALUES ($1, $2::deliveryplatform, $3, $4, $5)
        ON CONFLICT (restaurant_id, platform)
        DO UPDATE SET
            external_store_id = EXCLUDED.external_store_id,
            is_active = EXCLUDED.is_active,
            updated_at = NOW()
        RETURNING *
    """,
        body.restaurant_id,
        body.platform,
        body.external_store_id,
        body.is_active,
        PLATFORM_WEBHOOK_PATHS.get(body.platform)
    )
    return _row_to_dict(row)


@router.delete("/{integration_id}")
async def delete_integration(integration_id: UUID):
    """Remove a delivery platform integration."""
    pool = await get_pool()
    result = await pool.execute(
        "DELETE FROM delivery_integrations WHERE id = $1", integration_id
    )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Integration not found")
    return {"status": "deleted"}


@router.patch("/{integration_id}/toggle")
async def toggle_integration(integration_id: UUID):
    """Enable or disable a delivery platform integration."""
    pool = await get_pool()
    row = await pool.fetchrow("""
        UPDATE delivery_integrations
        SET is_active = NOT is_active, updated_at = NOW()
        WHERE id = $1
        RETURNING *
    """, integration_id)
    if not row:
        raise HTTPException(status_code=404, detail="Integration not found")
    return _row_to_dict(row)
