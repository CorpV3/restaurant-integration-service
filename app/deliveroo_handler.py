"""
Deliveroo Order Handler
Processes Deliveroo Partner Platform webhook events and creates orders in the system
"""
import httpx
import os
from typing import Dict, Any
from shared.utils.logger import setup_logger

logger = setup_logger("deliveroo-handler")

ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8004")


async def get_restaurant_id_for_site(deliveroo_site_id: str) -> str:
    """
    Lookup the restaurant UUID that maps to the given Deliveroo site ID.
    Falls back to DEFAULT_RESTAURANT_ID env var if not found.
    """
    from .db import get_pool
    fallback = os.getenv("DEFAULT_RESTAURANT_ID", "6956017d-3aea-4ae2-9709-0ca0ac0a1a09")
    if not deliveroo_site_id:
        return fallback
    try:
        pool = await get_pool()
        row = await pool.fetchrow(
            "SELECT restaurant_id FROM delivery_integrations "
            "WHERE platform = 'deliveroo' AND external_store_id = $1 AND is_active = TRUE",
            deliveroo_site_id
        )
        if row:
            return str(row["restaurant_id"])
    except Exception as e:
        logger.warning(f"DB lookup failed for deliveroo_site_id={deliveroo_site_id}: {e}")
    return fallback


async def process_deliveroo_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a new Deliveroo order webhook (order_created event).

    Deliveroo payload structure:
    {
      "event_type": "order_created",
      "site_id": "...",
      "order": {
        "id": "...",
        "display_id": "...",
        "customer": { "name": "...", "phone": "..." },
        "fulfillment": { "type": "DELIVERY" | "COLLECTION", "delivery_address": {...} },
        "items": [ { "name": "...", "quantity": 1, "unit_price": {...} } ],
        "total_price": { "fractional": 1200, "currency": "GBP" }
      }
    }
    """
    try:
        order_data_raw = payload.get("order", payload.get("data", {}))
        # site_id can be in payload root or inside order data
        site_id = (payload.get("site_id") or payload.get("location_id")
                   or order_data_raw.get("location_id") or order_data_raw.get("site_id"))
        restaurant_id = await get_restaurant_id_for_site(str(site_id) if site_id else "")
        logger.info(f"Processing Deliveroo order id={order_data_raw.get('id')} site={site_id} → restaurant={restaurant_id}")

        # Customer — sandbox may omit customer block; fall back gracefully
        customer = order_data_raw.get("customer", {})
        customer_name = (customer.get("name") or
                         f"{customer.get('first_name','')} {customer.get('last_name','')}".strip()
                         or "Deliveroo Customer")

        # Fulfillment type
        fulfillment = order_data_raw.get("fulfillment", {})
        fulfillment_type = fulfillment.get("type", "DELIVERY").upper()
        order_type = "ONLINE" if fulfillment_type == "DELIVERY" else "TAKEAWAY"

        # Delivery address
        delivery_address = ""
        if fulfillment_type == "DELIVERY":
            addr = fulfillment.get("delivery_address", {})
            parts = [
                addr.get("address_line_1", ""),
                addr.get("address_line_2", ""),
                addr.get("city", ""),
                addr.get("postcode", ""),
            ]
            delivery_address = ", ".join(p for p in parts if p)

        # Deliveroo display_id for reference
        display_id = order_data_raw.get("display_id", "")
        notes = f"Deliveroo #{display_id}" if display_id else "Deliveroo order"
        if order_data_raw.get("customer_notes"):
            notes += f" | {order_data_raw['customer_notes']}"

        order_payload = {
            "restaurant_id": restaurant_id,
            "order_type": order_type,
            "customer_name": customer_name,
            "customer_phone": customer.get("phone_number", customer.get("phone", "")),
            "customer_email": customer.get("email", ""),
            "delivery_address": delivery_address,
            "special_instructions": notes,
            "items": [],
        }

        # Map line items — Deliveroo sandbox may send items[] or order_items[]
        raw_items = (order_data_raw.get("items")
                     or order_data_raw.get("order_items")
                     or [])

        if not raw_items:
            # Sandbox test orders sometimes have no items — add a default
            logger.warning("No items in Deliveroo payload — using default menu item")
            default_id = await find_menu_item_id("")
            if default_id:
                order_payload["items"].append({"menu_item_id": default_id, "quantity": 1})
        else:
            for item in raw_items:
                item_name = item.get("name") or item.get("title") or item.get("description", "")
                menu_item_id = await find_menu_item_id(item_name)
                if menu_item_id:
                    order_payload["items"].append({
                        "menu_item_id": menu_item_id,
                        "quantity": item.get("quantity", 1),
                        "special_requests": item.get("modifier_text", item.get("special_instructions", "")),
                    })
                else:
                    logger.warning(f"Could not match Deliveroo item: {item_name}")

        if not order_payload["items"]:
            logger.error("No items could be matched — order not created")
            raise Exception("No matching menu items found for Deliveroo order")

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{ORDER_SERVICE_URL}/api/v1/orders",
                json=order_payload,
                timeout=10.0,
            )
            if resp.status_code in (200, 201):
                created = resp.json()
                logger.info(f"Deliveroo order created: {created.get('order_number')}")
                await publish_order_notification(created)
                return created
            else:
                logger.error(f"Order creation failed: {resp.status_code} {resp.text}")
                raise Exception(f"Order service error: {resp.text}")

    except Exception as e:
        logger.error(f"Error processing Deliveroo order: {e}")
        raise


async def find_menu_item_id(item_name: str) -> str:
    """Match Deliveroo item name to our menu item ID (case-insensitive substring match)."""
    from .db import get_pool
    try:
        pool = await get_pool()
        row = await pool.fetchrow(
            "SELECT id FROM menu_items WHERE LOWER(name) LIKE '%' || LOWER($1) || '%' LIMIT 1",
            item_name,
        )
        if row:
            return str(row["id"])
    except Exception as e:
        logger.warning(f"Menu lookup failed for '{item_name}': {e}")

    # Fallback: return first available menu item
    try:
        pool = await get_pool()
        row = await pool.fetchrow("SELECT id FROM menu_items LIMIT 1")
        if row:
            return str(row["id"])
    except Exception:
        pass
    return None


async def publish_order_notification(order: Dict[str, Any]):
    """Publish to RabbitMQ so the POS WebSocket picks it up."""
    try:
        import aio_pika, json
        from datetime import datetime

        connection = await aio_pika.connect_robust(
            f"amqp://{os.getenv('RABBITMQ_USER','guest')}:{os.getenv('RABBITMQ_PASSWORD','guest')}"
            f"@{os.getenv('RABBITMQ_HOST','rabbitmq-service')}/"
        )
        async with connection:
            channel = await connection.channel()
            exchange = await channel.declare_exchange("orders", aio_pika.ExchangeType.TOPIC, durable=True)
            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps({
                        "event": "order.created",
                        "order_id": order.get("id"),
                        "order_number": order.get("order_number"),
                        "restaurant_id": order.get("restaurant_id"),
                        "order_type": order.get("order_type"),
                        "customer_name": order.get("customer_name"),
                        "total": order.get("total"),
                        "source": "deliveroo",
                    }).encode(),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=f"order.created.{order.get('restaurant_id')}",
            )
            logger.info(f"Published Deliveroo order notification: {order.get('order_number')}")
    except Exception as e:
        logger.error(f"Failed to publish notification: {e}")


async def handle_deliveroo_cancel(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Handle order_cancelled event."""
    order_id = payload.get("order", {}).get("id") or payload.get("order_id")
    logger.info(f"Deliveroo order cancelled: {order_id}")
    # TODO: look up internal order by deliveroo order id and cancel it
    return {"status": "cancelled", "deliveroo_order_id": order_id}


async def handle_deliveroo_status(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Handle order status update events."""
    order_id = payload.get("order", {}).get("id") or payload.get("order_id")
    status = payload.get("order", {}).get("status") or payload.get("status")
    logger.info(f"Deliveroo order status update: {order_id} → {status}")
    return {"status": "updated", "deliveroo_order_id": order_id, "new_status": status}
