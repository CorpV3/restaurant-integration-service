"""
Uber Eats Order Handler
Processes Uber Eats webhook events and creates orders in the database
"""
import httpx
import os
from typing import Dict, Any
from datetime import datetime
from shared.utils.logger import setup_logger

logger = setup_logger("uber-handler")

# Order service URL
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8004")
RESTAURANT_ID = os.getenv("DEFAULT_RESTAURANT_ID", "6956017d-3aea-4ae2-9709-0ca0ac0a1a09")


async def process_uber_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process Uber Eats new order webhook

    Args:
        payload: Webhook payload from Uber Eats

    Returns:
        Created order details
    """
    try:
        # Extract order data from Uber payload
        # Note: Uber Eats webhook format may vary, adjust as needed
        uber_order = payload.get("order", {}) or payload.get("data", {})

        logger.info(f"Processing Uber order: {uber_order.get('id', 'unknown')}")

        # Map Uber Eats order to our order format
        order_data = {
            "restaurant_id": RESTAURANT_ID,
            "table_id": None,  # Uber orders don't have table_id
            "order_type": "ONLINE",  # Use ONLINE type for Uber Eats orders
            "customer_name": uber_order.get("eater", {}).get("first_name", "Uber Customer") + " " +
                           uber_order.get("eater", {}).get("last_name", ""),
            "customer_phone": uber_order.get("eater", {}).get("phone", ""),
            "customer_email": uber_order.get("eater", {}).get("email", ""),
            "delivery_address": format_delivery_address(uber_order.get("delivery", {})),
            "special_instructions": uber_order.get("special_instructions", ""),
            "items": []
        }

        # Process order items
        cart = uber_order.get("cart", {})
        items = cart.get("items", [])

        for uber_item in items:
            # Try to match Uber item with our menu items by name
            item_data = {
                "menu_item_id": await find_menu_item_id(uber_item.get("title", "")),
                "quantity": uber_item.get("quantity", 1),
                "special_requests": uber_item.get("special_instructions", "")
            }

            if item_data["menu_item_id"]:
                order_data["items"].append(item_data)
            else:
                logger.warning(f"Could not match Uber item: {uber_item.get('title')}")

        # Create order via order service API
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{ORDER_SERVICE_URL}/api/v1/orders",
                json=order_data,
                timeout=10.0
            )

            if response.status_code == 201:
                created_order = response.json()
                logger.info(f"Successfully created order {created_order.get('order_number')}")

                # Publish notification event
                await publish_order_notification(created_order)

                return created_order
            else:
                logger.error(f"Failed to create order: {response.status_code} - {response.text}")
                raise Exception(f"Order creation failed: {response.text}")

    except Exception as e:
        logger.error(f"Error processing Uber order: {str(e)}")
        raise


def format_delivery_address(delivery_data: Dict[str, Any]) -> str:
    """Format Uber delivery address into a single string"""
    if not delivery_data:
        return ""

    location = delivery_data.get("location", {})
    parts = [
        location.get("address_1", ""),
        location.get("address_2", ""),
        location.get("city", ""),
        location.get("state", ""),
        location.get("postal_code", "")
    ]

    return ", ".join([p for p in parts if p])


async def find_menu_item_id(item_name: str) -> str:
    """
    Find menu item ID by name
    For now, returns a default menu item ID
    TODO: Implement proper menu item matching logic
    """
    # Hardcoded menu item mapping for testing
    # In production, you should query the restaurant service to match items
    menu_mapping = {
        "biriyani": "aaf9ad8c-ee6a-4bb6-84f9-4f0f7cf3f11e",
        "biryani": "aaf9ad8c-ee6a-4bb6-84f9-4f0f7cf3f11e",
        "salad": "18386a1a-89a8-497d-b90b-02d0cf33b48a",
        "pizza": "aaf9ad8c-ee6a-4bb6-84f9-4f0f7cf3f11e",  # Default to biriyani for testing
    }

    item_key = item_name.lower().strip()
    return menu_mapping.get(item_key, menu_mapping["biriyani"])  # Default fallback


async def publish_order_notification(order: Dict[str, Any]):
    """
    Publish order notification to RabbitMQ
    This will be consumed by the order service WebSocket handler
    """
    try:
        import aio_pika
        import json

        RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq-service")
        RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
        RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(
            f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}/"
        )

        async with connection:
            channel = await connection.channel()

            # Declare exchange
            exchange = await channel.declare_exchange(
                "orders",
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )

            # Prepare notification message
            notification = {
                "event": "order.created",
                "order_id": order.get("id"),
                "order_number": order.get("order_number"),
                "restaurant_id": order.get("restaurant_id"),
                "order_type": order.get("order_type"),
                "customer_name": order.get("customer_name"),
                "total": order.get("total"),
                "created_at": order.get("created_at"),
                "timestamp": datetime.utcnow().isoformat()
            }

            # Publish message
            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps(notification).encode(),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=f"order.created.{order.get('restaurant_id')}"
            )

            logger.info(f"Published order notification for {order.get('order_number')}")

    except Exception as e:
        logger.error(f"Failed to publish notification: {str(e)}")
        # Don't raise - order was created successfully, notification is secondary


async def handle_order_cancel(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Handle Uber Eats order cancellation"""
    try:
        uber_order_id = payload.get("order_id") or payload.get("data", {}).get("id")

        # TODO: Find order by Uber order ID and cancel it
        # For now, just log it
        logger.info(f"Uber order cancelled: {uber_order_id}")

        return {"status": "cancelled", "uber_order_id": uber_order_id}

    except Exception as e:
        logger.error(f"Error handling order cancellation: {str(e)}")
        raise


async def handle_status_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Handle Uber Eats order status update"""
    try:
        uber_order_id = payload.get("order_id") or payload.get("data", {}).get("id")
        new_status = payload.get("status") or payload.get("data", {}).get("status")

        # TODO: Find order by Uber order ID and update status
        logger.info(f"Uber order status updated: {uber_order_id} -> {new_status}")

        return {"status": "updated", "uber_order_id": uber_order_id, "new_status": new_status}

    except Exception as e:
        logger.error(f"Error handling status update: {str(e)}")
        raise
