"""
Integration Service - Uber Eats Integration
Handles webhooks and OAuth for third-party delivery platforms
"""
from fastapi import FastAPI, Request, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Optional
import logging
import hmac
import hashlib
import os
import secrets

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment
UBER_CLIENT_SECRET = os.getenv("UBER_CLIENT_SECRET", "m4a4DRDfkgtoPDTh9JVI0RHx-5J7nAJ9x4pntgSU")
WEBHOOK_USERNAME = os.getenv("WEBHOOK_USERNAME", "uber-webhook")
WEBHOOK_PASSWORD = os.getenv("WEBHOOK_PASSWORD", "secure-password-123")

# Basic Auth security
security = HTTPBasic(auto_error=False)

app = FastAPI(
    title="Integration Service",
    description="Third-party delivery platform integration",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "service": "Integration Service",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "integration-service"}

# Helper functions
def verify_basic_auth(credentials: Optional[HTTPBasicCredentials]) -> bool:
    """Verify Basic Authentication credentials"""
    if not credentials:
        return False

    correct_username = secrets.compare_digest(credentials.username, WEBHOOK_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, WEBHOOK_PASSWORD)

    return correct_username and correct_password

def verify_uber_signature(body: bytes, signature: str) -> bool:
    """Verify X-Uber-Signature header"""
    if not signature:
        return False

    # Calculate HMAC-SHA256 signature
    expected_signature = hmac.new(
        UBER_CLIENT_SECRET.encode(),
        body,
        hashlib.sha256
    ).hexdigest().lower()

    return secrets.compare_digest(signature.lower(), expected_signature)

# Uber Eats Webhook Endpoint
@app.post("/api/v1/webhooks/uber-eats")
async def uber_eats_webhook(
    request: Request,
    x_uber_signature: Optional[str] = Header(None),
    credentials: Optional[HTTPBasicCredentials] = Depends(security)
):
    """
    Receive webhooks from Uber Eats

    Events handled:
    - orders.notification: New order
    - orders.cancel: Order cancelled
    - orders.status_update: Status changed

    Authentication:
    - Optional Basic Auth: username/password
    - Uber Signature: X-Uber-Signature header (HMAC-SHA256)
    """
    try:
        # Get request body
        body = await request.body()
        logger.info(f"Received Uber Eats webhook: {body[:200]}")

        # Verify authentication (if Basic Auth is provided, validate it)
        if credentials and not verify_basic_auth(credentials):
            logger.warning("Invalid Basic Auth credentials")
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # Verify Uber signature if provided
        if x_uber_signature:
            if not verify_uber_signature(body, x_uber_signature):
                logger.warning("Invalid Uber signature")
                raise HTTPException(status_code=401, detail="Invalid signature")
            logger.info("Uber signature verified successfully")

        # Parse JSON
        import json
        payload = json.loads(body.decode())

        event_type = payload.get("event_type", "unknown")
        logger.info(f"Event type: {event_type}")

        # Import handler functions
        from .uber_handler import process_uber_order, handle_order_cancel, handle_status_update

        # Handle different event types
        if event_type == "orders.notification":
            # New order received
            logger.info("New Uber Eats order received")
            order = await process_uber_order(payload)
            return {
                "status": "success",
                "message": "Order created successfully",
                "order_number": order.get("order_number"),
                "order_id": order.get("id")
            }

        elif event_type == "orders.cancel":
            # Order cancelled
            logger.info("Uber Eats order cancelled")
            result = await handle_order_cancel(payload)
            return {"status": "success", "message": "Order cancelled", "data": result}

        elif event_type == "orders.status_update":
            # Status updated
            logger.info("Uber Eats order status updated")
            result = await handle_status_update(payload)
            return {"status": "success", "message": "Status updated", "data": result}

        return {"status": "success", "message": "Webhook processed"}

    except Exception as e:
        logger.error(f"Webhook error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# OAuth Callback Endpoint
@app.get("/api/v1/integrations/uber-eats/callback")
async def uber_oauth_callback(code: Optional[str] = None, error: Optional[str] = None):
    """
    Handle OAuth callback from Uber
    """
    if error:
        logger.error(f"OAuth error: {error}")
        return {"status": "error", "message": error}

    if code:
        logger.info(f"OAuth authorization code received: {code[:10]}...")
        # TODO: Exchange code for access token
        return {"status": "success", "message": "Authorization successful"}

    return {"status": "error", "message": "No code provided"}

# Test endpoint
@app.get("/api/v1/webhooks/uber-eats/test")
async def test_webhook():
    """Test endpoint to verify webhook is accessible"""
    return {
        "status": "ok",
        "message": "Webhook endpoint is accessible",
        "webhook_url": "https://restaurant.corpv3.com/api/v1/webhooks/uber-eats"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8015)
