"""
Database connection for integration service using asyncpg
"""
import asyncpg
import os
import re
import logging

logger = logging.getLogger(__name__)

_pool = None


def _get_dsn() -> str:
    url = os.getenv("DATABASE_URL", "")
    # Strip sslmode query param — asyncpg doesn't accept it
    url = re.sub(r'\?sslmode=\w+', '', url)
    return url


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        dsn = _get_dsn()
        _pool = await asyncpg.create_pool(dsn, ssl=False, min_size=1, max_size=5)
        await _ensure_table(_pool)
    return _pool


async def _ensure_table(pool: asyncpg.Pool):
    """Create delivery_integrations table if it doesn't exist."""
    await pool.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'deliveryplatform') THEN
                CREATE TYPE deliveryplatform AS ENUM ('uber_eats', 'just_eat', 'deliveroo');
            END IF;
        END $$;

        CREATE TABLE IF NOT EXISTS delivery_integrations (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            restaurant_id   UUID NOT NULL,
            platform        deliveryplatform NOT NULL,
            external_store_id TEXT NOT NULL,
            is_active       BOOLEAN NOT NULL DEFAULT TRUE,
            webhook_url     TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (restaurant_id, platform)
        );
    """)
    logger.info("delivery_integrations table ready")
