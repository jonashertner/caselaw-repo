from __future__ import annotations

import logging
import os
from sqlmodel import Session, create_engine

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# In parquet mode, we don't need a database connection
# Create a dummy engine that won't actually be used
_use_parquet = os.environ.get("USE_PARQUET_SEARCH", "").lower().strip() in ("1", "true", "yes")

if _use_parquet:
    # Use SQLite in-memory as a placeholder (won't actually be used)
    logger.info("session.py: PARQUET mode - using SQLite in-memory placeholder (USE_PARQUET_SEARCH=%s)",
                os.environ.get("USE_PARQUET_SEARCH", ""))
    engine = create_engine("sqlite:///:memory:", echo=False)
else:
    logger.info("session.py: DATABASE mode - connecting to %s", settings.database_url[:50])
    engine = create_engine(
        settings.database_url,
        echo=settings.db_echo,
        pool_pre_ping=True,
    )


def get_session() -> Session:
    return Session(engine)
