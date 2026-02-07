from __future__ import annotations

import logging
from sqlmodel import Session, create_engine

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

logger.info("session.py: connecting to %s", settings.database_url[:50])
engine = create_engine(
    settings.database_url,
    echo=settings.db_echo,
    pool_pre_ping=True,
)


def get_session() -> Session:
    return Session(engine)
