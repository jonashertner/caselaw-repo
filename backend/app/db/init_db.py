from __future__ import annotations

import logging

from sqlalchemy import text
from sqlmodel import SQLModel

from app.db.session import engine
from app.core.config import get_settings

logger = logging.getLogger(__name__)


def _is_sqlite() -> bool:
    """Check if we're using SQLite."""
    settings = get_settings()
    return settings.database_url.startswith("sqlite")


def init_db() -> None:
    """Create database schema and extensions.

    For PostgreSQL: Creates pgvector extension and tables.
    For SQLite: Creates tables and FTS5 virtual table for search.
    """
    if _is_sqlite():
        _init_sqlite()
    else:
        _init_postgres()


def _init_postgres() -> None:
    """Initialize PostgreSQL database with pgvector."""
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        except Exception as e:
            logger.warning("Could not create pgvector extension: %s", e)

    SQLModel.metadata.create_all(engine)


def _init_sqlite() -> None:
    """Initialize SQLite database with FTS5."""
    # For SQLite, we need to create tables manually without pgvector-specific columns
    with engine.begin() as conn:
        # Create decisions table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                level TEXT NOT NULL,
                canton TEXT,
                court TEXT,
                chamber TEXT,
                docket TEXT,
                decision_date TEXT,
                published_date TEXT,
                title TEXT,
                language TEXT,
                url TEXT UNIQUE NOT NULL,
                pdf_url TEXT,
                content_text TEXT NOT NULL,
                content_hash TEXT,
                meta TEXT,
                indexed_at TEXT,
                updated_at TEXT
            )
        """))

        # Create indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_source_id ON decisions(source_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_level ON decisions(level)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_canton ON decisions(canton)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_decision_date ON decisions(decision_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_language ON decisions(language)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_docket ON decisions(docket)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_court ON decisions(court)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_url ON decisions(url)"))

        # Create FTS5 virtual table for full-text search
        conn.execute(text("""
            CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
                id,
                content_text,
                title,
                docket,
                content='decisions',
                content_rowid='rowid'
            )
        """))

        # Create ingestion_runs table (optional, for tracking)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scraper_name TEXT NOT NULL,
                source_id TEXT,
                started_at TEXT,
                completed_at TEXT,
                duration_seconds REAL,
                status TEXT DEFAULT 'running',
                decisions_found INTEGER DEFAULT 0,
                decisions_imported INTEGER DEFAULT 0,
                decisions_skipped INTEGER DEFAULT 0,
                decisions_updated INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                from_date TEXT,
                to_date TEXT,
                error_message TEXT,
                details TEXT
            )
        """))

    logger.info("SQLite database initialized with FTS5 support")
