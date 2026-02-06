#!/usr/bin/env python3
"""Export PostgreSQL database to SQLite for HuggingFace Spaces deployment.

This script exports the decisions table from PostgreSQL to a SQLite database,
which is simpler to deploy on HuggingFace Spaces (no pgvector needed).

Usage:
    python scripts/export_sqlite.py [output_path]

Example:
    python scripts/export_sqlite.py /app/data/swisslaw.db
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Generator

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func

from app.db.session import get_session
from app.models.decision import Decision

BATCH_SIZE = 5000


def create_sqlite_schema(conn: sqlite3.Connection) -> None:
    """Create the decisions table in SQLite."""
    cursor = conn.cursor()

    cursor.execute("""
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
    """)

    # Create indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_id ON decisions(source_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_level ON decisions(level)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_canton ON decisions(canton)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_decision_date ON decisions(decision_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_language ON decisions(language)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_docket ON decisions(docket)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_court ON decisions(court)")

    # Create FTS virtual table for full-text search
    cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
            id,
            content_text,
            title,
            docket,
            content='decisions',
            content_rowid='rowid'
        )
    """)

    conn.commit()


def generate_decisions() -> Generator[Decision, None, None]:
    """Generate all decisions from PostgreSQL in batches."""
    with get_session() as session:
        total = session.exec(select(func.count(Decision.id))).one()
        print(f"Total decisions to export: {total}")

        offset = 0
        while offset < total:
            print(f"  Loading batch {offset}-{offset + BATCH_SIZE}...")
            # IMPORTANT: ORDER BY id ensures consistent pagination
            decisions = session.exec(
                select(Decision).order_by(Decision.id).offset(offset).limit(BATCH_SIZE)
            ).all()

            for d in decisions:
                yield d

            offset += BATCH_SIZE
            session.expire_all()


def export_to_sqlite(output_path: str) -> None:
    """Export all decisions from PostgreSQL to SQLite."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing database if exists
    if output_path.exists():
        output_path.unlink()

    conn = sqlite3.connect(str(output_path))

    print(f"Creating SQLite database at {output_path}...")
    create_sqlite_schema(conn)

    cursor = conn.cursor()
    count = 0

    # Use INSERT OR REPLACE to handle duplicate URLs gracefully
    insert_sql = """
        INSERT OR REPLACE INTO decisions (
            id, source_id, source_name, level, canton, court, chamber,
            docket, decision_date, published_date, title, language,
            url, pdf_url, content_text, content_hash, meta, indexed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch = []
    for d in generate_decisions():
        row = (
            d.id,
            d.source_id,
            d.source_name,
            d.level,
            d.canton,
            d.court,
            d.chamber,
            d.docket,
            str(d.decision_date) if d.decision_date else None,
            str(d.published_date) if d.published_date else None,
            d.title,
            d.language,
            d.url,
            d.pdf_url,
            d.content_text,
            d.content_hash,
            json.dumps(d.meta) if d.meta else None,
            d.indexed_at.isoformat() if d.indexed_at else None,
            d.updated_at.isoformat() if d.updated_at else None,
        )
        batch.append(row)
        count += 1

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            batch = []
            print(f"  Inserted {count} decisions...")

    # Insert remaining
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()

    # Populate FTS index
    print("Building full-text search index...")
    cursor.execute("""
        INSERT INTO decisions_fts(id, content_text, title, docket)
        SELECT id, content_text, title, docket FROM decisions
    """)
    conn.commit()

    # Optimize
    print("Optimizing database...")
    cursor.execute("VACUUM")
    cursor.execute("ANALYZE")
    conn.commit()

    conn.close()

    # Get file size
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\nDone! Exported {count} decisions to {output_path}")
    print(f"Database size: {size_mb:.1f} MB")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        output = sys.argv[1]
    else:
        output = "data/swisslaw.db"

    export_to_sqlite(output)
