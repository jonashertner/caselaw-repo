#!/usr/bin/env python3
"""Import decisions from SQLite database to PostgreSQL.

This script imports decisions from a SQLite database (exported by export_sqlite.py)
into the PostgreSQL database. It handles deduplication via content_hash.

Usage:
    python scripts/import_sqlite.py <sqlite_path>

Example:
    python scripts/import_sqlite.py data/baseline.db
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select
from app.db.session import get_session
from app.models.decision import Decision

BATCH_SIZE = 1000


def import_from_sqlite(sqlite_path: str) -> int:
    """Import decisions from SQLite to PostgreSQL.

    Args:
        sqlite_path: Path to the SQLite database

    Returns:
        Number of decisions imported (new + updated)
    """
    sqlite_path = Path(sqlite_path)
    if not sqlite_path.exists():
        print(f"SQLite database not found: {sqlite_path}")
        return 0

    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Count total rows
    cursor.execute("SELECT COUNT(*) FROM decisions")
    total = cursor.fetchone()[0]
    print(f"Found {total:,} decisions in {sqlite_path}")

    if total == 0:
        conn.close()
        return 0

    # Get existing content hashes from PostgreSQL
    print("Loading existing content hashes...")
    with get_session() as session:
        existing_hashes = set(
            session.exec(
                select(Decision.content_hash).where(Decision.content_hash.isnot(None))
            ).all()
        )
        existing_ids = set(session.exec(select(Decision.id)).all())
    print(f"Found {len(existing_hashes):,} existing hashes, {len(existing_ids):,} existing IDs")

    # Iterate through SQLite rows
    cursor.execute("""
        SELECT id, source_id, source_name, level, canton, court, chamber,
               docket, decision_date, published_date, title, language,
               url, pdf_url, content_text, content_hash, meta, indexed_at, updated_at
        FROM decisions
    """)

    imported = 0
    skipped = 0
    batch = []

    for row in cursor:
        row = dict(row)

        # Skip if we already have this content
        content_hash = row.get("content_hash")
        if content_hash and content_hash in existing_hashes:
            skipped += 1
            continue

        # Skip if we already have this ID
        if row["id"] in existing_ids:
            skipped += 1
            continue

        # Parse dates
        decision_date = None
        if row.get("decision_date"):
            try:
                decision_date = dt.date.fromisoformat(row["decision_date"])
            except (ValueError, TypeError):
                pass

        published_date = None
        if row.get("published_date"):
            try:
                published_date = dt.date.fromisoformat(row["published_date"])
            except (ValueError, TypeError):
                pass

        indexed_at = None
        if row.get("indexed_at"):
            try:
                indexed_at = dt.datetime.fromisoformat(row["indexed_at"])
            except (ValueError, TypeError):
                indexed_at = dt.datetime.now(dt.timezone.utc)

        updated_at = None
        if row.get("updated_at"):
            try:
                updated_at = dt.datetime.fromisoformat(row["updated_at"])
            except (ValueError, TypeError):
                pass

        # Parse meta JSON
        meta = None
        if row.get("meta"):
            try:
                meta = json.loads(row["meta"])
            except (json.JSONDecodeError, TypeError):
                pass

        decision = Decision(
            id=row["id"],
            source_id=row["source_id"],
            source_name=row["source_name"],
            level=row["level"],
            canton=row.get("canton"),
            court=row.get("court"),
            chamber=row.get("chamber"),
            docket=row.get("docket"),
            decision_date=decision_date,
            published_date=published_date,
            title=row.get("title"),
            language=row.get("language"),
            url=row["url"],
            pdf_url=row.get("pdf_url"),
            content_text=row["content_text"],
            content_hash=content_hash,
            meta=meta,
            indexed_at=indexed_at,
            updated_at=updated_at,
        )

        batch.append(decision)

        # Track for deduplication within this import
        if content_hash:
            existing_hashes.add(content_hash)
        existing_ids.add(row["id"])

        if len(batch) >= BATCH_SIZE:
            with get_session() as session:
                for d in batch:
                    session.add(d)
                session.commit()
            imported += len(batch)
            print(f"  Imported {imported:,} decisions (skipped {skipped:,})...")
            batch = []

    # Insert remaining
    if batch:
        with get_session() as session:
            for d in batch:
                session.add(d)
            session.commit()
        imported += len(batch)

    conn.close()

    print(f"\nDone! Imported {imported:,} new decisions (skipped {skipped:,} duplicates)")
    return imported


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/import_sqlite.py <sqlite_path>")
        sys.exit(1)

    sqlite_path = sys.argv[1]
    import_from_sqlite(sqlite_path)
