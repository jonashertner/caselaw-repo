#!/usr/bin/env python3
"""Import decisions from Hugging Face dataset to SQLite.

This script is designed for HuggingFace Spaces deployment where we use SQLite
instead of PostgreSQL. It handles the import with SQLite-specific optimizations.

Usage:
    python scripts/import_huggingface_sqlite.py <repo-id> [output.db]

Example:
    python scripts/import_huggingface_sqlite.py voilaj/swiss-caselaw /app/data/swisslaw.db
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from datasets import load_dataset

BATCH_SIZE = 1000


def create_schema(conn: sqlite3.Connection) -> None:
    """Create the SQLite schema with FTS5 support."""
    cursor = conn.cursor()

    # Main decisions table
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

    # Indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_id ON decisions(source_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_level ON decisions(level)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_canton ON decisions(canton)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_decision_date ON decisions(decision_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_language ON decisions(language)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_docket ON decisions(docket)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_court ON decisions(court)")

    conn.commit()


def create_fts_index(conn: sqlite3.Connection) -> None:
    """Create FTS5 virtual table for full-text search."""
    cursor = conn.cursor()

    # Drop existing FTS table if exists (to rebuild)
    cursor.execute("DROP TABLE IF EXISTS decisions_fts")

    # Create FTS5 virtual table
    cursor.execute("""
        CREATE VIRTUAL TABLE decisions_fts USING fts5(
            id,
            content_text,
            title,
            docket,
            content='decisions',
            content_rowid='rowid'
        )
    """)

    # Populate FTS index
    print("Building full-text search index...")
    cursor.execute("""
        INSERT INTO decisions_fts(id, content_text, title, docket)
        SELECT id, content_text, title, docket FROM decisions
    """)

    conn.commit()


def import_from_huggingface(repo_id: str, db_path: str) -> None:
    """Import decisions from HuggingFace dataset to SQLite."""
    print(f"Importing from {repo_id} to {db_path}")

    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # Load dataset in streaming mode for memory efficiency
    print("Loading dataset (streaming mode)...")
    dataset = load_dataset(repo_id, split="train", streaming=True)

    # Connect to SQLite
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")

    # Create schema
    create_schema(conn)

    cursor = conn.cursor()

    # Get existing IDs to avoid duplicates
    existing_ids = set()
    try:
        cursor.execute("SELECT id FROM decisions")
        existing_ids = {row[0] for row in cursor.fetchall()}
        print(f"Existing decisions: {len(existing_ids)}")
    except sqlite3.OperationalError:
        pass  # Table doesn't exist yet

    insert_sql = """
        INSERT OR IGNORE INTO decisions (
            id, source_id, source_name, level, canton, court, chamber,
            docket, decision_date, published_date, title, language,
            url, pdf_url, content_text, content_hash, meta, indexed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    imported = 0
    skipped = 0
    batch = []

    print("Processing records...")
    for row in dataset:
        record_id = row.get("id")
        if record_id in existing_ids:
            skipped += 1
            continue

        values = (
            record_id,
            row.get("source_id", ""),
            row.get("source_name", ""),
            row.get("level", ""),
            row.get("canton") or None,
            row.get("court") or None,
            row.get("chamber") or None,
            row.get("docket") or None,
            row.get("decision_date") or None,
            row.get("published_date") or None,
            row.get("title") or None,
            row.get("language") or None,
            row.get("url", ""),
            row.get("pdf_url") or None,
            row.get("content_text") or "",
            None,  # content_hash
            None,  # meta
            None,  # indexed_at
            None,  # updated_at
        )
        batch.append(values)
        imported += 1

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            print(f"  Imported {imported} decisions...")
            batch = []

    # Insert remaining batch
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()

    print(f"Imported {imported} new decisions, skipped {skipped} existing")

    # Build FTS index
    create_fts_index(conn)

    # Optimize database
    print("Optimizing database...")
    conn.execute("VACUUM")
    conn.execute("ANALYZE")

    conn.close()

    # Report file size
    size_mb = Path(db_path).stat().st_size / (1024 * 1024)
    print(f"Done! Database size: {size_mb:.1f} MB")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <huggingface-repo-id> [output.db]")
        print(f"Example: {sys.argv[0]} voilaj/swiss-caselaw /app/data/swisslaw.db")
        sys.exit(1)

    repo_id = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "data/swisslaw.db"

    import_from_huggingface(repo_id, db_path)
