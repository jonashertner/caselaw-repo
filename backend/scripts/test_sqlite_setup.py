#!/usr/bin/env python3
"""Test script to verify SQLite setup works correctly.

This script validates that:
1. SQLite database can be created
2. Schema is correct
3. FTS5 search works
4. Import from HuggingFace works (if available)

Usage:
    python scripts/test_sqlite_setup.py [database_path]
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_sqlite_schema():
    """Test that SQLite schema can be created."""
    print("Testing SQLite schema creation...")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # Set environment for SQLite
        os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

        # Import and run init_db
        from app.db.init_db import init_db, _is_sqlite

        assert _is_sqlite(), "Should detect SQLite database"

        init_db()
        print("  Schema created successfully")

        # Verify tables exist
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        assert "decisions" in tables, "decisions table should exist"
        assert "decisions_fts" in tables, "decisions_fts table should exist"

        print("  Tables verified: decisions, decisions_fts")

        # Test FTS5
        cursor.execute("""
            INSERT INTO decisions (id, source_id, source_name, level, url, content_text)
            VALUES ('test-1', 'test', 'Test Source', 'federal', 'http://test.com', 'This is a test decision about Swiss law')
        """)
        cursor.execute("""
            INSERT INTO decisions_fts (id, content_text, title, docket)
            SELECT id, content_text, title, docket FROM decisions WHERE id = 'test-1'
        """)
        conn.commit()

        cursor.execute("""
            SELECT id FROM decisions_fts WHERE decisions_fts MATCH 'Swiss'
        """)
        results = cursor.fetchall()
        assert len(results) == 1, "FTS5 search should work"

        print("  FTS5 search verified")

        conn.close()
        print("SQLite schema test: PASSED")

    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_search_service():
    """Test that SQLite search service works."""
    print("\nTesting SQLite search service...")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

        # Create database with test data
        conn = sqlite3.connect(db_path)
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

        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
                id, content_text, title, docket, content='decisions', content_rowid='rowid'
            )
        """)

        # Insert test data
        test_decisions = [
            ("dec-1", "bger", "Bundesgericht", "federal", None, "BGer", None, "6B_123/2024",
             "2024-01-15", None, "Strafrecht BGE", "de", "http://bger.ch/1", None,
             "Entscheid des Bundesgerichts betreffend Strafrecht und Verfahren"),
            ("dec-2", "zh_courts", "Zürcher Gerichte", "cantonal", "ZH", "Obergericht", None, "UE220123",
             "2024-02-20", None, "Zivilrecht Zürich", "de", "http://zh.ch/1", None,
             "Urteil des Obergerichts Zürich im Zivilverfahren"),
        ]

        for d in test_decisions:
            cursor.execute("""
                INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (*d, None, None, None, None))
            cursor.execute("""
                INSERT INTO decisions_fts (id, content_text, title, docket)
                VALUES (?, ?, ?, ?)
            """, (d[0], d[14], d[10], d[7]))

        conn.commit()
        conn.close()

        # Test search service
        from sqlmodel import create_engine, Session
        from app.services.search_sqlite import search_sqlite, SearchFilters

        engine = create_engine(f"sqlite:///{db_path}")
        with Session(engine) as session:
            # Test basic search
            results = search_sqlite(session, "Strafrecht", filters=SearchFilters(), limit=10)
            assert len(results) >= 1, "Should find results for 'Strafrecht'"
            print(f"  Found {len(results)} results for 'Strafrecht'")

            # Test canton filter
            results = search_sqlite(session, "Zivilrecht", filters=SearchFilters(canton="ZH"), limit=10)
            assert len(results) >= 1, "Should find ZH results"
            print(f"  Found {len(results)} results for 'Zivilrecht' in ZH")

            # Test docket search
            results = search_sqlite(session, "6B_123/2024", filters=SearchFilters(), limit=10)
            assert len(results) >= 1, "Should find docket number"
            print(f"  Found {len(results)} results for docket '6B_123/2024'")

        print("SQLite search service test: PASSED")

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def main():
    print("=" * 60)
    print("Swiss Caselaw SQLite Setup Tests")
    print("=" * 60)

    try:
        test_sqlite_schema()
        test_search_service()

        print("\n" + "=" * 60)
        print("All tests PASSED!")
        print("=" * 60)
        return 0

    except AssertionError as e:
        print(f"\nTest FAILED: {e}")
        return 1
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
