#!/usr/bin/env python3
"""
Rebuild full snapshot from source database and publish everywhere.

Usage:
    python rebuild_full_snapshot.py
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Paths
SOURCE_DB = Path("/Users/jonashertner/swiss-caselaw/backend/scripts/data/swisslaw.db")
BUILD_DIR = Path("/Users/jonashertner/caselaw-repo/_build")
LOCAL_APP_DATA = Path.home() / "Library/Application Support/swiss-caselaw"

# Week label
WEEK = datetime.now().strftime("%G-W%V")


def create_snapshot_schema(conn: sqlite3.Connection) -> None:
    """Create the snapshot database schema."""
    conn.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        PRAGMA temp_store=MEMORY;
        PRAGMA cache_size=-200000;
        PRAGMA page_size=4096;

        CREATE TABLE IF NOT EXISTS decisions (
            doc_id INTEGER PRIMARY KEY,
            id TEXT NOT NULL UNIQUE,
            source_id TEXT,
            source_name TEXT,
            level TEXT,
            canton TEXT,
            court TEXT,
            chamber TEXT,
            language TEXT,
            docket TEXT,
            decision_date TEXT,
            publication_date TEXT,
            title TEXT,
            url TEXT,
            pdf_url TEXT,
            content_text TEXT,
            content_sha256 TEXT,
            fetched_at TEXT,
            updated_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_decisions_id ON decisions(id);
        CREATE INDEX IF NOT EXISTS idx_decisions_decision_date ON decisions(decision_date);
        CREATE INDEX IF NOT EXISTS idx_decisions_source_id ON decisions(source_id);
        CREATE INDEX IF NOT EXISTS idx_decisions_canton ON decisions(canton);
        CREATE INDEX IF NOT EXISTS idx_decisions_language ON decisions(language);
        CREATE INDEX IF NOT EXISTS idx_decisions_level ON decisions(level);
        CREATE INDEX IF NOT EXISTS idx_decisions_docket ON decisions(docket);

        CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
            title,
            docket,
            content_text,
            content='decisions',
            content_rowid='doc_id',
            tokenize='unicode61 remove_diacritics 2',
            prefix='2 3 4'
        );

        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    conn.commit()


def copy_decisions(source_conn: sqlite3.Connection, dest_conn: sqlite3.Connection) -> int:
    """Copy all decisions from source to destination with schema mapping."""
    source_conn.row_factory = sqlite3.Row

    # Count source
    total = source_conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    log.info(f"Source has {total:,} decisions")

    # Read and insert in batches
    BATCH_SIZE = 10000
    inserted = 0

    cursor = source_conn.execute("""
        SELECT id, source_id, source_name, level, canton, court, chamber,
               docket, decision_date, published_date, title, language,
               url, pdf_url, content_text, content_hash, indexed_at, updated_at
        FROM decisions
        ORDER BY id
    """)

    batch = []
    for row in cursor:
        batch.append((
            row["id"],
            row["source_id"],
            row["source_name"],
            row["level"],
            row["canton"],
            row["court"],
            row["chamber"],
            row["language"],
            row["docket"],
            row["decision_date"],
            row["published_date"],  # maps to publication_date
            row["title"],
            row["url"],
            row["pdf_url"],
            row["content_text"],
            row["content_hash"],  # maps to content_sha256
            row["indexed_at"],    # maps to fetched_at
            row["updated_at"],
        ))

        if len(batch) >= BATCH_SIZE:
            dest_conn.executemany("""
                INSERT INTO decisions (
                    id, source_id, source_name, level, canton, court, chamber,
                    language, docket, decision_date, publication_date, title,
                    url, pdf_url, content_text, content_sha256, fetched_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            dest_conn.commit()
            inserted += len(batch)
            log.info(f"  Inserted {inserted:,} / {total:,} ({100*inserted//total}%)")
            batch = []

    # Final batch
    if batch:
        dest_conn.executemany("""
            INSERT INTO decisions (
                id, source_id, source_name, level, canton, court, chamber,
                language, docket, decision_date, publication_date, title,
                url, pdf_url, content_text, content_sha256, fetched_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        dest_conn.commit()
        inserted += len(batch)

    log.info(f"Inserted {inserted:,} decisions total")
    return inserted


def build_fts_index(conn: sqlite3.Connection) -> None:
    """Populate FTS index from decisions table."""
    log.info("Building FTS index...")
    conn.execute("""
        INSERT INTO decisions_fts(rowid, title, docket, content_text)
        SELECT doc_id, title, docket, content_text FROM decisions
    """)
    conn.commit()
    log.info("FTS index built")


def optimize_db(conn: sqlite3.Connection) -> None:
    """Optimize the database."""
    log.info("Optimizing FTS...")
    conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize')")
    conn.commit()

    log.info("Running VACUUM...")
    conn.execute("VACUUM")
    conn.commit()

    log.info("Analyzing...")
    conn.execute("ANALYZE")
    conn.commit()


def compress_zstd(input_path: Path, output_path: Path, level: int = 10) -> None:
    """Compress file with zstd."""
    import subprocess
    log.info(f"Compressing {input_path.name} with zstd level {level}...")
    subprocess.run(
        ["zstd", f"-{level}", "-f", str(input_path), "-o", str(output_path)],
        check=True
    )
    log.info(f"Compressed to {output_path.name} ({output_path.stat().st_size / 1e9:.2f} GB)")


def main():
    log.info("=" * 60)
    log.info("REBUILDING FULL SNAPSHOT FROM SOURCE DATABASE")
    log.info("=" * 60)

    # Check source exists
    if not SOURCE_DB.exists():
        raise SystemExit(f"Source database not found: {SOURCE_DB}")

    # Create build directory
    snapshot_dir = BUILD_DIR / "snapshot" / WEEK
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)
    snapshot_dir.mkdir(parents=True)

    snapshot_path = snapshot_dir / f"swiss-caselaw-{WEEK}.sqlite"

    log.info(f"Source: {SOURCE_DB}")
    log.info(f"Output: {snapshot_path}")
    log.info(f"Week: {WEEK}")

    # Open source
    source_conn = sqlite3.connect(str(SOURCE_DB))
    source_conn.row_factory = sqlite3.Row

    # Create destination
    dest_conn = sqlite3.connect(str(snapshot_path))

    try:
        # Create schema
        log.info("Creating schema...")
        create_snapshot_schema(dest_conn)

        # Copy decisions
        count = copy_decisions(source_conn, dest_conn)

        # Build FTS
        build_fts_index(dest_conn)

        # Optimize
        optimize_db(dest_conn)

        # Store meta
        dest_conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("generated_at", datetime.utcnow().isoformat() + "Z")
        )
        dest_conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("decisions_count", str(count))
        )
        dest_conn.commit()

    finally:
        source_conn.close()
        dest_conn.close()

    log.info(f"Snapshot created: {snapshot_path}")
    log.info(f"Size: {snapshot_path.stat().st_size / 1e9:.2f} GB")

    # Compress
    snapshot_zst = snapshot_dir / f"swiss-caselaw-{WEEK}.sqlite.zst"
    compress_zstd(snapshot_path, snapshot_zst, level=10)

    # Copy to local app
    log.info("Copying to local app data directory...")
    LOCAL_APP_DATA.mkdir(parents=True, exist_ok=True)
    local_db = LOCAL_APP_DATA / "caselaw.sqlite"

    # Remove old files
    for f in LOCAL_APP_DATA.glob("caselaw.sqlite*"):
        f.unlink()

    shutil.copy2(snapshot_path, local_db)
    log.info(f"Copied to {local_db}")

    # Update local state
    state = {
        "snapshot_week": WEEK,
        "applied_deltas": [],
        "remote_generated_at": datetime.utcnow().isoformat() + "Z"
    }
    (LOCAL_APP_DATA / "state.json").write_text(json.dumps(state))

    # Verify
    verify_conn = sqlite3.connect(str(local_db))
    final_count = verify_conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    verify_conn.close()

    log.info("=" * 60)
    log.info(f"SUCCESS: {final_count:,} decisions now available locally")
    log.info("=" * 60)

    # Print manifest update info
    import hashlib
    sha256 = hashlib.sha256(snapshot_zst.read_bytes()).hexdigest()

    print("\n" + "=" * 60)
    print("MANIFEST UPDATE (for HuggingFace):")
    print("=" * 60)
    print(json.dumps({
        "schema": "swiss-caselaw-artifacts-v1",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "snapshot": {
            "week": WEEK,
            "sqlite_zst": {
                "path": f"artifacts/sqlite/snapshots/swiss-caselaw-{WEEK}.sqlite.zst",
                "sha256": sha256,
                "bytes": snapshot_zst.stat().st_size,
                "decisions_count": count
            },
            "parquet": None
        },
        "deltas": []
    }, indent=2))

    print(f"\nSnapshot file to upload: {snapshot_zst}")


if __name__ == "__main__":
    main()
