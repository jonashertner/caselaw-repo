from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Keep in sync with pipeline schema (minus doc_id)
DECISION_COLS = [
    "id",
    "source_id",
    "source_name",
    "level",
    "canton",
    "court",
    "chamber",
    "language",
    "docket",
    "decision_date",
    "publication_date",
    "title",
    "url",
    "pdf_url",
    "content_text",
    "content_sha256",
    "fetched_at",
    "updated_at",
]


def connect(db_path: Path, read_only: bool = False) -> sqlite3.Connection:
    if read_only:
        uri = f"file:{db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    else:
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def apply_pragmas(conn: sqlite3.Connection, *, read_only: bool) -> None:
    if read_only:
        conn.execute("PRAGMA query_only=ON;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-200000;")  # ~200MB
        conn.execute("PRAGMA mmap_size=268435456;")  # 256MB
    else:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-200000;")


def ensure_schema(conn: sqlite3.Connection) -> None:
    # decisions table
    conn.execute(
        """
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
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_decision_date ON decisions(decision_date);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_source_id ON decisions(source_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_canton ON decisions(canton);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_language ON decisions(language);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_docket ON decisions(docket);")

    # FTS
    conn.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
          title,
          docket,
          content_text,
          content='decisions',
          content_rowid='doc_id',
          tokenize='unicode61 remove_diacritics 2',
          prefix='2 3 4'
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
          key TEXT PRIMARY KEY,
          value TEXT
        );
        """
    )

    # triggers
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name='decisions_ai';")
    if not cur.fetchone():
        conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
              INSERT INTO decisions_fts(rowid, title, docket, content_text)
              VALUES (new.doc_id, new.title, new.docket, new.content_text);
            END;

            CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
              INSERT INTO decisions_fts(decisions_fts, rowid, title, docket, content_text)
              VALUES('delete', old.doc_id, old.title, old.docket, old.content_text);
            END;

            CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
              INSERT INTO decisions_fts(decisions_fts, rowid, title, docket, content_text)
              VALUES('delete', old.doc_id, old.title, old.docket, old.content_text);
              INSERT INTO decisions_fts(rowid, title, docket, content_text)
              VALUES (new.doc_id, new.title, new.docket, new.content_text);
            END;
            """
        )
    conn.commit()


def apply_delta(snapshot_db: Path, delta_db: Path) -> None:
    conn = connect(snapshot_db, read_only=False)
    try:
        apply_pragmas(conn, read_only=False)
        ensure_schema(conn)

        conn.execute("ATTACH DATABASE ? AS delta", (str(delta_db),))
        cols = ",".join(DECISION_COLS)
        excluded_set = ",".join([f"{c}=excluded.{c}" for c in DECISION_COLS if c != "id"])
        conn.execute(
            f"""
            INSERT INTO decisions ({cols})
            SELECT {cols} FROM delta.decisions
            ON CONFLICT(id) DO UPDATE SET {excluded_set};
            """
        )
        conn.execute("DETACH DATABASE delta;")
        conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize');")
        conn.commit()
    finally:
        conn.close()


def stats(snapshot_db: Path) -> Dict[str, Any]:
    conn = connect(snapshot_db, read_only=True)
    try:
        apply_pragmas(conn, read_only=True)
        cur = conn.execute("SELECT COUNT(*) AS n FROM decisions;")
        n = int(cur.fetchone()["n"])
        cur = conn.execute("SELECT MAX(updated_at) AS last_update FROM decisions;")
        last_update = cur.fetchone()["last_update"]
        return {"count": n, "last_update": last_update}
    finally:
        conn.close()


def get_database_stats(conn: sqlite3.Connection, detailed: bool = False) -> Dict[str, Any]:
    """
    Get comprehensive database statistics for the stats dashboard.

    Args:
        conn: Database connection
        detailed: If True, return all data (for dashboard). If False, return summary.

    Returns:
        Dictionary with total_decisions, date_range, by_level, by_language, by_canton, by_year
    """
    # Total count
    total = conn.execute("SELECT COUNT(*) AS n FROM decisions;").fetchone()["n"]

    # Date range
    date_range_row = conn.execute(
        "SELECT MIN(decision_date) AS min_date, MAX(decision_date) AS max_date FROM decisions WHERE decision_date IS NOT NULL;"
    ).fetchone()
    date_range = {
        "min": date_range_row["min_date"],
        "max": date_range_row["max_date"]
    }

    # By level (federal/cantonal)
    by_level = []
    level_rows = conn.execute(
        """
        SELECT level, COUNT(*) AS count
        FROM decisions
        WHERE level IS NOT NULL
        GROUP BY level
        ORDER BY count DESC;
        """
    ).fetchall()
    for r in level_rows:
        pct = round((r["count"] / total) * 100, 1) if total > 0 else 0
        by_level.append({
            "level": r["level"],
            "count": r["count"],
            "percentage": pct
        })

    # By language with percentage
    by_language = []
    lang_rows = conn.execute(
        """
        SELECT language, COUNT(*) AS count
        FROM decisions
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY count DESC;
        """
    ).fetchall()
    for r in lang_rows:
        pct = round((r["count"] / total) * 100, 1) if total > 0 else 0
        by_language.append({
            "language": r["language"],
            "count": r["count"],
            "percentage": pct
        })

    # By canton - all for detailed, top 30 otherwise
    canton_limit = "" if detailed else "LIMIT 30"
    by_canton = []
    canton_rows = conn.execute(
        f"""
        SELECT canton, COUNT(*) AS count
        FROM decisions
        WHERE canton IS NOT NULL AND canton != ''
        GROUP BY canton
        ORDER BY count DESC
        {canton_limit};
        """
    ).fetchall()
    for r in canton_rows:
        pct = round((r["count"] / total) * 100, 1) if total > 0 else 0
        by_canton.append({
            "canton": r["canton"],
            "count": r["count"],
            "percentage": pct
        })

    # By year - all for detailed, top 20 otherwise
    year_limit = "" if detailed else "LIMIT 20"
    by_year = [
        dict(r) for r in conn.execute(
            f"""
            SELECT substr(decision_date, 1, 4) AS year, COUNT(*) AS count
            FROM decisions
            WHERE decision_date IS NOT NULL AND length(decision_date) >= 4
            GROUP BY year
            ORDER BY year DESC
            {year_limit};
            """
        ).fetchall()
    ]

    result = {
        "total_decisions": total,
        "date_range": date_range,
        "by_level": by_level,
        "by_language": by_language,
        "by_canton": by_canton,
        "by_year": by_year
    }

    # Additional detailed stats for dashboard
    if detailed:
        # By source_name (courts)
        by_source = []
        source_rows = conn.execute(
            """
            SELECT source_name, COUNT(*) AS count
            FROM decisions
            WHERE source_name IS NOT NULL AND source_name != ''
            GROUP BY source_name
            ORDER BY count DESC;
            """
        ).fetchall()
        for r in source_rows:
            pct = round((r["count"] / total) * 100, 1) if total > 0 else 0
            by_source.append({
                "source_name": r["source_name"],
                "count": r["count"],
                "percentage": pct
            })
        result["by_source"] = by_source

        # By court
        by_court = []
        court_rows = conn.execute(
            """
            SELECT court, COUNT(*) AS count
            FROM decisions
            WHERE court IS NOT NULL AND court != ''
            GROUP BY court
            ORDER BY count DESC
            LIMIT 50;
            """
        ).fetchall()
        for r in court_rows:
            pct = round((r["count"] / total) * 100, 1) if total > 0 else 0
            by_court.append({
                "court": r["court"],
                "count": r["count"],
                "percentage": pct
            })
        result["by_court"] = by_court

        # Recent decisions (last 30 days by decision_date)
        recent_by_day = [
            dict(r) for r in conn.execute(
                """
                SELECT decision_date AS date, COUNT(*) AS count
                FROM decisions
                WHERE decision_date IS NOT NULL
                  AND decision_date >= date('now', '-30 days')
                GROUP BY decision_date
                ORDER BY decision_date DESC;
                """
            ).fetchall()
        ]
        result["recent_by_day"] = recent_by_day

        # Database file info
        db_info = conn.execute("PRAGMA page_count;").fetchone()
        page_size = conn.execute("PRAGMA page_size;").fetchone()
        if db_info and page_size:
            db_size_bytes = db_info[0] * page_size[0]
            result["db_size_mb"] = round(db_size_bytes / (1024 * 1024), 1)

        # Last update timestamp
        last_update = conn.execute(
            "SELECT MAX(updated_at) AS last_update FROM decisions;"
        ).fetchone()
        result["last_update"] = last_update["last_update"] if last_update else None

    return result
