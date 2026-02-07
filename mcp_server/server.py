#!/usr/bin/env python3
"""Swiss Caselaw MCP Server - Legal research tools for Claude Code.

Provides tools to search and analyze Swiss court decisions.
Supports PostgreSQL (via DATABASE_URL) and SQLite backends.

Usage:
    # With SQLite (auto-detected or via env var)
    python mcp_server/server.py

    # With PostgreSQL
    DATABASE_URL=postgresql://... python mcp_server/server.py

    # Auto-download from HuggingFace
    python mcp_server/server.py --huggingface

Configuration in ~/.claude/settings.json:
    {
      "mcpServers": {
        "swiss-caselaw": {
          "command": "python3",
          "args": ["/path/to/caselaw-repo/mcp_server/server.py"]
        }
      }
    }
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print("Error: mcp package not installed. Run: pip install mcp", file=sys.stderr)
    sys.exit(1)

# Initialize MCP server
mcp = FastMCP("swiss-caselaw")

# ---------------------------------------------------------------------------
# Database connection layer
# ---------------------------------------------------------------------------

_db_conn: Any = None
_db_type: str = "none"  # "sqlite" | "postgresql"
_sqlite_schema: str = "pipeline"  # "pipeline" (doc_id rowid) | "export" (fts id)

# Swiss canton codes
CANTON_CODES = {
    "AG": "Aargau", "AI": "Appenzell Innerrhoden", "AR": "Appenzell Ausserrhoden",
    "BE": "Bern", "BL": "Basel-Landschaft", "BS": "Basel-Stadt",
    "FR": "Fribourg", "GE": "Geneve", "GL": "Glarus", "GR": "Graubunden",
    "JU": "Jura", "LU": "Luzern", "NE": "Neuchatel", "NW": "Nidwalden",
    "OW": "Obwalden", "SG": "St. Gallen", "SH": "Schaffhausen", "SO": "Solothurn",
    "SZ": "Schwyz", "TG": "Thurgau", "TI": "Ticino", "UR": "Uri",
    "VD": "Vaud", "VS": "Valais", "ZG": "Zug", "ZH": "Zurich",
}

# Docket number patterns
DOCKET_PATTERNS = [
    re.compile(r'\b\d[A-Z]_\d+/\d{4}\b', re.IGNORECASE),
    re.compile(r'\b[A-Z]-\d+/\d{4}\b', re.IGNORECASE),
    re.compile(r'\bBGE\s+\d+\s+[IVX]+\s+\d+\b', re.IGNORECASE),
]


def _detect_sqlite_schema(conn: sqlite3.Connection) -> str:
    """Detect whether SQLite DB uses pipeline schema (doc_id) or export schema."""
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(decisions)").fetchall()]
        if "doc_id" in cols:
            return "pipeline"
        return "export"
    except Exception:
        return "pipeline"


def _fts_join(schema: str) -> str:
    """Return the correct FTS JOIN clause based on schema."""
    if schema == "pipeline":
        return "JOIN decisions d ON d.doc_id = decisions_fts.rowid"
    return "JOIN decisions d ON decisions_fts.id = d.id"


def _download_from_huggingface() -> Optional[str]:
    """Download SQLite database from HuggingFace."""
    try:
        from huggingface_hub import hf_hub_download
        print("Downloading database from HuggingFace...", file=sys.stderr)
        path = hf_hub_download(
            repo_id="voilaj/swiss-caselaw",
            filename="swisslaw.db",
            repo_type="dataset",
        )
        print(f"Downloaded to: {path}", file=sys.stderr)
        return path
    except Exception as e:
        print(f"Failed to download from HuggingFace: {e}", file=sys.stderr)
        return None


def get_db() -> tuple[Any, str]:
    """Get database connection, initializing if needed.

    Returns (connection, db_type) where db_type is 'sqlite' or 'postgresql'.
    """
    global _db_conn, _db_type, _sqlite_schema

    if _db_conn is not None:
        return _db_conn, _db_type

    # 1. DATABASE_URL -> PostgreSQL
    database_url = os.environ.get("DATABASE_URL")
    if database_url and database_url.startswith("postgresql"):
        try:
            from sqlmodel import create_engine
            engine = create_engine(database_url)
            _db_conn = engine
            _db_type = "postgresql"
            return _db_conn, _db_type
        except Exception as e:
            print(f"PostgreSQL connection failed: {e}", file=sys.stderr)

    # 2. SQLITE_PATH or CASELAW_DB_PATH
    for env_key in ("SQLITE_PATH", "CASELAW_DB_PATH"):
        sqlite_path = os.environ.get(env_key)
        if sqlite_path and Path(sqlite_path).exists():
            _db_conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
            _db_conn.row_factory = sqlite3.Row
            _db_type = "sqlite"
            _sqlite_schema = _detect_sqlite_schema(_db_conn)
            return _db_conn, _db_type

    # 3. Default paths
    default_paths = [
        Path(__file__).parent.parent / "data" / "swisslaw.db",
        Path.home() / "Library" / "Application Support" / "swiss-caselaw" / "caselaw.sqlite",
        Path.home() / ".local" / "share" / "swiss-caselaw" / "caselaw.sqlite",
        Path("caselaw.sqlite"),
    ]
    for p in default_paths:
        if p.exists() and p.stat().st_size > 0:
            _db_conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
            _db_conn.row_factory = sqlite3.Row
            _db_type = "sqlite"
            _sqlite_schema = _detect_sqlite_schema(_db_conn)
            return _db_conn, _db_type

    # 4. --huggingface flag
    if "--huggingface" in sys.argv or os.environ.get("USE_HUGGINGFACE"):
        path = _download_from_huggingface()
        if path:
            _db_conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            _db_conn.row_factory = sqlite3.Row
            _db_type = "sqlite"
            _sqlite_schema = _detect_sqlite_schema(_db_conn)
            return _db_conn, _db_type

    raise RuntimeError(
        "No database found. Set DATABASE_URL (PostgreSQL), SQLITE_PATH / CASELAW_DB_PATH "
        "(SQLite), use --huggingface, or install via: pip install caselaw-local && caselaw-local update"
    )


def _is_docket_number(query: str) -> bool:
    return any(p.search(query) for p in DOCKET_PATTERNS)


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def _pg_search(query: str, filters: dict, limit: int) -> list[dict]:
    from sqlmodel import Session, text
    db, _ = get_db()
    with Session(db) as session:
        conditions = []
        params: dict[str, Any] = {"query": query, "limit": limit}

        for key, col in [("canton", "canton"), ("level", "level"),
                         ("language", "language"), ("date_from", "decision_date"),
                         ("date_to", "decision_date")]:
            if filters.get(key):
                op = ">=" if key == "date_from" else ("<=" if key == "date_to" else "=")
                conditions.append(f"{col} {op} :{key}")
                params[key] = filters[key]

        filter_sql = " AND ".join(conditions) if conditions else "TRUE"

        if _is_docket_number(query):
            sql = text(f"""
                SELECT id, source_id, source_name, level, canton, court, docket,
                       decision_date, title, language, url, pdf_url,
                       substr(content_text, 1, 2000) as content_preview
                FROM decisions
                WHERE (docket ILIKE :docket_pattern OR title ILIKE :docket_pattern)
                AND {filter_sql}
                ORDER BY decision_date DESC NULLS LAST
                LIMIT :limit
            """)
            params["docket_pattern"] = f"%{query}%"
        else:
            sql = text(f"""
                SELECT id, source_id, source_name, level, canton, court, docket,
                       decision_date, title, language, url, pdf_url,
                       substr(content_text, 1, 2000) as content_preview,
                       ts_rank_cd(
                           setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
                           setweight(to_tsvector('simple', coalesce(docket, '')), 'A') ||
                           to_tsvector('simple', substr(content_text, 1, 50000)),
                           websearch_to_tsquery('simple', :query)
                       ) as rank
                FROM decisions
                WHERE (
                    setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
                    setweight(to_tsvector('simple', coalesce(docket, '')), 'A') ||
                    to_tsvector('simple', substr(content_text, 1, 50000))
                ) @@ websearch_to_tsquery('simple', :query)
                AND {filter_sql}
                ORDER BY rank DESC, decision_date DESC NULLS LAST
                LIMIT :limit
            """)

        rows = session.execute(sql, params).fetchall()
        return [
            {
                "id": r.id, "source_id": r.source_id, "source_name": r.source_name,
                "level": r.level, "canton": r.canton, "court": r.court,
                "docket": r.docket, "decision_date": str(r.decision_date) if r.decision_date else None,
                "title": r.title, "language": r.language, "url": r.url,
                "content_preview": (r.content_preview[:1000] if r.content_preview else None),
            }
            for r in rows
        ]


def _pg_count(query: str, filters: dict) -> int:
    from sqlmodel import Session, text
    db, _ = get_db()
    with Session(db) as session:
        conditions = []
        params: dict[str, Any] = {"query": query}
        for key, col in [("canton", "canton"), ("level", "level"),
                         ("language", "language"), ("date_from", "decision_date"),
                         ("date_to", "decision_date")]:
            if filters.get(key):
                op = ">=" if key == "date_from" else ("<=" if key == "date_to" else "=")
                conditions.append(f"{col} {op} :{key}")
                params[key] = filters[key]
        filter_sql = " AND ".join(conditions) if conditions else "TRUE"

        if query.strip():
            sql = text(f"""
                SELECT COUNT(*) FROM decisions
                WHERE (
                    setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
                    setweight(to_tsvector('simple', coalesce(docket, '')), 'A') ||
                    to_tsvector('simple', substr(content_text, 1, 50000))
                ) @@ websearch_to_tsquery('simple', :query)
                AND {filter_sql}
            """)
        else:
            sql = text(f"SELECT COUNT(*) FROM decisions WHERE {filter_sql}")
        return session.execute(sql, params).scalar() or 0


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def _sqlite_fts_search(query: str, filters: dict, limit: int, *, with_snippet: bool = True) -> list[dict]:
    db, _ = get_db()
    join = _fts_join(_sqlite_schema)
    params: dict[str, Any] = {"fts": query.strip(), "limit": limit}
    filter_parts = []

    for key, col in [("language", "d.language"), ("canton", "d.canton"),
                     ("level", "d.level"), ("date_from", "d.decision_date"),
                     ("date_to", "d.decision_date")]:
        if filters.get(key):
            op = ">=" if key == "date_from" else ("<=" if key == "date_to" else "=")
            filter_parts.append(f"{col} {op} :{key}")
            params[key] = filters[key]

    filter_sql = " AND " + " AND ".join(filter_parts) if filter_parts else ""

    snippet_col = "snippet(decisions_fts, 2, '**', '**', '...', 32) AS snippet" if with_snippet else "substr(d.content_text, 1, 300) AS snippet"

    rows = db.execute(f"""
        SELECT d.id, d.title, d.docket, d.decision_date, d.canton, d.language, d.level,
               d.source_name, d.court, d.url,
               {snippet_col}
        FROM decisions_fts
        {join}
        WHERE decisions_fts MATCH :fts {filter_sql}
        ORDER BY bm25(decisions_fts) ASC
        LIMIT :limit
    """, params).fetchall()
    return [dict(r) for r in rows]


def _sqlite_browse(filters: dict, limit: int) -> list[dict]:
    db, _ = get_db()
    params: dict[str, Any] = {"limit": limit}
    where_parts = ["1=1"]

    for key, col in [("language", "language"), ("canton", "canton"),
                     ("level", "level"), ("date_from", "decision_date"),
                     ("date_to", "decision_date")]:
        if filters.get(key):
            op = ">=" if key == "date_from" else ("<=" if key == "date_to" else "=")
            where_parts.append(f"{col} {op} :{key}")
            params[key] = filters[key]

    where_sql = " AND ".join(where_parts)
    rows = db.execute(f"""
        SELECT id, title, docket, decision_date, canton, language, level,
               source_name, court, url, substr(content_text, 1, 300) AS snippet
        FROM decisions WHERE {where_sql}
        ORDER BY decision_date DESC LIMIT :limit
    """, params).fetchall()
    return [dict(r) for r in rows]


def _sqlite_count(query: str, filters: dict) -> int:
    db, _ = get_db()
    params: dict[str, Any] = {}
    filter_parts = []

    for key, col_prefix in [("language", "d.language"), ("canton", "d.canton"),
                            ("level", "d.level"), ("date_from", "d.decision_date"),
                            ("date_to", "d.decision_date")]:
        if filters.get(key):
            op = ">=" if key == "date_from" else ("<=" if key == "date_to" else "=")
            filter_parts.append(f"{col_prefix} {op} :{key}")
            params[key] = filters[key]

    filter_sql = " AND " + " AND ".join(filter_parts) if filter_parts else ""

    if query.strip():
        join = _fts_join(_sqlite_schema)
        params["fts"] = query.strip()
        row = db.execute(f"""
            SELECT COUNT(*) FROM decisions_fts {join}
            WHERE decisions_fts MATCH :fts {filter_sql}
        """, params).fetchone()
    else:
        # Rewrite column refs without "d." prefix
        plain_filter = filter_sql.replace("d.", "")
        row = db.execute(f"SELECT COUNT(*) FROM decisions WHERE 1=1 {plain_filter}", params).fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def search_caselaw(
    query: str,
    language: Optional[str] = None,
    canton: Optional[str] = None,
    level: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 500,
) -> str:
    """Search Swiss court decisions (Bundesgericht, cantonal courts). Supports FTS5 syntax: quoted phrases, AND/OR/NOT operators, field prefixes (title:, docket:). Use this to find relevant case law. When answering legal research questions, format output as a litigation-focused legal memorandum.

    Args:
        query: Search query. Examples: 'Datenschutz', '"Bundesgericht" AND Steuer', 'title:BGE', 'docket:6B_123'
        language: Filter by language (de, fr, it, rm)
        canton: Filter by canton code (ZH, BE, VD, GE, etc.)
        level: Filter by court level (federal, cantonal)
        date_from: Filter from date (YYYY-MM-DD)
        date_to: Filter until date (YYYY-MM-DD)
        limit: Max results (default 500, max 500)

    Returns:
        JSON with matching decisions and total count
    """
    limit = min(int(limit), 500)
    filters = {
        "canton": canton.upper() if canton else None,
        "level": level,
        "language": language,
        "date_from": date_from,
        "date_to": date_to,
    }

    try:
        _, db_type = get_db()

        if db_type == "postgresql":
            if not query.strip():
                # Browse mode for PG
                from sqlmodel import Session, text
                db, _ = get_db()
                with Session(db) as session:
                    conditions = []
                    params: dict[str, Any] = {"limit": limit}
                    for key, col in [("canton", "canton"), ("level", "level"),
                                     ("language", "language"), ("date_from", "decision_date"),
                                     ("date_to", "decision_date")]:
                        if filters.get(key):
                            op = ">=" if key == "date_from" else ("<=" if key == "date_to" else "=")
                            conditions.append(f"{col} {op} :{key}")
                            params[key] = filters[key]
                    filter_sql = " AND ".join(conditions) if conditions else "TRUE"
                    sql = text(f"""
                        SELECT id, title, docket, decision_date, canton, language, level,
                               source_name, court, url, substr(content_text, 1, 300) AS snippet
                        FROM decisions WHERE {filter_sql}
                        ORDER BY decision_date DESC NULLS LAST LIMIT :limit
                    """)
                    rows = session.execute(sql, params).fetchall()
                    results = [
                        {"id": r.id, "title": r.title, "docket": r.docket,
                         "decision_date": str(r.decision_date) if r.decision_date else None,
                         "canton": r.canton, "language": r.language, "level": r.level,
                         "source_name": r.source_name, "court": r.court, "url": r.url,
                         "snippet": r.snippet}
                        for r in rows
                    ]
                total = _pg_count("", filters)
            else:
                results = _pg_search(query, filters, limit)
                total = _pg_count(query, filters)
        else:
            if not query.strip():
                results = _sqlite_browse(filters, limit)
                total = _sqlite_count("", filters)
            else:
                results = _sqlite_fts_search(query, filters, limit)
                total = min(_sqlite_count(query, filters), 10000)

        return json.dumps({
            "total": total, "count": len(results), "results": results
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_decision(decision_id: str) -> str:
    """Get the full text and metadata of a specific Swiss court decision by ID. Use after searching to retrieve complete decision content.

    Args:
        decision_id: The unique decision ID (UUID format)

    Returns:
        JSON with full decision metadata and content text
    """
    try:
        db, db_type = get_db()

        if db_type == "postgresql":
            from sqlmodel import Session, text
            with Session(db) as session:
                sql = text("""
                    SELECT id, source_id, source_name, level, canton, court, chamber,
                           docket, decision_date, publication_date, title, language,
                           url, pdf_url, content_text
                    FROM decisions WHERE id = :id
                """)
                row = session.execute(sql, {"id": decision_id}).fetchone()
                if not row:
                    return json.dumps({"error": f"Decision not found: {decision_id}"})
                return json.dumps({
                    "id": row.id, "source_id": row.source_id, "source_name": row.source_name,
                    "level": row.level, "canton": row.canton, "court": row.court,
                    "chamber": row.chamber, "docket": row.docket,
                    "decision_date": str(row.decision_date) if row.decision_date else None,
                    "publication_date": str(row.publication_date) if row.publication_date else None,
                    "title": row.title, "language": row.language,
                    "url": row.url, "pdf_url": row.pdf_url, "content_text": row.content_text,
                }, ensure_ascii=False, indent=2)
        else:
            row = db.execute("""
                SELECT id, source_id, source_name, level, canton, court, chamber,
                       docket, decision_date, publication_date, title, language,
                       url, pdf_url, content_text
                FROM decisions WHERE id = ?
            """, (decision_id,)).fetchone()
            if not row:
                return json.dumps({"error": f"Decision not found: {decision_id}"})
            return json.dumps(dict(row), ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_caselaw_statistics() -> str:
    """Get statistics about the Swiss caselaw database: total decisions, date range, breakdown by language, canton, and court level.

    Returns:
        JSON with database statistics
    """
    try:
        db, db_type = get_db()

        if db_type == "postgresql":
            from sqlmodel import Session, text
            with Session(db) as session:
                total = session.execute(text("SELECT COUNT(*) FROM decisions")).scalar()
                level_rows = session.execute(text(
                    "SELECT level, COUNT(*) as count FROM decisions GROUP BY level"
                )).fetchall()
                by_level = [{"level": r.level, "count": r.count} for r in level_rows]
                lang_rows = session.execute(text(
                    "SELECT language, COUNT(*) as count FROM decisions WHERE language IS NOT NULL GROUP BY language ORDER BY count DESC"
                )).fetchall()
                by_language = [{"language": r.language, "count": r.count} for r in lang_rows]
                canton_rows = session.execute(text(
                    "SELECT canton, COUNT(*) as count FROM decisions WHERE canton IS NOT NULL GROUP BY canton ORDER BY count DESC"
                )).fetchall()
                top_cantons = [{"canton": r.canton, "count": r.count} for r in canton_rows]
                date_row = session.execute(text(
                    "SELECT MIN(decision_date), MAX(decision_date) FROM decisions"
                )).fetchone()
                date_range = {"min": str(date_row[0]) if date_row[0] else None,
                              "max": str(date_row[1]) if date_row[1] else None}
        else:
            total = db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
            by_level = [{"level": r[0], "count": r[1]} for r in db.execute(
                "SELECT level, COUNT(*) as count FROM decisions GROUP BY level ORDER BY count DESC"
            ).fetchall()]
            by_language = [{"language": r[0], "count": r[1]} for r in db.execute(
                "SELECT language, COUNT(*) as count FROM decisions WHERE language IS NOT NULL GROUP BY language ORDER BY count DESC"
            ).fetchall()]
            top_cantons = [{"canton": r[0], "count": r[1]} for r in db.execute(
                "SELECT canton, COUNT(*) as count FROM decisions WHERE canton IS NOT NULL GROUP BY canton ORDER BY count DESC"
            ).fetchall()]
            date_row = db.execute(
                "SELECT MIN(decision_date), MAX(decision_date) FROM decisions"
            ).fetchone()
            date_range = {"min": date_row[0], "max": date_row[1]}

        return json.dumps({
            "total_decisions": total,
            "date_range": date_range,
            "by_level": by_level,
            "by_language": by_language,
            "top_cantons": top_cantons,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def find_citing_decisions(citation: str, limit: int = 100) -> str:
    """Find Swiss court decisions that cite a specific case reference. Useful for tracking how a precedent has been applied.

    Args:
        citation: Citation to search for (e.g., 'BGE 140 III 264', '6B_123/2024')
        limit: Max results (default 100, max 500)

    Returns:
        JSON with decisions containing the citation
    """
    limit = min(int(limit), 500)

    try:
        db, db_type = get_db()

        if db_type == "postgresql":
            from sqlmodel import Session, text
            with Session(db) as session:
                sql = text("""
                    SELECT id, title, docket, decision_date, canton, language,
                           source_name, url, substr(content_text, 1, 500) AS snippet
                    FROM decisions
                    WHERE content_text ILIKE :pattern OR title ILIKE :pattern
                    ORDER BY decision_date DESC NULLS LAST
                    LIMIT :limit
                """)
                rows = session.execute(sql, {"pattern": f"%{citation}%", "limit": limit}).fetchall()
                results = [
                    {"id": r.id, "title": r.title, "docket": r.docket,
                     "decision_date": str(r.decision_date) if r.decision_date else None,
                     "canton": r.canton, "language": r.language,
                     "source_name": r.source_name, "url": r.url, "snippet": r.snippet}
                    for r in rows
                ]
        else:
            join = _fts_join(_sqlite_schema)
            rows = db.execute(f"""
                SELECT d.id, d.title, d.docket, d.decision_date, d.canton, d.language,
                       d.source_name, d.url,
                       snippet(decisions_fts, 2, '**', '**', '...', 32) AS snippet
                FROM decisions_fts
                {join}
                WHERE decisions_fts MATCH ?
                ORDER BY d.decision_date DESC
                LIMIT ?
            """, (f'"{citation}"', limit)).fetchall()
            results = [dict(r) for r in rows]

        return json.dumps({
            "citation": citation, "count": len(results), "citing_decisions": results
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def analyze_search_results(
    query: str,
    language: Optional[str] = None,
    canton: Optional[str] = None,
    level: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> str:
    """Analyze Swiss court decisions matching a query to provide aggregate insights: breakdown by year, canton, court level, language, and court; plus key federal decisions, most recent cases, and sample cantonal decisions. Use this as the starting point for legal research questions. Output should be formatted as a litigation-focused legal memorandum suitable for a top-tier law firm.

    Args:
        query: Search query. Examples: 'Konkurrenzverbot Arzt', 'Datenschutz DSGVO', '"fristlose Kundigung"'
        language: Filter by language (de, fr, it, rm)
        canton: Filter by canton code (ZH, BE, VD, GE, etc.)
        level: Filter by court level (federal, cantonal)
        date_from: Filter from date (YYYY-MM-DD)
        date_to: Filter until date (YYYY-MM-DD)

    Returns:
        JSON with aggregate analysis and key decisions
    """
    filters = {
        "canton": canton.upper() if canton else None,
        "level": level,
        "language": language,
        "date_from": date_from,
        "date_to": date_to,
    }

    try:
        _, db_type = get_db()

        if db_type == "postgresql":
            return _analyze_pg(query, filters)
        return _analyze_sqlite(query, filters)
    except Exception as e:
        return json.dumps({"error": str(e)})


def _analyze_pg(query: str, filters: dict) -> str:
    from sqlmodel import Session, text
    db, _ = get_db()

    with Session(db) as session:
        conditions = []
        params: dict[str, Any] = {"query": query}
        for key, col in [("canton", "canton"), ("level", "level"),
                         ("language", "language"), ("date_from", "decision_date"),
                         ("date_to", "decision_date")]:
            if filters.get(key):
                op = ">=" if key == "date_from" else ("<=" if key == "date_to" else "=")
                conditions.append(f"{col} {op} :{key}")
                params[key] = filters[key]
        filter_sql = " AND " + " AND ".join(conditions) if conditions else ""
        fts_where = """(
            setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
            setweight(to_tsvector('simple', coalesce(docket, '')), 'A') ||
            to_tsvector('simple', substr(content_text, 1, 50000))
        ) @@ websearch_to_tsquery('simple', :query)""" if query.strip() else "TRUE"

        total = session.execute(text(f"SELECT COUNT(*) FROM decisions WHERE {fts_where} {filter_sql}"), params).scalar()

        by_year = [{"year": str(r[0]), "count": r[1]} for r in session.execute(text(f"""
            SELECT EXTRACT(YEAR FROM decision_date)::int as year, COUNT(*) as count
            FROM decisions WHERE {fts_where} {filter_sql} AND decision_date IS NOT NULL
            GROUP BY year ORDER BY year DESC LIMIT 20
        """), params).fetchall()]

        by_canton = [{"canton": r[0] or "CH (Bund)", "count": r[1]} for r in session.execute(text(f"""
            SELECT COALESCE(canton, 'CH (Bund)'), COUNT(*) FROM decisions
            WHERE {fts_where} {filter_sql} GROUP BY canton ORDER BY COUNT(*) DESC LIMIT 15
        """), params).fetchall()]

        by_level = [{"level": r[0], "count": r[1]} for r in session.execute(text(f"""
            SELECT level, COUNT(*) FROM decisions WHERE {fts_where} {filter_sql}
            GROUP BY level ORDER BY COUNT(*) DESC
        """), params).fetchall()]

        by_language = [{"language": r[0], "count": r[1]} for r in session.execute(text(f"""
            SELECT language, COUNT(*) FROM decisions WHERE {fts_where} {filter_sql}
            AND language IS NOT NULL GROUP BY language ORDER BY COUNT(*) DESC
        """), params).fetchall()]

        by_court = [{"court": r[0], "count": r[1]} for r in session.execute(text(f"""
            SELECT COALESCE(court, source_name), COUNT(*) FROM decisions
            WHERE {fts_where} {filter_sql} GROUP BY COALESCE(court, source_name)
            ORDER BY COUNT(*) DESC LIMIT 15
        """), params).fetchall()]

        # Key federal decisions
        fed_params = {**params, "fed_level": "federal"}
        federal = [
            {"id": r.id, "title": r.title, "docket": r.docket,
             "decision_date": str(r.decision_date) if r.decision_date else None,
             "canton": r.canton, "language": r.language, "url": r.url}
            for r in session.execute(text(f"""
                SELECT id, title, docket, decision_date, canton, language, url
                FROM decisions WHERE {fts_where} {filter_sql} AND level = :fed_level
                ORDER BY decision_date DESC NULLS LAST LIMIT 10
            """), fed_params).fetchall()
        ]

        recent = [
            {"id": r.id, "title": r.title, "docket": r.docket,
             "decision_date": str(r.decision_date) if r.decision_date else None,
             "canton": r.canton, "language": r.language, "level": r.level, "url": r.url}
            for r in session.execute(text(f"""
                SELECT id, title, docket, decision_date, canton, language, level, url
                FROM decisions WHERE {fts_where} {filter_sql}
                ORDER BY decision_date DESC NULLS LAST LIMIT 10
            """), params).fetchall()
        ]

    return json.dumps({
        "query": query, "total_results": total,
        "analysis": {"by_year": by_year, "by_canton": by_canton, "by_level": by_level,
                     "by_language": by_language, "by_court": by_court},
        "key_decisions": {"federal": federal, "most_recent": recent, "by_canton": {}},
    }, ensure_ascii=False, indent=2)


def _analyze_sqlite(query: str, filters: dict) -> str:
    db, _ = get_db()
    join = _fts_join(_sqlite_schema)
    params: dict[str, Any] = {"fts": query.strip()}
    filter_parts = []

    for key, col in [("language", "d.language"), ("canton", "d.canton"),
                     ("level", "d.level"), ("date_from", "d.decision_date"),
                     ("date_to", "d.decision_date")]:
        if filters.get(key):
            op = ">=" if key == "date_from" else ("<=" if key == "date_to" else "=")
            filter_parts.append(f"{col} {op} :{key}")
            params[key] = filters[key]

    filter_sql = " AND " + " AND ".join(filter_parts) if filter_parts else ""

    total = db.execute(f"""
        SELECT COUNT(*) FROM decisions_fts {join}
        WHERE decisions_fts MATCH :fts {filter_sql}
    """, params).fetchone()[0]

    by_year = [{"year": r[0], "count": r[1]} for r in db.execute(f"""
        SELECT strftime('%Y', d.decision_date) as year, COUNT(*) as count
        FROM decisions_fts {join}
        WHERE decisions_fts MATCH :fts {filter_sql} AND d.decision_date IS NOT NULL
        GROUP BY year ORDER BY year DESC LIMIT 20
    """, params).fetchall()]

    by_canton = [{"canton": r[0], "count": r[1]} for r in db.execute(f"""
        SELECT COALESCE(d.canton, 'CH (Bund)') as canton, COUNT(*) as count
        FROM decisions_fts {join}
        WHERE decisions_fts MATCH :fts {filter_sql}
        GROUP BY d.canton ORDER BY count DESC LIMIT 15
    """, params).fetchall()]

    by_level = [{"level": r[0], "count": r[1]} for r in db.execute(f"""
        SELECT d.level, COUNT(*) as count FROM decisions_fts {join}
        WHERE decisions_fts MATCH :fts {filter_sql}
        GROUP BY d.level ORDER BY count DESC
    """, params).fetchall()]

    by_language = [{"language": r[0], "count": r[1]} for r in db.execute(f"""
        SELECT d.language, COUNT(*) as count FROM decisions_fts {join}
        WHERE decisions_fts MATCH :fts {filter_sql}
        GROUP BY d.language ORDER BY count DESC
    """, params).fetchall()]

    by_court = [{"court": r[0], "count": r[1]} for r in db.execute(f"""
        SELECT COALESCE(d.court, d.source_name) as court, COUNT(*) as count
        FROM decisions_fts {join}
        WHERE decisions_fts MATCH :fts {filter_sql}
        GROUP BY court ORDER BY count DESC LIMIT 15
    """, params).fetchall()]

    # Key federal decisions
    fed_params = {**params, "level": "federal"}
    fed_filter = filter_sql + " AND d.level = :level"
    federal = [dict(r) for r in db.execute(f"""
        SELECT d.id, d.title, d.docket, d.decision_date, d.canton, d.language,
               d.source_name, d.url,
               snippet(decisions_fts, 2, '**', '**', '...', 32) AS snippet
        FROM decisions_fts {join}
        WHERE decisions_fts MATCH :fts {fed_filter}
        ORDER BY bm25(decisions_fts) ASC LIMIT 10
    """, fed_params).fetchall()]

    recent = [dict(r) for r in db.execute(f"""
        SELECT d.id, d.title, d.docket, d.decision_date, d.canton, d.language,
               d.level, d.source_name, d.url
        FROM decisions_fts {join}
        WHERE decisions_fts MATCH :fts {filter_sql}
        ORDER BY d.decision_date DESC LIMIT 10
    """, params).fetchall()]

    # Sample cantonal decisions
    cantonal_samples: dict[str, list] = {}
    top_ct: list[str] = []
    if by_canton:
        top_ct = [r["canton"] for r in by_canton if r["canton"] != "CH (Bund)"][:3]
    for ct in top_ct:
        ct_params = {**params, "ct": ct}
        ct_filter = filter_sql + " AND d.canton = :ct"
        rows = db.execute(f"""
            SELECT d.id, d.title, d.docket, d.decision_date, d.language,
                   d.source_name, d.url,
                   snippet(decisions_fts, 2, '**', '**', '...', 32) AS snippet
            FROM decisions_fts {join}
            WHERE decisions_fts MATCH :fts {ct_filter}
            ORDER BY bm25(decisions_fts) ASC LIMIT 3
        """, ct_params).fetchall()
        cantonal_samples[ct] = [dict(r) for r in rows]

    return json.dumps({
        "query": query, "total_results": total,
        "analysis": {"by_year": by_year, "by_canton": by_canton, "by_level": by_level,
                     "by_language": by_language, "by_court": by_court},
        "key_decisions": {"federal": federal, "most_recent": recent, "by_canton": cantonal_samples},
    }, ensure_ascii=False, indent=2)


@mcp.tool()
def search_by_court(
    court: str,
    year: Optional[int] = None,
    limit: int = 20,
) -> str:
    """Search decisions from a specific court.

    Args:
        court: Court name or abbreviation (e.g., "BGer", "Bundesgericht", "Obergericht ZH", "Tribunal federal")
        year: Filter by decision year
        limit: Maximum results (default 20, max 100)

    Returns:
        JSON with decisions from the specified court
    """
    limit = min(int(limit), 100)

    try:
        db, db_type = get_db()

        if db_type == "postgresql":
            from sqlmodel import Session, text
            with Session(db) as session:
                params: dict[str, Any] = {"court_pattern": f"%{court}%", "limit": limit}
                year_filter = ""
                if year:
                    year_filter = "AND EXTRACT(YEAR FROM decision_date) = :year"
                    params["year"] = year
                sql = text(f"""
                    SELECT id, source_name, court, canton, docket, decision_date,
                           title, language, url
                    FROM decisions
                    WHERE (court ILIKE :court_pattern OR source_name ILIKE :court_pattern)
                    {year_filter}
                    ORDER BY decision_date DESC NULLS LAST LIMIT :limit
                """)
                rows = session.execute(sql, params).fetchall()
                results = [
                    {"id": r.id, "source_name": r.source_name, "court": r.court,
                     "canton": r.canton, "docket": r.docket,
                     "decision_date": str(r.decision_date) if r.decision_date else None,
                     "title": r.title, "language": r.language, "url": r.url}
                    for r in rows
                ]
        else:
            params_list = [f"%{court}%", f"%{court}%"]
            year_filter = ""
            if year:
                year_filter = "AND substr(decision_date, 1, 4) = ?"
                params_list.append(str(year))
            params_list.append(limit)
            rows = db.execute(f"""
                SELECT id, source_name, court, canton, docket, decision_date,
                       title, language, url
                FROM decisions
                WHERE (court LIKE ? OR source_name LIKE ?)
                {year_filter}
                ORDER BY decision_date DESC LIMIT ?
            """, params_list).fetchall()
            results = [dict(r) for r in rows]

        return json.dumps({
            "court_query": court, "year": year, "count": len(results), "decisions": results
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_cantons() -> str:
    """List all Swiss cantons with their codes and decision counts.

    Returns:
        JSON with canton codes, names, and number of decisions available
    """
    try:
        db, db_type = get_db()

        if db_type == "postgresql":
            from sqlmodel import Session, text
            with Session(db) as session:
                rows = session.execute(text(
                    "SELECT canton, COUNT(*) as count FROM decisions WHERE canton IS NOT NULL GROUP BY canton ORDER BY count DESC"
                )).fetchall()
                counts = {r.canton: r.count for r in rows}
        else:
            rows = db.execute(
                "SELECT canton, COUNT(*) as count FROM decisions WHERE canton IS NOT NULL GROUP BY canton ORDER BY count DESC"
            ).fetchall()
            counts = {r["canton"]: r["count"] for r in rows}

        cantons = [
            {"code": code, "name": name, "decisions": counts.get(code, 0)}
            for code, name in sorted(CANTON_CODES.items())
        ]
        total = sum(c["decisions"] for c in cantons)

        return json.dumps({
            "total_cantonal_decisions": total, "cantons": cantons
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        sys.exit(0)

    try:
        _, db_type = get_db()
        print(f"Connected to {db_type} database (schema: {_sqlite_schema})", file=sys.stderr)
    except Exception as e:
        print(f"Warning: {e}", file=sys.stderr)
        print("Will attempt to connect when first tool is called.", file=sys.stderr)

    mcp.run()
