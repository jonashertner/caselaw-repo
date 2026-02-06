#!/usr/bin/env python3
"""MCP Server for Swiss Caselaw Database.

Provides tools to search and retrieve Swiss court decisions for use with Claude Code.

Usage:
    # With PostgreSQL (requires DATABASE_URL env var)
    python mcp_server.py

    # With SQLite file
    SQLITE_PATH=/path/to/swisslaw.db python mcp_server.py

    # Auto-download from HuggingFace (no setup required)
    python mcp_server.py --huggingface

Configuration in ~/.claude/settings.json:
    {
      "mcpServers": {
        "swiss-caselaw": {
          "command": "python",
          "args": ["/path/to/swiss-caselaw/backend/mcp_server.py"],
          "env": {
            "DATABASE_URL": "postgresql://..."
          }
        }
      }
    }
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent))

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print("Error: mcp package not installed. Run: pip install mcp", file=sys.stderr)
    sys.exit(1)


# Initialize MCP server
mcp = FastMCP("swiss-caselaw")

# Global database connection
_db_conn: Optional[sqlite3.Connection] = None
_db_type: str = "none"


def get_db() -> tuple[Any, str]:
    """Get database connection, initializing if needed."""
    global _db_conn, _db_type

    if _db_conn is not None:
        return _db_conn, _db_type

    # Try PostgreSQL first
    database_url = os.environ.get("DATABASE_URL")
    if database_url and database_url.startswith("postgresql"):
        try:
            from sqlmodel import Session, create_engine
            engine = create_engine(database_url)
            _db_conn = engine
            _db_type = "postgresql"
            return _db_conn, _db_type
        except Exception as e:
            print(f"PostgreSQL connection failed: {e}", file=sys.stderr)

    # Try SQLite
    sqlite_path = os.environ.get("SQLITE_PATH")
    if sqlite_path and Path(sqlite_path).exists():
        _db_conn = sqlite3.connect(sqlite_path)
        _db_conn.row_factory = sqlite3.Row
        _db_type = "sqlite"
        return _db_conn, _db_type

    # Try default SQLite location
    default_sqlite = Path(__file__).parent.parent / "data" / "swisslaw.db"
    if default_sqlite.exists() and default_sqlite.stat().st_size > 0:
        _db_conn = sqlite3.connect(str(default_sqlite))
        _db_conn.row_factory = sqlite3.Row
        _db_type = "sqlite"
        return _db_conn, _db_type

    # Auto-download from HuggingFace
    if "--huggingface" in sys.argv or os.environ.get("USE_HUGGINGFACE"):
        sqlite_path = _download_from_huggingface()
        if sqlite_path:
            _db_conn = sqlite3.connect(sqlite_path)
            _db_conn.row_factory = sqlite3.Row
            _db_type = "sqlite"
            return _db_conn, _db_type

    raise RuntimeError(
        "No database configured. Set DATABASE_URL for PostgreSQL, "
        "SQLITE_PATH for SQLite, or use --huggingface flag."
    )


def _download_from_huggingface() -> Optional[str]:
    """Download SQLite database from HuggingFace."""
    try:
        from huggingface_hub import hf_hub_download
        print("Downloading database from HuggingFace...", file=sys.stderr)
        path = hf_hub_download(
            repo_id="voilaj/swiss-caselaw-db",
            filename="swisslaw.db",
            repo_type="dataset",
        )
        print(f"Downloaded to: {path}", file=sys.stderr)
        return path
    except Exception as e:
        print(f"Failed to download from HuggingFace: {e}", file=sys.stderr)
        return None


# Swiss canton codes
CANTON_CODES = {
    "AG": "Aargau", "AI": "Appenzell Innerrhoden", "AR": "Appenzell Ausserrhoden",
    "BE": "Bern", "BL": "Basel-Landschaft", "BS": "Basel-Stadt",
    "FR": "Fribourg", "GE": "Genève", "GL": "Glarus", "GR": "Graubünden",
    "JU": "Jura", "LU": "Luzern", "NE": "Neuchâtel", "NW": "Nidwalden",
    "OW": "Obwalden", "SG": "St. Gallen", "SH": "Schaffhausen", "SO": "Solothurn",
    "SZ": "Schwyz", "TG": "Thurgau", "TI": "Ticino", "UR": "Uri",
    "VD": "Vaud", "VS": "Valais", "ZG": "Zug", "ZH": "Zürich"
}

# Docket number patterns
DOCKET_PATTERNS = [
    re.compile(r'\b\d[A-Z]_\d+/\d{4}\b', re.IGNORECASE),  # BGer: 6B_316/2015
    re.compile(r'\b[A-Z]-\d+/\d{4}\b', re.IGNORECASE),     # BVGer: E-5164/2007
    re.compile(r'\bBGE\s+\d+\s+[IVX]+\s+\d+\b', re.IGNORECASE),  # BGE 143 IV 241
]


def _is_docket_number(query: str) -> bool:
    """Check if query looks like a docket number."""
    for pattern in DOCKET_PATTERNS:
        if pattern.search(query):
            return True
    return False


def _search_postgresql(query: str, filters: dict, limit: int) -> list[dict]:
    """Search using PostgreSQL full-text search."""
    from sqlmodel import Session, select, text

    db, _ = get_db()
    with Session(db) as session:
        # Build filter conditions
        conditions = []
        params = {"query": query, "limit": limit}

        if filters.get("canton"):
            conditions.append("canton = :canton")
            params["canton"] = filters["canton"]
        if filters.get("level"):
            conditions.append("level = :level")
            params["level"] = filters["level"]
        if filters.get("language"):
            conditions.append("language = :language")
            params["language"] = filters["language"]
        if filters.get("date_from"):
            conditions.append("decision_date >= :date_from")
            params["date_from"] = filters["date_from"]
        if filters.get("date_to"):
            conditions.append("decision_date <= :date_to")
            params["date_to"] = filters["date_to"]

        filter_sql = " AND ".join(conditions) if conditions else "TRUE"

        # Check for docket number search
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
            # Full-text search
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

        result = session.execute(sql, params)
        rows = result.fetchall()

        return [
            {
                "id": row.id,
                "source_id": row.source_id,
                "source_name": row.source_name,
                "level": row.level,
                "canton": row.canton,
                "court": row.court,
                "docket": row.docket,
                "decision_date": str(row.decision_date) if row.decision_date else None,
                "title": row.title,
                "language": row.language,
                "url": row.url,
                "pdf_url": row.pdf_url,
                "content_preview": row.content_preview[:1000] if row.content_preview else None,
            }
            for row in rows
        ]


def _search_sqlite(query: str, filters: dict, limit: int) -> list[dict]:
    """Search using SQLite FTS5 or LIKE fallback."""
    db, _ = get_db()
    cursor = db.cursor()

    # Build filter conditions
    conditions = []
    params = []

    if filters.get("canton"):
        conditions.append("d.canton = ?")
        params.append(filters["canton"])
    if filters.get("level"):
        conditions.append("d.level = ?")
        params.append(filters["level"])
    if filters.get("language"):
        conditions.append("d.language = ?")
        params.append(filters["language"])
    if filters.get("date_from"):
        conditions.append("d.decision_date >= ?")
        params.append(filters["date_from"])
    if filters.get("date_to"):
        conditions.append("d.decision_date <= ?")
        params.append(filters["date_to"])

    filter_sql = " AND ".join(conditions) if conditions else "1=1"

    # Try FTS5 first
    try:
        # Format query for FTS5
        fts_query = " OR ".join(word for word in query.split() if len(word) >= 2)
        if not fts_query:
            fts_query = query

        sql = f"""
            SELECT d.id, d.source_id, d.source_name, d.level, d.canton, d.court,
                   d.docket, d.decision_date, d.title, d.language, d.url, d.pdf_url,
                   substr(d.content_text, 1, 2000) as content_preview
            FROM decisions_fts
            JOIN decisions d ON decisions_fts.id = d.id
            WHERE decisions_fts MATCH ?
            AND {filter_sql}
            ORDER BY bm25(decisions_fts)
            LIMIT ?
        """
        cursor.execute(sql, [fts_query] + params + [limit])
        rows = cursor.fetchall()
    except sqlite3.OperationalError:
        # Fallback to LIKE search
        sql = f"""
            SELECT id, source_id, source_name, level, canton, court,
                   docket, decision_date, title, language, url, pdf_url,
                   substr(content_text, 1, 2000) as content_preview
            FROM decisions d
            WHERE (content_text LIKE ? OR title LIKE ? OR docket LIKE ?)
            AND {filter_sql}
            ORDER BY decision_date DESC
            LIMIT ?
        """
        like_pattern = f"%{query}%"
        cursor.execute(sql, [like_pattern, like_pattern, like_pattern] + params + [limit])
        rows = cursor.fetchall()

    return [
        {
            "id": row["id"],
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "level": row["level"],
            "canton": row["canton"],
            "court": row["court"],
            "docket": row["docket"],
            "decision_date": row["decision_date"],
            "title": row["title"],
            "language": row["language"],
            "url": row["url"],
            "pdf_url": row["pdf_url"],
            "content_preview": row["content_preview"][:1000] if row["content_preview"] else None,
        }
        for row in rows
    ]


@mcp.tool()
def search_decisions(
    query: str,
    canton: Optional[str] = None,
    level: Optional[str] = None,
    language: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 10
) -> str:
    """Search Swiss court decisions by keyword, docket number, or legal topic.

    Args:
        query: Search query - can be keywords, legal terms, docket numbers (e.g., "6B_316/2015"), or BGE references (e.g., "BGE 143 IV 241")
        canton: Filter by canton code (e.g., "ZH", "BE", "GE"). Use list_cantons() to see all codes.
        level: Filter by court level - "federal" or "cantonal"
        language: Filter by language - "de", "fr", "it"
        date_from: Filter decisions from this date (YYYY-MM-DD)
        date_to: Filter decisions until this date (YYYY-MM-DD)
        limit: Maximum number of results (default 10, max 50)

    Returns:
        JSON with matching decisions including id, title, court, date, docket number, and content preview
    """
    limit = min(limit, 50)
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
            results = _search_postgresql(query, filters, limit)
        else:
            results = _search_sqlite(query, filters, limit)

        return json.dumps({
            "count": len(results),
            "query": query,
            "filters": {k: v for k, v in filters.items() if v},
            "decisions": results
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_decision(decision_id: str) -> str:
    """Get the full text and metadata of a specific court decision.

    Args:
        decision_id: The decision ID (UUID) from search results

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
                           docket, decision_date, published_date, title, language,
                           url, pdf_url, content_text, meta
                    FROM decisions
                    WHERE id = :id
                """)
                result = session.execute(sql, {"id": decision_id})
                row = result.fetchone()
        else:
            cursor = db.cursor()
            cursor.execute("""
                SELECT id, source_id, source_name, level, canton, court, chamber,
                       docket, decision_date, published_date, title, language,
                       url, pdf_url, content_text, meta
                FROM decisions
                WHERE id = ?
            """, [decision_id])
            row = cursor.fetchone()

        if not row:
            return json.dumps({"error": f"Decision {decision_id} not found"})

        # Handle meta field
        meta = row["meta"] if db_type == "sqlite" else row.meta
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except:
                meta = {}

        return json.dumps({
            "id": row["id"] if db_type == "sqlite" else row.id,
            "source_id": row["source_id"] if db_type == "sqlite" else row.source_id,
            "source_name": row["source_name"] if db_type == "sqlite" else row.source_name,
            "level": row["level"] if db_type == "sqlite" else row.level,
            "canton": row["canton"] if db_type == "sqlite" else row.canton,
            "court": row["court"] if db_type == "sqlite" else row.court,
            "chamber": row["chamber"] if db_type == "sqlite" else row.chamber,
            "docket": row["docket"] if db_type == "sqlite" else row.docket,
            "decision_date": str(row["decision_date"]) if row["decision_date"] else None,
            "published_date": str(row["published_date"]) if row["published_date"] else None,
            "title": row["title"] if db_type == "sqlite" else row.title,
            "language": row["language"] if db_type == "sqlite" else row.language,
            "url": row["url"] if db_type == "sqlite" else row.url,
            "pdf_url": row["pdf_url"] if db_type == "sqlite" else row.pdf_url,
            "content_text": row["content_text"] if db_type == "sqlite" else row.content_text,
            "meta": meta,
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
                sql = text("""
                    SELECT canton, COUNT(*) as count
                    FROM decisions
                    WHERE canton IS NOT NULL
                    GROUP BY canton
                    ORDER BY count DESC
                """)
                result = session.execute(sql)
                rows = result.fetchall()
                counts = {row.canton: row.count for row in rows}
        else:
            cursor = db.cursor()
            cursor.execute("""
                SELECT canton, COUNT(*) as count
                FROM decisions
                WHERE canton IS NOT NULL
                GROUP BY canton
                ORDER BY count DESC
            """)
            rows = cursor.fetchall()
            counts = {row["canton"]: row["count"] for row in rows}

        cantons = [
            {
                "code": code,
                "name": name,
                "decisions": counts.get(code, 0)
            }
            for code, name in sorted(CANTON_CODES.items())
        ]

        total = sum(c["decisions"] for c in cantons)

        return json.dumps({
            "total_cantonal_decisions": total,
            "cantons": cantons
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_statistics() -> str:
    """Get overall statistics about the Swiss caselaw database.

    Returns:
        JSON with total counts by level, canton, language, and date range
    """
    try:
        db, db_type = get_db()

        if db_type == "postgresql":
            from sqlmodel import Session, text
            with Session(db) as session:
                # Total count
                total = session.execute(text("SELECT COUNT(*) FROM decisions")).scalar()

                # By level
                level_rows = session.execute(text(
                    "SELECT level, COUNT(*) as count FROM decisions GROUP BY level"
                )).fetchall()
                by_level = {row.level: row.count for row in level_rows}

                # By language
                lang_rows = session.execute(text(
                    "SELECT language, COUNT(*) as count FROM decisions WHERE language IS NOT NULL GROUP BY language"
                )).fetchall()
                by_language = {row.language: row.count for row in lang_rows}

                # Date range
                date_row = session.execute(text(
                    "SELECT MIN(decision_date) as min_date, MAX(decision_date) as max_date FROM decisions"
                )).fetchone()
                min_date = str(date_row.min_date) if date_row.min_date else None
                max_date = str(date_row.max_date) if date_row.max_date else None
        else:
            cursor = db.cursor()

            cursor.execute("SELECT COUNT(*) FROM decisions")
            total = cursor.fetchone()[0]

            cursor.execute("SELECT level, COUNT(*) as count FROM decisions GROUP BY level")
            by_level = {row["level"]: row["count"] for row in cursor.fetchall()}

            cursor.execute("SELECT language, COUNT(*) as count FROM decisions WHERE language IS NOT NULL GROUP BY language")
            by_language = {row["language"]: row["count"] for row in cursor.fetchall()}

            cursor.execute("SELECT MIN(decision_date) as min_date, MAX(decision_date) as max_date FROM decisions")
            date_row = cursor.fetchone()
            min_date = date_row["min_date"]
            max_date = date_row["max_date"]

        return json.dumps({
            "total_decisions": total,
            "by_level": by_level,
            "by_language": by_language,
            "date_range": {
                "earliest": min_date,
                "latest": max_date
            },
            "sources": {
                "federal_courts": ["BGer", "BVGer", "BStGer", "BPatGer"],
                "cantonal_courts": list(CANTON_CODES.keys()),
            }
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def search_by_court(
    court: str,
    year: Optional[int] = None,
    limit: int = 20
) -> str:
    """Search decisions from a specific court.

    Args:
        court: Court name or abbreviation (e.g., "BGer", "Bundesgericht", "Obergericht ZH", "Tribunal fédéral")
        year: Filter by decision year
        limit: Maximum results (default 20, max 100)

    Returns:
        JSON with decisions from the specified court
    """
    limit = min(limit, 100)

    try:
        db, db_type = get_db()

        if db_type == "postgresql":
            from sqlmodel import Session, text
            with Session(db) as session:
                params = {"court_pattern": f"%{court}%", "limit": limit}
                year_filter = ""
                if year:
                    year_filter = "AND EXTRACT(YEAR FROM decision_date) = :year"
                    params["year"] = year

                sql = text(f"""
                    SELECT id, source_id, source_name, level, canton, court, docket,
                           decision_date, title, language, url, pdf_url,
                           substr(content_text, 1, 500) as content_preview
                    FROM decisions
                    WHERE (court ILIKE :court_pattern OR source_name ILIKE :court_pattern)
                    {year_filter}
                    ORDER BY decision_date DESC NULLS LAST
                    LIMIT :limit
                """)
                result = session.execute(sql, params)
                rows = result.fetchall()

                results = [
                    {
                        "id": row.id,
                        "source_name": row.source_name,
                        "court": row.court,
                        "canton": row.canton,
                        "docket": row.docket,
                        "decision_date": str(row.decision_date) if row.decision_date else None,
                        "title": row.title,
                        "language": row.language,
                        "url": row.url,
                    }
                    for row in rows
                ]
        else:
            cursor = db.cursor()
            params = [f"%{court}%", f"%{court}%"]
            year_filter = ""
            if year:
                year_filter = "AND substr(decision_date, 1, 4) = ?"
                params.append(str(year))
            params.append(limit)

            sql = f"""
                SELECT id, source_id, source_name, level, canton, court, docket,
                       decision_date, title, language, url, pdf_url
                FROM decisions
                WHERE (court LIKE ? OR source_name LIKE ?)
                {year_filter}
                ORDER BY decision_date DESC
                LIMIT ?
            """
            cursor.execute(sql, params)
            rows = cursor.fetchall()

            results = [
                {
                    "id": row["id"],
                    "source_name": row["source_name"],
                    "court": row["court"],
                    "canton": row["canton"],
                    "docket": row["docket"],
                    "decision_date": row["decision_date"],
                    "title": row["title"],
                    "language": row["language"],
                    "url": row["url"],
                }
                for row in rows
            ]

        return json.dumps({
            "court_query": court,
            "year": year,
            "count": len(results),
            "decisions": results
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


if __name__ == "__main__":
    # Check for help flag
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        sys.exit(0)

    # Initialize database connection
    try:
        _, db_type = get_db()
        print(f"Connected to {db_type} database", file=sys.stderr)
    except Exception as e:
        print(f"Warning: {e}", file=sys.stderr)
        print("Will attempt to connect when first tool is called.", file=sys.stderr)

    # Run MCP server
    mcp.run()
