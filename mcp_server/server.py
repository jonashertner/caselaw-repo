#!/usr/bin/env python3
"""
Swiss Caselaw MCP Server - Legal research tools for Claude Code.

Provides tools to search and analyze Swiss court decisions.
"""
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

# MCP protocol constants
JSONRPC_VERSION = "2.0"

# Database path - check environment variable first, then default locations
def get_db_path() -> Path:
    """Find the caselaw database."""
    # 1. Environment variable
    if env_path := os.environ.get("CASELAW_DB_PATH"):
        return Path(env_path)

    # 2. macOS default
    mac_path = Path.home() / "Library/Application Support/swiss-caselaw/caselaw.sqlite"
    if mac_path.exists():
        return mac_path

    # 3. Linux/generic default
    linux_path = Path.home() / ".local/share/swiss-caselaw/caselaw.sqlite"
    if linux_path.exists():
        return linux_path

    # 4. Current directory
    local_path = Path("caselaw.sqlite")
    if local_path.exists():
        return local_path

    raise RuntimeError(
        "Database not found. Set CASELAW_DB_PATH or install via: "
        "pip install caselaw-local && caselaw-local update"
    )

DB_PATH = None  # Lazy initialization


def get_db_connection():
    """Get a read-only database connection."""
    global DB_PATH
    if DB_PATH is None:
        DB_PATH = get_db_path()
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def search_caselaw(query: str, language: str = None, canton: str = None,
                   level: str = None, date_from: str = None, date_to: str = None,
                   limit: int = 20) -> dict:
    """
    Search Swiss court decisions using full-text search.

    Args:
        query: Search query (supports FTS5 syntax: quotes, AND, OR, NOT, title:, docket:)
        language: Filter by language (de, fr, it, rm)
        canton: Filter by canton code (ZH, BE, VD, etc.)
        level: Filter by court level (federal, cantonal)
        date_from: Filter decisions from this date (YYYY-MM-DD)
        date_to: Filter decisions until this date (YYYY-MM-DD)
        limit: Maximum results to return (default 20, max 100)

    Returns:
        Search results with decision metadata and snippets
    """
    conn = get_db_connection()
    limit = min(int(limit), 100)

    try:
        if not query.strip():
            # Browse mode
            where_clauses = ["1=1"]
            params = {}

            if language:
                where_clauses.append("language = :language")
                params["language"] = language
            if canton:
                where_clauses.append("canton = :canton")
                params["canton"] = canton
            if level:
                where_clauses.append("level = :level")
                params["level"] = level
            if date_from:
                where_clauses.append("decision_date >= :date_from")
                params["date_from"] = date_from
            if date_to:
                where_clauses.append("decision_date <= :date_to")
                params["date_to"] = date_to

            params["limit"] = limit
            where_sql = " AND ".join(where_clauses)

            rows = conn.execute(f"""
                SELECT id, title, docket, decision_date, canton, language, level,
                       source_name, court, url,
                       substr(content_text, 1, 300) AS snippet
                FROM decisions
                WHERE {where_sql}
                ORDER BY decision_date DESC
                LIMIT :limit
            """, params).fetchall()

            total = conn.execute(f"SELECT COUNT(*) FROM decisions WHERE {where_sql}", params).fetchone()[0]
        else:
            # FTS search
            params = {"fts": query.strip(), "limit": limit}
            filter_clauses = []

            if language:
                filter_clauses.append("d.language = :language")
                params["language"] = language
            if canton:
                filter_clauses.append("d.canton = :canton")
                params["canton"] = canton
            if level:
                filter_clauses.append("d.level = :level")
                params["level"] = level
            if date_from:
                filter_clauses.append("d.decision_date >= :date_from")
                params["date_from"] = date_from
            if date_to:
                filter_clauses.append("d.decision_date <= :date_to")
                params["date_to"] = date_to

            filter_sql = " AND " + " AND ".join(filter_clauses) if filter_clauses else ""

            rows = conn.execute(f"""
                SELECT d.id, d.title, d.docket, d.decision_date, d.canton, d.language, d.level,
                       d.source_name, d.court, d.url,
                       snippet(decisions_fts, 2, '**', '**', '...', 32) AS snippet
                FROM decisions_fts
                JOIN decisions d ON d.doc_id = decisions_fts.rowid
                WHERE decisions_fts MATCH :fts {filter_sql}
                ORDER BY bm25(decisions_fts) ASC
                LIMIT :limit
            """, params).fetchall()

            total_row = conn.execute(f"""
                SELECT COUNT(*) FROM decisions_fts
                JOIN decisions d ON d.doc_id = decisions_fts.rowid
                WHERE decisions_fts MATCH :fts {filter_sql}
                LIMIT 10001
            """, params).fetchone()
            total = min(total_row[0], 10000)

        results = [dict(r) for r in rows]
        return {
            "total": total,
            "count": len(results),
            "results": results
        }
    finally:
        conn.close()


def get_decision(decision_id: str) -> dict:
    """
    Get full details of a specific court decision.

    Args:
        decision_id: The unique ID of the decision

    Returns:
        Complete decision record including full text
    """
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT id, title, docket, decision_date, publication_date,
                   canton, language, level, source_name, source_id,
                   court, chamber, url, pdf_url, content_text
            FROM decisions
            WHERE id = ?
        """, (decision_id,)).fetchone()

        if not row:
            return {"error": f"Decision not found: {decision_id}"}

        return dict(row)
    finally:
        conn.close()


def get_statistics() -> dict:
    """
    Get database statistics and coverage information.

    Returns:
        Statistics about the caselaw database
    """
    conn = get_db_connection()
    try:
        total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

        date_range = conn.execute("""
            SELECT MIN(decision_date), MAX(decision_date) FROM decisions
        """).fetchone()

        by_level = conn.execute("""
            SELECT level, COUNT(*) as count FROM decisions
            GROUP BY level ORDER BY count DESC
        """).fetchall()

        by_language = conn.execute("""
            SELECT language, COUNT(*) as count FROM decisions
            GROUP BY language ORDER BY count DESC
        """).fetchall()

        by_canton = conn.execute("""
            SELECT canton, COUNT(*) as count FROM decisions
            WHERE canton IS NOT NULL
            GROUP BY canton ORDER BY count DESC LIMIT 10
        """).fetchall()

        return {
            "total_decisions": total,
            "date_range": {"min": date_range[0], "max": date_range[1]},
            "by_level": [{"level": r[0], "count": r[1]} for r in by_level],
            "by_language": [{"language": r[0], "count": r[1]} for r in by_language],
            "top_cantons": [{"canton": r[0], "count": r[1]} for r in by_canton]
        }
    finally:
        conn.close()


def find_citing_decisions(citation: str, limit: int = 20) -> dict:
    """
    Find decisions that cite a specific case reference.

    Args:
        citation: Citation to search for (e.g., "BGE 140 III 264" or "6B_123/2024")
        limit: Maximum results to return

    Returns:
        Decisions containing the citation
    """
    conn = get_db_connection()
    limit = min(int(limit), 100)

    try:
        # Search for the citation in content
        rows = conn.execute("""
            SELECT d.id, d.title, d.docket, d.decision_date, d.canton, d.language,
                   d.source_name, d.url,
                   snippet(decisions_fts, 2, '**', '**', '...', 32) AS snippet
            FROM decisions_fts
            JOIN decisions d ON d.doc_id = decisions_fts.rowid
            WHERE decisions_fts MATCH ?
            ORDER BY d.decision_date DESC
            LIMIT ?
        """, (f'"{citation}"', limit)).fetchall()

        return {
            "citation": citation,
            "count": len(rows),
            "citing_decisions": [dict(r) for r in rows]
        }
    finally:
        conn.close()


# Tool definitions for MCP
TOOLS = [
    {
        "name": "search_caselaw",
        "description": "Search Swiss court decisions (Bundesgericht, cantonal courts). Supports FTS5 syntax: quoted phrases, AND/OR/NOT operators, field prefixes (title:, docket:). Use this to find relevant case law.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query. Examples: 'Datenschutz', '\"Bundesgericht\" AND Steuer', 'title:BGE', 'docket:6B_123'"
                },
                "language": {
                    "type": "string",
                    "enum": ["de", "fr", "it", "rm"],
                    "description": "Filter by language"
                },
                "canton": {
                    "type": "string",
                    "description": "Filter by canton code (ZH, BE, VD, GE, etc.)"
                },
                "level": {
                    "type": "string",
                    "enum": ["federal", "cantonal"],
                    "description": "Filter by court level"
                },
                "date_from": {
                    "type": "string",
                    "description": "Filter from date (YYYY-MM-DD)"
                },
                "date_to": {
                    "type": "string",
                    "description": "Filter until date (YYYY-MM-DD)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 20, max 100)"
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_decision",
        "description": "Get the full text and metadata of a specific Swiss court decision by ID. Use after searching to retrieve complete decision content.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "decision_id": {
                    "type": "string",
                    "description": "The unique decision ID (UUID format)"
                }
            },
            "required": ["decision_id"]
        }
    },
    {
        "name": "get_caselaw_statistics",
        "description": "Get statistics about the Swiss caselaw database: total decisions, date range, breakdown by language, canton, and court level.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "find_citing_decisions",
        "description": "Find Swiss court decisions that cite a specific case reference. Useful for tracking how a precedent has been applied.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "citation": {
                    "type": "string",
                    "description": "Citation to search for (e.g., 'BGE 140 III 264', '6B_123/2024')"
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 20)"
                }
            },
            "required": ["citation"]
        }
    }
]


def handle_request(request: dict) -> dict:
    """Handle an MCP JSON-RPC request."""
    method = request.get("method")
    params = request.get("params", {})
    req_id = request.get("id")

    if method == "initialize":
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "swiss-caselaw",
                    "version": "1.0.0"
                }
            }
        }

    elif method == "tools/list":
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": req_id,
            "result": {"tools": TOOLS}
        }

    elif method == "tools/call":
        tool_name = params.get("name")
        tool_args = params.get("arguments", {})

        try:
            if tool_name == "search_caselaw":
                result = search_caselaw(**tool_args)
            elif tool_name == "get_decision":
                result = get_decision(**tool_args)
            elif tool_name == "get_caselaw_statistics":
                result = get_statistics()
            elif tool_name == "find_citing_decisions":
                result = find_citing_decisions(**tool_args)
            else:
                return {
                    "jsonrpc": JSONRPC_VERSION,
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}
                }

            return {
                "jsonrpc": JSONRPC_VERSION,
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, indent=2, ensure_ascii=False)}]
                }
            }
        except Exception as e:
            return {
                "jsonrpc": JSONRPC_VERSION,
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error: {str(e)}"}],
                    "isError": True
                }
            }

    elif method == "notifications/initialized":
        return None  # No response for notifications

    else:
        return {
            "jsonrpc": JSONRPC_VERSION,
            "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        }


def main():
    """Run the MCP server using stdio transport."""
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break

            request = json.loads(line)
            response = handle_request(request)

            if response is not None:
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()

        except json.JSONDecodeError:
            continue
        except Exception as e:
            error_response = {
                "jsonrpc": JSONRPC_VERSION,
                "id": None,
                "error": {"code": -32603, "message": str(e)}
            }
            sys.stdout.write(json.dumps(error_response) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
