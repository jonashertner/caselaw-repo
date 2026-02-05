from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .query_parser import validate_fts5_query, sanitize_query, extract_search_terms
from .fuzzy import get_suggestion, get_suggestions_for_terms


DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100
FACET_SAMPLE_LIMIT = 5000
COUNT_CAP = 10000

# Ranking weights (tunable)
FRESHNESS_WEIGHT = 0.5  # Boost for documents < 2 years old
TITLE_MATCH_BONUS = 2.0  # Bonus for exact match in title
DOCKET_MATCH_BONUS = 3.0  # Bonus for exact match in docket
FRESHNESS_YEARS = 2  # Documents newer than this get freshness boost


def _build_filter_sql(filters: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    clauses: List[str] = []
    params: Dict[str, Any] = {}

    def in_list(field: str, key: str):
        vals = filters.get(key) or []
        if not vals:
            return
        placeholders = []
        for i, v in enumerate(vals):
            pname = f"{key}_{i}"
            params[pname] = v
            placeholders.append(f":{pname}")
        clauses.append(f"{field} IN ({','.join(placeholders)})")

    in_list("d.canton", "canton")
    in_list("d.language", "language")
    in_list("d.level", "level")
    in_list("d.source_id", "source_id")

    # date range
    if filters.get("date_from"):
        clauses.append("d.decision_date >= :date_from")
        params["date_from"] = filters["date_from"]
    if filters.get("date_to"):
        clauses.append("d.decision_date <= :date_to")
        params["date_to"] = filters["date_to"]

    # docket exact/prefix (not FTS)
    if filters.get("docket"):
        clauses.append("d.docket LIKE :docket_like")
        params["docket_like"] = f"{filters['docket']}%"

    sql = " AND ".join(clauses)
    if sql:
        sql = " AND " + sql
    return sql, params


def validate_and_search(
    conn: sqlite3.Connection,
    *,
    q: str,
    filters: Optional[Dict[str, Any]] = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    sort: str = "relevance",
) -> Dict[str, Any]:
    """
    Validate query and perform search with error handling.

    Returns search results or error information.
    """
    q = q.strip() if q else ""

    # Validate query if not empty
    if q:
        validation = validate_fts5_query(q)
        if not validation.valid:
            return {
                "error": True,
                "message": validation.error or "Invalid query syntax",
                "suggestion": validation.suggestion,
                "query": q,
                "results": [],
                "total": 0,
            }
        # Use sanitized query
        q = validation.sanitized or q

    # Perform search
    try:
        result = search(conn, q=q, filters=filters, page=page, page_size=page_size, sort=sort)

        # If zero results, try to get a "did you mean" suggestion
        if q and result.get("total", 0) == 0:
            suggestion = get_suggestion(conn, q)
            if suggestion:
                result["did_you_mean"] = suggestion

        return result

    except sqlite3.OperationalError as e:
        error_msg = str(e)
        # Try to provide helpful error message for FTS errors
        if "fts5" in error_msg.lower() or "syntax" in error_msg.lower():
            # Extract search terms for fuzzy suggestions
            terms = extract_search_terms(q)
            suggestions = get_suggestions_for_terms(conn, terms, limit=1)
            suggestion = suggestions[0][1] if suggestions else None

            return {
                "error": True,
                "message": f"Search syntax error: {error_msg}",
                "suggestion": suggestion,
                "query": q,
                "results": [],
                "total": 0,
            }
        raise


def search(
    conn: sqlite3.Connection,
    *,
    q: str,
    filters: Optional[Dict[str, Any]] = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    sort: str = "relevance",
) -> Dict[str, Any]:
    filters = filters or {}
    page = max(1, int(page))
    page_size = max(1, min(int(page_size), MAX_PAGE_SIZE))
    offset = (page - 1) * page_size

    filter_sql, filter_params = _build_filter_sql(filters)

    # Empty query = browse
    if not q.strip():
        where = "1=1" + filter_sql.replace("d.", "")
        params = dict(filter_params)
        # Total count (fast with indexes)
        total = conn.execute(f"SELECT COUNT(*) AS n FROM decisions WHERE {where};", params).fetchone()["n"]

        order = "decision_date DESC" if sort in ("relevance", "date_desc") else "decision_date ASC"
        rows = conn.execute(
            f"""
            SELECT id, source_name, canton, level, language, docket, decision_date, publication_date, title, url, pdf_url,
                   substr(content_text, 1, 300) AS snippet
            FROM decisions
            WHERE {where}
            ORDER BY {order}
            LIMIT :limit OFFSET :offset;
            """,
            {**params, "limit": page_size, "offset": offset},
        ).fetchall()

        return {
            "query": q,
            "page": page,
            "page_size": page_size,
            "total": int(total),
            "total_capped": False,
            "results": [dict(r) for r in rows],
            "facets": _facets_browse(conn, where=where, params=params),
        }

    # FTS search
    fts = q.strip()

    # Calculate freshness cutoff date (2 years ago)
    freshness_cutoff = (datetime.now() - timedelta(days=FRESHNESS_YEARS * 365)).strftime("%Y-%m-%d")

    params = {
        "fts": fts,
        "limit": page_size,
        "offset": offset,
        "freshness_cutoff": freshness_cutoff,
        "fts_lower": fts.lower(),
        **filter_params
    }

    # Sort - enhanced ranking includes freshness and exact match bonuses
    if sort == "date_desc":
        order_by = "d.decision_date DESC, final_rank ASC"
    elif sort == "date_asc":
        order_by = "d.decision_date ASC, final_rank ASC"
    else:
        order_by = "final_rank ASC, d.decision_date DESC"

    # Page results with enhanced ranking
    # Composite score: BM25 - freshness_boost - title_bonus - docket_bonus
    # (lower is better for BM25, so we subtract bonuses)
    rows = conn.execute(
        f"""
        WITH hits AS (
          SELECT rowid AS rid,
                 bm25(decisions_fts, 3.0, 2.0, 1.0) AS bm25_score
          FROM decisions_fts
          WHERE decisions_fts MATCH :fts
          LIMIT {COUNT_CAP}
        ),
        filtered AS (
          SELECT d.*,
                 h.bm25_score,
                 h.rid,
                 -- Freshness boost: documents newer than cutoff get bonus
                 CASE WHEN d.decision_date >= :freshness_cutoff THEN {FRESHNESS_WEIGHT} ELSE 0 END AS freshness_boost,
                 -- Title match bonus: if query appears in title
                 CASE WHEN lower(d.title) LIKE '%' || :fts_lower || '%' THEN {TITLE_MATCH_BONUS} ELSE 0 END AS title_bonus,
                 -- Docket match bonus: if query matches docket
                 CASE WHEN lower(d.docket) LIKE '%' || :fts_lower || '%' THEN {DOCKET_MATCH_BONUS} ELSE 0 END AS docket_bonus
          FROM hits h
          JOIN decisions d ON d.rowid = h.rid
          WHERE 1=1 {filter_sql}
        ),
        ranked AS (
          SELECT *,
                 (bm25_score - freshness_boost - title_bonus - docket_bonus) AS final_rank
          FROM filtered
        )
        SELECT d.id, d.source_name, d.canton, d.level, d.language, d.docket,
               d.decision_date, d.publication_date, d.title, d.url, d.pdf_url,
               snippet(decisions_fts, 2, '<mark>', '</mark>', '…', 24) AS snippet,
               ranked.final_rank AS rank
        FROM ranked
        JOIN decisions_fts ON decisions_fts.rowid = ranked.rid
        JOIN decisions d ON d.rowid = ranked.rid
        ORDER BY {order_by}
        LIMIT :limit OFFSET :offset;
        """,
        params,
    ).fetchall()

    # Capped count (fast-ish)
    count_row = conn.execute(
        f"""
        WITH hits AS (
          SELECT rowid AS rid
          FROM decisions_fts
          WHERE decisions_fts MATCH :fts
          LIMIT {COUNT_CAP}
        ),
        filtered AS (
          SELECT 1
          FROM hits h
          JOIN decisions d ON d.rowid = h.rid
          WHERE 1=1 {filter_sql}
          LIMIT {COUNT_CAP + 1}
        )
        SELECT COUNT(*) AS n FROM filtered;
        """,
        params,
    ).fetchone()
    total = int(count_row["n"])
    total_capped = total >= COUNT_CAP

    facets = _facets_fts(conn, fts=fts, filter_sql=filter_sql, params=params)

    return {
        "query": q,
        "page": page,
        "page_size": page_size,
        "total": min(total, COUNT_CAP),
        "total_capped": total_capped,
        "results": [dict(r) for r in rows],
        "facets": facets,
    }


def _facets_browse(conn: sqlite3.Connection, *, where: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # Basic facets for browse mode. Keep fast; no sampling needed.
    facets = {}
    facets["language"] = [
        dict(r)
        for r in conn.execute(
            f"SELECT language AS value, COUNT(*) AS count FROM decisions WHERE {where} GROUP BY language ORDER BY count DESC LIMIT 20;",
            params,
        ).fetchall()
    ]
    facets["canton"] = [
        dict(r)
        for r in conn.execute(
            f"SELECT canton AS value, COUNT(*) AS count FROM decisions WHERE {where} GROUP BY canton ORDER BY count DESC LIMIT 30;",
            params,
        ).fetchall()
    ]
    facets["source_name"] = [
        dict(r)
        for r in conn.execute(
            f"SELECT source_name AS value, COUNT(*) AS count FROM decisions WHERE {where} GROUP BY source_name ORDER BY count DESC LIMIT 30;",
            params,
        ).fetchall()
    ]
    return facets


def _facets_fts(conn: sqlite3.Connection, *, fts: str, filter_sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # Facets computed on a sample of top hits to keep latency predictable.
    facets = {}
    facets["language"] = [
        dict(r)
        for r in conn.execute(
            f"""
            WITH hits AS (
              SELECT rowid AS rid
              FROM decisions_fts
              WHERE decisions_fts MATCH :fts
              LIMIT {FACET_SAMPLE_LIMIT}
            ),
            filtered AS (
              SELECT d.language AS value
              FROM hits h
              JOIN decisions d ON d.rowid = h.rid
              WHERE 1=1 {filter_sql}
            )
            SELECT value, COUNT(*) AS count
            FROM filtered
            GROUP BY value
            ORDER BY count DESC
            LIMIT 20;
            """,
            params,
        ).fetchall()
    ]
    facets["canton"] = [
        dict(r)
        for r in conn.execute(
            f"""
            WITH hits AS (
              SELECT rowid AS rid
              FROM decisions_fts
              WHERE decisions_fts MATCH :fts
              LIMIT {FACET_SAMPLE_LIMIT}
            ),
            filtered AS (
              SELECT d.canton AS value
              FROM hits h
              JOIN decisions d ON d.rowid = h.rid
              WHERE 1=1 {filter_sql}
            )
            SELECT value, COUNT(*) AS count
            FROM filtered
            GROUP BY value
            ORDER BY count DESC
            LIMIT 30;
            """,
            params,
        ).fetchall()
    ]
    facets["source_name"] = [
        dict(r)
        for r in conn.execute(
            f"""
            WITH hits AS (
              SELECT rowid AS rid
              FROM decisions_fts
              WHERE decisions_fts MATCH :fts
              LIMIT {FACET_SAMPLE_LIMIT}
            ),
            filtered AS (
              SELECT d.source_name AS value
              FROM hits h
              JOIN decisions d ON d.rowid = h.rid
              WHERE 1=1 {filter_sql}
            )
            SELECT value, COUNT(*) AS count
            FROM filtered
            GROUP BY value
            ORDER BY count DESC
            LIMIT 30;
            """,
            params,
        ).fetchall()
    ]
    return facets


def get_doc(conn: sqlite3.Connection, doc_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT id, source_id, source_name, level, canton, court, chamber, language, docket,
               decision_date, publication_date, title, url, pdf_url, content_text, fetched_at, updated_at
        FROM decisions
        WHERE id = ?
        LIMIT 1;
        """,
        (doc_id,),
    ).fetchone()
    return dict(row) if row else None


def suggest(conn: sqlite3.Connection, prefix: str, limit: int = 8) -> List[Dict[str, Any]]:
    p = prefix.strip()
    if not p:
        return []
    # Suggest by titles/dockets of top matches for a prefix query
    # Example: "6B_12*" or "steuer*"
    q = f"{p}*"
    rows = conn.execute(
        """
        SELECT d.id, d.title, d.docket, d.source_name, d.decision_date
        FROM decisions_fts
        JOIN decisions d ON d.rowid = decisions_fts.rowid
        WHERE decisions_fts MATCH ?
        ORDER BY bm25(decisions_fts) ASC
        LIMIT ?;
        """,
        (q, int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def search_for_export(
    conn: sqlite3.Connection,
    *,
    q: str,
    filters: Optional[Dict[str, Any]] = None,
    max_results: int = 1000,
) -> List[Dict[str, Any]]:
    """Search and return results for CSV export (no pagination, limited fields)."""
    filters = filters or {}
    filter_sql, filter_params = _build_filter_sql(filters)

    if not q.strip():
        # Browse mode
        where = "1=1" + filter_sql.replace("d.", "")
        rows = conn.execute(
            f"""
            SELECT id, docket, title, decision_date, court, canton, language, level, source_name, url, pdf_url
            FROM decisions
            WHERE {where}
            ORDER BY decision_date DESC
            LIMIT :max_results;
            """,
            {**filter_params, "max_results": max_results},
        ).fetchall()
    else:
        # FTS search
        fts = q.strip()
        params = {"fts": fts, "max_results": max_results, **filter_params}
        rows = conn.execute(
            f"""
            WITH hits AS (
              SELECT rowid AS rid, bm25(decisions_fts, 3.0, 2.0, 1.0) AS score
              FROM decisions_fts
              WHERE decisions_fts MATCH :fts
              LIMIT {max_results * 2}
            ),
            filtered AS (
              SELECT d.id, d.docket, d.title, d.decision_date, d.court, d.canton,
                     d.language, d.level, d.source_name, d.url, d.pdf_url, h.score
              FROM hits h
              JOIN decisions d ON d.rowid = h.rid
              WHERE 1=1 {filter_sql}
            )
            SELECT id, docket, title, decision_date, court, canton, language, level, source_name, url, pdf_url
            FROM filtered
            ORDER BY score ASC
            LIMIT :max_results;
            """,
            params,
        ).fetchall()

    return [dict(r) for r in rows]
