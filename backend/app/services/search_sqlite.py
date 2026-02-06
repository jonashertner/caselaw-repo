"""SQLite-compatible search service.

This module provides search functionality for SQLite databases,
used in HuggingFace Spaces deployment where PostgreSQL is not available.
Uses SQLite FTS5 for full-text search.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlmodel import Session

logger = logging.getLogger(__name__)


@dataclass
class SearchFilters:
    source_ids: Optional[list[str]] = None
    level: Optional[str] = None
    canton: Optional[str] = None
    language: Optional[str] = None
    date_from: Optional[dt.date] = None
    date_to: Optional[dt.date] = None


@dataclass
class DecisionMinimal:
    id: str
    source_id: str
    source_name: str
    level: str
    canton: Optional[str] = None
    court: Optional[str] = None
    docket: Optional[str] = None
    decision_date: Optional[dt.date] = None
    title: Optional[str] = None
    language: Optional[str] = None
    url: str = ""
    pdf_url: Optional[str] = None

    def model_dump(self) -> dict:
        """Return dict representation for compatibility with SQLModel."""
        return {
            "id": self.id,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "level": self.level,
            "canton": self.canton,
            "court": self.court,
            "docket": self.docket,
            "decision_date": self.decision_date,
            "title": self.title,
            "language": self.language,
            "url": self.url,
            "pdf_url": self.pdf_url,
        }


@dataclass
class SearchHit:
    decision: DecisionMinimal
    score: float
    snippet: str
    chunk_id: Optional[str] = None
    chunk_index: Optional[int] = None


# Swiss canton codes for detection
CANTON_CODES = {
    "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
    "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
    "TI", "UR", "VD", "VS", "ZG", "ZH"
}

# Patterns for Swiss court docket numbers
DOCKET_PATTERNS = [
    # Federal Supreme Court: 6B_316/2015, 4A_123/2020
    re.compile(r'\b\d[A-Z]_\d+/\d{4}\b', re.IGNORECASE),
    # BVGer: E-5164/2007, A-1234/2020
    re.compile(r'\b[A-Z]-\d+/\d{4}\b', re.IGNORECASE),
    # BGE references: BGE 143 IV 241
    re.compile(r'\bBGE\s+\d+\s+[IVX]+\s+\d+\b', re.IGNORECASE),
]


def _detect_docket_pattern(query: str) -> Optional[str]:
    """Detect if query contains a docket number pattern."""
    for pattern in DOCKET_PATTERNS:
        match = pattern.search(query)
        if match:
            return match.group()
    return None


def _extract_snippet(text: str, query: str, max_len: int = 400) -> str:
    """Extract a relevant snippet around query terms."""
    if not text:
        return ""

    query_words = [w.lower() for w in query.split() if len(w) > 2]
    text_lower = text.lower()

    best_pos = 0
    for word in query_words:
        pos = text_lower.find(word)
        if pos != -1:
            best_pos = max(0, pos - 100)
            break

    snippet = text[best_pos:best_pos + max_len]
    if best_pos > 0:
        snippet = "..." + snippet
    if best_pos + max_len < len(text):
        snippet = snippet + "..."

    return snippet.strip()


def search_sqlite(
    session: Session,
    query: str,
    *,
    filters: SearchFilters,
    limit: int = 20,
    offset: int = 0,
    api_key: Optional[str] = None  # Ignored for SQLite (no embeddings)
) -> list[SearchHit]:
    """Search decisions using SQLite FTS5.

    Args:
        session: Database session
        query: Search query string
        filters: Search filters
        limit: Maximum results
        offset: Pagination offset

    Returns:
        List of SearchHit objects
    """
    query = (query or "").strip()
    if not query:
        return []

    # Check for docket number pattern first
    docket_pattern = _detect_docket_pattern(query)
    if docket_pattern:
        hits = _docket_search_sqlite(session, docket_pattern, filters=filters, limit=limit)
        if hits:
            return hits

    # Full-text search using FTS5
    return _fts_search_sqlite(session, query, filters=filters, limit=limit, offset=offset)


def _build_filter_sql(filters: SearchFilters, params: dict) -> str:
    """Build WHERE clause conditions for filters."""
    conditions = []

    if filters.source_ids:
        placeholders = ", ".join(f":source_id_{i}" for i in range(len(filters.source_ids)))
        conditions.append(f"d.source_id IN ({placeholders})")
        for i, sid in enumerate(filters.source_ids):
            params[f"source_id_{i}"] = sid
    if filters.level:
        conditions.append("d.level = :level")
        params["level"] = filters.level
    if filters.canton:
        conditions.append("d.canton = :canton")
        params["canton"] = filters.canton
    if filters.language:
        conditions.append("d.language = :language")
        params["language"] = filters.language
    if filters.date_from:
        conditions.append("d.decision_date >= :date_from")
        params["date_from"] = str(filters.date_from)
    if filters.date_to:
        conditions.append("d.decision_date <= :date_to")
        params["date_to"] = str(filters.date_to)

    return " AND ".join(conditions) if conditions else "1=1"


def _docket_search_sqlite(
    session: Session,
    docket: str,
    *,
    filters: SearchFilters,
    limit: int
) -> list[SearchHit]:
    """Search by docket number (exact or LIKE match)."""
    params: dict = {"docket_exact": docket, "docket_like": f"%{docket}%", "limit": limit}
    filter_sql = _build_filter_sql(filters, params)

    sql = text(f"""
        SELECT
            d.id, d.source_id, d.source_name, d.level, d.canton, d.court,
            d.docket, d.decision_date, d.title, d.language, d.url, d.pdf_url,
            substr(d.content_text, 1, 1000) as snippet_text,
            CASE
                WHEN d.docket = :docket_exact THEN 100.0
                WHEN d.docket LIKE :docket_like THEN 80.0
                ELSE 50.0
            END as score
        FROM decisions d
        WHERE (d.docket = :docket_exact OR d.docket LIKE :docket_like OR d.title LIKE :docket_like)
        AND {filter_sql}
        ORDER BY score DESC, d.decision_date DESC
        LIMIT :limit
    """)

    try:
        result = session.execute(sql, params)
        rows = result.fetchall()
    except Exception as e:
        logger.warning("SQLite docket search failed: %s", e)
        return []

    return _rows_to_hits(rows, docket)


def _fts_search_sqlite(
    session: Session,
    query: str,
    *,
    filters: SearchFilters,
    limit: int,
    offset: int
) -> list[SearchHit]:
    """Full-text search using SQLite FTS5."""
    # Escape special FTS5 characters and format query
    fts_query = _format_fts5_query(query)

    params: dict = {"query": fts_query, "limit": limit, "offset": offset}
    filter_sql = _build_filter_sql(filters, params)

    # Use FTS5 MATCH with bm25 scoring
    sql = text(f"""
        SELECT
            d.id, d.source_id, d.source_name, d.level, d.canton, d.court,
            d.docket, d.decision_date, d.title, d.language, d.url, d.pdf_url,
            substr(d.content_text, 1, 1000) as snippet_text,
            bm25(decisions_fts) as score
        FROM decisions_fts
        JOIN decisions d ON decisions_fts.id = d.id
        WHERE decisions_fts MATCH :query
        AND {filter_sql}
        ORDER BY score
        LIMIT :limit OFFSET :offset
    """)

    try:
        result = session.execute(sql, params)
        rows = result.fetchall()
    except Exception as e:
        logger.warning("SQLite FTS search failed: %s, trying fallback", e)
        return _fallback_like_search(session, query, filters=filters, limit=limit, offset=offset)

    if not rows:
        # Fallback to LIKE search if FTS returns nothing
        return _fallback_like_search(session, query, filters=filters, limit=limit, offset=offset)

    return _rows_to_hits(rows, query)


def _format_fts5_query(query: str) -> str:
    """Format query for FTS5 MATCH.

    FTS5 uses a different query syntax. We convert natural language
    to a format that works well with FTS5.
    """
    # Remove special characters that could break FTS5
    query = re.sub(r'[^\w\s\-]', ' ', query)

    # Split into words and filter short ones
    words = [w.strip() for w in query.split() if len(w.strip()) >= 2]

    if not words:
        return query

    # Use prefix matching for the last word (partial typing support)
    # and regular matching for other words
    if len(words) == 1:
        return f"{words[0]}*"

    # Join with OR for broader matches
    return " OR ".join(words)


def _fallback_like_search(
    session: Session,
    query: str,
    *,
    filters: SearchFilters,
    limit: int,
    offset: int
) -> list[SearchHit]:
    """Fallback to LIKE search when FTS5 is not available or returns nothing."""
    params: dict = {"query": f"%{query}%", "limit": limit, "offset": offset}
    filter_sql = _build_filter_sql(filters, params)

    sql = text(f"""
        SELECT
            d.id, d.source_id, d.source_name, d.level, d.canton, d.court,
            d.docket, d.decision_date, d.title, d.language, d.url, d.pdf_url,
            substr(d.content_text, 1, 1000) as snippet_text,
            1.0 as score
        FROM decisions d
        WHERE (d.content_text LIKE :query OR d.title LIKE :query OR d.docket LIKE :query)
        AND {filter_sql}
        ORDER BY d.decision_date DESC
        LIMIT :limit OFFSET :offset
    """)

    try:
        result = session.execute(sql, params)
        rows = result.fetchall()
    except Exception as e:
        logger.error("SQLite fallback search failed: %s", e)
        return []

    return _rows_to_hits(rows, query)


def _rows_to_hits(rows, query: str) -> list[SearchHit]:
    """Convert database rows to SearchHit objects."""
    hits: list[SearchHit] = []
    for row in rows:
        snippet = _extract_snippet(row.snippet_text or "", query)

        # Parse decision_date if it's a string
        decision_date = row.decision_date
        if isinstance(decision_date, str) and decision_date:
            try:
                decision_date = dt.date.fromisoformat(decision_date)
            except ValueError:
                decision_date = None

        hits.append(
            SearchHit(
                decision=DecisionMinimal(
                    id=row.id,
                    source_id=row.source_id,
                    source_name=row.source_name,
                    level=row.level,
                    canton=row.canton,
                    court=row.court,
                    docket=row.docket,
                    decision_date=decision_date,
                    title=row.title,
                    language=row.language,
                    url=row.url,
                    pdf_url=row.pdf_url,
                ),
                score=abs(float(row.score)) if row.score else 0.0,
                snippet=snippet,
            )
        )
    return hits
