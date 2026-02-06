from __future__ import annotations

import datetime as dt
import logging
import re
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import func, case, literal, or_, and_
from sqlmodel import Session, select

from app.ai.embeddings import get_embeddings_provider
from app.models.chunk import Chunk
from app.models.decision import Decision, DecisionMinimal

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


def _detect_canton_in_query(query: str) -> Optional[str]:
    """Detect if query contains a canton code."""
    words = query.upper().split()
    for word in words:
        if word in CANTON_CODES:
            return word
    return None


def _detect_docket_pattern(query: str) -> Optional[str]:
    """Detect if query contains a docket number pattern."""
    for pattern in DOCKET_PATTERNS:
        match = pattern.search(query)
        if match:
            return match.group()
    return None


def _docket_search(
    session: Session,
    docket: str,
    *,
    filters: SearchFilters,
    limit: int
) -> list[SearchHit]:
    """Fast search by docket number - tries exact match first, then FTS."""
    from sqlalchemy import text

    # Build filter conditions
    filter_conditions = []
    params = {"docket_exact": docket, "docket_like": f"%{docket}%", "limit": limit}

    if filters.source_ids:
        filter_conditions.append("source_id = ANY(:source_ids)")
        params["source_ids"] = filters.source_ids
    if filters.level:
        filter_conditions.append("level = :level")
        params["level"] = filters.level
    if filters.canton:
        filter_conditions.append("canton = :canton")
        params["canton"] = filters.canton
    if filters.language:
        filter_conditions.append("language = :language")
        params["language"] = filters.language
    if filters.date_from:
        filter_conditions.append("decision_date >= :date_from")
        params["date_from"] = filters.date_from
    if filters.date_to:
        filter_conditions.append("decision_date <= :date_to")
        params["date_to"] = filters.date_to

    filter_sql = " AND ".join(filter_conditions) if filter_conditions else "TRUE"

    # Two-stage approach: exact docket match (fast, uses index), then ILIKE on docket/title only
    sql = text(f"""
        WITH exact_matches AS (
            SELECT id, source_id, source_name, level, canton, court, docket,
                   decision_date, title, language, url, pdf_url,
                   substr(content_text, 1, 1000) as snippet_text,
                   100.0 as score
            FROM decisions
            WHERE docket = :docket_exact
            AND {filter_sql}
            LIMIT :limit
        ),
        like_matches AS (
            SELECT id, source_id, source_name, level, canton, court, docket,
                   decision_date, title, language, url, pdf_url,
                   substr(content_text, 1, 1000) as snippet_text,
                   CASE WHEN docket ILIKE :docket_like THEN 80.0 ELSE 50.0 END as score
            FROM decisions
            WHERE (docket ILIKE :docket_like OR title ILIKE :docket_like)
            AND id NOT IN (SELECT id FROM exact_matches)
            AND {filter_sql}
            ORDER BY decision_date DESC NULLS LAST
            LIMIT :limit
        )
        SELECT * FROM exact_matches
        UNION ALL
        SELECT * FROM like_matches
        ORDER BY score DESC, decision_date DESC NULLS LAST
        LIMIT :limit
    """)

    try:
        result = session.execute(sql, params)
        rows = result.fetchall()
    except Exception as e:
        logger.warning("Docket search failed: %s", e)
        return []

    if not rows:
        return []

    out: list[SearchHit] = []
    for row in rows[:limit]:
        snippet = row.snippet_text[:400] if row.snippet_text else ""
        out.append(
            SearchHit(
                decision=DecisionMinimal(
                    id=row.id,
                    source_id=row.source_id,
                    source_name=row.source_name,
                    level=row.level,
                    canton=row.canton,
                    court=row.court,
                    docket=row.docket,
                    decision_date=row.decision_date,
                    title=row.title,
                    language=row.language,
                    url=row.url,
                    pdf_url=row.pdf_url,
                ),
                score=float(row.score),
                snippet=snippet,
            )
        )
    return out


def _apply_decision_filters(stmt, f: SearchFilters):
    if f.source_ids:
        stmt = stmt.where(Decision.source_id.in_(f.source_ids))
    if f.level:
        stmt = stmt.where(Decision.level == f.level)
    if f.canton:
        stmt = stmt.where(Decision.canton == f.canton)
    if f.language:
        stmt = stmt.where(Decision.language == f.language)
    if f.date_from:
        stmt = stmt.where(Decision.decision_date >= f.date_from)
    if f.date_to:
        stmt = stmt.where(Decision.decision_date <= f.date_to)
    return stmt


def _extract_snippet(text: str, query: str, max_len: int = 400) -> str:
    """Extract a relevant snippet around query terms."""
    if not text:
        return ""

    # Find first occurrence of any query word
    query_words = [w.lower() for w in query.split() if len(w) > 2]
    text_lower = text.lower()

    best_pos = 0
    for word in query_words:
        pos = text_lower.find(word)
        if pos != -1:
            best_pos = max(0, pos - 100)
            break

    # Extract snippet around the match
    snippet = text[best_pos:best_pos + max_len]
    if best_pos > 0:
        snippet = "..." + snippet
    if best_pos + max_len < len(text):
        snippet = snippet + "..."

    return snippet.strip()


def search(session: Session, query: str, *, filters: SearchFilters, limit: int = 20, offset: int = 0, api_key: Optional[str] = None) -> list[SearchHit]:
    query = (query or "").strip()
    if not query:
        return []

    # Detect docket number pattern for exact matching
    docket_pattern = _detect_docket_pattern(query)
    if docket_pattern:
        hits = _docket_search(session, docket_pattern, filters=filters, limit=limit)
        if hits:
            return hits

    # Detect canton in query for boosting
    detected_canton = _detect_canton_in_query(query)

    # Primary: Full-text search on decisions (works without embeddings)
    hits = _fts_decision_search(session, query, filters=filters, limit=limit, offset=offset, boost_canton=detected_canton)

    # If we have good FTS hits, return them
    if hits:
        return hits

    # Fallback: Try vector search on chunks if available
    hits = _vector_search(session, query, filters=filters, limit=limit, api_key=api_key)
    if hits:
        return hits

    # Last resort: chunk-based FTS
    return _fts_chunk_search(session, query, filters=filters, limit=limit)


def _fts_decision_search(
    session: Session,
    query: str,
    *,
    filters: SearchFilters,
    limit: int,
    offset: int = 0,
    boost_canton: Optional[str] = None
) -> list[SearchHit]:
    """Full-text search directly on Decision.content_text with relevance boosting.

    Uses a two-stage approach for performance:
    1. First, find candidates using the GIN index (fast, limited to 2000)
    2. Then, rank only the candidates (avoids ranking millions of rows)
    """
    from sqlalchemy import text

    # Build filter conditions using parameterized queries for safety
    filter_conditions = []
    params = {"query": query, "limit": limit, "offset": offset, "candidate_limit": 2000 + offset}

    if filters.source_ids:
        filter_conditions.append("source_id = ANY(:source_ids)")
        params["source_ids"] = filters.source_ids
    if filters.level:
        filter_conditions.append("level = :level")
        params["level"] = filters.level
    if filters.canton:
        filter_conditions.append("canton = :canton_filter")
        params["canton_filter"] = filters.canton
    if filters.language:
        filter_conditions.append("language = :language")
        params["language"] = filters.language
    if filters.date_from:
        filter_conditions.append("decision_date >= :date_from")
        params["date_from"] = filters.date_from
    if filters.date_to:
        filter_conditions.append("decision_date <= :date_to")
        params["date_to"] = filters.date_to

    filter_sql = " AND ".join(filter_conditions) if filter_conditions else "TRUE"

    # Canton boost - use parameterized if provided
    canton_boost_sql = "1.0"
    if boost_canton:
        canton_boost_sql = "CASE WHEN canton = :boost_canton THEN 3.0 ELSE 1.0 END"
        params["boost_canton"] = boost_canton

    # FTS expression matching the GIN index idx_decisions_fts exactly
    tsvector_expr = """(
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(docket, '')), 'A') ||
        setweight(to_tsvector('simple', substr(content_text, 1, 50000)), 'D')
    )"""

    # Two-stage query: first get candidates (fast), then rank (limited set)
    # Stage 1 uses index, orders by decision_date DESC as a proxy for relevance
    # Stage 2 applies expensive ts_rank_cd only to the limited candidate set
    sql = text(f"""
        WITH candidates AS (
            SELECT id, source_id, source_name, level, canton, court, docket,
                   decision_date, title, language, url, pdf_url,
                   substr(content_text, 1, 1000) as snippet_text,
                   content_text
            FROM decisions
            WHERE {tsvector_expr} @@ websearch_to_tsquery('simple', :query)
            AND {filter_sql}
            ORDER BY decision_date DESC NULLS LAST
            LIMIT :candidate_limit
        )
        SELECT
            id, source_id, source_name, level, canton, court, docket,
            decision_date, title, language, url, pdf_url, snippet_text,
            (
                ts_rank_cd(
                    (setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
                     setweight(to_tsvector('simple', coalesce(docket, '')), 'A') ||
                     setweight(to_tsvector('simple', substr(content_text, 1, 50000)), 'D')),
                    websearch_to_tsquery('simple', :query)
                ) * {canton_boost_sql} *
                CASE
                    WHEN decision_date >= CURRENT_DATE - 365 THEN 1.2
                    WHEN decision_date >= CURRENT_DATE - 1095 THEN 1.1
                    ELSE 1.0
                END
            ) as score
        FROM candidates
        ORDER BY score DESC
        LIMIT :limit OFFSET :offset
    """)

    try:
        result = session.execute(sql, params)
        rows = result.fetchall()
    except Exception as e:
        logger.warning("FTS decision search failed: %s", e)
        return []

    if not rows:
        return []

    out: list[SearchHit] = []
    for row in rows[:limit]:
        snippet = _extract_snippet(row.snippet_text or "", query)
        out.append(
            SearchHit(
                decision=DecisionMinimal(
                    id=row.id,
                    source_id=row.source_id,
                    source_name=row.source_name,
                    level=row.level,
                    canton=row.canton,
                    court=row.court,
                    docket=row.docket,
                    decision_date=row.decision_date,
                    title=row.title,
                    language=row.language,
                    url=row.url,
                    pdf_url=row.pdf_url,
                ),
                score=float(row.score) if row.score else 0.0,
                snippet=snippet,
            )
        )
    return out


def _vector_search(session: Session, query: str, *, filters: SearchFilters, limit: int, api_key: Optional[str] = None) -> list[SearchHit]:
    try:
        embedder = get_embeddings_provider(api_key=api_key)
        q_emb = embedder.embed([query])[0]
    except Exception as e:
        logger.info("Vector search disabled (embedding failed): %s", e)
        return []

    # Fetch top chunks by cosine distance, then aggregate to decisions.
    distance = Chunk.embedding.cosine_distance(q_emb)  # type: ignore[attr-defined]

    stmt = (
        select(Chunk, Decision, distance.label("distance"))
        .join(Decision, Decision.id == Chunk.decision_id)
        .where(Chunk.embedding.is_not(None))
        .order_by(distance.asc())
        .limit(max(limit * 8, 80))
    )
    stmt = _apply_decision_filters(stmt, filters)
    rows = session.exec(stmt).all()

    # Aggregate by decision: keep the best (smallest) distance and one representative chunk.
    best: dict[str, tuple[float, Chunk, Decision]] = {}
    for ch, dec, dist in rows:
        if dec.id not in best or dist < best[dec.id][0]:
            best[dec.id] = (float(dist), ch, dec)

    # Convert cosine distance -> similarity score (higher is better). Clamp.
    scored = []
    for dist, ch, dec in best.values():
        score = max(0.0, 1.0 - dist)
        scored.append((score, ch, dec))
    scored.sort(key=lambda x: x[0], reverse=True)
    scored = scored[:limit]

    out: list[SearchHit] = []
    for score, ch, dec in scored:
        snippet = ch.text[:800]
        out.append(
            SearchHit(
                decision=DecisionMinimal(
                    id=dec.id,
                    source_id=dec.source_id,
                    source_name=dec.source_name,
                    level=dec.level,
                    canton=dec.canton,
                    court=dec.court,
                    docket=dec.docket,
                    decision_date=dec.decision_date,
                    title=dec.title,
                    language=dec.language,
                    url=dec.url,
                    pdf_url=dec.pdf_url,
                ),
                score=score,
                snippet=snippet,
                chunk_id=ch.id,
                chunk_index=ch.chunk_index,
            )
        )
    return out


def _fts_chunk_search(session: Session, query: str, *, filters: SearchFilters, limit: int) -> list[SearchHit]:
    """Fallback: Full-text search on chunks (legacy behavior)."""
    # Use websearch_to_tsquery if available, fallback to plainto_tsquery.
    tsq = func.websearch_to_tsquery("simple", query)
    tsv = func.to_tsvector("simple", Chunk.text)
    rank = func.ts_rank_cd(tsv, tsq)

    stmt = (
        select(Chunk, Decision, rank.label("rank"))
        .join(Decision, Decision.id == Chunk.decision_id)
        .where(tsv.op("@@")(tsq))
        .order_by(rank.desc())
        .limit(max(limit * 8, 80))
    )
    stmt = _apply_decision_filters(stmt, filters)
    rows = session.exec(stmt).all()
    if not rows:
        return []

    best: dict[str, tuple[float, Chunk, Decision]] = {}
    for ch, dec, r in rows:
        rr = float(r)
        if dec.id not in best or rr > best[dec.id][0]:
            best[dec.id] = (rr, ch, dec)

    scored = sorted(best.values(), key=lambda x: x[0], reverse=True)[:limit]

    out: list[SearchHit] = []
    for r, ch, dec in scored:
        snippet = ch.text[:800]
        out.append(
            SearchHit(
                decision=DecisionMinimal(
                    id=dec.id,
                    source_id=dec.source_id,
                    source_name=dec.source_name,
                    level=dec.level,
                    canton=dec.canton,
                    court=dec.court,
                    docket=dec.docket,
                    decision_date=dec.decision_date,
                    title=dec.title,
                    language=dec.language,
                    url=dec.url,
                    pdf_url=dec.pdf_url,
                ),
                score=r,
                snippet=snippet,
                chunk_id=ch.id,
                chunk_index=ch.chunk_index,
            )
        )
    return out
