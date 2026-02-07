from __future__ import annotations

import logging
from typing import Optional

from fastapi import Depends, FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, select, func

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import get_session
from app.db.init_db import init_db

from app.models.decision import Decision
from app.schemas.answer import AnswerRequest, AnswerResponse, Citation as CitationSchema
from app.schemas.search import SearchRequest, SearchResponse, SearchHit as SearchHitSchema, SearchDecision
from app.services.answer import answer_question
from app.services.source_registry import SourceRegistry

settings = get_settings()
configure_logging()
logger = logging.getLogger(__name__)

def _is_sqlite() -> bool:
    return settings.database_url.startswith("sqlite")

_SQLITE_MODE = _is_sqlite()

if _SQLITE_MODE:
    logger.info("Search backend: SQLITE (database_url=%s)", settings.database_url[:50])
    from app.services.search_sqlite import search_sqlite as search, SearchFilters
else:
    logger.info("Search backend: POSTGRESQL (database_url=%s)", settings.database_url[:50])
    from app.services.search import search, SearchFilters

app = FastAPI(title=settings.app_name, version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def db_session() -> Session:
    with get_session() as s:
        yield s


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "search_backend": "sqlite" if _is_sqlite() else "postgres"}


@app.get("/api/sources")
def list_sources() -> list[dict]:
    reg = SourceRegistry.load_default()
    return [
        {
            "id": s.id,
            "name": s.name,
            "level": s.level,
            "canton": s.canton,
            "homepage": s.homepage,
            "languages": s.languages,
            "notes": s.notes,
            "connector": s.connector,
        }
        for s in reg.list()
    ]


@app.post("/api/search", response_model=SearchResponse)
def api_search(
    req: SearchRequest,
    session: Session = Depends(db_session),
    x_openai_key: Optional[str] = Header(None, alias="X-OpenAI-Key"),
) -> SearchResponse:
    filters = SearchFilters(
        source_ids=req.source_ids,
        level=req.level,
        canton=req.canton,
        language=req.language,
        date_from=req.date_from,
        date_to=req.date_to,
    )

    try:
        logger.info("Search request: query=%r", req.query[:100] if req.query else "")
        result = search(session, req.query, filters=filters, limit=req.limit, offset=req.offset, api_key=x_openai_key)
    except Exception as e:
        logger.error("Search failed: %s", e, exc_info=True)
        raise

    hits = result
    out_hits: list[SearchHitSchema] = []
    for h in hits:
        out_hits.append(
            SearchHitSchema(
                decision=SearchDecision(**h.decision.model_dump()),
                score=h.score,
                snippet=h.snippet,
                chunk_id=getattr(h, 'chunk_id', None),
                chunk_index=getattr(h, 'chunk_index', None),
            )
        )
    return SearchResponse(hits=out_hits, total=None, offset=req.offset, limit=req.limit)


@app.get("/api/decisions/{decision_id}")
def get_decision(decision_id: str, session: Session = Depends(db_session)) -> dict:
    dec = session.exec(select(Decision).where(Decision.id == decision_id)).first()
    if not dec:
        return {"error": "not_found"}
    return {
        "id": dec.id,
        "source_id": dec.source_id,
        "source_name": dec.source_name,
        "level": dec.level,
        "canton": dec.canton,
        "court": dec.court,
        "chamber": dec.chamber,
        "docket": dec.docket,
        "decision_date": dec.decision_date,
        "published_date": dec.published_date,
        "title": dec.title,
        "language": dec.language,
        "url": dec.url,
        "pdf_url": dec.pdf_url,
        "content_text": dec.content_text,
        "meta": dec.meta,
    }


@app.get("/api/stats")
def api_stats(session: Session = Depends(db_session)) -> dict:
    """Return comprehensive ingestion statistics."""
    import datetime as dt
    from sqlalchemy import text

    reg = SourceRegistry.load_default()
    is_sqlite = _is_sqlite()

    # Get counts per source_id
    counts_query = (
        select(Decision.source_id, func.count(Decision.id))
        .group_by(Decision.source_id)
    )
    counts_result = session.exec(counts_query).all()
    counts_by_source = {row[0]: row[1] for row in counts_result}

    # Get counts by level
    level_query = (
        select(Decision.level, func.count(Decision.id))
        .group_by(Decision.level)
    )
    level_result = session.exec(level_query).all()
    counts_by_level = {row[0]: row[1] for row in level_result}

    # Get counts by canton
    canton_query = (
        select(Decision.canton, func.count(Decision.id))
        .where(Decision.canton.isnot(None))
        .group_by(Decision.canton)
    )
    canton_result = session.exec(canton_query).all()
    counts_by_canton = {row[0]: row[1] for row in canton_result}

    # Get counts by year (decision_date) - use database-specific SQL
    if is_sqlite:
        year_sql = text("""
            SELECT CAST(strftime('%Y', decision_date) AS INTEGER) as year, COUNT(*) as count
            FROM decisions
            WHERE decision_date IS NOT NULL AND decision_date != ''
            GROUP BY year
            ORDER BY year
        """)
    else:
        year_sql = text("""
            SELECT EXTRACT(YEAR FROM decision_date)::int as year, COUNT(*) as count
            FROM decisions
            WHERE decision_date IS NOT NULL
            GROUP BY year
            ORDER BY year
        """)
    year_result = session.execute(year_sql).fetchall()
    counts_by_year = {int(row[0]): row[1] for row in year_result if row[0]}

    # Get counts by language
    lang_query = (
        select(Decision.language, func.count(Decision.id))
        .where(Decision.language.isnot(None))
        .group_by(Decision.language)
    )
    lang_result = session.exec(lang_query).all()
    counts_by_language = {row[0]: row[1] for row in lang_result}

    # Recent ingestion counts
    recent_24h = session.exec(
        select(func.count(Decision.id)).where(
            Decision.decision_date >= (dt.date.today() - dt.timedelta(days=1))
        )
    ).one() or 0

    recent_7d = session.exec(
        select(func.count(Decision.id)).where(
            Decision.decision_date >= (dt.date.today() - dt.timedelta(days=7))
        )
    ).one() or 0

    recent_30d = session.exec(
        select(func.count(Decision.id)).where(
            Decision.decision_date >= (dt.date.today() - dt.timedelta(days=30))
        )
    ).one() or 0

    # Get total count
    total = sum(counts_by_source.values())

    # Build source stats
    sources_stats = []
    for source in reg.list():
        count = counts_by_source.get(source.id, 0)
        sources_stats.append({
            "id": source.id,
            "name": source.name,
            "level": source.level,
            "canton": source.canton,
            "connector": source.connector,
            "count": count,
            "status": "indexed" if count > 0 else "pending",
        })

    # Sort: indexed sources first (by count desc), then pending
    sources_stats.sort(key=lambda x: (-x["count"], x["id"]))

    return {
        "total_decisions": total,
        "federal_decisions": counts_by_level.get("federal", 0),
        "cantonal_decisions": counts_by_level.get("cantonal", 0),
        "decisions_by_canton": counts_by_canton,
        "decisions_by_year": counts_by_year,
        "decisions_by_language": counts_by_language,
        "recent_decisions": {
            "last_24h": recent_24h,
            "last_7d": recent_7d,
            "last_30d": recent_30d,
        },
        "coverage": {
            "total_sources": len(reg.list()),
            "indexed_sources": len([s for s in sources_stats if s["count"] > 0]),
            "pending_sources": len([s for s in sources_stats if s["count"] == 0]),
            "cantons_covered": len(counts_by_canton),
        },
        "sources": sources_stats,
    }


@app.get("/api/stats/coverage")
def api_coverage(session: Session = Depends(db_session)) -> dict:
    """Return detailed coverage statistics."""
    is_sqlite = _is_sqlite()

    # Date range
    date_range = session.exec(
        select(
            func.min(Decision.decision_date),
            func.max(Decision.decision_date)
        )
    ).one()

    # Count decisions with embeddings (have chunks) - skip for SQLite (no chunks table)
    embedded_count = 0
    if not is_sqlite:
        try:
            from app.models.chunk import Chunk
            embedded_count = session.exec(
                select(func.count(func.distinct(Chunk.decision_id))).where(
                    Chunk.embedding.isnot(None)
                )
            ).one() or 0
        except Exception:
            pass

    total_decisions = session.exec(select(func.count(Decision.id))).one()

    # Court distribution
    court_query = (
        select(Decision.court, func.count(Decision.id))
        .where(Decision.court.isnot(None))
        .group_by(Decision.court)
        .order_by(func.count(Decision.id).desc())
        .limit(20)
    )
    court_result = session.exec(court_query).all()
    top_courts = {row[0]: row[1] for row in court_result}

    return {
        "date_range": {
            "earliest": str(date_range[0]) if date_range[0] else None,
            "latest": str(date_range[1]) if date_range[1] else None,
        },
        "embeddings": {
            "decisions_with_embeddings": embedded_count,
            "decisions_total": total_decisions,
            "coverage_percent": round(embedded_count / total_decisions * 100, 1) if total_decisions > 0 else 0,
        },
        "top_courts": top_courts,
    }


@app.get("/api/stats/detailed")
def api_stats_detailed(
    session: Session = Depends(db_session),
    group_by: str = "source",  # source, canton, court
    year_from: int = 2000,
    year_to: int = 2030,
) -> dict:
    """Return detailed cross-tabulated statistics."""
    from sqlalchemy import text
    is_sqlite = _is_sqlite()

    # Determine grouping column - use COALESCE for fallbacks
    # For canton: federal decisions have NULL canton, so show level instead
    # For court: many scrapers only set source_name, not court
    group_col = {
        "source": "source_id",
        "canton": "COALESCE(canton, CASE WHEN level = 'federal' THEN 'Federal' ELSE source_name END)",
        "court": "COALESCE(court, source_name)",
    }.get(group_by, "source_id")

    # Query: group x year matrix (database-specific SQL)
    if is_sqlite:
        sql = text(f"""
            SELECT
                COALESCE({group_col}, 'unknown') as group_key,
                CAST(strftime('%Y', decision_date) AS INTEGER) as year,
                COUNT(*) as count
            FROM decisions
            WHERE decision_date IS NOT NULL AND decision_date != ''
              AND CAST(strftime('%Y', decision_date) AS INTEGER) >= :year_from
              AND CAST(strftime('%Y', decision_date) AS INTEGER) <= :year_to
            GROUP BY group_key, year
            ORDER BY group_key, year
        """)
    else:
        sql = text(f"""
            SELECT
                COALESCE({group_col}, 'unknown') as group_key,
                EXTRACT(YEAR FROM decision_date)::int as year,
                COUNT(*) as count
            FROM decisions
            WHERE decision_date IS NOT NULL
              AND EXTRACT(YEAR FROM decision_date) >= :year_from
              AND EXTRACT(YEAR FROM decision_date) <= :year_to
            GROUP BY group_key, year
            ORDER BY group_key, year
        """)

    result = session.execute(sql, {"year_from": year_from, "year_to": year_to})
    rows = result.fetchall()

    # Build matrix structure
    matrix: dict[str, dict[int, int]] = {}
    years_set: set[int] = set()

    for row in rows:
        group_key, year, count = row
        if year is None:
            continue
        year = int(year)
        years_set.add(year)
        if group_key not in matrix:
            matrix[group_key] = {}
        matrix[group_key][year] = count

    years = sorted(years_set)

    # Calculate totals
    totals_by_year: dict[int, int] = {}
    totals_by_group: dict[str, int] = {}

    for group_key, year_counts in matrix.items():
        totals_by_group[group_key] = sum(year_counts.values())
        for year, count in year_counts.items():
            totals_by_year[year] = totals_by_year.get(year, 0) + count

    # Sort groups by total count descending
    sorted_groups = sorted(matrix.keys(), key=lambda g: totals_by_group.get(g, 0), reverse=True)

    return {
        "group_by": group_by,
        "years": years,
        "groups": sorted_groups[:50],  # Top 50 groups
        "matrix": {g: matrix[g] for g in sorted_groups[:50]},
        "totals_by_year": totals_by_year,
        "totals_by_group": {g: totals_by_group[g] for g in sorted_groups[:50]},
    }


@app.get("/api/stats/trends")
def api_stats_trends(
    session: Session = Depends(db_session),
) -> dict:
    """Return trend data for visualizations."""
    from sqlalchemy import text
    import datetime as dt
    is_sqlite = _is_sqlite()

    # Monthly counts for last 5 years
    if is_sqlite:
        sql = text("""
            SELECT
                CAST(strftime('%Y', decision_date) AS INTEGER) as year,
                CAST(strftime('%m', decision_date) AS INTEGER) as month,
                level,
                COUNT(*) as count
            FROM decisions
            WHERE decision_date >= date('now', '-5 years')
              AND decision_date IS NOT NULL AND decision_date != ''
            GROUP BY year, month, level
            ORDER BY year, month
        """)
    else:
        sql = text("""
            SELECT
                EXTRACT(YEAR FROM decision_date)::int as year,
                EXTRACT(MONTH FROM decision_date)::int as month,
                level,
                COUNT(*) as count
            FROM decisions
            WHERE decision_date >= CURRENT_DATE - INTERVAL '5 years'
              AND decision_date IS NOT NULL
            GROUP BY year, month, level
            ORDER BY year, month
        """)

    result = session.execute(sql)
    rows = result.fetchall()

    monthly: dict[str, list] = {"federal": [], "cantonal": []}

    for row in rows:
        year, month, level, count = row
        if year and month and level:
            key = f"{int(year)}-{int(month):02d}"
            if level in monthly:
                monthly[level].append({"month": key, "count": count})

    # Top growing sources (comparing this year to last year)
    current_year = dt.date.today().year
    if is_sqlite:
        growth_sql = text(f"""
            WITH this_year AS (
                SELECT source_id, COUNT(*) as count
                FROM decisions
                WHERE CAST(strftime('%Y', decision_date) AS INTEGER) = {current_year}
                GROUP BY source_id
            ),
            last_year AS (
                SELECT source_id, COUNT(*) as count
                FROM decisions
                WHERE CAST(strftime('%Y', decision_date) AS INTEGER) = {current_year - 1}
                GROUP BY source_id
            )
            SELECT
                COALESCE(t.source_id, l.source_id) as source_id,
                COALESCE(t.count, 0) as this_year,
                COALESCE(l.count, 0) as last_year,
                COALESCE(t.count, 0) - COALESCE(l.count, 0) as growth
            FROM this_year t
            LEFT JOIN last_year l ON t.source_id = l.source_id
            UNION
            SELECT
                l.source_id,
                0 as this_year,
                l.count as last_year,
                -l.count as growth
            FROM last_year l
            WHERE l.source_id NOT IN (SELECT source_id FROM this_year)
            ORDER BY growth DESC
            LIMIT 10
        """)
    else:
        growth_sql = text("""
            WITH this_year AS (
                SELECT source_id, COUNT(*) as count
                FROM decisions
                WHERE decision_date >= DATE_TRUNC('year', CURRENT_DATE)
                GROUP BY source_id
            ),
            last_year AS (
                SELECT source_id, COUNT(*) as count
                FROM decisions
                WHERE decision_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year'
                  AND decision_date < DATE_TRUNC('year', CURRENT_DATE)
                GROUP BY source_id
            )
            SELECT
                COALESCE(t.source_id, l.source_id) as source_id,
                COALESCE(t.count, 0) as this_year,
                COALESCE(l.count, 0) as last_year,
                COALESCE(t.count, 0) - COALESCE(l.count, 0) as growth
            FROM this_year t
            FULL OUTER JOIN last_year l ON t.source_id = l.source_id
            ORDER BY growth DESC
            LIMIT 10
        """)

    growth_result = session.execute(growth_sql)
    growth_rows = growth_result.fetchall()

    top_growth = [
        {
            "source_id": row[0],
            "this_year": row[1],
            "last_year": row[2],
            "growth": row[3],
        }
        for row in growth_rows
    ]

    return {
        "monthly_by_level": monthly,
        "top_growth": top_growth,
    }


@app.get("/api/stats/ingestion")
def api_ingestion_runs(
    session: Session = Depends(db_session),
    limit: int = 50,
) -> dict:
    """Return recent ingestion runs."""
    try:
        from app.models.ingestion import IngestionRun

        runs_query = (
            select(IngestionRun)
            .order_by(IngestionRun.started_at.desc())
            .limit(limit)
        )
        runs = session.exec(runs_query).all()

        return {
            "runs": [
                {
                    "id": r.id,
                    "scraper_name": r.scraper_name,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                    "duration_seconds": r.duration_seconds,
                    "status": r.status,
                    "decisions_imported": r.decisions_imported,
                    "decisions_skipped": r.decisions_skipped,
                    "errors": r.errors,
                    "from_date": str(r.from_date) if r.from_date else None,
                    "to_date": str(r.to_date) if r.to_date else None,
                    "error_message": r.error_message,
                }
                for r in runs
            ]
        }
    except Exception as e:
        # Table might not exist yet
        return {"runs": [], "error": str(e)}


@app.post("/api/answer", response_model=AnswerResponse)
def api_answer(
    req: AnswerRequest,
    session: Session = Depends(db_session),
    x_openai_key: Optional[str] = Header(None, alias="X-OpenAI-Key"),
) -> AnswerResponse:
    filters = SearchFilters(
        source_ids=req.source_ids,
        level=req.level,
        canton=req.canton,
        language=req.language,
        date_from=req.date_from,
        date_to=req.date_to,
    )
    try:
        logger.info("Answer request: query=%r", req.query[:100] if req.query else "")
        result = answer_question(session, req.query, filters=filters, api_key=x_openai_key)
    except Exception as e:
        logger.error("Answer failed: %s", e, exc_info=True)
        raise

    return AnswerResponse(
        answer=result.answer,
        citations=[CitationSchema(**c.__dict__) for c in result.citations],
        hits_count=result.hits_count,
    )
