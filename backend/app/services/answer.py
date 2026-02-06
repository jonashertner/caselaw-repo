from __future__ import annotations

import datetime as dt
import logging
import os
from dataclasses import dataclass
from typing import Optional

from sqlmodel import Session, select

from app.ai.llm import get_llm_provider
from app.models.chunk import Chunk
from app.models.decision import Decision

logger = logging.getLogger(__name__)

# Use parquet search if enabled, otherwise use SQLite/PostgreSQL search
_use_parquet = os.environ.get("USE_PARQUET_SEARCH", "").lower().strip() in ("1", "true", "yes")

if _use_parquet:
    logger.info("answer.py: Using PARQUET search (USE_PARQUET_SEARCH=%s)", os.environ.get("USE_PARQUET_SEARCH", ""))
    from app.services.search_parquet import SearchFilters, search_parquet
    # Wrapper to match the expected signature
    def search(session, query, *, filters, limit=20, offset=0, api_key=None):
        return search_parquet(query, filters=filters, limit=limit, offset=offset)
else:
    logger.info("answer.py: Using DATABASE search (USE_PARQUET_SEARCH=%s)", os.environ.get("USE_PARQUET_SEARCH", ""))
    from app.services.search import SearchFilters, search


@dataclass
class Citation:
    marker: str  # e.g. S1
    decision_id: str
    chunk_id: str
    source_name: str
    docket: Optional[str]
    decision_date: Optional[dt.date]
    url: str
    pdf_url: Optional[str]


@dataclass
class AnswerResult:
    answer: str
    citations: list[Citation]
    hits_count: int


_SYSTEM = """You are a Swiss case-law research assistant.
Rules:
- Use ONLY the provided excerpts. If the excerpts are insufficient, say what is missing.
- Cite every non-trivial legal claim using the markers like [S1], [S2].
- Do not invent docket numbers, dates, courts, holdings, or quotations.
- Write in the user's language (match the question language)."""


def answer_question(session: Session, question: str, *, filters: SearchFilters, api_key: Optional[str] = None) -> AnswerResult:
    result = search(session, question, filters=filters, limit=12, api_key=api_key)

    # Handle both parquet (SearchResult with .hits) and SQLite/Postgres (list) results
    if hasattr(result, 'hits'):
        hits = result.hits
    else:
        hits = result if result else []

    if not hits:
        return AnswerResult(
            answer="No indexed decisions match your query yet. Ingest sources and try again.",
            citations=[],
            hits_count=0,
        )

    citations: list[Citation] = []
    excerpts: list[str] = []

    if _use_parquet:
        # Parquet mode: use data directly from search hits
        for i, h in enumerate(hits, start=1):
            marker = f"S{i}"
            dec = h.decision
            text = h.snippet
            if not text:
                continue

            citations.append(
                Citation(
                    marker=marker,
                    decision_id=dec.id,
                    chunk_id=getattr(h, 'chunk_id', None) or "",
                    source_name=dec.source_name,
                    docket=dec.docket,
                    decision_date=dec.decision_date,
                    url=dec.url,
                    pdf_url=dec.pdf_url,
                )
            )

            header_parts = [dec.source_name]
            if dec.canton:
                header_parts.append(dec.canton)
            if dec.docket:
                header_parts.append(dec.docket)
            if dec.decision_date:
                header_parts.append(str(dec.decision_date))
            header = " | ".join(header_parts)
            excerpts.append(f"[{marker}] {header}\n{text}")
    else:
        # Database mode: load chunks and decisions from DB
        chunk_ids = [h.chunk_id for h in hits if h.chunk_id]
        chunks = session.exec(select(Chunk).where(Chunk.id.in_(chunk_ids))).all()
        chunk_by_id = {c.id: c for c in chunks}

        dec_ids = [h.decision.id for h in hits]
        decs = session.exec(select(Decision).where(Decision.id.in_(dec_ids))).all()
        dec_by_id = {d.id: d for d in decs}

        for i, h in enumerate(hits, start=1):
            marker = f"S{i}"
            dec = dec_by_id.get(h.decision.id)
            if not dec:
                continue

            ch = chunk_by_id.get(h.chunk_id) if h.chunk_id else None
            text = ch.text if ch else h.snippet
            if not text:
                continue

            citations.append(
                Citation(
                    marker=marker,
                    decision_id=dec.id,
                    chunk_id=ch.id if ch else "",
                    source_name=dec.source_name,
                    docket=dec.docket,
                    decision_date=dec.decision_date,
                    url=dec.url,
                    pdf_url=dec.pdf_url,
                )
            )

            header_parts = [dec.source_name]
            if dec.canton:
                header_parts.append(dec.canton)
            if dec.docket:
                header_parts.append(dec.docket)
            if dec.decision_date:
                header_parts.append(str(dec.decision_date))
            header = " | ".join(header_parts)
            excerpts.append(f"[{marker}] {header}\n{text}")

    user = """Question:
{question}

Excerpts:
{excerpts}

Task:
Answer the question using the excerpts. Use citations like [S1] inline.""".format(
        question=question.strip(),
        excerpts="\n\n".join(excerpts),
    )

    llm = get_llm_provider(api_key=api_key)
    answer = llm.generate(system=_SYSTEM, user=user).strip()
    return AnswerResult(answer=answer, citations=citations, hits_count=len(hits))
