from __future__ import annotations

import datetime as dt
from typing import Optional

from pydantic import BaseModel, Field

from app.schemas.search import SearchRequest


class AnswerRequest(SearchRequest):
    # same fields; query becomes 'question'
    query: str = Field(min_length=1, description="Question")


class Citation(BaseModel):
    marker: str
    decision_id: str
    chunk_id: str
    source_name: str
    docket: Optional[str] = None
    decision_date: Optional[dt.date] = None
    url: str
    pdf_url: Optional[str] = None


class AnswerResponse(BaseModel):
    answer: str
    citations: list[Citation]
    hits_count: int
