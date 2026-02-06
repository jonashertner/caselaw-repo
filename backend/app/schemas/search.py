from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field


class SortBy(str, Enum):
    relevance = "relevance"
    date_desc = "date_desc"
    date_asc = "date_asc"


class SearchRequest(BaseModel):
    query: str = Field(min_length=1)
    source_ids: Optional[list[str]] = None
    level: Optional[str] = None
    canton: Optional[str] = None
    language: Optional[str] = None
    date_from: Optional[dt.date] = None
    date_to: Optional[dt.date] = None
    sort_by: SortBy = Field(default=SortBy.relevance)
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class SearchDecision(BaseModel):
    id: str
    source_id: str
    source_name: str
    level: str
    canton: Optional[str] = None
    court: Optional[str] = None
    chamber: Optional[str] = None
    docket: Optional[str] = None
    decision_date: Optional[dt.date] = None
    title: Optional[str] = None
    language: Optional[str] = None
    url: str
    pdf_url: Optional[str] = None


class SearchHit(BaseModel):
    decision: SearchDecision
    score: float
    snippet: str
    chunk_id: Optional[str] = None
    chunk_index: Optional[int] = None


class SearchResponse(BaseModel):
    hits: list[SearchHit]
    total: Optional[int] = None
    offset: int = 0
    limit: int = 20
