from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from sqlalchemy import Column, func
from sqlalchemy.dialects.postgresql import JSONB, TEXT, TIMESTAMP
from sqlmodel import Field, SQLModel


class Decision(SQLModel, table=True):
    __tablename__ = "decisions"

    id: str = Field(primary_key=True, default=None)  # assigned in service (stable UUID string)

    source_id: str = Field(index=True)
    source_name: str = Field(index=True)

    level: str = Field(index=True)  # federal / cantonal
    canton: Optional[str] = Field(default=None, index=True)  # AG, ZH, ...

    court: Optional[str] = Field(default=None, index=True)
    chamber: Optional[str] = Field(default=None, index=True)

    docket: Optional[str] = Field(default=None, index=True)
    decision_date: Optional[dt.date] = Field(default=None, index=True)
    published_date: Optional[dt.date] = Field(default=None, index=True)

    title: Optional[str] = Field(default=None)
    language: Optional[str] = Field(default=None, index=True)

    url: str = Field(index=True, unique=True)
    pdf_url: Optional[str] = Field(default=None)

    content_text: str = Field(sa_column=Column(TEXT, nullable=False))
    content_hash: str = Field(index=True)

    meta: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=False))

    # Audit timestamps
    indexed_at: Optional[dt.datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), server_default=func.now(), index=True)
    )
    updated_at: Optional[dt.datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), onupdate=func.now())
    )


class DecisionMinimal(SQLModel):
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
    url: str
    pdf_url: Optional[str] = None
