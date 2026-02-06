"""Models for tracking ingestion runs and metrics."""
from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from sqlalchemy import Column, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlmodel import Field, SQLModel


class IngestionRun(SQLModel, table=True):
    """Tracks each ingestion run for a scraper/source."""

    __tablename__ = "ingestion_runs"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Which scraper ran
    scraper_name: str = Field(index=True)  # e.g., "bger", "bvger", "zh_courts"
    source_id: Optional[str] = Field(default=None, index=True)  # Optional link to source

    # Run timing
    started_at: dt.datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), server_default=func.now())
    )
    completed_at: Optional[dt.datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True))
    )
    duration_seconds: Optional[float] = Field(default=None)

    # Status
    status: str = Field(default="running", index=True)  # running, completed, failed

    # Metrics
    decisions_found: int = Field(default=0)
    decisions_imported: int = Field(default=0)
    decisions_skipped: int = Field(default=0)
    decisions_updated: int = Field(default=0)
    errors: int = Field(default=0)

    # Run parameters
    from_date: Optional[dt.date] = Field(default=None)
    to_date: Optional[dt.date] = Field(default=None)

    # Additional details
    error_message: Optional[str] = Field(default=None)
    details: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=False))


class IngestionStats(SQLModel):
    """Aggregated ingestion statistics (not a table, just a schema)."""

    total_decisions: int
    federal_decisions: int
    cantonal_decisions: int
    decisions_by_canton: dict[str, int]
    decisions_by_source: dict[str, int]
    decisions_by_year: dict[int, int]
    last_ingestion: Optional[dt.datetime]
    decisions_last_24h: int
    decisions_last_7d: int
    decisions_last_30d: int
