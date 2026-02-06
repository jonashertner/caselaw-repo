from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Protocol

from sqlmodel import Session

from app.ingest.common import IngestArgs
from app.services.indexer import Indexer
from app.services.source_registry import Source


class Connector(Protocol):
    """Protocol for court decision connectors.

    Connectors fetch decisions from court websites and APIs. They support two modes:

    1. **Incremental mode** (default): Fetch decisions within a date range.
       - Use args.since/args.until or args.effective_since()/args.effective_until()
       - Suitable for daily/weekly updates

    2. **Historical mode**: Fetch ALL decisions without date restrictions.
       - Enabled when args.historical=True
       - Use args.effective_since()/args.effective_until() which return None
       - Suitable for initial database population or complete archive refresh

    Connectors should check args.historical or use the effective_* methods
    to properly handle both modes.
    """

    async def run(self, session: Session, source: Source, *, args: IngestArgs, indexer: Indexer) -> int:
        """Fetch and store decisions from the source.

        Args:
            session: Database session for persistence.
            source: Source configuration (URLs, metadata).
            args: Ingestion arguments including date filters and historical flag.
            indexer: Indexer service for upserting decisions.

        Returns:
            Number of new decisions inserted.
        """
        ...


@dataclass
class ConnectorResult:
    """Result from a connector run."""

    inserted: int
