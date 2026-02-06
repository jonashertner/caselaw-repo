from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class IngestArgs:
    """Arguments for ingestion connectors.

    Attributes:
        since: Only fetch decisions after this date (inclusive).
        until: Only fetch decisions before this date (inclusive).
        historical: If True, fetch ALL decisions regardless of date filters.
            When historical=True, since/until are ignored and connectors
            should capture the complete archive without date restrictions.
        max_pages: Maximum number of pages to fetch (pagination limit).
        max_depth: Maximum crawl depth for crawler-based connectors.
    """

    since: Optional[dt.date] = None
    until: Optional[dt.date] = None
    historical: bool = False
    max_pages: Optional[int] = None
    max_depth: Optional[int] = None

    def effective_since(self) -> Optional[dt.date]:
        """Get effective since date (None if historical mode)."""
        return None if self.historical else self.since

    def effective_until(self) -> Optional[dt.date]:
        """Get effective until date (None if historical mode)."""
        return None if self.historical else self.until
