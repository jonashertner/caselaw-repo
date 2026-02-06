from __future__ import annotations

from sqlmodel import Session

from app.ingest.common import IngestArgs
from app.ingest.crawler import crawl_source
from app.ingest.connectors.base import Connector
from app.services.indexer import Indexer
from app.services.source_registry import Source


class CrawlerConnector(Connector):
    """Generic HTML crawler connector for court websites.

    Uses breadth-first search to discover and extract decisions.

    Supports historical mode: when args.historical=True, the crawler does not
    apply date filtering, enabling complete archive capture. Date filtering
    for crawlers relies on extracted decision dates from content.
    """

    async def run(self, session: Session, source: Source, *, args: IngestArgs, indexer: Indexer) -> int:
        return await crawl_source(session, source, args=args, indexer=indexer)
