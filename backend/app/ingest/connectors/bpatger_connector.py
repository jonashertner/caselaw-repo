from __future__ import annotations

import asyncio
import datetime as dt
import logging
import re
import urllib.parse
from dataclasses import dataclass
from typing import Optional

import httpx
from sqlmodel import Session, select

from app.core.config import get_settings
from app.ingest.common import IngestArgs
from app.ingest.connectors.base import Connector
from app.ingest.connectors.sitemap_connector import SitemapUrl, _discover_sitemaps, _in_date_window, _load_sitemap_urls
from app.ingest.crawler import crawl_source
from app.models.decision import Decision
from app.services.extract import maybe_extract
from app.services.indexer import Indexer, stable_uuid_url
from app.services.source_registry import Source
from app.utils.http import RobotsCache, fetch_bytes

logger = logging.getLogger(__name__)
settings = get_settings()


_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _parse_bpatger_pdf_url(url: str) -> tuple[Optional[dt.date], Optional[str]]:
    """Best-effort extraction of (decision_date, docket) from BPatGer filenames.

    Typical pattern:
      /fileadmin/entscheide/O2022_007-Entscheid_2024-03-05.pdf
    """
    try:
        path = urllib.parse.urlparse(url).path
        filename = urllib.parse.unquote(path.split("/")[-1])
    except Exception:
        return None, None

    decision_date = None
    m = _DATE_RE.search(filename)
    if m:
        try:
            decision_date = dt.date.fromisoformat(m.group(1))
        except Exception:
            decision_date = None

    docket = None
    # Everything before the first '-': e.g. 'O2022_007'
    if "-" in filename:
        docket = filename.split("-", 1)[0].strip() or None

    return decision_date, docket


class BPatGerConnector(Connector):
    """Connector for the Federal Patent Court.

    The court publishes decisions primarily as PDFs under `/fileadmin/entscheide/`.
    We try to discover *all* PDFs via sitemap. If no sitemap is discoverable, we
    fall back to the generic HTML crawler on the configured start URLs.

    Supports historical mode: when args.historical=True, fetches ALL decisions
    without date filtering for complete archive capture.
    """

    async def run(self, session: Session, source: Source, *, args: IngestArgs, indexer: Indexer) -> int:
        inserted = 0

        max_urls = args.max_pages or max(settings.ingest_max_pages_per_source, 50_000)

        robots = RobotsCache() if settings.ingest_respect_robots else None
        sem = asyncio.Semaphore(min(settings.ingest_concurrency, 10))

        async with httpx.AsyncClient() as client:
            sitemap_urls = await _discover_sitemaps(client, source.start_urls)
            if not sitemap_urls:
                # Fallback: the public site is not JS-only, so crawling works reasonably well.
                return await crawl_source(session, source, args=args, indexer=indexer)

            urls = await _load_sitemap_urls(client, sitemap_urls, max_urls=max_urls)
            candidates: list[SitemapUrl] = []
            for u in urls:
                if not _in_date_window(u.lastmod, args):
                    continue
                if "/fileadmin/entscheide/" in u.loc and u.loc.lower().endswith(".pdf"):
                    candidates.append(u)

            candidates.sort(key=lambda x: (x.lastmod or dt.date.min), reverse=True)

            async def ingest_one(item: SitemapUrl) -> None:
                nonlocal inserted
                url = item.loc

                decision_id = stable_uuid_url(url)
                exists = session.exec(select(Decision.id).where(Decision.id == decision_id)).first()
                if exists:
                    return

                try:
                    if robots and not await robots.allowed(client, url, settings.ingest_user_agent):
                        return
                    async with sem:
                        res = await fetch_bytes(client, url)
                except Exception as e:
                    logger.debug("fetch failed %s (%s)", url, e)
                    return
                if res.status_code >= 400:
                    return

                extracted = maybe_extract(res.content, res.content_type, res.url)
                if not extracted.text or len(extracted.text) < 300:
                    return

                decision_date, docket = _parse_bpatger_pdf_url(url)
                meta = {
                    "sitemap_lastmod": item.lastmod.isoformat() if item.lastmod else None,
                    "discovery": "sitemap",
                }

                try:
                    _, is_new = indexer.upsert_decision(
                        session,
                        source_id=source.id,
                        source_name=source.name,
                        level=source.level,
                        canton=source.canton,
                        url=res.url,
                        pdf_url=res.url,
                        title=None,
                        decision_date=decision_date,
                        published_date=item.lastmod,
                        court=source.name,
                        chamber=None,
                        docket=docket,
                        language=None,
                        text=extracted.text,
                        meta=meta,
                    )
                    if is_new:
                        inserted += 1
                        print(f"  [{inserted}] {docket or url[:80]}", flush=True)
                except Exception as e:
                    logger.debug("index failed %s (%s)", url, e)

            batch_size = max(20, min(500, settings.ingest_concurrency * 20))
            for i in range(0, len(candidates), batch_size):
                batch = candidates[i : i + batch_size]
                await asyncio.gather(*(ingest_one(it) for it in batch))

        return inserted
