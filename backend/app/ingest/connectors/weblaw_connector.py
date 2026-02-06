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
from app.models.decision import Decision
from app.services.extract import maybe_extract
from app.services.indexer import Indexer, stable_uuid_url
from app.services.source_registry import Source
from app.utils.http import RobotsCache, fetch_bytes

logger = logging.getLogger(__name__)
settings = get_settings()


UUID_RE = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"

# Weblaw portals typically expose a JS-only UI under /<lang>/cache/<uuid> and
# a direct PDF endpoint under /api/getDocumentContent/<uuid>.
_CACHE_RE = re.compile(rf"/(?:[a-z]{{2}}/)?cache/(?P<uuid>{UUID_RE})", re.IGNORECASE)
_GETDOC_RE = re.compile(rf"/api/getDocumentContent/(?P<uuid>{UUID_RE})", re.IGNORECASE)


@dataclass(frozen=True)
class WeblawTarget:
    pdf_url: str
    cache_url: Optional[str]
    lastmod: Optional[dt.date]


def _base(url: str) -> str:
    p = urllib.parse.urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _to_target(loc: str, *, lastmod: Optional[dt.date]) -> Optional[WeblawTarget]:
    """Map a sitemap URL to a PDF fetch target.

    Accepts:
    - direct PDFs (.pdf)
    - /api/getDocumentContent/<uuid>
    - /<lang>/cache/<uuid> (mapped to /api/getDocumentContent/<uuid>)
    """
    u = loc.strip()
    if not u:
        return None

    # Direct PDF links
    if u.lower().endswith(".pdf"):
        return WeblawTarget(pdf_url=u, cache_url=None, lastmod=lastmod)

    m = _GETDOC_RE.search(u)
    if m:
        return WeblawTarget(pdf_url=u, cache_url=None, lastmod=lastmod)

    m = _CACHE_RE.search(u)
    if m:
        uid = m.group("uuid")
        pdf_url = f"{_base(u)}/api/getDocumentContent/{uid}"
        return WeblawTarget(pdf_url=pdf_url, cache_url=u, lastmod=lastmod)

    return None


class WeblawConnector(Connector):
    """Connector for Weblaw-hosted court portals (JS-only UI, PDF behind stable API).

    The primary discovery method is the site sitemap. If the sitemap exposes only
    /cache/<uuid> pages, we deterministically map them to the PDF endpoint.

    This is intended for federal courts hosted on weblaw.ch (e.g. BVGer, BStGer).

    Supports historical mode: when args.historical=True, fetches ALL decisions
    without date filtering for complete archive capture.
    """

    async def run(self, session: Session, source: Source, *, args: IngestArgs, indexer: Indexer) -> int:
        inserted = 0

        # Weblaw portals can have >10k items; default caps are too low.
        # If the caller didn't specify a cap, pick a large-but-safe default.
        max_urls = args.max_pages or max(settings.ingest_max_pages_per_source, 50_000)

        robots = RobotsCache() if settings.ingest_respect_robots else None
        sem = asyncio.Semaphore(min(settings.ingest_concurrency, 10))

        async with httpx.AsyncClient() as client:
            sitemap_urls = await _discover_sitemaps(client, source.start_urls)
            if not sitemap_urls:
                logger.warning("No sitemaps discovered for %s (%s)", source.id, source.start_urls)
                return 0

            urls = await _load_sitemap_urls(client, sitemap_urls, max_urls=max_urls)

            targets: dict[str, WeblawTarget] = {}
            for u in urls:
                if not _in_date_window(u.lastmod, args):
                    continue
                t = _to_target(u.loc, lastmod=u.lastmod)
                if not t:
                    continue
                # De-duplicate by PDF URL.
                targets.setdefault(t.pdf_url, t)

            # Newest first when lastmod is present.
            ordered = sorted(targets.values(), key=lambda x: (x.lastmod or dt.date.min), reverse=True)

            async def ingest_one(t: WeblawTarget) -> None:
                nonlocal inserted

                decision_id = stable_uuid_url(t.pdf_url)
                exists = session.exec(select(Decision.id).where(Decision.id == decision_id)).first()
                if exists:
                    return

                try:
                    if robots and not await robots.allowed(client, t.pdf_url, settings.ingest_user_agent):
                        return
                    async with sem:
                        res = await fetch_bytes(client, t.pdf_url)
                except Exception as e:
                    logger.debug("fetch failed %s (%s)", t.pdf_url, e)
                    return
                if res.status_code >= 400:
                    return

                try:
                    extracted = maybe_extract(res.content, res.content_type, res.url)
                    if not extracted.text or len(extracted.text) < 300:
                        return

                    meta = {
                        "sitemap_lastmod": t.lastmod.isoformat() if t.lastmod else None,
                        "discovery": "weblaw_sitemap",
                        "weblaw_cache_url": t.cache_url,
                    }

                    _, is_new = indexer.upsert_decision(
                        session,
                        source_id=source.id,
                        source_name=source.name,
                        level=source.level,
                        canton=source.canton,
                        url=t.pdf_url,
                        pdf_url=t.pdf_url,
                        title=None,
                        decision_date=None,
                        published_date=t.lastmod,
                        court=source.name,
                        chamber=None,
                        docket=None,
                        language=None,
                        text=extracted.text,
                        meta=meta,
                    )
                    if is_new:
                        inserted += 1
                        print(f"  [{inserted}] {t.pdf_url[:80]}", flush=True)
                except Exception as e:
                    logger.debug("index failed %s (%s)", t.pdf_url, e)

            # Work in bounded batches.
            batch_size = max(20, min(500, settings.ingest_concurrency * 20))
            for i in range(0, len(ordered), batch_size):
                batch = ordered[i : i + batch_size]
                await asyncio.gather(*(ingest_one(t) for t in batch))

        return inserted
