from __future__ import annotations

import asyncio
import datetime as dt
import logging
import re
import urllib.parse
from collections import deque
from dataclasses import dataclass
from typing import Iterable, Optional, Set

import httpx
from bs4 import BeautifulSoup
from sqlmodel import Session

from app.core.config import get_settings
from app.ingest.common import IngestArgs
from app.services.extract import maybe_extract
from app.services.indexer import Indexer
from app.services.source_registry import Source
from app.utils.http import RobotsCache, fetch_bytes
from app.utils.text import normalize_text

logger = logging.getLogger(__name__)
settings = get_settings()


_DECISION_HINTS = re.compile(
    r"(entscheid|urteil|sentenza|jugement|d[ée]cision|arr[eê]t|rechtsprechung|jurisprudence|leitsatz)",
    re.IGNORECASE,
)


@dataclass
class DiscoveredUrl:
    url: str
    depth: int
    referrer: str


def _normalize_url(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("mailto:") or href.startswith("javascript:"):
        return None
    try:
        url = urllib.parse.urljoin(base, href)
        # remove fragments
        url, _frag = urllib.parse.urldefrag(url)
        return url
    except Exception:
        return None


def _allowed(url: str, allowed_netlocs: Set[str]) -> bool:
    try:
        netloc = urllib.parse.urlparse(url).netloc.lower()
        return netloc in allowed_netlocs
    except Exception:
        return False


def _is_candidate_decision(url: str, anchor_text: str | None, content_type: str | None = None) -> bool:
    u = url.lower()
    if u.endswith(".pdf"):
        return True
    if content_type and "application/pdf" in content_type.lower():
        return True
    if anchor_text and _DECISION_HINTS.search(anchor_text):
        return True
    # Heuristic: common paths
    if any(k in u for k in ["/entscheid", "/urteil", "/sentenz", "/juris", "/rechtsprech", "/entscheide"]):
        return True
    return False


async def crawl_source(
    session: Session,
    source: Source,
    *,
    args: IngestArgs,
    indexer: Indexer,
) -> int:
    allowed_netlocs = {urllib.parse.urlparse(u).netloc.lower() for u in source.start_urls if u}
    if not allowed_netlocs:
        return 0

    max_pages = args.max_pages or settings.ingest_max_pages_per_source
    max_depth = args.max_depth or settings.ingest_max_depth

    seen_pages: Set[str] = set()
    seen_decisions: Set[str] = set()
    q: deque[DiscoveredUrl] = deque([DiscoveredUrl(url=u, depth=0, referrer="seed") for u in source.start_urls])

    robots = RobotsCache() if settings.ingest_respect_robots else None

    inserted = 0
    sem = asyncio.Semaphore(settings.ingest_concurrency)

    async with httpx.AsyncClient() as client:
        async def worker(item: DiscoveredUrl) -> None:
            nonlocal inserted
            url = item.url

            if url in seen_pages:
                return
            seen_pages.add(url)

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

            # Candidate decision?
            if _is_candidate_decision(res.url, anchor_text=None, content_type=res.content_type):
                if res.url not in seen_decisions:
                    seen_decisions.add(res.url)
                    try:
                        extracted = maybe_extract(res.content, res.content_type, res.url)
                        if extracted.text and len(extracted.text) > 300:
                            _, is_new = indexer.upsert_decision(
                                session,
                                source_id=source.id,
                                source_name=source.name,
                                level=source.level,
                                canton=source.canton,
                                url=res.url,
                                pdf_url=res.url if res.url.lower().endswith(".pdf") else None,
                                title=extracted.title,
                                decision_date=None,
                                published_date=None,
                                court=None,
                                chamber=None,
                                docket=None,
                                language=None,
                                text=extracted.text,
                                meta={"referrer": item.referrer},
                            )
                            if is_new:
                                inserted += 1
                                print(f"  [{inserted}] {extracted.title or res.url[:80]}", flush=True)
                    except Exception as e:
                        logger.debug("index failed %s (%s)", res.url, e)
                return

            if item.depth >= max_depth:
                return

            # Parse links for further crawling and possible decision candidates
            ct = (res.content_type or "").lower()
            if "text/html" not in ct and "application/xhtml" not in ct and not ct.startswith("text/"):
                return

            try:
                html = res.content.decode("utf-8", errors="ignore")
                soup = BeautifulSoup(html, "lxml")
            except Exception:
                return

            anchors = soup.find_all("a")
            for a in anchors:
                href = a.get("href")
                text = a.get_text(" ", strip=True) if a else ""
                next_url = _normalize_url(res.url, href or "")
                if not next_url:
                    continue
                if not _allowed(next_url, allowed_netlocs):
                    continue

                # If anchor looks like decision link, prioritize by enqueuing at same depth
                if _is_candidate_decision(next_url, anchor_text=text):
                    if next_url not in seen_pages and next_url not in seen_decisions:
                        q.appendleft(DiscoveredUrl(url=next_url, depth=item.depth + 1, referrer=res.url))
                else:
                    if next_url not in seen_pages:
                        q.append(DiscoveredUrl(url=next_url, depth=item.depth + 1, referrer=res.url))

        while q and len(seen_pages) < max_pages:
            batch: list[DiscoveredUrl] = []
            while q and len(batch) < settings.ingest_concurrency * 2:
                batch.append(q.popleft())
            await asyncio.gather(*(worker(it) for it in batch))

    return inserted
