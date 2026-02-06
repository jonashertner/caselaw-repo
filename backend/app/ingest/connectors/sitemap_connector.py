from __future__ import annotations

import asyncio
import datetime as dt
import gzip
import logging
import re
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable, Optional

import httpx
from sqlmodel import Session, select

from app.core.config import get_settings
from app.ingest.common import IngestArgs
from app.ingest.connectors.base import Connector
from app.models.decision import Decision
from app.services.extract import maybe_extract
from app.services.indexer import Indexer, stable_uuid_url
from app.services.source_registry import Source
from app.utils.http import RobotsCache, fetch_bytes

logger = logging.getLogger(__name__)
settings = get_settings()


_SITEMAP_RE = re.compile(r"^\s*sitemap\s*:\s*(\S+)\s*$", re.IGNORECASE)

# Very permissive candidate matcher: sitemap URLs often include both HTML pages and PDF downloads.
_DECISION_HINTS = re.compile(
    r"(entscheid|urteil|sentenza|jugement|d[ée]cision|arr[eê]t|rechtsprechung|jurisprudence|leitsatz)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SitemapUrl:
    loc: str
    lastmod: Optional[dt.date] = None


def _strip_ns(tag: str) -> str:
    # '{namespace}tag' -> 'tag'
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_lastmod(value: str | None) -> Optional[dt.date]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None
    # Sitemaps typically use ISO date or datetime.
    try:
        if "T" in v:
            # 2025-01-31T12:34:56+00:00
            return dt.datetime.fromisoformat(v.replace("Z", "+00:00")).date()
        return dt.date.fromisoformat(v[:10])
    except Exception:
        return None


def _is_candidate_decision_url(url: str) -> bool:
    u = url.lower()
    if u.endswith(".pdf"):
        return True
    if "servletdownload" in u:
        return True
    if "download" in u and "pdf" in u:
        return True
    # Heuristic: common paths
    if any(k in u for k in ["/entscheid", "/urteil", "/sentenz", "/juris", "/rechtsprech", "/entscheide"]):
        return True
    return False


def _extract_dossiernummer(url: str) -> Optional[str]:
    try:
        q = urllib.parse.urlparse(url).query
        params = urllib.parse.parse_qs(q)
        if "dossiernummer" in params and params["dossiernummer"]:
            return params["dossiernummer"][0]
    except Exception:
        return None
    return None


async def _fetch_text(client: httpx.AsyncClient, url: str) -> str:
    r = await client.get(url, headers={"User-Agent": settings.ingest_user_agent}, follow_redirects=True, timeout=10)
    r.raise_for_status()
    return r.text


async def _discover_sitemaps(client: httpx.AsyncClient, start_urls: list[str]) -> list[str]:
    """Best-effort sitemap discovery.

    Strategy:
    1) Read robots.txt and collect declared Sitemap: entries.
    2) Probe common sitemap locations.

    Returns a de-duplicated list.
    """
    bases: set[str] = set()
    prefixes: set[str] = set()
    for u in start_urls:
        try:
            p = urllib.parse.urlparse(u)
            if p.scheme and p.netloc:
                bases.add(f"{p.scheme}://{p.netloc}")
                if p.path and p.path != "/":
                    # First path component as prefix: '/tribunapublikation', '/le', ...
                    seg = p.path.strip("/").split("/", 1)[0]
                    if seg:
                        prefixes.add("/" + seg)
        except Exception:
            continue

    found: set[str] = set()

    # 1) robots.txt
    for base in sorted(bases):
        try:
            txt = await _fetch_text(client, f"{base}/robots.txt")
        except Exception:
            continue
        for line in txt.splitlines():
            m = _SITEMAP_RE.match(line)
            if not m:
                continue
            loc = m.group(1).strip()
            if loc:
                found.add(loc)

    # 2) Common paths
    candidates: set[str] = set()
    for base in bases:
        candidates.update(
            {
                f"{base}/sitemap.xml",
                f"{base}/sitemap_index.xml",
                f"{base}/sitemap-index.xml",
                f"{base}/sitemap/sitemap.xml",
                f"{base}/sitemap/sitemap_index.xml",
                f"{base}/sitemap.xml.gz",
                f"{base}/sitemap_index.xml.gz",
            }
        )
        for pref in prefixes:
            candidates.update(
                {
                    f"{base}{pref}/sitemap.xml",
                    f"{base}{pref}/sitemap_index.xml",
                    f"{base}{pref}/sitemap.xml.gz",
                    f"{base}{pref}/sitemap_index.xml.gz",
                }
            )

    # Probe candidates quickly (stop at first success per base/prefix isn't reliable; just collect existing ones).
    sem = asyncio.Semaphore(6)

    async def probe(url: str) -> None:
        nonlocal found
        try:
            async with sem:
                res = await fetch_bytes(client, url, timeout_s=10)
            if res.status_code >= 400:
                return
            # Only accept if it looks like XML (best-effort)
            ct = (res.content_type or "").lower()
            if "xml" not in ct and "text" not in ct and "application/octet-stream" not in ct:
                # Many servers send octet-stream for .gz
                return
            if not res.content:
                return
            found.add(url)
        except Exception:
            return

    await asyncio.gather(*(probe(u) for u in sorted(candidates)))

    return sorted(found)


async def _load_sitemap_urls(
    client: httpx.AsyncClient,
    sitemap_urls: Iterable[str],
    *,
    max_urls: int,
) -> list[SitemapUrl]:
    """Flatten sitemap indexes to a list of URLs (with optional lastmod)."""
    out: list[SitemapUrl] = []
    seen_sitemaps: set[str] = set()
    q: list[str] = list(sitemap_urls)

    sem = asyncio.Semaphore(6)

    async def fetch_and_parse(sitemap_url: str) -> tuple[list[str], list[SitemapUrl]]:
        """Returns (child_sitemaps, urls)."""
        try:
            async with sem:
                res = await fetch_bytes(client, sitemap_url, timeout_s=20)
        except Exception as e:
            logger.debug("sitemap fetch failed %s (%s)", sitemap_url, e)
            return ([], [])
        if res.status_code >= 400 or not res.content:
            return ([], [])

        raw = res.content
        if sitemap_url.lower().endswith(".gz") or raw[:2] == b"\x1f\x8b":
            try:
                raw = gzip.decompress(raw)
            except Exception:
                # best-effort; leave as-is
                pass

        try:
            root = ET.fromstring(raw)
        except Exception as e:
            logger.debug("sitemap parse failed %s (%s)", sitemap_url, e)
            return ([], [])

        tag = _strip_ns(root.tag)
        children: list[str] = []
        urls: list[SitemapUrl] = []

        if tag == "sitemapindex":
            for sm in root.findall(".//{*}sitemap"):
                loc_el = sm.find("{*}loc")
                if loc_el is None or not (loc_el.text or "").strip():
                    continue
                children.append(loc_el.text.strip())
        elif tag == "urlset":
            for u in root.findall(".//{*}url"):
                loc_el = u.find("{*}loc")
                if loc_el is None:
                    continue
                loc = (loc_el.text or "").strip()
                if not loc:
                    continue
                lastmod_el = u.find("{*}lastmod")
                lastmod = _parse_lastmod(lastmod_el.text if lastmod_el is not None else None)
                urls.append(SitemapUrl(loc=loc, lastmod=lastmod))
        else:
            # Unknown format
            return ([], [])

        return (children, urls)

    while q and len(out) < max_urls:
        sm = q.pop(0)
        if sm in seen_sitemaps:
            continue
        seen_sitemaps.add(sm)

        child_sitemaps, urls = await fetch_and_parse(sm)
        # BFS to avoid deep recursion
        for c in child_sitemaps:
            if c not in seen_sitemaps:
                q.append(c)

        for u in urls:
            out.append(u)
            if len(out) >= max_urls:
                break

    return out


def _in_date_window(d: Optional[dt.date], args: IngestArgs) -> bool:
    """Check if date falls within the ingestion date window.

    In historical mode (args.historical=True), all dates pass the check
    to enable complete archive capture.
    """
    # Historical mode: no date filtering
    if args.historical:
        return True
    if d is None:
        return True
    since = args.effective_since()
    until = args.effective_until()
    if since and d < since:
        return False
    if until and d > until:
        return False
    return True


class SitemapConnector(Connector):
    """Connector for JS-only portals that still expose a sitemap.

    The sitemap is used as a server-side discovery mechanism (no headless browser required).

    Supports historical mode: when args.historical=True, fetches ALL decisions
    without date filtering for complete archive capture.
    """

    async def run(self, session: Session, source: Source, *, args: IngestArgs, indexer: Indexer) -> int:
        max_urls = args.max_pages or settings.ingest_max_pages_per_source
        inserted = 0

        robots = RobotsCache() if settings.ingest_respect_robots else None
        sem = asyncio.Semaphore(settings.ingest_concurrency)

        async with httpx.AsyncClient() as client:
            sitemap_urls = await _discover_sitemaps(client, source.start_urls)
            if not sitemap_urls:
                logger.warning("No sitemaps discovered for %s (%s)", source.id, source.start_urls)
                return 0

            urls = await _load_sitemap_urls(client, sitemap_urls, max_urls=max_urls)
            # Filter down to plausible decision URLs.
            candidates = [u for u in urls if _is_candidate_decision_url(u.loc) and _in_date_window(u.lastmod, args)]

            # Process newest first when lastmod is present.
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

                try:
                    extracted = maybe_extract(res.content, res.content_type, res.url)
                    if not extracted.text or len(extracted.text) < 300:
                        return

                    docket = _extract_dossiernummer(res.url)
                    meta = {
                        "sitemap_lastmod": item.lastmod.isoformat() if item.lastmod else None,
                        "discovery": "sitemap",
                    }

                    _, is_new = indexer.upsert_decision(
                        session,
                        source_id=source.id,
                        source_name=source.name,
                        level=source.level,
                        canton=source.canton,
                        url=res.url,
                        pdf_url=res.url if (res.content_type or "").lower().startswith("application/pdf") or res.url.lower().endswith(".pdf") else None,
                        title=extracted.title,
                        decision_date=None,
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
                        print(f"  [{inserted}] {extracted.title or url[:80]}", flush=True)
                except Exception as e:
                    logger.debug("index failed %s (%s)", url, e)

            # Work in small batches to keep memory stable.
            batch_size = max(10, settings.ingest_concurrency * 4)
            for i in range(0, len(candidates), batch_size):
                batch = candidates[i : i + batch_size]
                await asyncio.gather(*(ingest_one(it) for it in batch))

        return inserted
