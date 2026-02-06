from __future__ import annotations

import asyncio
import datetime as dt
import logging
import re
import urllib.parse
from dataclasses import dataclass
from typing import Iterable, Optional, Set

import httpx
from bs4 import BeautifulSoup
from sqlmodel import Session

from app.core.config import get_settings
from app.ingest.common import IngestArgs
from app.ingest.connectors.base import Connector
from app.services.extract import extract_html
from app.services.indexer import Indexer, stable_uuid_url
from app.services.source_registry import Source
from app.utils.http import fetch_bytes
from app.utils.text import normalize_text

logger = logging.getLogger(__name__)
settings = get_settings()

# Prefer the canonical domain (works for all languages and is what the court links to).
BGer_DEFAULT_BASE = "https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php"

_DOCID_RE = re.compile(r"highlight_docid=([^&]+)")


def _date_param(d: dt.date) -> str:
    # Eurospider commonly accepts dd.mm.yyyy
    return d.strftime("%d.%m.%Y")


_COLLECTION_RE = re.compile(r"/php/(?P<collection>[^/]+)/http/index\.php", re.IGNORECASE)


def _pick_base(start_urls: list[str]) -> str:
    for u in start_urls or []:
        if "ext/eurospider" in u and "index.php" in u:
            try:
                p = urllib.parse.urlparse(u)
                return f"{p.scheme}://{p.netloc}{p.path}"
            except Exception:
                continue
    return BGer_DEFAULT_BASE


def _collection(base: str) -> str:
    m = _COLLECTION_RE.search(base)
    return (m.group("collection") if m else "aza").lower()


def _build_search_url(base: str, *, since: Optional[dt.date], until: Optional[dt.date], page: int) -> str:
    coll = _collection(base)
    params: dict[str, str] = {
        "lang": "de",
        # The public Eurospider endpoint accepts an *empty* query to list everything.
        # This is the most reliable way to enumerate all published decisions.
        "type": "simple_query",
        "query_words": "",
        f"top_subcollection_{coll}": "all",
        "page": str(page),
        "sort": "date_desc",
    }
    if since:
        params["from_date"] = _date_param(since)
    if until:
        params["to_date"] = _date_param(until)
    return f"{base}?{urllib.parse.urlencode(params)}"


def _show_document_url(base: str, docid: str, *, lang: str = "de") -> str:
    params = {
        "lang": lang,
        "type": "show_document",
        "highlight_docid": docid,
        # Print layout is much easier to extract and is stable.
        "print": "yes",
        "zoom": "",
    }
    return f"{base}?{urllib.parse.urlencode(params)}"


def _extract_decision_links(base: str, html: str) -> list[str]:
    """Extract *document* URLs from a result page.

    Result pages link to a variety of handlers (highlight views, navigation, etc.).
    We normalize everything to `type=show_document` with `print=yes`.
    """
    soup = BeautifulSoup(html, "lxml")
    docids: set[str] = set()
    direct: set[str] = set()

    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        full = urllib.parse.urljoin(base, href)
        if "highlight_docid=" in full:
            try:
                parsed = urllib.parse.urlparse(full)
                qs = urllib.parse.parse_qs(parsed.query)
                did = qs.get("highlight_docid", [None])[0]
                if did:
                    docids.add(did)
            except Exception:
                continue
        elif "type=show_document" in full:
            direct.add(full)

    out: set[str] = set()
    out.update(_show_document_url(base, did) for did in docids)
    out.update(direct)
    return sorted(out)


def _docid_from_url(url: str) -> Optional[str]:
    m = _DOCID_RE.search(url)
    if not m:
        return None
    return urllib.parse.unquote_plus(m.group(1))


def _parse_docid(docid: str) -> tuple[Optional[dt.date], Optional[str]]:
    """Parse Eurospider docids.

    Examples:
      aza://31-01-2022-4A_500-2021
      atf://149-IV-123

    The format differs slightly across collections; we only attempt to parse
    the common *date + docket* pattern used in the AZA collection.
    """
    m0 = re.match(r"^(?P<scheme>[a-z]+)://(?P<rest>.+)$", docid)
    if not m0:
        return None, None
    rest = m0.group("rest")
    parts = rest.split("-")
    if len(parts) < 4:
        return None, None
    # dd-mm-yyyy-docket...
    try:
        day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
        d = dt.date(year, month, day)
    except Exception:
        d = None
    docket_raw = "-".join(parts[3:])
    # Most docids end with '-YYYY' where YYYY is the docket year; present it as '/YYYY'.
    m = re.match(r"^(.*?)-(\d{4})$", docket_raw)
    if m:
        docket = f"{m.group(1)}/{m.group(2)}"
    else:
        docket = docket_raw
    return d, docket or None


class BGerSearchConnector(Connector):
    """Connector for the Federal Supreme Court (Bundesgericht) Eurospider API.

    Supports historical mode: when args.historical=True, fetches ALL decisions
    without date filtering for complete archive capture.
    """

    async def run(self, session: Session, source: Source, *, args: IngestArgs, indexer: Indexer) -> int:
        inserted = 0
        base = _pick_base(source.start_urls)

        # Historical mode: no date filtering for complete archive capture
        if args.historical:
            since = None
            until = None
            logger.info("BGer: Historical mode - fetching complete archive")
        else:
            since = args.effective_since()
            until = args.effective_until() or dt.date.today()

        async with httpx.AsyncClient() as client:
            page = 1
            seen: Set[str] = set()
            while True:
                url = _build_search_url(base, since=since, until=until, page=page)
                try:
                    res = await fetch_bytes(client, url, user_agent=settings.ingest_user_agent)
                except Exception as e:
                    logger.warning("BGer search fetch failed: %s", e)
                    break
                if res.status_code >= 400:
                    break
                html = res.content.decode("utf-8", errors="ignore")
                links = _extract_decision_links(base, html)
                new_links = [l for l in links if l not in seen]
                if not new_links:
                    break
                for link in new_links:
                    seen.add(link)
                # Fetch decision pages concurrently (conservative)
                sem = asyncio.Semaphore(min(settings.ingest_concurrency, 6))

                async def fetch_one(dec_url: str) -> None:
                    nonlocal inserted
                    async with sem:
                        try:
                            dres = await fetch_bytes(client, dec_url, user_agent=settings.ingest_user_agent)
                        except Exception:
                            return
                    if dres.status_code >= 400:
                        return
                    extracted = extract_html(dres.content, url=dres.url)
                    text = extracted.text
                    if len(text) < 300:
                        return
                    docid = _docid_from_url(dec_url) or _docid_from_url(dres.url)
                    decision_date, docket = _parse_docid(docid) if docid else (None, None)

                    try:
                        _, is_new = indexer.upsert_decision(
                            session,
                            source_id=source.id,
                            source_name=source.name,
                            level=source.level,
                            canton=source.canton,
                            url=dres.url,
                            pdf_url=None,
                            title=extracted.title,
                            decision_date=decision_date,
                            published_date=None,
                            court="Bundesgericht",
                            chamber=None,
                            docket=docket,
                            language=None,
                            text=text,
                            meta={"docid": docid or None},
                        )
                        if is_new:
                            inserted += 1
                            print(f"  [{inserted}] {docket or extracted.title or dec_url[:60]}", flush=True)
                    except Exception:
                        return

                await asyncio.gather(*(fetch_one(l) for l in new_links))

                page += 1
                if args.max_pages and page > args.max_pages:
                    break

        return inserted
