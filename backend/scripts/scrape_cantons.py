#!/usr/bin/env python3
"""Scrape decisions from Swiss cantonal court websites.

This script implements scrapers for various cantonal court databases.
Each canton may have different systems:
- FindInfoWeb/Omnis (SO, BS, NE)
- Custom HTML/PDF (AI, TG)
- AJAX/JSON APIs (LU)
- LexWork/WebLaw platforms (AG, ZG)
- Sitemap-based (BE)
- HTML crawlers (SG, SH, SZ, VS, BL, FR)

Usage:
    python scripts/scrape_cantons.py --canton=SO [--limit N] [--from-date YYYY-MM-DD]
    python scripts/scrape_cantons.py --list  # List available scrapers
    python scripts/scrape_cantons.py --all   # Run all scrapers
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlencode, unquote

import httpx
from bs4 import BeautifulSoup

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url

from scripts.scraper_common import (
    DEFAULT_HEADERS,
    RateLimiter,
    ScraperStats,
    compute_hash,
    extract_pdf_text,
    parse_date_flexible,
    retry,
    upsert_decision,
)
from scripts.scrape_zh_courts import scrape_zh_courts

# Rate limiter: 2 requests per second (shared across all canton scrapers)
rate_limiter = RateLimiter(requests_per_second=2.0)


def _url_year(url: str) -> int | None:
    """Extract a 4-digit year (2000-2029) from a URL path or filename."""
    m = re.search(r'[/_-](20[012]\d)(?:[/_.\-#?]|$)', url)
    return int(m.group(1)) if m else None


@retry(max_attempts=3, backoff_base=2.0)
def fetch_page(url: str, timeout: int = 60) -> httpx.Response:
    """Fetch a page with retry logic."""
    rate_limiter.wait()
    resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp


# Canton database URLs
CANTON_SOURCES = {
    "AI": {
        "name": "Appenzell Innerrhoden",
        "type": "pdf_archive",
        "base_url": "https://www.ai.ch",
        "decisions_url": "https://www.ai.ch/gerichte/gerichtsentscheide",
        "archive_url": "https://www.ai.ch/themen/staat-und-recht/veroeffentlichungen/verwaltungs-und-gerichtsentscheide",
    },
    "SO": {
        "name": "Solothurn",
        "type": "findinfoweb",
        "base_url": "https://gerichtsentscheide.so.ch",
        "search_url": "https://gerichtsentscheide.so.ch/cgi-bin/nph-omniscgi.exe",
    },
    "BS": {
        "name": "Basel-Stadt",
        "type": "findinfoweb",
        "base_url": "https://rechtsprechung.gerichte.bs.ch",
        "search_url": "https://rechtsprechung.gerichte.bs.ch/cgi-bin/nph-omniscgi.exe",
    },
    "TG": {
        "name": "Thurgau",
        "type": "confluence",
        "base_url": "https://rechtsprechung.tg.ch",
        "decisions_url": "https://rechtsprechung.tg.ch/og/entscheide",
    },
    "LU": {
        "name": "Luzern",
        "type": "ajax",
        "base_url": "https://gerichte.lu.ch",
        "decisions_url": "https://gerichte.lu.ch/recht_sprechung/lgve",
    },
    "ZG": {
        "name": "Zug",
        "type": "external",
        "base_url": "https://obergericht.zg.ch",
        "note": "Database since 2022",
    },
}


# =============================================================================
# SOLOTHURN (SO) - FindInfoWeb
# =============================================================================

def scrape_so_findinfoweb(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Solothurn FindInfoWeb database.

    Note: FindInfoWeb doesn't support date filtering in the API,
    so we fetch all and filter by extracted date.
    """
    print("Scraping Solothurn (gerichtsentscheide.so.ch)...")

    if from_date:
        print(f"  Date filter: {from_date} to {to_date or date.today()}")

    base_url = "https://gerichtsentscheide.so.ch/cgi-bin/nph-omniscgi.exe"
    stats = ScraperStats()
    page = 1

    with get_session() as session:
        while True:
            # Fetch the "home" page which lists newest decisions
            params = {
                "OmnisPlatform": "WINDOWS",
                "WebServerUrl": "",
                "WebServerScript": "/cgi-bin/nph-omniscgi.exe",
                "OmnisLibrary": "JURISWEB",
                "OmnisClass": "rtFindinfoWebHtmlService",
                "OmnisServer": "7001",
                "Aufruf": "home",
                "Template": "home.html",
                "Schema": "JGWEB",
                "cSprache": "DE",
                "Parametername": "WEB",
                "nAnzahlTrefferProSeite": "50",
                "nSeite": str(page),
                "bInstanzInt": "all",
            }

            url = f"{base_url}?{urlencode(params)}"

            try:
                resp = fetch_page(url)
            except Exception as e:
                print(f"  Error fetching page {page}: {e}")
                break

            # Find decision links with nF30_KEY pattern
            decision_ids = re.findall(r"nF30_KEY=(\d+)", resp.text)
            decision_ids = list(dict.fromkeys(decision_ids))  # Remove duplicates, preserve order

            if not decision_ids:
                print(f"  No more decisions found on page {page}")
                break

            print(f"  Page {page}: found {len(decision_ids)} decisions")

            for decision_id in decision_ids:
                stable_id = stable_uuid_url(f"so-findinfo:{decision_id}")

                # Check if exists
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                # Fetch decision detail
                detail_params = {
                    "OmnisPlatform": "WINDOWS",
                    "WebServerUrl": "",
                    "WebServerScript": "/cgi-bin/nph-omniscgi.exe",
                    "OmnisLibrary": "JURISWEB",
                    "OmnisClass": "rtFindinfoWebHtmlService",
                    "OmnisServer": "7001",
                    "Parametername": "WEB",
                    "Schema": "JGWEB",
                    "Aufruf": "getMarkupDocument",
                    "cSprache": "DE",
                    "nF30_KEY": decision_id,
                    "Template": "/simple/search_result_document.html",
                }
                detail_url = f"{base_url}?{urlencode(detail_params)}"

                try:
                    detail_resp = fetch_page(detail_url)
                except Exception as e:
                    print(f"    Error fetching {decision_id}: {e}")
                    stats.add_error()
                    continue

                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")

                # Extract content from the document body
                content_div = detail_soup.find("div", class_="dokument") or detail_soup.find("body")
                if not content_div:
                    stats.add_skipped()
                    continue

                content = content_div.get_text(separator="\n", strip=True)
                if len(content) < 100:
                    stats.add_skipped()
                    continue

                # Extract case number from content
                case_match = re.search(r"([A-Z]+\.\d{4}\.\d+)", content)
                case_number = case_match.group(1) if case_match else decision_id

                # Try to extract date from content
                decision_date = None
                date_match = re.search(r"(\d{1,2}\.\s*\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", content[:1000])
                if date_match:
                    decision_date = parse_date_flexible(date_match.group(1))

                # Apply date filter
                if from_date and decision_date and decision_date < from_date:
                    stats.add_skipped()
                    continue
                if to_date and decision_date and decision_date > to_date:
                    stats.add_skipped()
                    continue

                # Extract title
                title_elem = detail_soup.find("h1") or detail_soup.find("title")
                title_text = title_elem.get_text(strip=True) if title_elem else f"SO {case_number}"

                # Create decision
                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="so",
                        source_name="Solothurn",
                        level="cantonal",
                        canton="SO",
                        court="Obergericht",
                        chamber=None,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=title_text[:500],
                        language="de",
                        url=detail_url,
                        pdf_url=None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "gerichtsentscheide.so.ch",
                            "findinfo_id": decision_id,
                        },
                    )
                    session.merge(dec)
                    stats.add_imported()

                    if stats.imported % 10 == 0:
                        print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                    if limit and stats.imported >= limit:
                        break

                except Exception as e:
                    print(f"    Error saving: {e}")
                    stats.add_error()
                    continue

            if limit and stats.imported >= limit:
                break

            page += 1

        session.commit()

    print(stats.summary("Solothurn"))
    return stats.imported


# =============================================================================
# BASEL-STADT (BS) - FindInfoWeb
# =============================================================================

def scrape_bs_findinfoweb(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Basel-Stadt FindInfoWeb database."""
    print("Scraping Basel-Stadt (rechtsprechung.gerichte.bs.ch)...")

    if from_date:
        print(f"  Date filter: {from_date} to {to_date or date.today()}")

    base_url = "https://rechtsprechung.gerichte.bs.ch/cgi-bin/nph-omniscgi.exe"
    stats = ScraperStats()
    page = 1

    with get_session() as session:
        while True:
            params = {
                "OmnisPlatform": "WINDOWS",
                "WebServerUrl": "rechtsprechung.gerichte.bs.ch",
                "WebServerScript": "/cgi-bin/nph-omniscgi.exe",
                "OmnisLibrary": "JURISWEB",
                "OmnisClass": "rtFindinfoWebHtmlService",
                "OmnisServer": "JURISWEB,7000",
                "Aufruf": "loadTemplate",
                "cTemplate": "search_result.html",
                "Schema": "BS_FI_WEB",
                "cSprache": "DE",
                "Parametername": "WEB",
                "nAnzahlTrefferProSeite": "50",
                "nSeite": str(page),
                "bInstanzInt": "all",
            }

            url = f"{base_url}?{urlencode(params)}"

            try:
                resp = fetch_page(url)
            except Exception as e:
                print(f"  Error fetching page {page}: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find decision links
            decision_links = []
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if "nId=" in href:
                    decision_links.append(href)

            if not decision_links:
                print(f"  No more decisions found on page {page}")
                break

            print(f"  Page {page}: found {len(decision_links)} decisions")

            for href in decision_links:
                id_match = re.search(r"nId=(\d+)", href)
                if not id_match:
                    continue

                decision_id = id_match.group(1)
                stable_id = stable_uuid_url(f"bs-findinfo:{decision_id}")

                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                detail_url = urljoin("https://rechtsprechung.gerichte.bs.ch", href)
                try:
                    detail_resp = fetch_page(detail_url)
                except Exception as e:
                    stats.add_error()
                    continue

                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
                content_div = detail_soup.find("div", class_="content") or detail_soup.find("body")
                if not content_div:
                    stats.add_skipped()
                    continue

                content = content_div.get_text(separator="\n", strip=True)
                if len(content) < 100:
                    stats.add_skipped()
                    continue

                # Try to extract date from content
                decision_date = None
                date_match = re.search(r"(\d{1,2}\.\s*\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", content[:1000])
                if date_match:
                    decision_date = parse_date_flexible(date_match.group(1))

                # Apply date filter
                if from_date and decision_date and decision_date < from_date:
                    stats.add_skipped()
                    continue
                if to_date and decision_date and decision_date > to_date:
                    stats.add_skipped()
                    continue

                title = detail_soup.find("title")
                title_text = title.get_text(strip=True) if title else f"BS Decision {decision_id}"

                case_match = re.search(r"([A-Z]+\.\d{4}\.\d+)", title_text)
                case_number = case_match.group(1) if case_match else decision_id

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="bs",
                        source_name="Basel-Stadt",
                        level="cantonal",
                        canton="BS",
                        court="Appellationsgericht",
                        chamber=None,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=title_text[:500],
                        language="de",
                        url=detail_url,
                        pdf_url=None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "rechtsprechung.gerichte.bs.ch",
                            "findinfo_id": decision_id,
                        },
                    )
                    session.merge(dec)
                    stats.add_imported()

                    if stats.imported % 10 == 0:
                        print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                    if limit and stats.imported >= limit:
                        break

                except Exception as e:
                    stats.add_error()
                    continue

            if limit and stats.imported >= limit:
                break

            page += 1

        session.commit()

    print(stats.summary("Basel-Stadt"))
    return stats.imported


# =============================================================================
# APPENZELL INNERRHODEN (AI) - PDF Archives
# =============================================================================

def scrape_ai_pdfs(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Appenzell Innerrhoden PDF archives."""
    print("Scraping Appenzell Innerrhoden (ai.ch)...")

    base_url = "https://www.ai.ch"
    min_year = from_date.year if from_date else 1995

    # Known PDF URLs for different years
    pdf_urls = []

    # Recent years (2021+) - separate court decisions
    for year in range(2024, max(2020, min_year - 1), -1):
        pdf_urls.append(f"{base_url}/gerichte/gerichtsentscheide/gerichtsentscheide/gerichtsentscheide-{year}.pdf/download")

    # Older years - combined admin + court decisions
    if min_year <= 2020:
        for year in range(2020, min_year - 1, -1):
            pdf_urls.append(f"{base_url}/themen/staat-und-recht/veroeffentlichungen/verwaltungs-und-gerichtsentscheide/ftw-simplelayout-filelistingblock/verwaltungs-und-gerichtsentscheide-{year}.pdf/download")

    imported = 0
    skipped = 0

    with get_session() as session:
        for pdf_url in pdf_urls:
            year_match = re.search(r"(\d{4})", pdf_url)
            year = year_match.group(1) if year_match else "unknown"

            print(f"  Fetching {year}...")

            try:
                resp = httpx.get(pdf_url, headers=DEFAULT_HEADERS, timeout=120, follow_redirects=True)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
            except Exception as e:
                print(f"    Error: {e}")
                continue

            content = extract_pdf_text(resp.content)
            if not content or len(content) < 500:
                continue

            stable_id = stable_uuid_url(f"ai-yearly:{year}")

            existing = session.get(Decision, stable_id)
            if existing:
                skipped += 1
                continue

            try:
                dec = Decision(
                    id=stable_id,
                    source_id="ai",
                    source_name="Appenzell Innerrhoden",
                    level="cantonal",
                    canton="AI",
                    court="Kantonsgericht",
                    chamber=None,
                    docket=f"Sammlung {year}",
                    decision_date=date(int(year), 7, 1) if year.isdigit() else None,
                    published_date=None,
                    title=f"Gerichtsentscheide {year}",
                    language="de",
                    url=pdf_url,
                    pdf_url=pdf_url,
                    content_text=content,
                    content_hash=compute_hash(content),
                    meta={
                        "source": "ai.ch",
                        "year": year,
                        "type": "yearly_collection",
                    },
                )
                session.merge(dec)
                imported += 1
                session.commit()

                if limit and imported >= limit:
                    break

            except Exception as e:
                print(f"    Error saving: {e}")
                skipped += 1

            time.sleep(1)

        session.commit()

    print(f"\nImported {imported} yearly collections from AI")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# THURGAU (TG) - Confluence-based
# =============================================================================

def scrape_tg_confluence(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Thurgau Confluence portal."""
    print("Scraping Thurgau (rechtsprechung.tg.ch)...")

    base_url = "https://rechtsprechung.tg.ch"
    min_year = from_date.year if from_date else None

    imported = 0
    skipped = 0

    with get_session() as session:
        # Fetch main page to get year links
        try:
            resp = httpx.get(f"{base_url}/og/entscheide", headers=DEFAULT_HEADERS, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            print(f"  Error: {e}")
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find year links (e.g., rbog-2024, rbog-2023, etc.)
        year_links = []
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if "rbog-" in href.lower():
                # Skip years older than from_date
                m = re.search(r"rbog-(\d{4})", href, re.I)
                if min_year and m and int(m.group(1)) < min_year:
                    continue
                year_links.append(urljoin(base_url, href))

        print(f"  Found {len(year_links)} year collections")

        for year_url in year_links:
            year_match = re.search(r"rbog-(\d{4})", year_url, re.I)
            year = year_match.group(1) if year_match else "unknown"

            print(f"  Processing RBOG {year}...")

            try:
                year_resp = httpx.get(year_url, headers=DEFAULT_HEADERS, timeout=60)
                year_resp.raise_for_status()
            except Exception as e:
                print(f"    Error: {e}")
                continue

            year_soup = BeautifulSoup(year_resp.text, "html.parser")

            # Find individual decision links
            for link in year_soup.find_all("a", href=True):
                href = link.get("href", "")
                text = link.get_text(strip=True)

                # Skip navigation links
                if not text or len(text) < 5:
                    continue

                # Look for decision patterns
                if re.search(r"\d+\s*/\s*\d{4}", text) or "Entscheid" in text:
                    decision_url = urljoin(base_url, href)

                    # Generate stable ID from URL
                    stable_id = stable_uuid_url(f"tg:{href}")

                    existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        dec_resp = httpx.get(decision_url, headers=DEFAULT_HEADERS, timeout=60)
                        dec_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    dec_soup = BeautifulSoup(dec_resp.text, "html.parser")
                    content_div = dec_soup.find("div", class_="content") or dec_soup.find("article") or dec_soup.find("main")

                    if not content_div:
                        skipped += 1
                        continue

                    content = content_div.get_text(separator="\n", strip=True)
                    if len(content) < 200:
                        skipped += 1
                        continue

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="tg",
                            source_name="Thurgau",
                            level="cantonal",
                            canton="TG",
                            court="Obergericht",
                            chamber=None,
                            docket=text[:100],
                            decision_date=date(int(year), 7, 1) if year.isdigit() else None,
                            published_date=None,
                            title=text[:500],
                            language="de",
                            url=decision_url,
                            pdf_url=None,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={
                                "source": "rechtsprechung.tg.ch",
                                "rbog_year": year,
                            },
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 10 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                        if limit and imported >= limit:
                            break

                    except Exception as e:
                        skipped += 1

                    time.sleep(0.3)

            if limit and imported >= limit:
                break

        session.commit()

    print(f"\nImported {imported} decisions from Thurgau")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# BERN (BE) - Sitemap-based (ZSG + VG)
# =============================================================================

def scrape_be_sitemap(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Bern via sitemap discovery."""
    print("Scraping Bern (apps.be.ch)...")

    min_year = from_date.year if from_date else None

    sitemaps = [
        ("https://www.zsg-entscheide.apps.be.ch/tribunapublikation/sitemap.xml", "ZSG"),
        ("https://www.vg-urteile.apps.be.ch/tribunapublikation/sitemap.xml", "VG"),
    ]

    imported = 0
    skipped = 0

    max_urls = 500 if min_year else 10000

    with get_session() as session:
        for sitemap_url, court_type in sitemaps:
            print(f"  Fetching {court_type} sitemap...")

            try:
                resp = httpx.get(sitemap_url, headers=DEFAULT_HEADERS, timeout=60)
                resp.raise_for_status()
            except Exception as e:
                print(f"    Error fetching sitemap: {e}")
                continue

            # Parse sitemap XML
            soup = BeautifulSoup(resp.text, "xml")
            urls = soup.find_all("loc")

            print(f"    Found {len(urls)} URLs in sitemap")

            for url_elem in urls[:max_urls]:
                url = url_elem.get_text(strip=True)

                # Skip non-decision URLs
                if "/decision/" not in url.lower() and "/entscheid/" not in url.lower():
                    continue

                # Date filter: skip entries from years before from_date
                if min_year:
                    parent = url_elem.parent
                    lastmod = parent.find("lastmod") if parent else None
                    if lastmod:
                        try:
                            if int(lastmod.get_text(strip=True)[:4]) < min_year:
                                continue
                        except (ValueError, IndexError):
                            pass
                    yr = _url_year(url)
                    if yr and yr < min_year:
                        continue

                stable_id = stable_uuid_url(f"be:{url}")

                existing = session.get(Decision, stable_id)
                if existing:
                    skipped += 1
                    continue

                try:
                    detail_resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60)
                    detail_resp.raise_for_status()
                except Exception as e:
                    skipped += 1
                    continue

                soup = BeautifulSoup(detail_resp.text, "html.parser")

                # Extract content
                content_div = soup.find("div", class_="decision") or soup.find("article") or soup.find("main") or soup.find("body")
                if not content_div:
                    skipped += 1
                    continue

                content = content_div.get_text(separator="\n", strip=True)
                if len(content) < 200:
                    skipped += 1
                    continue

                # Extract title
                title_elem = soup.find("h1") or soup.find("title")
                title = title_elem.get_text(strip=True) if title_elem else f"BE {court_type} Decision"

                # Extract case number
                case_match = re.search(r"(\d+[A-Z]*[\s_-]*\d+/\d{4}|\d{4}[\s_-]*\d+)", title) or re.search(r"(\d+[A-Z]*[\s_-]*\d+/\d{4}|\d{4}[\s_-]*\d+)", content[:500])
                case_number = case_match.group(1) if case_match else None

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="be",
                        source_name="Bern",
                        level="cantonal",
                        canton="BE",
                        court=court_type,
                        chamber=None,
                        docket=case_number,
                        decision_date=None,
                        published_date=None,
                        title=title[:500],
                        language="de",
                        url=url,
                        pdf_url=None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={"source": "apps.be.ch", "court_type": court_type},
                    )
                    session.merge(dec)
                    imported += 1

                    if imported % 50 == 0:
                        print(f"    Imported {imported} (skipped {skipped})...")
                        session.commit()

                    if limit and imported >= limit:
                        break

                except Exception as e:
                    skipped += 1

                time.sleep(0.3)

            if limit and imported >= limit:
                break

        session.commit()

    print(f"\nImported {imported} decisions from Bern")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# ST. GALLEN (SG) - HTML Crawler
# =============================================================================

def scrape_sg_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from St. Gallen court website."""
    print("Scraping St. Gallen (gerichte.sg.ch)...")

    base_url = "https://www.gerichte.sg.ch"
    start_url = "https://www.gerichte.sg.ch/home/dienstleistungen/rechtsprechung.html"
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    imported = 0
    skipped = 0
    visited = set()
    to_visit = [start_url]

    with get_session() as session:
        while to_visit and (not limit or imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                resp.raise_for_status()
            except Exception as e:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find all links
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if not href:
                    continue

                full_url = urljoin(base_url, href)

                # Only follow internal links
                if not full_url.startswith(base_url):
                    continue

                # Check if this is a decision page (PDF or HTML)
                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"sg:{full_url}")

                    existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        pdf_resp = httpx.get(full_url, headers=DEFAULT_HEADERS, timeout=120)
                        pdf_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        skipped += 1
                        continue

                    # Extract case number from filename or content
                    filename = href.split("/")[-1]
                    case_match = re.search(r"([A-Z]+[-_]?\d+[-_/]\d{4})", filename) or re.search(r"([A-Z]+[-_]?\d+[-_/]\d{4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="sg",
                            source_name="St. Gallen",
                            level="cantonal",
                            canton="SG",
                            court="Kantonsgericht",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=None,
                            published_date=None,
                            title=f"SG {case_number}"[:500],
                            language="de",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "gerichte.sg.ch"},
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 20 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                    except Exception as e:
                        skipped += 1

                elif "rechtsprechung" in href.lower() and full_url not in visited:
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        session.commit()

    print(f"\nImported {imported} decisions from St. Gallen")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# LUZERN (LU) - HTML Crawler (LGVE)
# =============================================================================

def scrape_lu_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Luzern LGVE."""
    print("Scraping Luzern (gerichte.lu.ch)...")

    base_url = "https://gerichte.lu.ch"
    start_urls = [
        "https://gerichte.lu.ch/recht_sprechung/lgve",
        "https://gerichte.lu.ch/recht_sprechung/Hinterlegungen",
    ]
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    imported = 0
    skipped = 0
    visited = set()
    to_visit = list(start_urls)

    with get_session() as session:
        while to_visit and (not limit or imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                resp.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                full_url = urljoin(base_url, href)

                if not full_url.startswith(base_url):
                    continue

                # Check for PDF decisions
                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"lu:{full_url}")

                    with session.no_autoflush:
                        existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        pdf_resp = httpx.get(full_url, headers=DEFAULT_HEADERS, timeout=120)
                        pdf_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        skipped += 1
                        continue

                    filename = href.split("/")[-1]
                    case_match = re.search(r"(\d+[A-Z]*\s*\d*/\d{2,4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="lu",
                            source_name="Luzern",
                            level="cantonal",
                            canton="LU",
                            court="Kantonsgericht",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=None,
                            published_date=None,
                            title=f"LU LGVE {case_number}"[:500],
                            language="de",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "gerichte.lu.ch"},
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 20 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                    except Exception:
                        session.rollback()
                        skipped += 1

                elif ("lgve" in href.lower() or "recht_sprechung" in href.lower()) and full_url not in visited:
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        try:
            session.commit()
        except Exception:
            session.rollback()

    print(f"\nImported {imported} decisions from Luzern")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# SCHAFFHAUSEN (SH) - HTML Crawler
# =============================================================================

def scrape_sh_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Schaffhausen Obergericht."""
    print("Scraping Schaffhausen (obergerichtsentscheide.sh.ch)...")

    base_url = "https://obergerichtsentscheide.sh.ch"
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    imported = 0
    skipped = 0
    visited = set()
    to_visit = [base_url]

    with get_session() as session:
        while to_visit and (not limit or imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                resp.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                full_url = urljoin(base_url, href)

                if not full_url.startswith(base_url) and not full_url.startswith("https://sh.ch"):
                    continue

                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"sh:{full_url}")

                    existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        pdf_resp = httpx.get(full_url, headers=DEFAULT_HEADERS, timeout=120)
                        pdf_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        skipped += 1
                        continue

                    filename = href.split("/")[-1]
                    case_match = re.search(r"(\d+/\d{4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="sh",
                            source_name="Schaffhausen",
                            level="cantonal",
                            canton="SH",
                            court="Obergericht",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=None,
                            published_date=None,
                            title=f"SH {case_number}"[:500],
                            language="de",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "obergerichtsentscheide.sh.ch"},
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 20 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                    except Exception:
                        skipped += 1

                elif full_url not in visited and "obergerichtsentscheide" in full_url:
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        session.commit()

    print(f"\nImported {imported} decisions from Schaffhausen")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# SCHWYZ (SZ) - HTML Crawler
# =============================================================================

def scrape_sz_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Schwyz Kantonsgericht."""
    print("Scraping Schwyz (kgsz.ch)...")

    base_url = "https://www.kgsz.ch"
    start_url = "https://www.kgsz.ch/rechtsprechung/"
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    imported = 0
    skipped = 0
    visited = set()
    to_visit = [start_url]

    with get_session() as session:
        while to_visit and (not limit or imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                resp.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                full_url = urljoin(base_url, href)

                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"sz:{full_url}")

                    with session.no_autoflush:
                        existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        pdf_resp = httpx.get(full_url, headers=DEFAULT_HEADERS, timeout=120)
                        pdf_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        skipped += 1
                        continue

                    filename = href.split("/")[-1]
                    case_match = re.search(r"(\d+/\d{4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="sz",
                            source_name="Schwyz",
                            level="cantonal",
                            canton="SZ",
                            court="Kantonsgericht",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=None,
                            published_date=None,
                            title=f"SZ {case_number}"[:500],
                            language="de",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "kgsz.ch"},
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 20 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                    except Exception:
                        session.rollback()
                        skipped += 1

                elif "rechtsprechung" in href.lower() and full_url not in visited:
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        try:
            session.commit()
        except Exception:
            session.rollback()

    print(f"\nImported {imported} decisions from Schwyz")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# VALAIS/WALLIS (VS) - HTML Crawler
# =============================================================================

def scrape_vs_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Valais lawsearch portal."""
    print("Scraping Valais (apps.vs.ch/le/)...")

    base_url = "https://apps.vs.ch"
    start_url = "https://apps.vs.ch/le/"
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    imported = 0
    skipped = 0
    visited = set()
    to_visit = [start_url]

    with get_session() as session:
        while to_visit and (not limit or imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                resp.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                full_url = urljoin(base_url, href)

                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"vs:{full_url}")

                    existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        pdf_resp = httpx.get(full_url, headers=DEFAULT_HEADERS, timeout=120)
                        pdf_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        skipped += 1
                        continue

                    filename = href.split("/")[-1]
                    case_match = re.search(r"(\d+/\d{4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename

                    # Detect language
                    lang = "fr" if re.search(r"\b(tribunal|canton|décision)\b", content[:1000], re.I) else "de"

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="vs",
                            source_name="Valais",
                            level="cantonal",
                            canton="VS",
                            court="Kantonsgericht",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=None,
                            published_date=None,
                            title=f"VS {case_number}"[:500],
                            language=lang,
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "apps.vs.ch"},
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 20 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                    except Exception:
                        skipped += 1

                elif "/le/" in full_url and full_url not in visited:
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        session.commit()

    print(f"\nImported {imported} decisions from Valais")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# NEUCHÂTEL (NE) - FindInfoWeb
# =============================================================================

def scrape_ne_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Neuchâtel FindInfoWeb database."""
    print("Scraping Neuchâtel (jurisprudence.ne.ch)...")

    base_url = "https://jurisprudence.ne.ch/scripts/omnisapi.dll"
    min_year = from_date.year if from_date else None

    imported = 0
    skipped = 0
    page = 1

    with get_session() as session:
        while True:
            params = {
                "OmnisPlatform": "WINDOWS",
                "WebServerUrl": "jurisprudence.ne.ch",
                "WebServerScript": "/scripts/omnisapi.dll",
                "OmnisLibrary": "JURISWEB",
                "OmnisClass": "rtFindinfoWebHtmlService",
                "OmnisServer": "JURISWEB,7000",
                "Aufruf": "home",
                "cTemplate": "home.html",
                "Schema": "NE_WEB",
                "cSprache": "FRE",
                "Parametername": "NEWEB",
                "nAnzahlTrefferProSeite": "50",
                "nSeite": str(page),
                "bSelectAll": "1",
                "bInstanzInt": "all",
            }

            url = f"{base_url}?{urlencode(params)}"

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60)
                resp.raise_for_status()
            except Exception as e:
                print(f"  Error fetching page {page}: {e}")
                break

            # Find decision IDs
            decision_ids = re.findall(r"nF30_KEY=(\d+)", resp.text)
            decision_ids = list(dict.fromkeys(decision_ids))

            if not decision_ids:
                print(f"  No more decisions on page {page}")
                break

            print(f"  Page {page}: found {len(decision_ids)} decisions")

            for decision_id in decision_ids:
                stable_id = stable_uuid_url(f"ne-findinfo:{decision_id}")

                existing = session.get(Decision, stable_id)
                if existing:
                    skipped += 1
                    continue

                # Fetch decision detail
                detail_params = {
                    "OmnisPlatform": "WINDOWS",
                    "WebServerUrl": "jurisprudence.ne.ch",
                    "WebServerScript": "/scripts/omnisapi.dll",
                    "OmnisLibrary": "JURISWEB",
                    "OmnisClass": "rtFindinfoWebHtmlService",
                    "OmnisServer": "JURISWEB,7000",
                    "Parametername": "NEWEB",
                    "Schema": "NE_WEB",
                    "Aufruf": "getMarkupDocument",
                    "cSprache": "FRE",
                    "nF30_KEY": decision_id,
                    "cTemplate": "/simple/search_result_document.html",
                }
                detail_url = f"{base_url}?{urlencode(detail_params)}"

                try:
                    detail_resp = httpx.get(detail_url, headers=DEFAULT_HEADERS, timeout=60)
                    detail_resp.raise_for_status()
                except Exception:
                    skipped += 1
                    continue

                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
                content_div = detail_soup.find("div", class_="dokument") or detail_soup.find("body")
                if not content_div:
                    skipped += 1
                    continue

                content = content_div.get_text(separator="\n", strip=True)
                if len(content) < 100:
                    skipped += 1
                    continue

                # Extract case number
                case_match = re.search(r"([A-Z]+\.?\d{4}\.\d+|[A-Z]+\.\d+[-/]\d{4})", content)
                case_number = case_match.group(1) if case_match else decision_id

                # Date filtering: extract year from case number or content
                if min_year and case_number:
                    yr_match = re.search(r'(20[012]\d)', case_number)
                    if yr_match and int(yr_match.group(1)) < min_year:
                        skipped += 1
                        continue

                decision_date = None
                date_match = re.search(r"(\d{1,2}\.\d{1,2}\.\d{4}|\d{1,2}\s+\w+\s+\d{4})", content[:1000])
                if date_match:
                    decision_date = parse_date_flexible(date_match.group(1))
                if from_date and decision_date and decision_date < from_date:
                    skipped += 1
                    continue

                title_elem = detail_soup.find("h1") or detail_soup.find("title")
                title_text = title_elem.get_text(strip=True) if title_elem else f"NE {case_number}"

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="ne",
                        source_name="Neuchâtel",
                        level="cantonal",
                        canton="NE",
                        court="Tribunal cantonal",
                        chamber=None,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=title_text[:500],
                        language="fr",
                        url=detail_url,
                        pdf_url=None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={"source": "jurisprudence.ne.ch", "findinfo_id": decision_id},
                    )
                    session.merge(dec)
                    imported += 1

                    if imported % 20 == 0:
                        print(f"    Imported {imported} (skipped {skipped})...")
                        session.commit()

                    if limit and imported >= limit:
                        break

                except Exception:
                    skipped += 1

                time.sleep(0.3)

            if limit and imported >= limit:
                break

            page += 1
            time.sleep(1)

        session.commit()

    print(f"\nImported {imported} decisions from Neuchâtel")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# AARGAU (AG) - AGVE Portal
# =============================================================================

def scrape_ag_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Aargau AGVE portal."""
    print("Scraping Aargau (ag.ch AGVE)...")

    base_url = "https://www.ag.ch"
    start_url = "https://www.ag.ch/de/themen/recht-justiz/gesetze-entscheide/agve"
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    imported = 0
    skipped = 0
    visited = set()
    to_visit = [start_url]

    with get_session() as session:
        while to_visit and (not limit or imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                resp.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                full_url = urljoin(base_url, href)

                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"ag:{full_url}")

                    with session.no_autoflush:
                        existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        pdf_resp = httpx.get(full_url, headers=DEFAULT_HEADERS, timeout=120)
                        pdf_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        skipped += 1
                        continue

                    filename = href.split("/")[-1]
                    case_match = re.search(r"(\d+[-/]\d{4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="ag",
                            source_name="Aargau",
                            level="cantonal",
                            canton="AG",
                            court="Obergericht",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=None,
                            published_date=None,
                            title=f"AG AGVE {case_number}"[:500],
                            language="de",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "ag.ch/agve"},
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 20 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                    except Exception:
                        session.rollback()
                        skipped += 1

                elif ("agve" in href.lower() or "entscheide" in href.lower()) and full_url not in visited and full_url.startswith(base_url):
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        try:
            session.commit()
        except Exception:
            session.rollback()

    print(f"\nImported {imported} decisions from Aargau")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# BASEL-LANDSCHAFT (BL) - Swisslex/BL Portal
# =============================================================================

def scrape_bl_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Basel-Landschaft portal."""
    print("Scraping Basel-Landschaft (baselland.ch)...")

    base_url = "https://www.baselland.ch"
    start_url = "https://www.baselland.ch/politik-und-behorden/gerichte/rechtsprechung"
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    imported = 0
    skipped = 0
    visited = set()
    to_visit = [start_url]

    with get_session() as session:
        while to_visit and (not limit or imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                resp.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                full_url = urljoin(base_url, href)

                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"bl:{full_url}")

                    existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        pdf_resp = httpx.get(full_url, headers=DEFAULT_HEADERS, timeout=120)
                        pdf_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        skipped += 1
                        continue

                    filename = href.split("/")[-1]
                    case_match = re.search(r"(\d+[-/]\d{4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="bl",
                            source_name="Basel-Landschaft",
                            level="cantonal",
                            canton="BL",
                            court="Kantonsgericht",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=None,
                            published_date=None,
                            title=f"BL {case_number}"[:500],
                            language="de",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "baselland.ch"},
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 20 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                    except Exception:
                        skipped += 1

                elif "rechtsprechung" in href.lower() and full_url not in visited and full_url.startswith(base_url):
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        session.commit()

    print(f"\nImported {imported} decisions from Basel-Landschaft")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# FRIBOURG (FR) - Tribuna Portal
# =============================================================================

def scrape_fr_crawler(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Fribourg Tribuna portal.

    The fr.ch portal organizes decisions by year in the URL path, e.g.
    ``/arrets-de-la-section-civile-du-tribunal-cantonal-2024``.
    When *from_date* is set we only follow links whose year >= from_date.year
    so that daily incremental runs don't crawl the entire 20-year archive.
    """
    print("Scraping Fribourg (fr.ch)...")

    base_url = "https://www.fr.ch"
    start_url = "https://www.fr.ch/de/staat-und-recht/justiz/suchmaschine-tribuna-publikation"

    # Year cutoff: skip pages for years older than from_date
    min_year = from_date.year if from_date else None
    if min_year:
        print(f"  Limiting to pages from year >= {min_year}")

    imported = 0
    skipped = 0
    visited = set()
    to_visit = [start_url]

    # Pattern to extract a trailing year from fr.ch URLs like
    # /arrets-de-la-section-civile-du-tribunal-cantonal-2024
    _year_in_url = re.compile(r"-(\d{4})(?:#.*)?$")

    with get_session() as session:
        while to_visit and (not limit or imported < limit):
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                resp.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                full_url = urljoin(base_url, href)

                if ".pdf" in href.lower():
                    stable_id = stable_uuid_url(f"fr:{full_url}")

                    existing = session.get(Decision, stable_id)
                    if existing:
                        skipped += 1
                        continue

                    try:
                        pdf_resp = httpx.get(full_url, headers=DEFAULT_HEADERS, timeout=120)
                        pdf_resp.raise_for_status()
                    except Exception:
                        skipped += 1
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        skipped += 1
                        continue

                    filename = href.split("/")[-1]
                    case_match = re.search(r"(\d+[-/]\d{4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename

                    # Detect language
                    lang = "fr" if re.search(r"\b(tribunal|canton|décision)\b", content[:1000], re.I) else "de"

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="fr",
                            source_name="Fribourg",
                            level="cantonal",
                            canton="FR",
                            court="Kantonsgericht",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=None,
                            published_date=None,
                            title=f"FR {case_number}"[:500],
                            language=lang,
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "fr.ch/tribuna"},
                        )
                        session.merge(dec)
                        imported += 1

                        if imported % 20 == 0:
                            print(f"    Imported {imported} (skipped {skipped})...")
                            session.commit()

                    except Exception:
                        skipped += 1

                elif ("tribuna" in href.lower() or "justiz" in href.lower()) and full_url not in visited and full_url.startswith(base_url):
                    # Skip year pages older than from_date
                    if min_year:
                        m = _year_in_url.search(full_url)
                        if m and int(m.group(1)) < min_year:
                            continue
                    to_visit.append(full_url)

            time.sleep(0.5)

        session.commit()

    print(f"\nImported {imported} decisions from Fribourg")
    print(f"Skipped {skipped}")
    return imported


# =============================================================================
# URI (UR) - Direct JSON/HTML Scraper
# =============================================================================

def scrape_ur_crawler(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Uri court website (ur.ch/rechtsprechung).

    Uri uses a JSON data structure embedded in the page's data-entities attribute.
    Documents are accessed via /_rte/publikation/{id} which redirects to PDFs.
    """
    import json
    import html

    print("Scraping Uri (ur.ch/rechtsprechung)...")

    base_url = "https://www.ur.ch"
    start_url = "https://www.ur.ch/rechtsprechung"

    stats = ScraperStats()

    with get_session() as session:
        try:
            resp = fetch_page(start_url)
        except Exception as e:
            print(f"  Error fetching main page: {e}")
            return 0

        # Parse the data-entities JSON attribute
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find elements with data-entities attribute
        data_elements = soup.find_all(attrs={"data-entities": True})

        doc_ids = []
        for elem in data_elements:
            try:
                # Decode HTML entities and parse JSON
                json_str = html.unescape(elem.get("data-entities", "{}"))
                data = json.loads(json_str)

                # Extract document info from the data array
                if isinstance(data, dict) and "data" in data:
                    for item in data["data"]:
                        # Extract the download URL pattern: /_rte/publikation/{id}
                        download_btn = item.get("_downloadBtn", "")
                        match = re.search(r'href=["\']([^"\']+)["\']', download_btn)
                        if match:
                            doc_url = match.group(1)
                            doc_name = item.get("name", "")
                            doc_date = item.get("datum", "")
                            doc_ids.append({
                                "url": doc_url,
                                "name": doc_name,
                                "date": doc_date,
                            })
            except (json.JSONDecodeError, KeyError):
                continue

        print(f"  Found {len(doc_ids)} documents in JSON data")

        for doc in doc_ids:
            if limit and stats.imported >= limit:
                break

            # Skip documents older than from_date
            if from_date and doc["date"]:
                doc_date = parse_date_flexible(doc["date"])
                if doc_date and doc_date < from_date:
                    continue

            doc_url = urljoin(base_url, doc["url"])
            stable_id = stable_uuid_url(f"ur:{doc_url}")

            existing = session.get(Decision, stable_id)
            if existing:
                stats.add_skipped()
                continue

            try:
                # Follow redirects to get the actual PDF
                pdf_resp = fetch_page(doc_url, timeout=120)
            except Exception:
                stats.add_skipped()
                continue

            content = extract_pdf_text(pdf_resp.content)
            if not content or len(content) < 200:
                stats.add_skipped()
                continue

            # Extract case number from document name or content
            case_number = doc["name"]
            case_match = re.search(r"(\d{4}_[A-Z]+\s*[A-Z]*\s*\d+\s*\d+)", doc["name"])
            if case_match:
                case_number = case_match.group(1)
            else:
                case_match = re.search(r"([A-Z]+\s*\d+[-/]\d{2,4})", content[:500])
                if case_match:
                    case_number = case_match.group(1)

            # Parse date
            decision_date = parse_date_flexible(doc["date"]) if doc["date"] else None
            if not decision_date:
                date_match = re.search(r"(\d{1,2}\.\s*\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", content[:1000])
                if date_match:
                    decision_date = parse_date_flexible(date_match.group(1))

            try:
                dec = Decision(
                    id=stable_id,
                    source_id="ur",
                    source_name="Uri",
                    level="cantonal",
                    canton="UR",
                    court="Obergericht",
                    chamber=None,
                    docket=case_number[:100] if case_number else None,
                    decision_date=decision_date,
                    published_date=None,
                    title=f"UR {case_number}"[:500] if case_number else doc["name"][:500],
                    language="de",
                    url=doc_url,
                    pdf_url=str(pdf_resp.url) if hasattr(pdf_resp, 'url') else doc_url,
                    content_text=content,
                    content_hash=compute_hash(content),
                    meta={"source": "ur.ch/rechtsprechung", "original_name": doc["name"]},
                )
                session.merge(dec)
                stats.add_imported()

                if stats.imported % 10 == 0:
                    print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                    session.commit()

            except Exception as e:
                print(f"    Error: {e}")
                stats.add_error()

            time.sleep(0.5)

        session.commit()

    print(stats.summary("Uri"))
    return stats.imported


# =============================================================================
# APPENZELL AUSSERRHODEN (AR) - LEv4 API (rechtsprechung.ar.ch)
# =============================================================================

def scrape_ar_lev4(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Appenzell Ausserrhoden via LEv4 API.

    AR uses the Weblaw LEv4 platform at rechtsprechung.ar.ch with a
    Netlify Functions API backend.  When *from_date* is set, decisions
    older than that date are skipped before downloading the PDF.
    """
    print("Scraping Appenzell Ausserrhoden (rechtsprechung.ar.ch via LEv4 API)...")

    api_url = "https://rechtsprechung.ar.ch/api/.netlify/functions/searchQueryService"
    stats = ScraperStats()
    page_size = 20

    with get_session() as session:
        from_idx = 0

        while True:
            if limit and stats.imported >= limit:
                break

            rate_limiter.wait()

            # LEv4 API requires specific aggs format
            payload = {
                "guiLanguage": "de",
                "aggs": {
                    "fields": ["treePath", "entscheidKategorie", "argvpBehoerde"],
                    "size": 100
                },
                "from": from_idx,
                "size": page_size,
            }

            try:
                resp = httpx.post(api_url, json=payload, headers=DEFAULT_HEADERS, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  Error fetching page {from_idx}: {e}")
                stats.add_error()
                break

            documents = data.get("documents", [])
            if not documents:
                break

            total = data.get("totalNumberOfDocuments", 0)
            if from_idx == 0:
                print(f"  Found {total} total documents")

            for doc in documents:
                if limit and stats.imported >= limit:
                    break

                leid = doc.get("leid", "")
                metadata = doc.get("metadataKeywordTextMap", {})
                date_map = doc.get("metadataDateMap", {})

                # Get PDF URL (may be a list or string)
                pdf_url_raw = metadata.get("originalUrl", "")
                if isinstance(pdf_url_raw, list):
                    pdf_url = pdf_url_raw[0] if pdf_url_raw else ""
                else:
                    pdf_url = pdf_url_raw
                if not pdf_url:
                    stats.add_skipped()
                    continue

                # Generate stable ID
                stable_id = stable_uuid_url(f"ar:{leid}")

                # Check if exists
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                # Parse decision date early so we can skip old decisions before PDF download
                decision_date = None
                date_str = date_map.get("decisionDate", "")
                if date_str:
                    decision_date = parse_date_flexible(date_str)
                if from_date and decision_date and decision_date < from_date:
                    stats.add_skipped()
                    continue
                if to_date and decision_date and decision_date > to_date:
                    stats.add_skipped()
                    continue

                # Download PDF
                try:
                    rate_limiter.wait()
                    pdf_resp = httpx.get(pdf_url, headers=DEFAULT_HEADERS, timeout=120, follow_redirects=True)
                    pdf_resp.raise_for_status()
                    content = extract_pdf_text(pdf_resp.content)
                except Exception as e:
                    print(f"    Error downloading PDF: {e}")
                    stats.add_error()
                    continue

                if not content or len(content) < 200:
                    stats.add_skipped()
                    continue

                # Extract metadata (fields may be lists or strings)
                def get_first(val):
                    if isinstance(val, list):
                        return val[0] if val else ""
                    return val or ""

                title = get_first(metadata.get("title", ""))
                gvp_number = get_first(metadata.get("gvpNumber", ""))
                filename = get_first(metadata.get("fileName", ""))
                case_number = gvp_number or filename.replace(".pdf", "") if filename else ""
                authority = get_first(metadata.get("argvpBehoerde", ""))
                category = get_first(metadata.get("entscheidKategorie", ""))

                # Map authority to court name
                court = "Obergericht"
                if authority == "KG":
                    court = "Kantonsgericht"
                elif authority == "Verwaltung":
                    court = "Verwaltungsgericht"

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="ar",
                        source_name="Appenzell Ausserrhoden",
                        level="cantonal",
                        canton="AR",
                        court=court,
                        chamber=None,
                        docket=case_number[:100] if case_number else None,
                        decision_date=decision_date,
                        published_date=None,
                        title=(title or f"AR {case_number}")[:500],
                        language="de",
                        url=f"https://rechtsprechung.ar.ch/#/document/{leid}",
                        pdf_url=pdf_url,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "rechtsprechung.ar.ch",
                            "leid": leid,
                            "category": category,
                            "authority": authority,
                        },
                    )
                    session.merge(dec)
                    stats.add_imported()

                    if stats.imported % 20 == 0:
                        print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                except Exception as e:
                    print(f"    Error saving: {e}")
                    stats.add_error()

            from_idx += page_size

            # Check if we've fetched all documents
            if not data.get("hasMoreResults", False):
                break

        session.commit()

    print(stats.summary("Appenzell Ausserrhoden"))
    return stats.imported


# =============================================================================
# JURA (JU) - Direct HTML Crawler (French)
# =============================================================================

def scrape_ju_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Jura (jura.ch/JUST)."""
    print("Scraping Jura (jura.ch)...")

    base_url = "https://www.jura.ch"
    start_urls = [
        "https://www.jura.ch/JUST/Instances-judiciaires/Tribunal-cantonal/Jurisprudence-recente.html",
        "https://www.jura.ch/JUST/Instances-judiciaires/Tribunal-cantonal/Revue-jurassienne-de-jurisprudence.html",
    ]
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    stats = ScraperStats()
    visited = set()
    to_visit = list(start_urls)

    with get_session() as session:
        while to_visit and (not limit or stats.imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = fetch_page(url)
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                full_url = urljoin(base_url, href)

                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"ju:{full_url}")

                    existing = session.get(Decision, stable_id)
                    if existing:
                        stats.add_skipped()
                        continue

                    try:
                        pdf_resp = fetch_page(full_url, timeout=120)
                    except Exception:
                        stats.add_skipped()
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        stats.add_skipped()
                        continue

                    filename = href.split("/")[-1]
                    case_match = re.search(r"([A-Z]*\d+[-/]\d{2,4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename.replace(".pdf", "")

                    decision_date = None
                    date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", content[:1000])
                    if date_match:
                        decision_date = parse_date_flexible(date_match.group(1))

                    if from_date and decision_date and decision_date < from_date:
                        stats.add_skipped()
                        continue

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="ju",
                            source_name="Jura",
                            level="cantonal",
                            canton="JU",
                            court="Tribunal cantonal",
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=decision_date,
                            published_date=None,
                            title=f"JU {case_number}"[:500],
                            language="fr",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "jura.ch/JUST"},
                        )
                        session.merge(dec)
                        stats.add_imported()

                        if stats.imported % 20 == 0:
                            print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                            session.commit()

                    except Exception:
                        stats.add_error()

                elif ("jurisprudence" in href.lower() or "just" in href.lower()) and full_url not in visited and full_url.startswith(base_url):
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        session.commit()

    print(stats.summary("Jura"))
    return stats.imported


# =============================================================================
# GLARUS (GL) - via entscheidsuche.ch API
# =============================================================================

def scrape_gl_entscheidsuche(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Glarus via entscheidsuche.ch API.

    GL blocks direct access to gl.ch, so we use entscheidsuche.ch which
    provides access to 70+ Glarus decisions via their Elasticsearch API.
    """
    print("Scraping Glarus (via entscheidsuche.ch API)...")

    api_url = "https://entscheidsuche.ch/_search.php"
    docs_base = "https://entscheidsuche.ch/docs/GL_Omni"
    stats = ScraperStats()
    batch_size = 100

    with get_session() as session:
        search_after = None

        while True:
            if limit and stats.imported >= limit:
                break

            rate_limiter.wait()

            # Build query with optional date range filter
            must_clauses: list[dict] = [{"term": {"canton": "GL"}}]
            if from_date:
                must_clauses.append({"range": {"date": {"gte": from_date.isoformat()}}})

            query = {
                "query": {"bool": {"must": must_clauses}} if len(must_clauses) > 1 else {"term": {"canton": "GL"}},
                "size": batch_size,
                "sort": [{"date": "desc"}, {"_id": "asc"}],
                "_source": ["id", "date", "canton", "title", "abstract", "attachment", "hierarchy", "reference"]
            }

            if search_after:
                query["search_after"] = search_after

            try:
                resp = httpx.post(api_url, json=query, headers=DEFAULT_HEADERS, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  Error querying API: {e}")
                stats.add_error()
                break

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            total = data.get("hits", {}).get("total", {})
            if isinstance(total, dict):
                total = total.get("value", 0)
            if search_after is None:
                print(f"  Found {total} total documents")

            search_after = hits[-1].get("sort")

            for hit in hits:
                if limit and stats.imported >= limit:
                    break

                src = hit.get("_source", {})
                doc_id = src.get("id") or hit.get("_id")

                # Generate stable ID
                stable_id = stable_uuid_url(f"gl:{doc_id}")

                # Check if exists by ID
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                # Also check by URL (may exist with different ID from old scraper)
                doc_url = f"{docs_base}/{doc_id}.html"
                existing_by_url = session.exec(
                    select(Decision).where(Decision.url == doc_url)
                ).first()
                if existing_by_url:
                    stats.add_skipped()
                    continue

                # Get content from entscheidsuche.ch
                attachment = src.get("attachment", {})
                content = attachment.get("content", "")

                # If no content in API, try fetching the HTML file
                if not content or len(content) < 100:
                    html_url = f"{docs_base}/{doc_id}.html"
                    try:
                        rate_limiter.wait()
                        html_resp = httpx.get(html_url, headers=DEFAULT_HEADERS, timeout=60)
                        if html_resp.status_code == 200:
                            soup = BeautifulSoup(html_resp.text, "html.parser")
                            content = soup.get_text(separator="\n", strip=True)
                    except Exception:
                        pass

                if not content or len(content) < 100:
                    stats.add_skipped()
                    continue

                # Parse date
                date_str = src.get("date")
                decision_date = None
                if date_str:
                    try:
                        decision_date = date.fromisoformat(date_str)
                    except ValueError:
                        decision_date = parse_date_flexible(date_str)

                # Extract case number from doc_id
                # Format: GL_VG_001_VG-2025-00030_2025-08-21
                case_match = re.search(r"_([A-Z]+-\d{4}-\d+)_", doc_id)
                case_number = case_match.group(1) if case_match else doc_id.split("_")[-2] if "_" in doc_id else None

                # Determine court from doc_id
                court = "Obergericht"
                if "_VG_" in doc_id:
                    court = "Verwaltungsgericht"
                elif "_KG_" in doc_id:
                    court = "Kantonsgericht"

                title_obj = src.get("title", {})
                if isinstance(title_obj, dict):
                    title = title_obj.get("de") or title_obj.get("fr") or doc_id
                else:
                    title = str(title_obj) if title_obj else doc_id

                # Get PDF URL from attachment
                pdf_url = attachment.get("content_url", "")

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="gl",
                        source_name="Glarus",
                        level="cantonal",
                        canton="GL",
                        court=court,
                        chamber=None,
                        docket=case_number[:100] if case_number else None,
                        decision_date=decision_date,
                        published_date=None,
                        title=(title or f"GL {case_number}")[:500],
                        language="de",
                        url=f"{docs_base}/{doc_id}.html",
                        pdf_url=pdf_url if pdf_url else None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "entscheidsuche.ch",
                            "doc_id": doc_id,
                        },
                    )
                    session.merge(dec)
                    stats.add_imported()

                    if stats.imported % 20 == 0:
                        print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                except Exception as e:
                    print(f"    Error saving: {e}")
                    stats.add_error()

        session.commit()

    print(stats.summary("Glarus"))
    return stats.imported


# =============================================================================
# NIDWALDEN (NW) - Direct HTML Crawler
# =============================================================================

def scrape_nw_dataentities(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Nidwalden via data-entities JSON.

    NW embeds all decisions as JSON in a data-entities attribute on the
    /rechtsprechung page. Each entry has a download link that redirects
    through /_rte/publikation/{id} → /_doc/{id} → /_docn/{id}/filename.pdf
    """
    import html
    import json

    print("Scraping Nidwalden (nw.ch via data-entities)...")

    base_url = "https://www.nw.ch"
    rechtsprechung_url = f"{base_url}/rechtsprechung"

    stats = ScraperStats()

    # Fetch the main page
    try:
        resp = fetch_page(rechtsprechung_url)
    except Exception as e:
        print(f"  Error fetching rechtsprechung page: {e}")
        return 0

    # Extract data-entities JSON
    match = re.search(r'data-entities="([^"]+)"', resp.text)
    if not match:
        print("  Error: Could not find data-entities attribute")
        return 0

    try:
        data = html.unescape(match.group(1))
        entities = json.loads(data)
    except Exception as e:
        print(f"  Error parsing data-entities JSON: {e}")
        return 0

    entries = entities.get("data", [])
    print(f"  Found {len(entries)} decisions in data-entities")

    with get_session() as session:
        for entry in entries:
            if limit and stats.imported >= limit:
                break

            name = entry.get("name", "")
            datum = entry.get("datum", "")
            download_html = entry.get("_downloadBtn", "")

            # Skip entries older than from_date
            if from_date and datum:
                entry_date = parse_date_flexible(datum)
                if entry_date and entry_date < from_date:
                    continue

            # Extract href from download button HTML
            href_match = re.search(r'href="([^"]+)"', download_html)
            if not href_match:
                stats.add_skipped()
                continue

            href = href_match.group(1)
            doc_url = urljoin(base_url, href)

            # Extract case number from title (e.g., "Topic (ZA 21 3)" -> "ZA 21 3")
            case_match = re.search(r'\(([A-Z]{2,3}\s+\d+\s+\d+)\)', name)
            case_number = case_match.group(1) if case_match else None

            # Generate stable ID using case number or URL
            doc_key = case_number.replace(" ", "_") if case_number else href.split("/")[-1]
            stable_id = stable_uuid_url(f"nw:{doc_key}")

            # Check if exists
            existing = session.get(Decision, stable_id)
            if existing:
                stats.add_skipped()
                continue

            # Follow redirects to get PDF
            try:
                pdf_resp = fetch_page(doc_url, timeout=120)
                pdf_url = str(pdf_resp.url)
            except Exception as e:
                print(f"    Error fetching {doc_url}: {e}")
                stats.add_error()
                continue

            # Extract PDF text
            content = extract_pdf_text(pdf_resp.content)
            if not content or len(content) < 200:
                stats.add_skipped()
                continue

            # Parse date
            decision_date = None
            if datum:
                decision_date = parse_date_flexible(datum)

            # Extract court from case number prefix
            court = "Kantonsgericht"
            if case_number:
                prefix = case_number.split()[0] if case_number else ""
                if prefix in ("VA", "SV"):
                    court = "Verwaltungsgericht"
                elif prefix in ("ZA", "SA", "BAS"):
                    court = "Obergericht"

            # Extract title (topic part before parentheses)
            title_part = re.sub(r'\s*\([^)]+\)\s*$', '', name).strip()
            title = f"NW {case_number}: {title_part}" if case_number else name[:200]

            try:
                dec = Decision(
                    id=stable_id,
                    source_id="nw",
                    source_name="Nidwalden",
                    level="cantonal",
                    canton="NW",
                    court=court,
                    chamber=None,
                    docket=case_number,
                    decision_date=decision_date,
                    published_date=None,
                    title=title[:500],
                    language="de",
                    url=doc_url,
                    pdf_url=pdf_url,
                    content_text=content,
                    content_hash=compute_hash(content),
                    meta={"source": "nw.ch/rechtsprechung", "original_name": name},
                )
                session.merge(dec)
                stats.add_imported()

                if stats.imported % 20 == 0:
                    print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                    session.commit()

            except Exception as e:
                print(f"    Error saving: {e}")
                stats.add_error()

        session.commit()

    print(stats.summary("Nidwalden"))
    return stats.imported


# =============================================================================
# ZUG (ZG) - Direct Crawler with API fallback
# =============================================================================

def scrape_zg_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Zug (zg.ch).

    Zug publishes decisions at zg.ch (without www) with PDF links
    at /dam/jcr:.../*.pdf paths.
    """
    print("Scraping Zug (zg.ch)...")

    base_url = "https://zg.ch"
    start_urls = [
        # Verwaltungsgericht (Administrative Court)
        "https://zg.ch/de/recht-justiz/einsicht-entscheide-und-urteile/entscheide-des-verwaltungsgerichtes-zug",
        # Obergericht (High Court)
        "https://zg.ch/de/recht-justiz/einsicht-entscheide-und-urteile/gerichtspraxis-des-obergerichts-des-kantons-zug",
    ]
    min_year = from_date.year if from_date else None
    max_pages = 200 if from_date else 5000

    stats = ScraperStats()
    visited = set()
    to_visit = list(start_urls)

    with get_session() as session:
        while to_visit and (not limit or stats.imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = fetch_page(url)
            except Exception as e:
                print(f"  Error fetching {url}: {e}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                # Handle both relative and absolute URLs
                if href.startswith("http"):
                    full_url = href
                else:
                    full_url = urljoin(base_url, href)

                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"zg:{full_url}")

                    with session.no_autoflush:
                        existing = session.get(Decision, stable_id)
                    if existing:
                        stats.add_skipped()
                        continue

                    try:
                        pdf_resp = fetch_page(full_url, timeout=120)
                    except Exception:
                        stats.add_skipped()
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        stats.add_skipped()
                        continue

                    # Extract filename for case number
                    filename = unquote(href.split("/")[-1])

                    # Try to extract case number from filename or content
                    # ZG format: "Urteil V 2021 59.pdf" or "V 2022 93"
                    case_match = re.search(r"([VS]\s*\d{4}\s*\d+)", filename) or re.search(r"([VS]\s*\d{4}\s*\d+)", content[:500])
                    case_number = case_match.group(1).replace(" ", " ") if case_match else filename.replace(".pdf", "")

                    # Determine court from filename/content
                    court = "Verwaltungsgericht" if "V " in case_number or "verwaltung" in url.lower() else "Obergericht"

                    decision_date = None
                    date_match = re.search(r"(\d{1,2}\.\s*\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", content[:1000])
                    if date_match:
                        decision_date = parse_date_flexible(date_match.group(1))

                    if from_date and decision_date and decision_date < from_date:
                        stats.add_skipped()
                        continue

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="zg",
                            source_name="Zug",
                            level="cantonal",
                            canton="ZG",
                            court=court,
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=decision_date,
                            published_date=None,
                            title=f"ZG {case_number}"[:500],
                            language="de",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "zg.ch/entscheide"},
                        )
                        session.merge(dec)
                        stats.add_imported()

                        if stats.imported % 20 == 0:
                            print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                            session.commit()

                    except Exception as e:
                        session.rollback()
                        print(f"    Error: {e}")
                        stats.add_error()

                elif ("entscheid" in href.lower() or "gericht" in href.lower() or "recht-justiz" in href.lower()) and full_url not in visited and full_url.startswith(base_url):
                    if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                        to_visit.append(full_url)

            time.sleep(0.5)

        try:
            session.commit()
        except Exception:
            session.rollback()

    print(stats.summary("Zug"))
    return stats.imported


# =============================================================================
# GRAUBÜNDEN (GR) - via entscheidsuche.ch (static files)
# =============================================================================

def scrape_gr_entscheidsuche(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Graubünden via entscheidsuche.ch.

    entscheidsuche.ch provides direct access to ~18,945 GR decisions
    via static files (PDF + JSON metadata), bypassing the complex
    Tribuna GWT application.

    Filenames contain ISO dates (e.g. GR_KG_006_ZK1-2019-48_2022-12-02.json).
    When from_date is set, files with dates before it are skipped.
    """
    print("Scraping Graubünden (via entscheidsuche.ch)...")

    index_url = "https://entscheidsuche.ch/docs/GR_Gerichte/"
    stats = ScraperStats()
    _date_in_filename = re.compile(r"(\d{4}-\d{2}-\d{2})\.json$")

    with get_session() as session:
        # Get directory listing
        try:
            resp = fetch_page(index_url)
        except Exception as e:
            print(f"  Error fetching index: {e}")
            return 0

        # Parse JSON files from directory listing
        soup = BeautifulSoup(resp.text, "html.parser")
        json_links = []

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if href.endswith(".json"):
                # Skip files with dates before from_date
                if from_date:
                    m = _date_in_filename.search(href)
                    if m:
                        try:
                            file_date = date.fromisoformat(m.group(1))
                            if file_date < from_date:
                                continue
                        except ValueError:
                            pass
                json_links.append(href)

        print(f"  Found {len(json_links)} decision metadata files")

        for json_file in json_links:
            if limit and stats.imported >= limit:
                break

            # Extract doc_id from filename (e.g., GR_KG_006_ZK1-2019-48_2022-12-02.json)
            doc_id = json_file.replace(".json", "")
            stable_id = stable_uuid_url(f"gr:{doc_id}")

            # Check if exists by ID
            existing = session.get(Decision, stable_id)
            if existing:
                stats.add_skipped()
                continue

            # Also check by URL (may exist with different ID from old scraper)
            pdf_file = json_file.replace(".json", ".pdf")
            pdf_url = f"{index_url}{pdf_file}"
            existing_by_url = session.exec(
                select(Decision).where(Decision.url == pdf_url)
            ).first()
            if not existing_by_url:
                existing_by_url = session.exec(
                    select(Decision).where(Decision.pdf_url == pdf_url)
                ).first()
            if existing_by_url:
                stats.add_skipped()
                continue

            # Fetch JSON metadata
            json_url = f"{index_url}{json_file}"
            try:
                rate_limiter.wait()
                meta_resp = httpx.get(json_url, headers=DEFAULT_HEADERS, timeout=60)
                meta_resp.raise_for_status()
                metadata = meta_resp.json()
            except Exception as e:
                print(f"    Error fetching metadata {json_file}: {e}")
                stats.add_error()
                continue

            # Download PDF (pdf_url already defined above)
            try:
                rate_limiter.wait()
                pdf_resp = httpx.get(pdf_url, headers=DEFAULT_HEADERS, timeout=120)
                pdf_resp.raise_for_status()
                content = extract_pdf_text(pdf_resp.content)
            except Exception:
                # Try getting content from abstract in metadata
                abstract = metadata.get("Abstract", [])
                if abstract:
                    content = "\n".join(item.get("Text", "") for item in abstract)
                else:
                    stats.add_skipped()
                    continue

            if not content or len(content) < 100:
                stats.add_skipped()
                continue

            # Parse metadata
            date_str = metadata.get("Datum", "")
            decision_date = None
            if date_str and date_str != "0000-00-00":
                try:
                    decision_date = date.fromisoformat(date_str)
                except ValueError:
                    decision_date = parse_date_flexible(date_str)

            # Extract case number from Num field
            num_list = metadata.get("Num", [])
            case_number = num_list[0] if num_list else None

            # Determine court from Signatur (e.g., GR_KG_006 -> Kantonsgericht)
            signatur = metadata.get("Signatur", "")
            court = "Kantonsgericht"
            if "_VG_" in signatur:
                court = "Verwaltungsgericht"

            # Get title from Kopfzeile
            kopfzeile = metadata.get("Kopfzeile", [])
            title = kopfzeile[0].get("Text", "") if kopfzeile else f"GR {case_number}"

            # Detect language
            lang = metadata.get("Sprache", "de") or "de"

            # Original URL from metadata
            orig_url = metadata.get("PDF", {}).get("URL", "")

            try:
                dec = Decision(
                    id=stable_id,
                    source_id="gr",
                    source_name="Graubünden",
                    level="cantonal",
                    canton="GR",
                    court=court,
                    chamber=None,
                    docket=case_number[:100] if case_number else None,
                    decision_date=decision_date,
                    published_date=None,
                    title=(title or f"GR {case_number}")[:500],
                    language=lang,
                    url=orig_url or pdf_url,
                    pdf_url=pdf_url,
                    content_text=content,
                    content_hash=compute_hash(content),
                    meta={
                        "source": "entscheidsuche.ch",
                        "signatur": signatur,
                        "spider": metadata.get("Spider", ""),
                    },
                )
                session.merge(dec)
                stats.add_imported()

                if stats.imported % 100 == 0:
                    print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                    session.commit()

            except Exception as e:
                print(f"    Error saving: {e}")
                stats.add_error()

        session.commit()

    print(stats.summary("Graubünden"))
    return stats.imported


# =============================================================================
# OBWALDEN (OW) - Playwright scraper (Vaadin 7.1.15 / LEv3 portal)
# =============================================================================

def scrape_ow_playwright(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Obwalden via Playwright (rechtsprechung.ow.ch).

    The OW portal uses Vaadin 7.1.15 (LEv3 from Weblaw) which requires a
    headless browser. Playwright handles pagination through search results,
    extracting metadata from the result list and full text from document pages.
    """
    from playwright.sync_api import sync_playwright

    print("Scraping Obwalden (rechtsprechung.ow.ch - Playwright)...")
    stats = ScraperStats()
    portal_url = "https://rechtsprechung.ow.ch/le/"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Load portal and search
        page.goto(portal_url, timeout=30000)
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        search_input = page.locator("input.v-textfield").first
        search_input.fill("*")
        search_input.press("Enter")
        time.sleep(5)

        # Sort by date (newest first) for incremental scraping
        option_btn = page.query_selector(".result-optionbutton")
        if option_btn:
            option_btn.click()
            time.sleep(1)
            sort_btn = page.query_selector(".result-optionbutton-item")
            if sort_btn:
                sort_btn.click()
                time.sleep(5)

        # Parse total result count
        body = page.inner_text("body")
        m = re.search(r"Resultat\s+\d+-\d+\s+von\s+(\d+)", body)
        total = int(m.group(1)) if m else 0
        print(f"  Total results: {total}")

        page_num = 0
        consecutive_skips = 0
        max_consecutive_skips = 50  # Stop after 50 consecutive existing decisions

        with get_session() as session:
            while True:
                if limit and stats.imported >= limit:
                    break
                if consecutive_skips >= max_consecutive_skips and from_date:
                    print(f"  Stopping: {consecutive_skips} consecutive skips (all existing)")
                    break

                # Extract results from current page
                entries = page.query_selector_all(
                    ".v-slot.v-slot-result-entry-item-normal"
                )
                if not entries:
                    print("  No entries on page, stopping.")
                    break

                page_num += 1
                print(f"  Page {page_num}: {len(entries)} entries")

                for entry in entries:
                    if limit and stats.imported >= limit:
                        break

                    try:
                        # Extract title and URL
                        title_el = entry.query_selector(
                            ".result-entry-title a"
                        )
                        if not title_el:
                            stats.add_skipped()
                            continue

                        title = title_el.inner_text().strip()
                        href = title_el.get_attribute("href") or ""

                        # Extract leid (file path) from URL
                        leid_m = re.search(r"leid=([^&]+)", href)
                        leid = unquote(leid_m.group(1)) if leid_m else ""

                        # Extract original download path
                        orig_el = entry.query_selector(
                            ".custom-result-component-original a, "
                            ".result-orig-link a"
                        )
                        orig_path = (
                            orig_el.get_attribute("href") if orig_el else ""
                        )

                        # Build stable URL from original path
                        if orig_path:
                            url = f"https://rechtsprechung.ow.ch{orig_path}"
                        elif leid:
                            url = f"https://rechtsprechung.ow.ch/le/doc/{leid}"
                        else:
                            url = href

                        stable_id = stable_uuid_url(f"ow:{orig_path or leid or title}")

                        # Check if already exists
                        existing = session.get(Decision, stable_id)
                        if existing:
                            stats.add_skipped()
                            consecutive_skips += 1
                            continue
                        consecutive_skips = 0

                        # Extract preview text
                        preview_el = entry.query_selector(".lepreview")
                        preview = (
                            preview_el.inner_text().strip()
                            if preview_el
                            else ""
                        )

                        # Extract labels (date, format, language)
                        labels_el = entry.query_selector(
                            ".result-entry-labels"
                        )
                        labels_text = (
                            labels_el.inner_text().strip()
                            if labels_el
                            else ""
                        )
                        label_parts = [
                            p.strip()
                            for p in labels_text.split("\n")
                            if p.strip()
                        ]

                        # Parse published date from labels
                        published = None
                        language = "de"
                        for lp in label_parts:
                            # Date like "26.11.2015"
                            dm = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", lp)
                            if dm:
                                try:
                                    published = date(
                                        int(dm.group(3)),
                                        int(dm.group(2)),
                                        int(dm.group(1)),
                                    )
                                except ValueError:
                                    pass
                            elif lp.lower() in ("de", "fr", "it", "rm"):
                                language = lp.lower()

                        # Skip decisions older than from_date (before expensive fetch)
                        if from_date and published and published < from_date:
                            stats.add_skipped()
                            consecutive_skips += 1
                            continue

                        # Use docket from title
                        docket = title[:100] if title else None

                        # Fetch full document text via cached URL
                        content = preview
                        full_text = _ow_fetch_doc(context, href)
                        court = None
                        decision_dt = None
                        text_for_parsing = full_text or preview

                        if full_text and len(full_text) > len(preview):
                            content = full_text

                        # Parse court from text
                        court = _ow_detect_court(
                            title, text_for_parsing
                        )

                        # Parse decision date from text
                        dt_m = re.search(
                            r"(?:Entscheid|Urteil|Beschluss"
                            r"|Verfügung)"
                            r".*?vom\s+(\d{1,2})\.\s*(\w+)"
                            r"\s+(\d{4})",
                            text_for_parsing,
                        )
                        if dt_m:
                            decision_dt = _parse_german_date(
                                dt_m.group(1),
                                dt_m.group(2),
                                dt_m.group(3),
                            )

                        dec = Decision(
                            id=stable_id,
                            source_id="ow_le",
                            source_name="Obwalden (rechtsprechung.ow.ch)",
                            level="cantonal",
                            canton="OW",
                            court=court,
                            chamber=None,
                            docket=docket,
                            decision_date=decision_dt,
                            published_date=published,
                            title=title[:500],
                            language=language,
                            url=url,
                            pdf_url=None,
                            content_text=content,
                            content_hash=compute_hash(content) if content else None,
                            meta={
                                "source": "rechtsprechung.ow.ch",
                                "leid": leid,
                                "orig_path": orig_path,
                            },
                        )
                        session.merge(dec)
                        stats.add_imported()

                        if stats.imported % 100 == 0:
                            print(
                                f"    Imported {stats.imported} "
                                f"(skipped {stats.skipped})..."
                            )
                            session.commit()

                    except Exception as e:
                        print(f"    Error processing entry: {e}")
                        stats.add_error()

                session.commit()

                # Click WEITER for next page
                weiter = page.query_selector(
                    ".result-pager-next-active"
                )
                if not weiter:
                    print("  No more pages (WEITER button inactive).")
                    break

                weiter.click()
                time.sleep(3)

                # Verify page changed
                new_body = page.inner_text("body")
                new_m = re.search(
                    r"Resultat\s+(\d+)-(\d+)\s+von\s+(\d+)", new_body
                )
                if new_m:
                    start = int(new_m.group(1))
                    end = int(new_m.group(2))
                    if start == 1 and page_num > 1:
                        # Pagination looped back to start
                        break

        browser.close()

    print(stats.summary("Obwalden"))
    return stats.imported


def _ow_fetch_doc(browser_context, cached_href: str) -> str | None:
    """Fetch full document text from OW cached URL via Playwright.

    The cached URL (from the result list) includes the session-specific
    authuser token needed for Vaadin to serve the document content.
    Returns the cleaned text or None on failure.
    """
    if not cached_href:
        return None

    # Ensure HTTPS and remove :80 port
    url = cached_href
    if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)
    url = re.sub(r":80(/)", r"\1", url)

    try:
        doc_page = browser_context.new_page()
        doc_page.goto(url, timeout=15000)
        time.sleep(2)

        full_text = doc_page.inner_text("body").strip()
        doc_page.close()

        # Skip error pages
        if "Authorization fail" in full_text or "not found" in full_text:
            return None

        # Remove navigation header (e.g., "Neue SucheOriginalAbR-00-01-01.htm")
        full_text = re.sub(
            r"^Neue Suche\s*Original\s*\S+\.htm\s*\n*",
            "",
            full_text,
        ).strip()

        return full_text if full_text else None

    except Exception:
        return None


def _ow_detect_court(title: str, text: str) -> str | None:
    """Detect the OW court from title prefix or decision text."""
    # Title prefix gives a strong signal for OGVE
    if title.startswith("OGVE") or title.startswith("OGE"):
        return "Obergericht"
    if title.startswith("VGE"):
        return "Verwaltungsgericht"

    # Parse from text (e.g., "Entscheid des Obergerichts vom ...")
    court_patterns = [
        (r"(?:des|der)\s+Obergericht", "Obergericht"),
        (r"Obergerichtskommission", "Obergericht"),
        (r"(?:des|der)\s+Verwaltungsgericht", "Verwaltungsgericht"),
        (r"(?:des|der)\s+Kantonsgericht", "Kantonsgericht"),
        (r"(?:des|der)\s+Regierungsrat", "Regierungsrat"),
        (r"(?:des|der)\s+Justizkommission", "Justizkommission"),
    ]
    for pattern, court_name in court_patterns:
        if re.search(pattern, text):
            return court_name

    return None


def _parse_german_date(day: str, month_name: str, year: str) -> date | None:
    """Parse a German date like '24. August 2000' into a date object."""
    months = {
        "Januar": 1, "February": 2, "Februar": 2, "März": 3, "April": 4,
        "Mai": 5, "Juni": 6, "Juli": 7, "August": 8, "September": 9,
        "Oktober": 10, "November": 11, "Dezember": 12,
    }
    m = months.get(month_name)
    if not m:
        return None
    try:
        return date(int(year), m, int(day))
    except ValueError:
        return None


# =============================================================================
# TICINO (TI) - Direct Scraper (Italian)
# =============================================================================

def scrape_ti_findinfoweb(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Ticino via FindInfoWeb (sentenze.ti.ch).

    Ticino uses FindInfoWeb like SO/BS but with Italian interface.
    """
    print("Scraping Ticino (sentenze.ti.ch - FindInfoWeb)...")

    base_url = "https://www.sentenze.ti.ch/cgi-bin/nph-omniscgi"
    stats = ScraperStats()
    min_year = from_date.year if from_date else None
    page = 1

    with get_session() as session:
        while True:
            # FindInfoWeb search parameters for TI
            params = {
                "OmnisPlatform": "WINDOWS",
                "WebServerUrl": "www.sentenze.ti.ch",
                "WebServerScript": "/cgi-bin/nph-omniscgi",
                "OmnisLibrary": "JURISWEB",
                "OmnisClass": "rtFindinfoWebHtmlService",
                "OmnisServer": "JURISWEB,193.246.182.54:6000",
                "Aufruf": "home",
                "Template": "home.fiw",
                "Schema": "TI_WEB",
                "cLanguage": "ITA",
                "Parametername": "WWWTI",
                "nAnzahlTrefferProSeite": "50",
                "nSeite": str(page),
            }

            url = f"{base_url}?{urlencode(params)}"

            try:
                resp = fetch_page(url)
            except Exception as e:
                print(f"  Error fetching page {page}: {e}")
                break

            # Find decision links with nF30_KEY pattern
            decision_ids = re.findall(r"nF30_KEY=(\d+)", resp.text)
            decision_ids = list(dict.fromkeys(decision_ids))  # Remove duplicates

            if not decision_ids:
                print(f"  No more decisions found on page {page}")
                break

            print(f"  Page {page}: found {len(decision_ids)} decisions")

            for decision_id in decision_ids:
                if limit and stats.imported >= limit:
                    break

                stable_id = stable_uuid_url(f"ti-findinfo:{decision_id}")

                # Check if exists
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                # Fetch decision detail
                detail_params = {
                    "OmnisPlatform": "WINDOWS",
                    "WebServerUrl": "www.sentenze.ti.ch",
                    "WebServerScript": "/cgi-bin/nph-omniscgi",
                    "OmnisLibrary": "JURISWEB",
                    "OmnisClass": "rtFindinfoWebHtmlService",
                    "OmnisServer": "JURISWEB,193.246.182.54:6000",
                    "Parametername": "WWWTI",
                    "Schema": "TI_WEB",
                    "Aufruf": "getMarkupDocument",
                    "cLanguage": "ITA",
                    "nF30_KEY": decision_id,
                    "Template": "results/document_ita.fiw",
                }
                detail_url = f"{base_url}?{urlencode(detail_params)}"

                try:
                    detail_resp = fetch_page(detail_url)
                except Exception as e:
                    print(f"    Error fetching {decision_id}: {e}")
                    stats.add_error()
                    continue

                soup = BeautifulSoup(detail_resp.text, "html.parser")

                # Extract title from page
                title = soup.find("title")
                title_text = title.get_text(strip=True) if title else f"TI {decision_id}"

                # Find PDF link
                pdf_link = None
                for link in soup.find_all("a", href=True):
                    href = link.get("href", "")
                    if ".pdf" in href.lower():
                        pdf_link = href if href.startswith("http") else urljoin("https://www.sentenze.ti.ch", href)
                        break

                # Extract text from HTML if no PDF
                content = None
                if pdf_link:
                    try:
                        pdf_resp = fetch_page(pdf_link, timeout=120)
                        content = extract_pdf_text(pdf_resp.content)
                    except Exception:
                        pass

                if not content:
                    # Extract from HTML content
                    content_div = soup.find("div", class_="document") or soup.find("body")
                    if content_div:
                        content = content_div.get_text(separator="\n", strip=True)

                if not content or len(content) < 100:
                    stats.add_skipped()
                    continue

                # Extract case number from content or title
                case_number = None
                case_match = re.search(r"(\d+\.\d{4}\.\d+)", content[:500]) or re.search(r"(\d+\.\d{4}\.\d+)", title_text)
                if case_match:
                    case_number = case_match.group(1)

                # Extract date
                decision_date = None
                date_match = re.search(r"data decisione:\s*(\d{2}\.\d{2}\.\d{4})", detail_resp.text)
                if date_match:
                    decision_date = parse_date_flexible(date_match.group(1))

                # Date filter: skip old decisions
                if from_date and decision_date and decision_date < from_date:
                    stats.add_skipped()
                    continue
                if min_year and case_number:
                    yr_match = re.search(r'(20[012]\d)', case_number)
                    if yr_match and int(yr_match.group(1)) < min_year:
                        stats.add_skipped()
                        continue

                # Extract court/authority
                court = "Tribunale cantonale"
                auth_match = re.search(r"Autorit[àa]:\s*(\w+)", detail_resp.text)
                if auth_match:
                    court = auth_match.group(1)

                decision_url = detail_url

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="ti",
                        source_name="Ticino Tribunali",
                        level="cantonal",
                        canton="TI",
                        court=court,
                        chamber=None,
                        docket=case_number[:100] if case_number else None,
                        decision_date=decision_date,
                        published_date=None,
                        title=f"TI {case_number}" if case_number else title_text[:500],
                        language="it",
                        url=decision_url,
                        pdf_url=pdf_link,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={"source": "sentenze.ti.ch", "findinfo_key": decision_id},
                    )
                    session.merge(dec)
                    stats.add_imported()

                    if stats.imported % 50 == 0:
                        print(f"  Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                except Exception as e:
                    print(f"    Error saving {decision_id}: {e}")
                    stats.add_error()

            if limit and stats.imported >= limit:
                break

            page += 1

        session.commit()

    print(stats.summary("Ticino"))
    return stats.imported


# Keep old function name as alias for backwards compatibility
def scrape_ti_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Alias for scrape_ti_findinfoweb."""
    return scrape_ti_findinfoweb(limit, from_date=from_date, to_date=to_date)


# =============================================================================
# VAUD (VD) - Direct Scraper (French)
# =============================================================================

def scrape_vd_findinfoweb(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape administrative law decisions from Vaud via FindInfoWeb.

    Vaud uses FindInfoWeb for administrative law (CDAP) at jurisprudence.vd.ch.
    Civil/criminal cases are in a separate JS app at prestations.vd.ch.
    The FindInfoWeb requires a POST search request.
    """
    print("Scraping Vaud (jurisprudence.vd.ch - FindInfoWeb)...")

    base_url = "https://jurisprudence.vd.ch/scripts/nph-omniscgi.exe"
    stats = ScraperStats()
    min_year = from_date.year if from_date else None

    # Search by year to get all decisions
    years = list(range(2026, 1983, -1))  # From 2026 back to 1984
    if min_year:
        years = [y for y in years if y >= min_year]

    with get_session() as session:
        for year in years:
            if limit and stats.imported >= limit:
                break

            page = 1
            while True:
                if limit and stats.imported >= limit:
                    break

                # POST search parameters for VD FindInfoWeb
                search_data = {
                    "OmnisPlatform": "WINDOWS",
                    "WebServerUrl": "",
                    "WebServerScript": "/scripts/nph-omniscgi.exe",
                    "OmnisLibrary": "JURISWEB",
                    "OmnisClass": "rtFindinfoWebHtmlService",
                    "OmnisServer": "7001",
                    "Schema": "VD_TA_WEB",
                    "Parametername": "WWW_V4",
                    "Source": "search.fiw",
                    "Aufruf": "search",
                    "cTemplate": "search/standard/results/resultpage.fiw",
                    "cTemplate_SuchstringValidateError": "search/standard/search.fiw",
                    "cSprache": "FRE",
                    "cGeschaeftsart": "",
                    "cGeschaeftsjahr": str(year),
                    "cGeschaeftsnummer": "",
                    "cHerkunft": "",
                    "cSuchstring": "",
                    "nAnzahlTrefferProSeite": "50",
                    "nSeite": str(page),
                }

                rate_limiter.wait()
                try:
                    resp = httpx.post(base_url, data=search_data, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
                    resp.raise_for_status()
                except Exception as e:
                    print(f"  Error fetching year {year} page {page}: {e}")
                    break

                # Find decision links with nF30_KEY pattern
                decision_ids = re.findall(r"nF30_KEY=(\d+)", resp.text)
                decision_ids = list(dict.fromkeys(decision_ids))  # Remove duplicates

                if not decision_ids:
                    if page == 1:
                        print(f"  Year {year}: no decisions found")
                    break

                if page == 1:
                    print(f"  Year {year}: found decisions, processing...")

                for decision_id in decision_ids:
                    if limit and stats.imported >= limit:
                        break

                    stable_id = stable_uuid_url(f"vd-findinfo:{decision_id}")

                    # Check if exists
                    existing = session.get(Decision, stable_id)
                    if existing:
                        stats.add_skipped()
                        continue

                    # Fetch decision detail
                    detail_params = {
                        "OmnisPlatform": "WINDOWS",
                        "WebServerUrl": "",
                        "WebServerScript": "/scripts/nph-omniscgi.exe",
                        "OmnisLibrary": "JURISWEB",
                        "OmnisClass": "rtFindinfoWebHtmlService",
                        "OmnisServer": "7001",
                        "Parametername": "WWW_V4",
                        "Schema": "VD_TA_WEB",
                        "Aufruf": "getMarkupDocument",
                        "cSprache": "FRE",
                        "nF30_KEY": decision_id,
                        "Template": "search/standard/results/document.fiw",
                    }
                    detail_url = f"{base_url}?{urlencode(detail_params)}"

                    try:
                        detail_resp = fetch_page(detail_url)
                    except Exception as e:
                        print(f"    Error fetching {decision_id}: {e}")
                        stats.add_error()
                        continue

                    soup = BeautifulSoup(detail_resp.text, "html.parser")

                    # Extract title
                    title = soup.find("title")
                    title_text = title.get_text(strip=True) if title else f"VD {decision_id}"

                    # Find PDF link
                    pdf_link = None
                    for link in soup.find_all("a", href=True):
                        href = link.get("href", "")
                        if ".pdf" in href.lower():
                            pdf_link = href if href.startswith("http") else urljoin("https://jurisprudence.vd.ch", href)
                            break

                    # Extract text
                    content = None
                    if pdf_link:
                        try:
                            pdf_resp = fetch_page(pdf_link, timeout=120)
                            content = extract_pdf_text(pdf_resp.content)
                        except Exception:
                            pass

                    if not content:
                        # Extract from HTML
                        content_div = soup.find("div", class_="document") or soup.find("body")
                        if content_div:
                            content = content_div.get_text(separator="\n", strip=True)

                    if not content or len(content) < 100:
                        stats.add_skipped()
                        continue

                    # Extract case number
                    case_number = None
                    case_match = re.search(r"([A-Z]+\.\d{4}\.\d+)", content[:500]) or re.search(r"([A-Z]+\.\d{4}\.\d+)", title_text)
                    if case_match:
                        case_number = case_match.group(1)

                    # Extract date
                    decision_date = None
                    date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", content[:1000])
                    if date_match:
                        decision_date = parse_date_flexible(date_match.group(1))

                    if from_date and decision_date and decision_date < from_date:
                        stats.add_skipped()
                        continue

                    decision_url = detail_url

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="vd",
                            source_name="Vaud Tribunal cantonal",
                            level="cantonal",
                            canton="VD",
                            court="Cour de droit administratif et public",
                            chamber=None,
                            docket=case_number[:100] if case_number else None,
                            decision_date=decision_date,
                            published_date=None,
                            title=f"VD {case_number}" if case_number else title_text[:500],
                            language="fr",
                            url=decision_url,
                            pdf_url=pdf_link,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "jurisprudence.vd.ch", "findinfo_key": decision_id},
                        )
                        session.merge(dec)
                        stats.add_imported()

                        if stats.imported % 50 == 0:
                            print(f"  Imported {stats.imported} (skipped {stats.skipped})...")
                            session.commit()

                    except Exception as e:
                        print(f"    Error saving {decision_id}: {e}")
                        stats.add_error()

                page += 1

        session.commit()

    print(stats.summary("Vaud"))
    return stats.imported


# Keep old function name as alias
def scrape_vd_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Alias for scrape_vd_findinfoweb."""
    return scrape_vd_findinfoweb(limit, from_date=from_date, to_date=to_date)


# =============================================================================
# GENEVA (GE) - Direct Scraper (French, JS app with PDF links)
# =============================================================================

# Geneva court mappings
GE_COURTS = {
    "cour-de-justice": "Cour de Justice",
    "chambre-administrative": "Chambre administrative",
    "chambre-civile": "Chambre civile",
    "chambre-penale": "Chambre pénale",
    "tribunal-civil": "Tribunal civil",
    "tribunal-penal": "Tribunal pénal",
    "tapi": "Tribunal administratif de première instance",
}


def scrape_ge_crawler(limit: int | None = None, from_date: date | None = None, to_date: date | None = None) -> int:
    """Scrape decisions from Geneva (justice.ge.ch)."""
    print("Scraping Geneva (justice.ge.ch)...")

    base_url = "https://justice.ge.ch"

    # Geneva uses a modern web app - we need to target the PDF archive URLs directly
    # The decisions are organized by court and year
    min_year = from_date.year if from_date else None
    max_pages = 50 if from_date else 5000
    stats = ScraperStats()
    visited = set()

    # Start URLs for different courts
    start_urls = [
        "https://justice.ge.ch/fr/recherche-jurisprudence",
        "https://ge.ch/justice/dans-la-jurisprudence",
    ]

    to_visit = list(start_urls)

    with get_session() as session:
        while to_visit and (not limit or stats.imported < limit) and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = fetch_page(url)
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Look for PDF links (decisions are stored as PDFs)
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")

                # Handle relative URLs
                if href.startswith("/"):
                    full_url = urljoin(base_url, href)
                elif href.startswith("http"):
                    full_url = href
                else:
                    full_url = urljoin(url, href)

                if ".pdf" in href.lower():
                    if min_year:
                        yr = _url_year(full_url)
                        if yr and yr < min_year:
                            continue

                    stable_id = stable_uuid_url(f"ge:{full_url}")

                    with session.no_autoflush:
                        existing = session.get(Decision, stable_id)
                    if existing:
                        stats.add_skipped()
                        continue

                    try:
                        pdf_resp = fetch_page(full_url, timeout=120)
                    except Exception:
                        stats.add_skipped()
                        continue

                    content = extract_pdf_text(pdf_resp.content)
                    if not content or len(content) < 200:
                        stats.add_skipped()
                        continue

                    filename = href.split("/")[-1]

                    # Extract case number - Geneva format: ACJC/123/2024, A/1234/2024-CA
                    case_match = re.search(r"([A-Z]+/\d+/\d{4}|[A-Z]-\d+-\d{4})", content[:500])
                    case_number = case_match.group(1) if case_match else filename.replace(".pdf", "")

                    # Detect court from URL or content
                    court = "Tribunal cantonal"
                    for key, name in GE_COURTS.items():
                        if key in full_url.lower() or key.replace("-", " ") in content[:2000].lower():
                            court = name
                            break

                    decision_date = None
                    date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", content[:1000])
                    if date_match:
                        decision_date = parse_date_flexible(date_match.group(1))

                    if from_date and decision_date and decision_date < from_date:
                        stats.add_skipped()
                        continue

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="ge",
                            source_name="Genève",
                            level="cantonal",
                            canton="GE",
                            court=court,
                            chamber=None,
                            docket=case_number[:100],
                            decision_date=decision_date,
                            published_date=None,
                            title=f"GE {case_number}"[:500],
                            language="fr",
                            url=full_url,
                            pdf_url=full_url,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={"source": "justice.ge.ch"},
                        )
                        session.merge(dec)
                        stats.add_imported()

                        if stats.imported % 20 == 0:
                            print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                            session.commit()

                    except Exception:
                        session.rollback()
                        stats.add_error()

                # Follow links to find more decisions (only jurisprudence paths)
                elif any(kw in href.lower() for kw in ["jurisprudence", "arret", "jugement"]):
                    if full_url not in visited and (full_url.startswith(base_url) or "ge.ch" in full_url):
                        if not min_year or not _url_year(full_url) or _url_year(full_url) >= min_year:
                            to_visit.append(full_url)

            time.sleep(0.5)

        try:
            session.commit()
        except Exception:
            session.rollback()

    print(stats.summary("Geneva"))
    return stats.imported


# =============================================================================
# Main
# =============================================================================

# SCRAPERS: canton code -> (name, function, supports_from_date)
# Used by scrape_all_cantons() and CLI --canton.
# NOTE: ZH, GE, VD, TI are NOT listed here because daily_update.py
# calls them explicitly (steps 7, 11-13) to avoid duplicate scraping.
# They can still be run individually via their own scripts.
SCRAPERS = {
    # FindInfoWeb-based
    "SO": ("Solothurn", scrape_so_findinfoweb, True),
    "BS": ("Basel-Stadt", scrape_bs_findinfoweb, True),
    "NE": ("Neuchâtel", scrape_ne_crawler, True),
    # Sitemap-based
    "BE": ("Bern", scrape_be_sitemap, True),
    # Custom/Special
    "AI": ("Appenzell Innerrhoden", scrape_ai_pdfs, True),
    "TG": ("Thurgau", scrape_tg_confluence, True),
    # HTML Crawlers - German-speaking
    "SG": ("St. Gallen", scrape_sg_crawler, True),
    "LU": ("Luzern", scrape_lu_crawler, True),
    "SH": ("Schaffhausen", scrape_sh_crawler, True),
    "SZ": ("Schwyz", scrape_sz_crawler, True),
    "AG": ("Aargau", scrape_ag_crawler, True),
    "BL": ("Basel-Landschaft", scrape_bl_crawler, True),
    "UR": ("Uri", scrape_ur_crawler, True),
    "NW": ("Nidwalden", scrape_nw_dataentities, True),
    "ZG": ("Zug", scrape_zg_crawler, True),
    # LEv4 API (Weblaw platform)
    "AR": ("Appenzell AR", scrape_ar_lev4, True),
    # Via entscheidsuche.ch (official portals use complex JS or block access)
    "GL": ("Glarus", scrape_gl_entscheidsuche, True),
    "GR": ("Graubünden", scrape_gr_entscheidsuche, True),
    # Playwright-based (Vaadin 7.1.15 portal requires headless browser)
    "OW": ("Obwalden", scrape_ow_playwright, True),
    # HTML Crawlers - French-speaking
    "FR": ("Fribourg", scrape_fr_crawler, True),
    "JU": ("Jura", scrape_ju_crawler, True),
    # Bilingual
    "VS": ("Valais", scrape_vs_crawler, True),
}


def list_scrapers():
    """List available scrapers."""
    print("Available cantonal scrapers:")
    print("-" * 60)
    for code, (name, _, supports_date) in SCRAPERS.items():
        info = CANTON_SOURCES.get(code, {})
        date_support = "[date filter]" if supports_date else ""
        print(f"  {code}: {name} {date_support}")
        if "base_url" in info:
            print(f"       URL: {info['base_url']}")
        print()


def scrape_all_cantons(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    historical: bool = False,
) -> int:
    """Run all cantonal scrapers.

    Args:
        limit: Maximum decisions per scraper
        from_date: Only import decisions after this date.
            Ignored if historical=True.
        to_date: Only import decisions before this date.
            Ignored if historical=True.
        historical: If True, fetch ALL decisions without date restrictions.
            This enables complete archive capture for initial database population.

    Returns:
        Total number of decisions imported
    """
    total = 0
    results = {}

    effective_from_date = None if historical else from_date
    effective_to_date = None if historical else to_date

    if historical:
        print("Historical mode: Fetching complete archives without date restrictions")

    for code, (name, scraper_func, supports_date) in SCRAPERS.items():
        print(f"\n{'='*60}")
        print(f"Running {name} ({code}) scraper")
        if historical:
            print("  [HISTORICAL MODE - No date filtering]")
        print("="*60)

        try:
            if supports_date:
                kwargs = dict(limit=limit, from_date=effective_from_date, to_date=effective_to_date)
            else:
                kwargs = dict(limit=limit)

            count = scraper_func(**kwargs)
            results[code] = {"status": "success", "count": count}
            total += count
        except Exception as e:
            print(f"  Error: {e}")
            results[code] = {"status": "error", "error": str(e)}

    print(f"\n{'='*60}")
    print("SUMMARY")
    print("="*60)
    for code, result in results.items():
        name = SCRAPERS[code][0]
        status = result["status"]
        if status == "success":
            print(f"  {code} ({name}): {result['count']} imported")
        else:
            print(f"  {code} ({name}): ERROR - {result['error']}")
    print(f"\nTotal imported: {total}")

    return total


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Swiss cantonal court decisions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Historical Mode:
  Use --historical to fetch ALL decisions without date restrictions.
  This is useful for initial database population or archive refresh.

Examples:
  python scrape_cantons.py --canton=SO                    # Scrape Solothurn
  python scrape_cantons.py --all --historical             # Full archive capture
  python scrape_cantons.py --all --from-date=2024-01-01   # Recent decisions only
        """,
    )
    parser.add_argument("--canton", help="Canton code (e.g., SO, BS, AI, TG)")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--historical", action="store_true",
                       help="Historical mode: fetch ALL decisions without date restrictions")
    parser.add_argument("--list", action="store_true", help="List available scrapers")
    parser.add_argument("--all", action="store_true", help="Run all scrapers")
    args = parser.parse_args()

    if args.list:
        list_scrapers()
        return

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    if args.all:
        scrape_all_cantons(
            limit=args.limit,
            from_date=from_dt,
            to_date=to_dt,
            historical=args.historical,
        )
        return

    if not args.canton:
        parser.print_help()
        print("\nUse --list to see available scrapers")
        return

    canton = args.canton.upper()
    if canton not in SCRAPERS:
        print(f"Error: No scraper for canton '{canton}'")
        print("Available cantons:", ", ".join(SCRAPERS.keys()))
        return

    name, scraper_func, supports_date = SCRAPERS[canton]
    # In historical mode or when no dates specified, skip date filtering
    if args.historical or (not from_dt and not to_dt):
        scraper_func(limit=args.limit)
    elif supports_date:
        scraper_func(limit=args.limit, from_date=from_dt, to_date=to_dt)
    else:
        scraper_func(limit=args.limit)


if __name__ == "__main__":
    main()
